import os
import sys
import tarfile
import tempfile
import dropbox
from dropbox.exceptions import AuthError, ApiError
from pathlib import Path
from tqdm import tqdm

def initialize_dropbox(access_token):
    """Initialize Dropbox client with access token."""
    try:
        dbx = dropbox.Dropbox(access_token)
        # Verify token works
        dbx.users_get_current_account()
        print("✓ Successfully authenticated with Dropbox")
        return dbx
    except AuthError:
        print("✗ ERROR: Invalid Dropbox access token")
        sys.exit(1)

def download_from_dropbox(dbx, dropbox_path):
    """Download file from Dropbox with progress tracking."""
    local_path = os.path.join(tempfile.gettempdir(), os.path.basename(dropbox_path))
    try:
        with open(local_path, "wb") as f:
            metadata = dbx.files_download_to_file(local_path, dropbox_path)
        print(f"✓ Downloaded {os.path.basename(dropbox_path)} from Dropbox")
        return local_path
    except ApiError as err:
        if err.error.is_path() and err.error.get_path().is_not_found():
            print(f"ℹ No existing file at {dropbox_path}, will create new one")
            return None
        print(f"✗ Error downloading from Dropbox: {err}")
        sys.exit(1)

def upload_to_dropbox(dbx, local_path, dropbox_path):
    """Upload file to Dropbox with chunked upload for large files."""
    CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunks
    
    try:
        file_size = os.path.getsize(local_path)
        
        if file_size <= CHUNK_SIZE:
            # Small file upload
            with open(local_path, "rb") as f:
                dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        else:
            # Large file upload with progress bar
            with open(local_path, "rb") as f:
                upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=upload_session_start_result.session_id,
                    offset=f.tell()
                )
                commit = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode.overwrite)
                
                with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading") as pbar:
                    pbar.update(f.tell())
                    while f.tell() < file_size:
                        if (file_size - f.tell()) <= CHUNK_SIZE:
                            dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                        else:
                            dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE), cursor)
                            cursor.offset = f.tell()
                        pbar.update(CHUNK_SIZE)
        
        print(f"✓ Successfully uploaded to Dropbox: {dropbox_path}")
    except Exception as e:
        print(f"✗ Error uploading to Dropbox: {e}")
        sys.exit(1)

def process_archive(file_path):
    """Ensure file is in correct format (.tar.gz, .tgz, or .tar)."""
    if file_path.endswith(('.tar.gz', '.tgz', '.tar')):
        return file_path
    else:
        print(f"✗ Unsupported file format: {file_path}")
        sys.exit(1)

def update_master_tar(master_tar_path, new_file_path, dropbox_path):
    """Update master tar with new file."""
    temp_tar_path = os.path.join(tempfile.gettempdir(), f"temp_{os.path.basename(dropbox_path)}")
    new_file_name = os.path.basename(new_file_path)
    
    if not master_tar_path:
        # Create new tar if none exists
        with tarfile.open(temp_tar_path, 'w') as tar:
            tar.add(new_file_path, arcname=new_file_name)
        return temp_tar_path
    
    # Update existing tar
    with tempfile.TemporaryDirectory() as tmpdir:
        # Extract existing contents
        with tarfile.open(master_tar_path, 'r:*') as tar:
            tar.extractall(path=tmpdir)
        
        # Remove existing version if present
        target_path = os.path.join(tmpdir, new_file_name)
        if os.path.exists(target_path):
            os.remove(target_path)
        
        # Add new file
        import shutil
        shutil.copy(new_file_path, target_path)
        
        # Create updated tar
        with tarfile.open(temp_tar_path, 'w') as tar:
            for item in os.listdir(tmpdir):
                tar.add(os.path.join(tmpdir, item), arcname=item)
    
    return temp_tar_path

def main(changed_files, dropbox_path):
    access_token = os.getenv('DROPBOX_ACCESS_TOKEN')
    if not access_token:
        print("✗ ERROR: Dropbox access token not found")
        sys.exit(1)
    
    dbx = initialize_dropbox(access_token)
    
    for file_path in changed_files.split(','):
        if not file_path.strip():
            continue
            
        file_path = file_path.strip()
        print(f"\nℹ Processing: {file_path}")
        
        # Validate and process file
        processed_path = process_archive(file_path)
        
        # Download existing master file
        master_path = download_from_dropbox(dbx, dropbox_path)
        
        # Update master archive
        updated_path = update_master_tar(master_path, processed_path, dropbox_path)
        
        # Upload updated version
        upload_to_dropbox(dbx, updated_path, dropbox_path)
        
        # Cleanup
        for path in [master_path, updated_path]:
            if path and os.path.exists(path):
                os.remove(path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python upload_to_dropbox.py <changed_files> [dropbox_path]")
        sys.exit(1)
    
    dropbox_path = sys.argv[2] if len(sys.argv) > 2 else os.getenv('DROPBOX_FILE_PATH', '/Bots_V3_splunkapps.tar')
    main(sys.argv[1], dropbox_path)
