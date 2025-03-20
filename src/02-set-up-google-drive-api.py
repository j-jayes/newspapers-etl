from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

def upload_to_drive(file_path, folder_id=None, share_with_email=None):
    """
    Upload a file to Google Drive
    
    Args:
        file_path: Path to the file to upload
        folder_id: Optional Google Drive folder ID to upload to
    
    Returns:
        Uploaded file ID
    """
    # Path to your service account credentials JSON file
    CREDENTIALS_FILE = 'newspapers-454313-7a72698b783e.json'
    
    # Authenticate with service account
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, 
        scopes=['https://www.googleapis.com/auth/drive']
    )
    
    # Build the Drive service
    drive_service = build('drive', 'v3', credentials=credentials)
    
    # File metadata
    file_metadata = {
        'name': os.path.basename(file_path)
    }
    
    # If a folder ID is specified, add it to the parent folders
    if folder_id:
        file_metadata['parents'] = [folder_id]
    
    # Upload file
    media = MediaFileUpload(file_path, resumable=True)
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    

    # Share the file with your personal account if email is provided
    if share_with_email:
        permission = {
            'type': 'user',
            'role': 'writer',  # or 'reader' if you only want read access
            'emailAddress': share_with_email
        }
        
        drive_service.permissions().create(
            fileId=file.get('id'),
            body=permission,
            fields='id',
        ).execute()
    
    return file.get('id')

# Example usage
if __name__ == "__main__":
    # Create a folder first (optional)
    def create_folder(folder_name, share_with_email=None):
        CREDENTIALS_FILE = 'newspapers-454313-7a72698b783e.json'
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, 
            scopes=['https://www.googleapis.com/auth/drive']
        )
        drive_service = build('drive', 'v3', credentials=credentials)
        
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        folder = drive_service.files().create(
            body=folder_metadata,
            fields='id'
        ).execute()
        
        # Share the folder with your personal account
        if share_with_email:
            permission = {
                'type': 'user',
                'role': 'writer',
                'emailAddress': share_with_email
            }
            
            drive_service.permissions().create(
                fileId=folder.get('id'),
                body=permission,
                fields='id',
            ).execute()
        
        return folder.get('id')
    
    # Add your personal Google account email here
    YOUR_EMAIL = 'j0nathanjayes@gmail.com'  

    # Create a folder for your scraped images
    folder_id = create_folder('Scraped_Images', share_with_email=YOUR_EMAIL)

    # Upload a sample image
    file_id = upload_to_drive('kb_newspapers/Dagens nyheter/1865-01-02/bib13991099_18650102_0_1_0001.jp2', 
                            folder_id, 
                            share_with_email=YOUR_EMAIL)
    
    print(f"File uploaded with ID: {file_id}")