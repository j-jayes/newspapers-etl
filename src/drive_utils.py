#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Google Drive Upload Utility for KB Newspaper Scraper

Handles authentication and uploading of files to Google Drive,
maintaining the same folder structure as the local filesystem.
"""

import os
import logging
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("drive_upload.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DriveUploader")

class GoogleDriveUploader:
    """
    Utility class to handle uploads to Google Drive.
    Maintains folder structure and handles authentication.
    """
    
    def __init__(self, credentials_file=None, credentials_json=None, root_folder_name="KB_Newspapers"):
        """
        Initialize the uploader with service account credentials.
        
        Args:
            credentials_file: Path to service account JSON credentials file
            credentials_json: JSON string of service account credentials (alternative to file)
            root_folder_name: Name of the root folder in Google Drive
        """
        self.root_folder_name = root_folder_name
        self.folder_cache = {}  # Cache folder IDs to avoid repeated API calls
        
        try:
            # Authenticate with service account
            if credentials_json:
                import json
                import tempfile
                
                # Create a temporary file for the credentials
                with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp:
                    temp.write(credentials_json.encode('utf-8'))
                    temp_credentials_file = temp.name
                
                self.credentials = service_account.Credentials.from_service_account_file(
                    temp_credentials_file,
                    scopes=['https://www.googleapis.com/auth/drive']
                )
                # Clean up the temporary file
                os.unlink(temp_credentials_file)
            else:
                self.credentials = service_account.Credentials.from_service_account_file(
                    credentials_file,
                    scopes=['https://www.googleapis.com/auth/drive']
                )
            
            # Build the Drive service
            self.drive_service = build('drive', 'v3', credentials=self.credentials)
            logger.info("Successfully authenticated with Google Drive API")
            
            # Initialize root folder
            self.root_folder_id = self.get_or_create_folder(self.root_folder_name)
            logger.info(f"Root folder ID: {self.root_folder_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Drive uploader: {e}", exc_info=True)
            raise
    
    def get_or_create_folder(self, folder_name, parent_id=None):
        """
        Get a folder ID by name, creating it if it doesn't exist.
        
        Args:
            folder_name: Name of the folder
            parent_id: ID of the parent folder (None for root)
            
        Returns:
            ID of the folder
        """
        # Generate a cache key
        cache_key = f"{parent_id}:{folder_name}"
        
        # Check cache first
        if cache_key in self.folder_cache:
            return self.folder_cache[cache_key]
        
        try:
            # Create query to search for the folder
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_id:
                query += f" and '{parent_id}' in parents"
            
            # Search for the folder
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            folders = results.get('files', [])
            
            if folders:
                # Folder exists, return its ID
                folder_id = folders[0]['id']
                logger.debug(f"Found existing folder '{folder_name}' with ID: {folder_id}")
            else:
                # Folder doesn't exist, create it
                folder_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                
                # If parent specified, add it
                if parent_id:
                    folder_metadata['parents'] = [parent_id]
                
                folder = self.drive_service.files().create(
                    body=folder_metadata,
                    fields='id'
                ).execute()
                
                folder_id = folder.get('id')
                logger.info(f"Created new folder '{folder_name}' with ID: {folder_id}")
            
            # Cache the folder ID
            self.folder_cache[cache_key] = folder_id
            return folder_id
            
        except Exception as e:
            logger.error(f"Error getting/creating folder '{folder_name}': {e}", exc_info=True)
            raise
    
    def get_folder_path(self, local_path, base_dir):
        """
        Get the folder structure path for Google Drive based on local path.
        
        Args:
            local_path: Local file path
            base_dir: Base directory to strip from path
            
        Returns:
            List of folder names to navigate/create in Google Drive
        """
        # Convert to Path objects for easier manipulation
        local_path = Path(local_path)
        base_dir = Path(base_dir)
        
        # Get relative path
        try:
            rel_path = local_path.relative_to(base_dir)
            # Get all parts except the filename
            folder_parts = list(rel_path.parts[:-1])
            return folder_parts
        except ValueError:
            # If local_path is not relative to base_dir
            logger.warning(f"Path {local_path} is not relative to {base_dir}")
            return []
    
    def create_folder_structure(self, folder_parts):
        """
        Create a folder structure in Google Drive and return the ID of the deepest folder.
        
        Args:
            folder_parts: List of folder names to create
            
        Returns:
            ID of the deepest folder
        """
        current_parent = self.root_folder_id
        
        # Empty folder parts means store in root folder
        if not folder_parts:
            return current_parent
            
        # Create each folder in the hierarchy
        for folder_name in folder_parts:
            current_parent = self.get_or_create_folder(folder_name, current_parent)
        
        return current_parent
    
    def upload_file(self, local_file, base_dir, share_with_email=None):
        """
        Upload a file to Google Drive preserving its folder structure.
        
        Args:
            local_file: Path to local file
            base_dir: Base directory to determine relative path
            share_with_email: Optional email to share the file with
            
        Returns:
            Dict with file_id, drive_path and success status
        """
        local_path = Path(local_file)
        filename = local_path.name
        
        # Only proceed if the file exists
        if not local_path.exists():
            logger.warning(f"File not found: {local_path}")
            return {"success": False, "file_id": None, "drive_path": None}
        
        try:
            # Get folder structure
            folder_parts = self.get_folder_path(local_path, base_dir)
            
            # Create folder structure and get ID of deepest folder
            parent_folder_id = self.create_folder_structure(folder_parts)
            
            # Prepare file metadata
            file_metadata = {
                'name': filename,
                'parents': [parent_folder_id]
            }
            
            # Check if file already exists
            query = f"name='{filename}' and '{parent_folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query, 
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            existing_files = results.get('files', [])
            
            if existing_files:
                file_id = existing_files[0]['id']
                logger.info(f"File already exists: {filename} (ID: {file_id})")
                drive_path = '/'.join([self.root_folder_name] + folder_parts + [filename])
                return {"success": True, "file_id": file_id, "drive_path": drive_path, "already_exists": True}
            
            # Upload the file
            media = MediaFileUpload(
                str(local_path),
                resumable=True
            )
            
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            file_id = file.get('id')
            
            # Share if an email is provided
            if share_with_email:
                permission = {
                    'type': 'user',
                    'role': 'reader',
                    'emailAddress': share_with_email
                }
                
                self.drive_service.permissions().create(
                    fileId=file_id,
                    body=permission,
                    fields='id'
                ).execute()
                logger.info(f"Shared file {filename} with {share_with_email}")
            
            # Construct the Drive path for logging
            drive_path = '/'.join([self.root_folder_name] + folder_parts + [filename])
            
            logger.info(f"Uploaded file: {filename} -> {drive_path} (ID: {file_id})")
            
            return {"success": True, "file_id": file_id, "drive_path": drive_path}
            
        except Exception as e:
            logger.error(f"Error uploading file {local_file}: {e}", exc_info=True)
            return {"success": False, "file_id": None, "drive_path": None, "error": str(e)}
    
    def share_root_folder(self, email, role='reader'):
        """
        Share the root folder with a specific email.
        
        Args:
            email: Email address to share with
            role: Permission role (reader, writer, commenter, owner)
            
        Returns:
            Success status
        """
        try:
            permission = {
                'type': 'user',
                'role': role,
                'emailAddress': email
            }
            
            self.drive_service.permissions().create(
                fileId=self.root_folder_id,
                body=permission,
                fields='id'
            ).execute()
            
            logger.info(f"Shared root folder {self.root_folder_name} with {email} as {role}")
            return True
            
        except Exception as e:
            logger.error(f"Error sharing root folder with {email}: {e}", exc_info=True)
            return False


# Example usage
if __name__ == "__main__":
    # For testing
    uploader = GoogleDriveUploader(credentials_file="path/to/your/credentials.json")
    
    # Share the root folder with your personal account
    uploader.share_root_folder("your.email@gmail.com")
    
    # Upload a test file
    result = uploader.upload_file(
        "path/to/local/file.jpg",
        "path/to/base/dir",
        share_with_email="your.email@gmail.com"
    )
    
    print(result)