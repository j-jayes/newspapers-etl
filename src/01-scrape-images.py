#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
KB Newspaper Scraper

A production-ready scraper for the KB newspaper archive.
Fetches newspaper images from the KB digital archive based on date ranges.
Uploads files to Google Drive under a top-level folder (default: "newspapers")
which is shared with a specified email address.
"""

import os
import re
import time
import logging
import argparse
import requests
import hashlib
import functools
from typing import List, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.remote.webelement import WebElement
from webdriver_manager.chrome import ChromeDriverManager

# --- Configuration via Environment Variables ---
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON", "newspapers-454313-7a72698b783e.json")
SHARED_EMAIL = os.environ.get("SHARED_EMAIL", "j0nathanjayes@gmail.com")
TOP_FOLDER_NAME = os.environ.get("DRIVE_TOP_FOLDER", "newspapers")
# -----------------------------------------------------

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kb_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("KBNewspaperScraper")
# ---------------------

# --- Retry Decorator for API Calls ---
def retry(max_attempts=5, initial_delay=1, backoff_factor=2):
    """
    Decorator for retrying a function with exponential backoff.
    """
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts.")
                        raise
                    else:
                        logger.warning(f"Error in {func.__name__}: {e}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= backoff_factor
        return wrapper_retry
    return decorator_retry
# ---------------------------------------

# --- File Integrity: Compute MD5 Checksum ---
def compute_md5(file_path: Path) -> str:
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
# ----------------------------------------------

# --- Google Drive Integration ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def get_drive_service():
    """Initialize and return a Google Drive API service instance."""
    scopes = ['https://www.googleapis.com/auth/drive']
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=scopes)
    return build('drive', 'v3', credentials=creds)

@retry()
def upload_to_drive(file_path: Path, file_name: str, folder_id: Optional[str] = None) -> str:
    """
    Upload a file to Google Drive and verify its integrity.
    
    Returns the uploaded file's ID if the MD5 checksum matches.
    """
    service = get_drive_service()
    file_metadata = {'name': file_name}
    if folder_id:
        file_metadata['parents'] = [folder_id]
    
    # Determine MIME type based on file extension.
    if str(file_path).lower().endswith('.jp2'):
        mime_type = 'image/jp2'
    elif str(file_path).lower().endswith(('.jpg', '.jpeg')):
        mime_type = 'image/jpeg'
    else:
        mime_type = 'application/octet-stream'
    
    media = MediaFileUpload(str(file_path), mimetype=mime_type)
    # Request md5Checksum along with id.
    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, md5Checksum'
    ).execute()
    
    local_md5 = compute_md5(file_path)
    remote_md5 = uploaded_file.get("md5Checksum")
    if local_md5 != remote_md5:
        raise ValueError(f"MD5 mismatch for {file_name}: local({local_md5}) != remote({remote_md5})")
    
    logger.info(f"MD5 check passed for {file_name}")
    return uploaded_file.get('id')

@retry()
def get_or_create_drive_folder(service, folder_name: str, parent_id: Optional[str] = None) -> str:
    """
    Get or create a folder in Google Drive.
    
    Returns the folder ID.
    """
    query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])
    if files:
        folder_id = files[0]['id']
        logger.debug(f"Found existing folder '{folder_name}' with ID: {folder_id}")
        return folder_id
    else:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]
        folder = service.files().create(body=file_metadata, fields='id').execute()
        folder_id = folder.get('id')
        logger.info(f"Created folder '{folder_name}' with ID: {folder_id}")
        return folder_id

@retry()
def share_drive_folder(service, folder_id: str, email: str):
    """
    Share a Google Drive folder with a specified email if not already shared.
    """
    if is_folder_shared_with(service, folder_id, email):
        logger.info(f"Folder {folder_id} is already shared with {email}. Skipping sharing.")
        return

    permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': email
    }
    service.permissions().create(
        fileId=folder_id, body=permission, fields='id'
    ).execute()
    logger.info(f"Shared folder ID {folder_id} with {email}")

def file_exists_in_drive_folder(service, file_name: str, folder_id: str) -> bool:
    """
    Check if a file with the given name exists in the specified Drive folder.
    """
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])
    return len(files) > 0

def is_folder_shared_with(service, folder_id: str, email: str) -> bool:
    """
    Check if a folder is already shared with the given email.
    """
    response = service.permissions().list(
        fileId=folder_id,
        fields="permissions(id, emailAddress)"
    ).execute()
    permissions = response.get("permissions", [])
    for permission in permissions:
        if permission.get("emailAddress") == email:
            return True
    return False
# --- End Google Drive Integration ---

@dataclass
class NewspaperIssue:
    """Data class to store information about a newspaper issue."""
    title: str
    date: str
    manifest_id: str
    filenames: List[str] = None
    
    def __post_init__(self):
        if self.filenames is None:
            self.filenames = []

class KBNewspaperScraper:
    """
    A scraper for the KB (Kungliga biblioteket) digital newspaper archive.
    
    Fetches newspaper pages as JP2 files based on date ranges and optional filters.
    Uses Selenium for web navigation and HTTP requests for file downloads.
    """
    
    BASE_URL = "https://tidningar.kb.se"
    SEARCH_URL = f"{BASE_URL}/search"
    API_BASE_URL = "https://data.kb.se"
    
    def __init__(self, download_dir: str = "newspaper_downloads", headless: bool = True, 
                 retry_count: int = 3, wait_time: int = 20, drive_parent_folder_id: Optional[str] = None):
        """
        Initialize the scraper.
        
        Args:
            download_dir: Directory for temporary local downloads.
            headless: Whether to run the browser in headless mode.
            retry_count: Number of times to retry failed operations.
            wait_time: Maximum wait time in seconds for page elements.
            drive_parent_folder_id: Google Drive folder ID under which uploads should go.
        """
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.retry_count = retry_count
        self.drive_parent_folder_id = drive_parent_folder_id
        
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        logger.info("Initializing Chrome WebDriver")
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            self.wait = WebDriverWait(self.driver, wait_time)
            logger.info("WebDriver initialized successfully")
        except WebDriverException as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise
    
    def extract_manifest_id_from_html(self, html_content: str) -> Optional[str]:
        pattern = r'data-src="https://data\.kb\.se/iiif/\d+/([^/%]+)'
        match = re.search(pattern, html_content)
        if match:
            return match.group(1)
        pattern = r'src="https://data\.kb\.se/iiif/\d+/([^/%]+)'
        match = re.search(pattern, html_content)
        if match:
            return match.group(1)
        return None
    
    def extract_date_from_html(self, html_content: str) -> Optional[str]:
        date_pattern = r'<p class="search-result-item-date[^>]*>([^<]+)</p>'
        match = re.search(date_pattern, html_content)
        if match:
            return match.group(1).strip()
        title_pattern = r'<title>([^|]+)\s+(\d{4}-\d{2}-\d{2})\s*[|]'
        match = re.search(title_pattern, html_content)
        if match:
            return match.group(2).strip()
        filename_pattern = r'bib\d+_(\d{4})(\d{2})(\d{2})_'
        match = re.search(filename_pattern, html_content)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        return None
    
    def extract_filenames_from_html(self, html_content: str) -> List[str]:
        filename_pattern = r'(bib\d+_\d+_\d+_\d+_\d+\.jp2)'
        matches = re.findall(filename_pattern, html_content)
        return list(set(matches))
    
    def extract_title_and_date_from_page_head(self, page_source: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            title_pattern = r'<title>([^|]+?)(?:\s+(\d{4}-\d{2}-\d{2}))?\s*[|]'
            match = re.search(title_pattern, page_source)
            if match:
                title = match.group(1).strip()
                date = match.group(2).strip() if match.group(2) else None
                if not date:
                    meta_pattern = r'<meta[^>]*og:title[^>]*content="[^"]*?\s+(\d{4}-\d{2}-\d{2})"'
                    meta_match = re.search(meta_pattern, page_source)
                    if meta_match:
                        date = meta_match.group(1)
                return title, date
            return None, None
        except Exception as e:
            logger.error(f"Error extracting title and date from head: {e}")
            return None, None
    
    def extract_jp2_from_manifest_data(self, manifest_url: str) -> List[str]:
        try:
            logger.info(f"Fetching manifest data from: {manifest_url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Referer': 'https://tidningar.kb.se/',
            }
            response = requests.get(f"{manifest_url}/manifest", headers=headers)
            response.raise_for_status()
            manifest_data = response.json()
            jp2_urls = []
            filenames = []
            if 'items' in manifest_data:
                for canvas in manifest_data['items']:
                    if 'items' in canvas:
                        for annotation_page in canvas['items']:
                            if 'items' in annotation_page:
                                for annotation in annotation_page['items']:
                                    if 'body' in annotation and 'id' in annotation['body']:
                                        body_id = annotation['body']['id']
                                        if body_id.endswith('.jp2'):
                                            jp2_urls.append(body_id)
                                            filename = body_id.split('/')[-1]
                                            filenames.append(filename)
            if filenames:
                logger.info(f"Extracted {len(filenames)} filenames from manifest")
                for filename in filenames[:5]:
                    logger.debug(f" - {filename}")
            return jp2_urls
        except Exception as e:
            logger.error(f"Error extracting JP2 files from manifest: {e}", exc_info=True)
            return []
    
    def download_file(self, url: str, filepath: Path) -> bool:
        try:
            start_time = time.time()
            logger.info(f"Downloading {url} to {filepath}")
            if filepath.exists():
                logger.info(f"File already exists: {filepath}")
                return True
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://tidningar.kb.se/',
                'Accept': 'image/jpeg, image/png, image/jp2, */*'
            }
            clean_url = url.replace('\\\\', '/').replace('\\/', '/')
            for attempt in range(self.retry_count):
                try:
                    response = requests.get(clean_url, headers=headers, stream=True, timeout=30)
                    response.raise_for_status()
                    filepath.parent.mkdir(parents=True, exist_ok=True)
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    elapsed = time.time() - start_time
                    logger.info(f"Downloaded: {filepath} in {elapsed:.2f} seconds")
                    return True
                except (requests.exceptions.RequestException, requests.exceptions.Timeout) as req_err:
                    if attempt < self.retry_count - 1:
                        logger.warning(f"Retry {attempt+1}/{self.retry_count} downloading {clean_url}: {req_err}")
                        time.sleep(2)
                    else:
                        raise
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}", exc_info=True)
            return False
    
    def process_search_result(self, result: WebElement) -> Optional[NewspaperIssue]:
        try:
            newspaper_title = None
            newspaper_date = None
            date_elements = result.find_elements(By.CSS_SELECTOR, "p.search-result-item-date")
            if date_elements:
                newspaper_date = date_elements[0].text.strip()
            title_elements = result.find_elements(By.CSS_SELECTOR, "div.search-result-item-title")
            if title_elements:
                newspaper_title = title_elements[0].text.strip()
            logger.info(f"Processing issue: {newspaper_title} - {newspaper_date}")
            inner_html = result.get_attribute('innerHTML')
            logger.debug("Extracting manifest ID from inner HTML")
            manifest_id = self.extract_manifest_id_from_html(inner_html)
            if not newspaper_date:
                extracted_date = self.extract_date_from_html(inner_html)
                if extracted_date:
                    newspaper_date = extracted_date
                    logger.info(f"Extracted date from HTML: {newspaper_date}")
            potential_filenames = self.extract_filenames_from_html(inner_html)
            if potential_filenames:
                logger.info(f"Found {len(potential_filenames)} potential JP2 filenames in HTML")
                for filename in potential_filenames[:3]:
                    logger.debug(f" - {filename}")
            if manifest_id:
                logger.info(f"Found manifest ID from HTML: {manifest_id}")
                if newspaper_title:
                    newspaper_title = re.sub(r'[^\w\s-]', '', newspaper_title).strip()
                else:
                    newspaper_title = "Unknown"
                if newspaper_date:
                    newspaper_date = newspaper_date.replace('/', '-')
                else:
                    newspaper_date = "Unknown_Date"
                return NewspaperIssue(
                    title=newspaper_title,
                    date=newspaper_date,
                    manifest_id=manifest_id,
                    filenames=potential_filenames
                )
            else:
                logger.warning("Failed to extract manifest ID from HTML")
                return None
        except Exception as e:
            logger.error(f"Error processing search result: {e}", exc_info=True)
            return None
    
    def download_newspaper_issue(self, issue: NewspaperIssue) -> bool:
        """
        Download all files for a newspaper issue, upload them to Google Drive under a folder structure:
        TOP_FOLDER_NAME -> issue title -> issue date, and then remove the local copies.
        """
        try:
            manifest_url = f"{self.API_BASE_URL}/{issue.manifest_id}"
            folder_path = self.download_dir / issue.title / issue.date
            folder_path.mkdir(parents=True, exist_ok=True)
            jp2_urls = self.extract_jp2_from_manifest_data(manifest_url)
            logger.info(f"Found {len(jp2_urls)} JP2 files from manifest data")
            if not jp2_urls:
                logger.warning(f"No JP2 files found for {issue.title} - {issue.date}")
                return False
            
            drive_service = get_drive_service()
            if self.drive_parent_folder_id:
                title_folder_id = get_or_create_drive_folder(drive_service, issue.title, parent_id=self.drive_parent_folder_id)
                date_folder_id = get_or_create_drive_folder(drive_service, issue.date, parent_id=title_folder_id)
            else:
                date_folder_id = None
            
            success_count = 0
            for file_url in jp2_urls:
                filename = Path(unquote(file_url)).name
                file_path = folder_path / filename

                # Skip download if the file already exists in Drive.
                if date_folder_id and file_exists_in_drive_folder(drive_service, filename, date_folder_id):
                    logger.info(f"File {filename} already exists on Google Drive. Skipping download.")
                    success_count += 1
                    continue

                if self.download_file(file_url, file_path):
                    start_upload = time.time()
                    try:
                        drive_file_id = upload_to_drive(file_path, file_name=filename, folder_id=date_folder_id)
                        elapsed_upload = time.time() - start_upload
                        logger.info(f"Uploaded {filename} to Google Drive with ID: {drive_file_id} in {elapsed_upload:.2f} seconds")
                        success_count += 1
                        file_path.unlink()  # Remove local file after successful upload.
                    except Exception as upload_err:
                        logger.error(f"Failed to upload {filename} to Google Drive: {upload_err}", exc_info=True)
            return success_count == len(jp2_urls)
        except Exception as e:
            logger.error(f"Error downloading newspaper issue: {e}", exc_info=True)
            return False
    
    def scrape_by_date_range(self, start_date: str, end_date: str, paper_id: str = None) -> List[NewspaperIssue]:
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(date_pattern, start_date) or not re.match(date_pattern, end_date):
            logger.error(f"Invalid date format. Must be YYYY-MM-DD. Got: {start_date} to {end_date}")
            return []
        url = f"{self.SEARCH_URL}?q=%2a&from={start_date}&to={end_date}"
        if paper_id:
            url += f"&isPartOf.%40id={paper_id}"
        else:
            url += "&isPartOf.%40id=https%3A%2F%2Flibris.kb.se%2Fm5z2w4lz3m2zxpk%23it"
        logger.info(f"Using search URL: {url}")
        logger.info(f"Navigating to search page: {url}")
        self.driver.get(url)
        time.sleep(3)
        try:
            results = self.wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.search-result-item"))
            )
            logger.info(f"Found {len(results)} newspaper issues")
            processed_issues = []
            for i, result in enumerate(results):
                try:
                    logger.info(f"Processing result {i+1}/{len(results)}")
                    issue = self.process_search_result(result)
                    if issue:
                        success = self.download_newspaper_issue(issue)
                        if success:
                            processed_issues.append(issue)
                            logger.info(f"Successfully downloaded issue: {issue.title} - {issue.date}")
                        else:
                            logger.warning(f"Failed to download issue: {issue.title} - {issue.date}")
                except Exception as e:
                    logger.error(f"Error processing result {i+1}: {e}", exc_info=True)
                    continue
            return processed_issues
        except TimeoutException:
            logger.error("Timeout waiting for search results")
            return []
        except Exception as e:
            logger.error(f"Error during scraping: {e}", exc_info=True)
            return []
    
    def close(self):
        """Close the browser and clean up resources."""
        logger.info("Closing WebDriver")
        try:
            self.driver.quit()
        except Exception:
            pass

def main():
    """Main entry point for the scraper."""
    parser = argparse.ArgumentParser(description='KB Newspaper Scraper')
    parser.add_argument('--start-date', type=str, required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, required=True, help='End date in YYYY-MM-DD format')
    parser.add_argument('--paper-id', type=str, help='Optional paper ID to filter by')
    parser.add_argument('--download-dir', type=str, default='kb_newspapers', help='Directory for temporary downloads')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', 
                        help='Logging level')
    args = parser.parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    drive_service = get_drive_service()
    newspapers_folder_id = get_or_create_drive_folder(drive_service, TOP_FOLDER_NAME)
    share_drive_folder(drive_service, newspapers_folder_id, SHARED_EMAIL)
    
    scraper = KBNewspaperScraper(download_dir=args.download_dir, headless=args.headless,
                                  drive_parent_folder_id=newspapers_folder_id)
    try:
        issues = scraper.scrape_by_date_range(args.start_date, args.end_date, args.paper_id)
        logger.info(f"Completed scraping. Downloaded {len(issues)} issues successfully.")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
