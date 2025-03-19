# KB Newspaper Scraper

A production-ready web scraper for the Kungliga Biblioteket (KB) digital newspaper archive. This tool automatically downloads JP2 image files of historical newspapers by date range.

## Features

- Scrape newspapers by date range
- Process dates sequentially using GitHub Actions
- Extract manifest IDs and image URLs directly from search results
- Download high-quality JP2 images of newspaper pages
- Robust error handling and retry mechanisms
- Detailed logging and progress tracking

## Installation

### Prerequisites

- Python 3.7 or higher
- Chrome browser (for Selenium WebDriver)

### Setup

1. Clone this repository:
```bash
git clone https://github.com/yourusername/kb-newspaper-scraper.git
cd kb-newspaper-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

The `requirements.txt` file should include:
```
selenium
webdriver-manager
requests
```

## Usage

### Command Line

```bash
python src/01-scrape-images.py --start-date 1865-01-01 --end-date 1865-01-02 --download-dir kb_newspapers --headless
```

#### Command Line Arguments

- `--start-date`: Start date in YYYY-MM-DD format (required)
- `--end-date`: End date in YYYY-MM-DD format (required)
- `--paper-id`: Newspaper ID to filter by (optional, defaults to Dagens Nyheter)
- `--download-dir`: Directory to save downloaded files (default: `kb_newspapers`)
- `--headless`: Run Chrome in headless mode (no GUI)
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR)

### GitHub Actions

This repository includes a GitHub Actions workflow that can process an entire month of newspapers day by day. The workflow can be triggered manually or scheduled to run automatically.

#### Running with GitHub Actions

1. Go to the Actions tab in your repository
2. Select "KB Newspaper Scraper" workflow
3. Click "Run workflow"
4. Fill in the parameters:
   - Year (e.g., 1865)
   - Month (1-12)
   - Operation (start-month, continue-scraping, retry-failed, verify-month)
   - Newspaper IDs (optional, comma-separated)

#### Operations

- **start-month**: Initialize and process all days in a month sequentially
- **continue-scraping**: Continue from where the scraper left off
- **retry-failed**: Retry any failed days
- **verify-month**: Check which days have been completed/failed

## How It Works

1. The scraper navigates to the KB newspaper search page
2. It searches for newspapers within the specified date range
3. For each result, it extracts the manifest ID from the HTML
4. It fetches the manifest data which contains links to JP2 image files
5. It downloads all JP2 files for each newspaper issue
6. Images are organized by newspaper title and date

## Output Structure

Downloaded files are organized in this structure:
```
kb_newspapers/
├── Newspaper Title 1/
│   ├── YYYY-MM-DD/
│   │   ├── page1.jp2
│   │   ├── page2.jp2
│   │   └── ...
│   └── ...
└── Newspaper Title 2/
    └── ...
```

## Troubleshooting

### Common Issues

- **WebDriver errors**: Make sure Chrome is installed and up to date
- **Rate limiting**: If you encounter rate limiting, add delays between requests
- **Missing manifest IDs**: Try updating the regular expressions that extract IDs

### Logs

Detailed logs are saved to:
- `kb_scraper.log`: Main scraper log file

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- [Kungliga Biblioteket](https://tidningar.kb.se/) for providing the digital newspaper archive
- Selenium WebDriver for browser automation
- GitHub Actions for workflow automation