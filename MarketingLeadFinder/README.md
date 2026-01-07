# SMB Marketing Lead Finder

A Python tool that finds small and medium businesses (SMBs) actively looking for marketing help in a specified city.

## What It Does

This tool searches multiple sources to find companies that are:
- **Hiring for marketing roles** (Marketing Manager, Social Media Manager, Digital Marketing Specialist, etc.)
- **Posting RFPs/RFQs** for marketing, advertising, or branding services
- **Running major campaigns or rebrands** that suggest they might need agency help

### Data Sources

1. **Indeed** - Job postings for marketing roles
2. **Google Search** - Job postings on company career pages, Lever, Greenhouse
3. **Google Search** - RFPs and RFQs for marketing services
4. **Google Search** - News about rebrands and campaign launches

### Output Format

The tool generates a CSV file with the following columns:

| Column | Description |
|--------|-------------|
| Company Name | Name of the business |
| Website | Company website URL |
| City / Neighborhood | Location within the target city |
| Approx. Company Size | Estimated employee count (1-10, 11-50, 51-100, etc.) |
| Evidence They Need Marketing Help | The signal that indicates they need marketing (e.g., "Indeed posting for Social Media Manager") |
| Source Link(s) | Link to the job posting, RFP, or article |
| Contact Name | Name of person to contact (if found) |
| Contact Email | Email address for outreach |

## Setup

### Prerequisites

- Python 3.9 or higher
- OpenAI API key (for AI-powered enrichment and validation)

### Installation

1. **Clone or navigate to the directory:**
   ```bash
   cd MarketingLeadFinder
   ```

2. **Run the setup script:**
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

   Or manually:
   ```bash
   # Create virtual environment
   python3 -m venv venv
   source venv/bin/activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Install Playwright browsers
   playwright install chromium
   ```

3. **Set up your API key:**
   Create a `.env` file with:
   ```
   OPENAI_API_KEY=sk-your-openai-api-key-here
   ```

## Usage

### Basic Usage (Los Angeles)

```bash
source venv/bin/activate
python main.py
```

This will search for SMBs in the Greater Los Angeles area (default).

### Search a Different City

```bash
python main.py "San Francisco Bay Area"
python main.py "New York City"
python main.py "Chicago metropolitan area"
```

### Output

Results are saved to `smb_marketing_leads.csv` in the same directory.

## Configuration

Edit the configuration variables at the top of `main.py`:

```python
# Target number of leads
TARGET_LEADS = 25

# Employee size thresholds for SMBs
SMB_MAX_EMPLOYEES = 100

# Wait time between requests (be respectful to servers)
WAIT_BETWEEN_REQUESTS = 1.0

# Marketing job titles to search for
MARKETING_JOB_TITLES = [
    "Marketing Manager",
    "Digital Marketing Specialist",
    ...
]
```

## How It Works

### Step 1: Search Indeed
Searches Indeed for recent marketing job postings (last 14 days) for various marketing roles.

### Step 2: Search Google
- Searches for job postings on company career pages
- Looks for RFPs/RFQs for marketing services
- Finds news about rebrands and campaign launches

### Step 3: Enrich Data
- Finds company websites
- Scrapes contact pages for email addresses
- Estimates company size using AI

### Step 4: Validate Leads
Uses AI to filter out:
- Marketing agencies (we want end-clients)
- Large enterprises (Fortune 500, etc.)
- Franchises
- Companies without clear marketing needs

### Step 5: Save Results
Outputs a clean CSV file with all lead information.

## Filtering Logic

The tool automatically excludes:
- Marketing/advertising/PR agencies
- Large enterprises (Netflix, Disney, Amazon, etc.)
- Franchises (McDonald's, Starbucks, etc.)
- Companies with 100+ employees

## Rate Limiting

The tool includes built-in delays between requests to:
- Avoid getting blocked by websites
- Be respectful to servers
- Comply with terms of service

## Troubleshooting

### "Playwright browsers not installed"
Run: `playwright install chromium`

### "OpenAI API key not found"
Create a `.env` file with your API key.

### "No leads found"
- Try a different city
- Check your internet connection
- Some job boards may block automated access

## Future Improvements

- [ ] Add LinkedIn job search (requires authentication)
- [ ] Add Glassdoor and ZipRecruiter scrapers
- [ ] Add contact name/email enrichment via Apollo or Hunter.io
- [ ] Add company size verification via LinkedIn/Crunchbase
- [ ] Build interactive web UI
- [ ] Add email verification

## License

MIT License - Use freely for your marketing outreach needs.









