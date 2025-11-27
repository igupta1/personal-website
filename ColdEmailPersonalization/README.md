# Deep Multiline Icebreaker System

A Python automation script that generates hyper-personalized cold email icebreakers by deeply analyzing prospect websites.

## What It Does

1. **Reads leads** from a CSV file
2. **Scrapes their websites** to find internal pages
3. **Analyzes content** using AI to understand what they do
4. **Generates personalized icebreakers** that show you actually researched them

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file with your OpenAI API key:

```bash
cp .env.example .env
```

Then edit `.env` and add your API key:

```
OPENAI_API_KEY=sk-...
```

### 3. Prepare Your Input

Place your leads CSV file in the same directory. Expected columns:
- First Name
- Last Name
- Email
- Website
- Title
- City
- State
- Country

## Usage

Run the script:

```bash
python main.py
```

The script will:
- Load leads from `apollo-contacts-export (21).csv`
- Process each lead (scraping + AI analysis)
- Output results to `leads_with_icebreakers.csv`

## How It Works

### The Deep Scrape Process

For each lead:

1. **Home Page Scraping**: Fetches the main website
2. **Link Extraction**: Finds all internal page links
3. **Link Filtering**: Keeps only internal paths (starting with `/`)
4. **Link Normalization**: Converts absolute URLs to paths, removes duplicates
5. **Sub-page Analysis**: Scrapes top 3 internal pages
6. **Content Summarization**: Uses GPT-4o to create abstracts of each page
7. **Icebreaker Generation**: Combines insights into a personalized opener

### Concurrency & Rate Limiting

- Uses `asyncio` and `aiohttp` for concurrent requests
- Limited to 5 concurrent connections to be respectful
- 30-second timeout per request
- Automatic error handling and retry logic

### The Icebreaker Format

Generated icebreakers follow this proven structure:

```
Hey {name}. Love {specific_thing}—also doing/like/a fan of {relatedThing}. 
Wanted to run something by you.

I hope you'll forgive me, but I creeped you/your site quite a bit, and know 
that {insight} is important to you guys (or at least I'm assuming this given 
the focus on {evidence}). I put something together a few months ago that I 
think could help...
```

## Configuration

Edit these constants in `main.py`:

```python
INPUT_CSV = "apollo-contacts-export (21).csv"
OUTPUT_CSV = "leads_with_icebreakers.csv"
MAX_LINKS_PER_SITE = 3
TIMEOUT = 30  # seconds
MAX_CONCURRENT_REQUESTS = 5
```

## Error Handling

The script handles:
- Missing websites or emails (skipped)
- Failed HTTP requests (logged and skipped)
- Timeouts (logged and moved on)
- Invalid HTML/content (skipped gracefully)
- API errors (logged with details)

## Output

Results are saved to `leads_with_icebreakers.csv` with columns:
- first_name
- last_name
- email
- website_url
- headline
- location
- phone_number
- **multiline_icebreaker** ← The generated opener

## Costs

Using GPT-4o:
- ~2-4 API calls per lead (1 summary + 1 icebreaker generation)
- Approximately $0.01-0.05 per lead
- 100 leads ≈ $1-5

## Tips

- Start with a small batch (10-20 leads) to test
- Review results and adjust prompts if needed
- Run during off-peak hours if processing many leads
- Check `leads_with_icebreakers.csv` for empty icebreakers (indicates failed processing)

## Troubleshooting

**"No valid leads found"**
- Check your CSV has columns: Website, Email, First Name, Last Name

**"OPENAI_API_KEY not found"**
- Make sure `.env` exists and contains your API key

**Many failed scrapes**
- Some sites block automated requests
- Try reducing MAX_CONCURRENT_REQUESTS to 3
- Add delays between batches if needed

**Empty icebreakers**
- Site might be blocking scrapers
- Content might be JavaScript-heavy (not scraped)
- Try visiting the website manually to verify it loads

## License

MIT

