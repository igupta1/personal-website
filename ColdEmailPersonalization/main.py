"""
Cold Email Icebreaker Generator
Generates personalized cold email icebreakers by scraping websites and using AI.

Usage:
  python main.py                              # Process all CSVs in MarketingAgencies/
  python main.py --input leads.csv            # Process single file
  python main.py --input leads.csv --output leads_with_icebreakers.csv
  python main.py --folder MyLeads/            # Process folder
"""

import argparse
import asyncio
import json
import os
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, urljoin

import pandas as pd
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import AsyncOpenAI
import html2text

# Load environment variables
load_dotenv()

# Initialize OpenAI client
openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# GPT models for different tasks
SUMMARIZE_MODEL = "gpt-4.1-nano"  # Fast, cheap for summarization
ICEBREAKER_MODEL = "gpt-4.1-mini"  # Better quality for creative writing

# Column mapping - maps internal names to possible CSV column names
COLUMN_MAPPINGS = {
    'first_name': ['First Name', 'first_name', 'firstName'],
    'last_name': ['Last Name', 'last_name', 'lastName'],
    'contact_name': ['Contact Name', 'contact_name', 'contactName', 'Name', 'name'],
    'email': ['Email', 'email', 'Contact Email', 'contact_email'],
    'website': ['Website', 'website', 'website_url', 'URL', 'url'],
    'title': ['Title', 'title', 'headline', 'Contact Title', 'contact_title', 'Headline'],
    'company_name': ['Company Name', 'company_name', 'companyName', 'Company', 'company'],
    'location': ['Location', 'location', 'City / Neighborhood', 'City', 'city'],
    'phone': ['Phone', 'phone', 'phone_number', 'Phone Number'],
}


def detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Detect which columns exist in the DataFrame and map to internal names.
    Returns dict mapping internal_name -> actual_column_name
    """
    column_map = {}
    df_columns = set(df.columns)

    for internal_name, possible_names in COLUMN_MAPPINGS.items():
        for possible in possible_names:
            if possible in df_columns:
                column_map[internal_name] = possible
                break

    return column_map


def parse_name(full_name: str) -> Tuple[str, str]:
    """Parse 'First Last' into (first_name, last_name)"""
    if not full_name or pd.isna(full_name):
        return '', ''
    parts = str(full_name).strip().split(' ', 1)
    first_name = parts[0] if parts else ''
    last_name = parts[1] if len(parts) > 1 else ''
    return first_name, last_name

# Configuration
INPUT_FOLDER = "MarketingAgencies"  # Process all CSVs in this folder
MAX_LINKS_PER_SITE = 3
TIMEOUT = 30  # seconds
MAX_CONCURRENT_REQUESTS = 2  # Concurrent requests per lead (reduced for rate limits)
MAX_CONCURRENT_LEADS = 2  # Process multiple leads simultaneously (reduced for rate limits)
MAX_LEADS_TO_PROCESS = None  # Set to None to process all leads (testing with 10 first)
MAX_RETRIES = 5  # Max retries for rate-limited requests
API_CALL_DELAY = 1.0  # Delay between API calls to avoid rate limits
BASE_RETRY_DELAY = 2  # Base delay in seconds for exponential backoff


class LeadProcessor:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.html_converter.ignore_emphasis = False
        
    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Fetch URL content with error handling"""
        try:
            async with self.semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT), allow_redirects=True) as response:
                    if response.status == 200:
                        return await response.text()
                    return None
        except:
            return None
    
    def extract_links(self, html: str) -> List[str]:
        """Extract all href attributes from anchor tags"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for a_tag in soup.find_all('a', href=True):
            links.append(a_tag['href'])
        return links
    
    def normalize_domain(self, domain: str) -> str:
        """Normalize domain by removing www. prefix for comparison"""
        return domain.lower().removeprefix('www.')
    
    def filter_and_normalize_links(self, links: List[str], base_url: str) -> List[str]:
        """
        Replicate n8n logic:
        1. Filter links that start with '/'
        2. Normalize: convert absolute URLs to pathnames
        3. Remove trailing slashes (except root '/')
        4. Deduplicate
        """
        normalized = []
        
        for link in links:
            if not link:
                continue
                
            # Case 1: Starts with '/' - already relative
            if link.startswith('/'):
                normalized.append(link)
            
            # Case 2: Absolute URL (http or https)
            elif link.startswith('http://') or link.startswith('https://'):
                try:
                    parsed = urlparse(link)
                    # Only include if it's from the same domain (normalize www)
                    base_parsed = urlparse(base_url)
                    if self.normalize_domain(parsed.netloc) == self.normalize_domain(base_parsed.netloc):
                        path = parsed.path
                        # Strip trailing slash unless root
                        if path != '/' and path.endswith('/'):
                            path = path[:-1]
                        if path:
                            normalized.append(path if path else '/')
                except Exception:
                    continue
        
        # Filter only those starting with '/' (as per n8n Filter node)
        filtered = [link for link in normalized if link.startswith('/')]
        
        # Deduplicate while preserving order
        seen = set()
        deduplicated = []
        for link in filtered:
            if link not in seen:
                seen.add(link)
                deduplicated.append(link)
        
        result = deduplicated[:MAX_LINKS_PER_SITE]
        return result
    
    def html_to_markdown(self, html: str) -> str:
        """Convert HTML to Markdown"""
        try:
            return self.html_converter.handle(html)
        except:
            return ""
    
    async def summarize_page(self, markdown_content: str) -> str:
        """Summarize page using GPT-4.1-nano (cheap and fast)"""
        prompt = f"""You're a helpful, intelligent website scraping assistant.

You're provided a Markdown scrape of a website page. Your task is to provide a two-paragraph abstract of what this page is about.

Return ONLY valid JSON in this exact format (no markdown code blocks):
{{"abstract":"your abstract goes here"}}

Rules:
- Your extract should be comprehensive—similar level of detail as an abstract to a published paper.
- Use a straightforward, spartan tone of voice.
- If it's empty, just say "no content".

Website content:
{markdown_content[:10000]}"""

        for attempt in range(MAX_RETRIES):
            try:
                response = await openai_client.chat.completions.create(
                    model=SUMMARIZE_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3
                )

                # Parse JSON from response
                text = response.choices[0].message.content.strip()
                # Remove markdown code blocks if present
                if text.startswith("```"):
                    text = text.split("```")[1]
                    if text.startswith("json"):
                        text = text[4:]
                    text = text.strip()

                result = json.loads(text)
                abstract = result.get("abstract", "no content")
                return abstract
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "rate limit" in error_str.lower() or "quota" in error_str.lower():
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    print(f"        ⏳ Rate limited, waiting {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    continue
                print(f"        ⚠️  Summarize error: {error_str[:100]}")
                return "no content"
        print(f"        ⚠️  Max retries exceeded for summarize")
        return "no content"
    
    async def generate_icebreaker(self, first_name: str, last_name: str, headline: str, abstracts: List[str]) -> str:
        """Generate icebreaker using GPT-4.1-mini"""
        website_content = "\n\n".join(abstracts)

        prompt = f"""You're a senior outbound copywriter specializing in hyper-personalized cold email icebreakers. You are given multiple summaries of a company's website. Your job is to generate a single icebreaker that clearly shows we studied the recipient's site.

Return ONLY valid JSON in this exact format (no markdown code blocks):
{{"icebreaker":"Hey {{name}} — went down a rabbit hole on {{ShortCompanyName}}'s site. The part about {{specific_niche_detail}} caught my eye. Your focus on {{core_value_or_theme}} stuck with me."}}

RULES:
- {{ShortCompanyName}}: shorten multi-word company names to one clean word (e.g., "Maki Agency" → "Maki", "Chartwell Agency" → "Chartwell").
- {{specific_niche_detail}}: choose ONE sharp, concrete detail from the summaries (a specific process, case study, philosophy, niche service, repeated phrase, or concept).
- {{core_value_or_theme}}: choose ONE recurring value or theme that appears multiple times across the summaries (e.g., empathy, clarity, storytelling, precision, long-term thinking, craftsmanship, community impact, rigor).
- Both variables MUST directly come from the summaries. No inventing or guessing.
- Tone: concise, calm, founder-to-founder.
- Avoid generic compliments ("love your site", "great work").
- Do not alter the template — only fill in the variables.

Profile: {first_name} {last_name} {headline}

Website Summaries:
{website_content}"""

        for attempt in range(MAX_RETRIES):
            try:
                response = await openai_client.chat.completions.create(
                    model=ICEBREAKER_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7
                )

                # Parse JSON from response
                text = response.choices[0].message.content.strip()
                # Remove markdown code blocks if present
                if text.startswith("```"):
                    text = text.split("```")[1]
                    if text.startswith("json"):
                        text = text[4:]
                    text = text.strip()

                result = json.loads(text)
                icebreaker = result.get("icebreaker", "")
                return icebreaker
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "rate limit" in error_str.lower() or "quota" in error_str.lower():
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    print(f"        ⏳ Rate limited, waiting {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    continue
                print(f"        ⚠️  Icebreaker error: {error_str[:100]}")
                return ""
        print(f"        ⚠️  Max retries exceeded for icebreaker")
        return ""
    
    async def process_lead(self, session: aiohttp.ClientSession, lead: Dict) -> Dict:
        """Process a single lead through the entire pipeline"""
        website_url = lead['website_url']
        first_name = lead['first_name']
        last_name = lead['last_name']
        
        print(f"  🔄 Processing: {first_name} {last_name} - {website_url}")
        
        # Ensure URL has scheme
        if not website_url.startswith('http'):
            website_url = 'https://' + website_url
        
        # Step 1: Scrape home page
        home_html = await self.fetch_url(session, website_url)
        if not home_html:
            print(f"     ❌ Failed to fetch {website_url}")
            lead['multiline_icebreaker'] = ""
            return lead
        
        print(f"     ✅ Fetched homepage ({len(home_html)} chars)")
        
        # Step 2: Extract and filter links
        all_links = self.extract_links(home_html)
        filtered_links = self.filter_and_normalize_links(all_links, website_url)
        
        abstracts = []
        
        if not filtered_links:
            print(f"     🔄 Using fallback (no internal links)")
            # FALLBACK: Use homepage content
            markdown = self.html_to_markdown(home_html)
            if markdown.strip():
                abstract = await self.summarize_page(markdown)
                if abstract and abstract != "no content":
                    abstracts.append(abstract)
                    print(f"     ✅ Got homepage abstract")
        else:
            print(f"     🔄 Scraping {len(filtered_links)} sub-pages")
            # Step 3: Scrape and summarize sub-pages
            for path in filtered_links:
                full_url = urljoin(website_url, path)
                page_html = await self.fetch_url(session, full_url)
                if page_html:
                    markdown = self.html_to_markdown(page_html)
                    if markdown.strip():
                        abstract = await self.summarize_page(markdown)
                        if abstract and abstract != "no content":
                            abstracts.append(abstract)
            print(f"     ✅ Got {len(abstracts)} abstracts")
        
        if not abstracts:
            print(f"     ❌ No content to summarize")
            lead['multiline_icebreaker'] = ""
            return lead
        
        # Step 4: Generate icebreaker
        print(f"     🔄 Generating icebreaker...")
        icebreaker = await self.generate_icebreaker(
            first_name,
            last_name,
            lead['headline'],
            abstracts
        )
        
        if icebreaker:
            print(f"     ✅ Generated icebreaker")
        else:
            print(f"     ❌ Failed to generate icebreaker")
        
        lead['multiline_icebreaker'] = icebreaker
        return lead


async def process_and_save_lead(session: aiohttp.ClientSession, lead: Dict, processor: LeadProcessor, 
                                 output_file: str, lead_num: int, total: int, lock: asyncio.Lock) -> Dict:
    """Process a single lead and save it immediately to CSV"""
    try:
        processed_lead = await processor.process_lead(session, lead)
        
        # Save to CSV incrementally with file locking
        async with lock:
            df = pd.DataFrame([processed_lead])
            # Check if file exists to determine if we need to write header
            file_exists = os.path.exists(output_file)
            df.to_csv(output_file, mode='a', header=not file_exists, index=False)
            
            status = "✅" if processed_lead['multiline_icebreaker'] else "❌"
            print(f"\n{status} [{lead_num}/{total}] Saved: {lead['first_name']} {lead['last_name']}")
        
        return processed_lead
    except Exception as e:
        print(f"\n⚠️  [{lead_num}/{total}] ERROR processing {lead['first_name']} {lead['last_name']}: {str(e)}")
        lead['multiline_icebreaker'] = ""
        return lead


async def process_csv_file(input_csv: str, output_csv: str):
    """Process a single CSV file with flexible column detection"""
    print(f"\n📂 Loading leads from {input_csv}...")
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"❌ Error: {input_csv} not found")
        return

    print(f"   Loaded {len(df)} rows")

    # Detect columns using flexible mapping
    col_map = detect_columns(df)
    print(f"   Detected columns: {list(col_map.keys())}")

    # Validate required columns (need at least website)
    if 'website' not in col_map:
        print("❌ Error: No website column found. Required: Website, website, or website_url")
        return

    # Check for name columns
    has_name = 'first_name' in col_map or 'contact_name' in col_map
    if not has_name:
        print("⚠️  Warning: No name columns found. Icebreakers may be generic.")

    # Filter rows with valid website
    website_col = col_map['website']
    df_filtered = df[
        (df[website_col].notna()) &
        (df[website_col] != '') &
        (df[website_col].astype(str).str.strip() != '')
    ].copy()

    print(f"   Filtered to {len(df_filtered)} leads with website")

    if len(df_filtered) == 0:
        print("❌ No valid leads found after filtering")
        return

    # Create lead dictionaries with flexible column access
    leads = []
    for _, row in df_filtered.iterrows():
        # Handle name - try first_name/last_name, then contact_name
        first_name = ''
        last_name = ''

        if 'first_name' in col_map:
            first_name = str(row.get(col_map['first_name'], '') or '')
        if 'last_name' in col_map:
            last_name = str(row.get(col_map['last_name'], '') or '')

        # If no separate name columns, try contact_name
        if not first_name and 'contact_name' in col_map:
            first_name, last_name = parse_name(row.get(col_map['contact_name'], ''))

        lead = {
            'first_name': first_name,
            'last_name': last_name,
            'email': str(row.get(col_map.get('email', ''), '') or ''),
            'website_url': str(row.get(col_map['website'], '')),
            'headline': str(row.get(col_map.get('title', ''), '') or ''),
            'location': str(row.get(col_map.get('location', ''), '') or ''),
            'phone_number': str(row.get(col_map.get('phone', ''), '') or ''),
            'company_name': str(row.get(col_map.get('company_name', ''), '') or ''),
            'multiline_icebreaker': ''
        }
        leads.append(lead)
    
    # Limit to first N leads if MAX_LEADS_TO_PROCESS is set
    if MAX_LEADS_TO_PROCESS is not None:
        leads = leads[:MAX_LEADS_TO_PROCESS]
        print(f"   ⚠️  LIMITED TO FIRST {MAX_LEADS_TO_PROCESS} LEADS FOR TESTING")
    
    print(f"\n⚡ Processing {len(leads)} leads with {MAX_CONCURRENT_LEADS} concurrent workers")
    print(f"   Results will be written incrementally to {output_csv}")
    
    # Process leads
    processor = LeadProcessor()
    lock = asyncio.Lock()  # For thread-safe CSV writing
    
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * MAX_CONCURRENT_LEADS)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    ) as session:
        # Process leads concurrently in batches
        processed_leads = []
        for i in range(0, len(leads), MAX_CONCURRENT_LEADS):
            batch = leads[i:i + MAX_CONCURRENT_LEADS]
            batch_num = (i // MAX_CONCURRENT_LEADS) + 1
            total_batches = (len(leads) + MAX_CONCURRENT_LEADS - 1) // MAX_CONCURRENT_LEADS
            
            print(f"\n{'='*80}")
            print(f"🔄 Batch {batch_num}/{total_batches} - Processing {len(batch)} leads concurrently")
            print(f"{'='*80}")
            
            # Create tasks for concurrent processing
            tasks = []
            for j, lead in enumerate(batch):
                lead_num = i + j + 1
                task = process_and_save_lead(session, lead, processor, output_csv, lead_num, len(leads), lock)
                tasks.append(task)
            
            # Execute batch concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Track results
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"   ⚠️  Exception: {result}")
                else:
                    processed_leads.append(result)
    
    # Final summary
    success_count = sum(1 for lead in processed_leads if lead.get('multiline_icebreaker', ''))
    print(f"\n{'='*80}")
    print(f"✅ Complete! Generated {success_count}/{len(leads)} icebreakers")
    print(f"📊 Results saved to: {output_csv}")
    print("=" * 80)
    return success_count, len(leads)


async def main():
    """Main execution function with CLI argument support"""
    parser = argparse.ArgumentParser(
        description='Generate personalized cold email icebreakers from CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                              # Process all CSVs in MarketingAgencies/
  python main.py --input leads.csv            # Process single file
  python main.py --input leads.csv --output leads_with_icebreakers.csv
  python main.py --folder MyLeads/            # Process folder

Supported CSV column names (auto-detected):
  Website:  Website, website, website_url
  Name:     First Name/Last Name, first_name/last_name, Contact Name
  Email:    Email, email, Contact Email
  Title:    Title, headline, Contact Title
        """
    )
    parser.add_argument('--input', '-i', help='Single CSV file to process')
    parser.add_argument('--output', '-o', help='Output file (default: input_with_icebreakers.csv)')
    parser.add_argument('--folder', '-f', default=INPUT_FOLDER,
                        help=f'Folder containing CSVs (default: {INPUT_FOLDER})')
    args = parser.parse_args()

    print("=" * 80)
    print("🚀 Cold Email Icebreaker Generator")
    print("=" * 80)

    # Check OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY not found in .env file")
        return

    # Mode 1: Single file processing
    if args.input:
        input_path = args.input
        if args.output:
            output_path = args.output
        else:
            # Generate output filename
            base = os.path.splitext(input_path)[0]
            output_path = f"{base}_with_icebreakers.csv"

        print(f"\n📄 Processing single file: {input_path}")
        print(f"   Output: {output_path}")

        result = await process_csv_file(input_path, output_path)
        if result:
            success, total = result
            print(f"\n{'='*80}")
            print(f"🎉 Complete! Generated {success}/{total} icebreakers")
            print(f"📊 Results saved to: {output_path}")
            print("=" * 80)
        return

    # Mode 2: Folder processing
    folder = args.folder
    if not os.path.exists(folder):
        print(f"❌ Error: {folder} folder not found")
        return

    csv_files = [f for f in os.listdir(folder) if f.endswith('.csv') and '_with_icebreakers' not in f]
    if not csv_files:
        print(f"❌ Error: No CSV files found in {folder}")
        return

    print(f"\n📁 Found {len(csv_files)} CSV files in {folder}:")
    for f in csv_files:
        print(f"   - {f}")

    total_success = 0
    total_leads = 0

    for csv_file in csv_files:
        input_path = os.path.join(folder, csv_file)
        output_path = os.path.join(folder, csv_file.replace('.csv', '_with_icebreakers.csv'))

        # Skip if output file already exists (already processed)
        if os.path.exists(output_path):
            print(f"\n⏭️  Skipping {csv_file} - output file already exists")
            continue

        print(f"\n{'#'*80}")
        print(f"📄 Processing: {csv_file}")
        print(f"{'#'*80}")

        result = await process_csv_file(input_path, output_path)
        if result:
            success, total = result
            total_success += success
            total_leads += total

    print(f"\n{'='*80}")
    print(f"🎉 ALL COMPLETE! Generated {total_success}/{total_leads} icebreakers across {len(csv_files)} files")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
