"""
Deep Multiline Icebreaker System
Converts n8n workflow to Python for generating personalized cold email icebreakers
"""

import asyncio
import os
import re
from typing import List, Dict, Optional
from urllib.parse import urlparse, urljoin

import pandas as pd
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import AsyncOpenAI
import html2text
import json

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Configuration
INPUT_CSV = "leads_without_icebreakers.csv"
OUTPUT_CSV = "recovered_icebreakers.csv"
MAX_LINKS_PER_SITE = 3
TIMEOUT = 30  # seconds
MAX_CONCURRENT_REQUESTS = 10  # Concurrent requests per lead
MAX_CONCURRENT_LEADS = 20  # Process multiple leads simultaneously
MAX_LEADS_TO_PROCESS = None  # Set to None to process all leads (testing with 10 first)


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
        try:
            response = await client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {
                        "role": "system",
                        "content": "You're a helpful, intelligent website scraping assistant."
                    },
                    {
                        "role": "user",
                        "content": """You're provided a Markdown scrape of a website page. Your task is to provide a two-paragraph abstract of what this page is about.

Return in this JSON format:

{"abstract":"your abstract goes here"}

Rules:
- Your extract should be comprehensive—similar level of detail as an abstract to a published paper.
- Use a straightforward, spartan tone of voice.
- If it's empty, just say "no content"."""
                    },
                    {
                        "role": "user",
                        "content": markdown_content[:10000]  # Limit content length
                    }
                ],
                response_format={"type": "json_object"},
                temperature=1.0
            )
            
            result = json.loads(response.choices[0].message.content)
            abstract = result.get("abstract", "no content")
            return abstract
        except Exception as e:
            print(f"        ⚠️  Summarize error: {str(e)[:100]}")
            return "no content"
    
    async def generate_icebreaker(self, first_name: str, last_name: str, headline: str, abstracts: List[str]) -> str:
        try:
            # Join abstracts
            website_content = "\n\n".join(abstracts)
            
            response = await client.chat.completions.create(
                model="gpt-5.1",
                messages=[
                    {
                        "role": "system",
                        "content": """You're a senior outbound copywriter specializing in hyper-personalized cold email icebreakers. You are given multiple summaries of a company's website. Your job is to generate a single icebreaker that clearly shows we studied the recipient's site.

Return ONLY valid JSON in this exact format:

{"icebreaker":"Hey {name} — went down a rabbit hole on {ShortCompanyName}'s site. The part about {specific_niche_detail} caught my eye. Your focus on {core_value_or_theme} stuck with me."}

RULES:
- {ShortCompanyName}: shorten multi-word company names to one clean word (e.g., "Maki Agency" → "Maki", "Chartwell Agency" → "Chartwell").
- {specific_niche_detail}: choose ONE sharp, concrete detail from the summaries (a specific process, case study, philosophy, niche service, repeated phrase, or concept).
- {core_value_or_theme}: choose ONE recurring value or theme that appears multiple times across the summaries (e.g., empathy, clarity, storytelling, precision, long-term thinking, craftsmanship, community impact, rigor).
- Both variables MUST directly come from the summaries. No inventing or guessing.
- Tone: concise, calm, founder-to-founder.
- Avoid generic compliments ("love your site", "great work").
- Do not alter the template — only fill in the variables."""
                    },
                    {
                        "role": "user",
                        "content": f"=Profile: {first_name} {last_name} {headline}\n\nWebsite Summaries:\n{website_content}"
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.5
            )
            
            result = json.loads(response.choices[0].message.content)
            icebreaker = result.get("icebreaker", "")
            return icebreaker
        except Exception as e:
            print(f"        ⚠️  Icebreaker error: {str(e)[:100]}")
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


async def main():
    """Main execution function"""
    print("=" * 80)
    print("🚀 Deep Multiline Icebreaker System")
    print("=" * 80)
    
    # Check API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY not found in .env file")
        return
    
    # Load CSV
    print(f"\n📂 Loading leads from {INPUT_CSV}...")
    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        print(f"❌ Error: {INPUT_CSV} not found")
        return
    
    print(f"   Loaded {len(df)} rows")
    
    # Detect column format
    if 'Website' in df.columns:
        # Original format
        df_filtered = df[
            (df['Website'].notna()) & 
            (df['Website'] != '') & 
            (df['Email'].notna()) & 
            (df['Email'] != '')
        ].copy()
        
        print(f"   Filtered to {len(df_filtered)} leads with website and email")
        
        if len(df_filtered) == 0:
            print("❌ No valid leads found after filtering")
            return
        
        # Create lead dictionaries
        leads = []
        for _, row in df_filtered.iterrows():
            lead = {
                'first_name': row.get('First Name', ''),
                'last_name': row.get('Last Name', ''),
                'email': row.get('Email', ''),
                'website_url': row.get('Website', ''),
                'headline': row.get('Title', ''),
                'location': f"{row.get('City', '')} {row.get('State', '')} {row.get('Country', '')}".strip(),
                'phone_number': row.get('Phone', ''),
                'multiline_icebreaker': ''
            }
            leads.append(lead)
    else:
        # Alternative format (lowercase columns)
        df_filtered = df[
            (df['website_url'].notna()) & 
            (df['website_url'] != '') & 
            (df['email'].notna()) & 
            (df['email'] != '')
        ].copy()
        
        print(f"   Filtered to {len(df_filtered)} leads with website and email")
        
        if len(df_filtered) == 0:
            print("❌ No valid leads found after filtering")
            return
        
        # Create lead dictionaries
        leads = []
        for _, row in df_filtered.iterrows():
            lead = {
                'first_name': str(row.get('first_name', '')),
                'last_name': str(row.get('last_name', '')),
                'email': str(row.get('email', '')),
                'website_url': str(row.get('website_url', '')),
                'headline': str(row.get('headline', '')),
                'location': str(row.get('location', '')),
                'phone_number': str(row.get('phone_number', '')),
                'multiline_icebreaker': ''
            }
            leads.append(lead)
    
    # Limit to first N leads if MAX_LEADS_TO_PROCESS is set
    if MAX_LEADS_TO_PROCESS is not None:
        leads = leads[:MAX_LEADS_TO_PROCESS]
        print(f"   ⚠️  LIMITED TO FIRST {MAX_LEADS_TO_PROCESS} LEADS FOR TESTING")
    
    # Remove existing output file for fresh start
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
        print(f"   🗑️  Removed existing {OUTPUT_CSV}")
    
    print(f"\n⚡ Processing {len(leads)} leads with {MAX_CONCURRENT_LEADS} concurrent workers")
    print(f"   Results will be written incrementally to {OUTPUT_CSV}")
    
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
                task = process_and_save_lead(session, lead, processor, OUTPUT_CSV, lead_num, len(leads), lock)
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
    print(f"📊 Results saved to: {OUTPUT_CSV}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
