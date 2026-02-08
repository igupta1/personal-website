#!/usr/bin/env python3
"""
Demo Lead Processor for Website

Takes manual company names, roles, and Indeed links as input. Uses Gemini with Google Search
to find contact info and website, then generates icebreakers using GPT.

Input CSV format: company_name, role, indeed_link

Usage:
    python process_demo_leads.py                    # Process demo_input.csv
    python process_demo_leads.py --input custom.csv # Process custom file
    python process_demo_leads.py --skip-upload      # Generate locally without uploading
"""

import argparse
import asyncio
import json
import os
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai
from google.genai import types
from openai import AsyncOpenAI
import html2text

# Load environment variables
load_dotenv()

# Initialize clients
gemini_client = genai.Client(api_key=os.getenv("GOOGLE_GEMINI_API_KEY"))
openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Configuration
VERCEL_API_URL = os.getenv("VERCEL_API_URL", "https://www.ishaangpta.com")
LEADS_UPLOAD_API_KEY = os.getenv("LEADS_UPLOAD_API_KEY")
DEMO_LOCATION = "demo"  # Location key for Vercel blob storage

# GPT models
SUMMARIZE_MODEL = "gpt-4.1-nano"
ICEBREAKER_MODEL = "gpt-4.1-mini"

# Scraping settings
TIMEOUT = 30
MAX_LINKS_PER_SITE = 3
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Retry settings
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2


class GeminiEnricher:
    """Use Gemini with Google Search grounding to find contact info and website"""

    def _generate_email_patterns(self, first_name: str, last_name: str, domain: str) -> List[str]:
        """Generate common email patterns to try"""
        if not first_name or not domain:
            return []

        first = first_name.lower().strip()
        last = last_name.lower().strip() if last_name else ""

        patterns = [
            f"{first}@{domain}",                           # john@company.com
            f"{first}.{last}@{domain}" if last else None,  # john.doe@company.com
            f"{first[0]}{last}@{domain}" if last else None, # jdoe@company.com
            f"{first}{last}@{domain}" if last else None,   # johndoe@company.com
            f"{first}_{last}@{domain}" if last else None,  # john_doe@company.com
            f"{first[0]}.{last}@{domain}" if last else None, # j.doe@company.com
            f"{last}@{domain}" if last else None,          # doe@company.com
        ]
        return [p for p in patterns if p]

    def _extract_domain(self, website: str) -> str:
        """Extract domain from website URL"""
        if not website:
            return ""
        from urllib.parse import urlparse
        parsed = urlparse(website)
        domain = parsed.netloc or parsed.path
        domain = domain.replace("www.", "")
        return domain

    async def find_contact(self, company_name: str) -> Dict:
        """Find contact info, website, email, LinkedIn, company size, and location"""
        result = {
            "contact_name": "",
            "contact_title": "",
            "website": "",
            "email": "",
            "linkedin": "",
            "company_size": "unknown",
            "location": ""
        }

        try:
            prompt = f"""Find the current key decision-maker, contact information, official website, company size, and location for:
Company: {company_name}

Target roles (in strict order):
1. CEO / Founder / Owner
2. President
3. CMO / VP of Marketing

CRITICAL - EMAIL FINDING INSTRUCTIONS:
- Search extensively for the person's WORK email address
- Check sources: LinkedIn profiles, company "About Us" or "Team" pages, press releases, news articles, industry directories, speaking engagements, podcast appearances, author bios
- Look for email patterns on the company website (e.g., if you find any employee email, use that pattern)
- If you find the company domain (e.g., company.com), try common patterns: firstname@company.com, firstname.lastname@company.com
- DO NOT return generic emails like info@, contact@, support@, hello@, sales@
- Only return a personal business email for the specific person

OTHER INSTRUCTIONS:
- Use Google Search to verify the CURRENT person in this role
- Find the official company website (the main homepage URL)
- Find the person's LinkedIn profile URL
- Estimate approximate employee count (e.g., "10-50", "50-100", "100-250", "unknown")
- Find the company's main office location (City, State format)
- Return valid JSON only

Return JSON format:
{{
  "name": "Full Name",
  "title": "Exact Job Title",
  "website": "https://www.company.com",
  "email": "person@company.com",
  "linkedin": "https://www.linkedin.com/in/username",
  "company_size": "10-50 employees",
  "location": "City, State"
}}"""

            response = await gemini_client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.0
                )
            )

            # Parse JSON from response
            response_text = response.text.strip()
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()

            data = json.loads(response_text)
            result["contact_name"] = data.get('name', '').strip()
            result["contact_title"] = data.get('title', '').strip()
            result["website"] = data.get('website', '').strip()
            result["email"] = data.get('email', '').strip()
            result["linkedin"] = data.get('linkedin', '').strip()
            result["company_size"] = data.get('company_size', 'unknown').strip()
            result["location"] = data.get('location', '').strip()

            # If no email found but we have name and website, try a focused email search
            if not result["email"] and result["contact_name"] and result["website"]:
                print(f"    No email found, trying focused email search...")
                email = await self._search_for_email(result["contact_name"], company_name, result["website"])
                if email:
                    result["email"] = email

        except Exception as e:
            print(f"    Gemini error: {str(e)[:80]}")

        return result

    async def _search_for_email(self, person_name: str, company_name: str, website: str) -> str:
        """Do a focused search specifically for the person's email"""
        try:
            domain = self._extract_domain(website)

            prompt = f"""Find the work email address for this specific person:

Person: {person_name}
Company: {company_name}
Company Website: {website}
Company Domain: {domain}

SEARCH STRATEGY:
1. Search "{person_name} {company_name} email"
2. Search "{person_name} @{domain}"
3. Look for their LinkedIn profile and check for email
4. Check company team/about pages
5. Look for press releases, interviews, or articles mentioning them
6. Check if they've authored any content with their email listed

If you cannot find their exact email but find the company's email pattern (e.g., you see another employee uses firstname.lastname@{domain}), then construct the email using that pattern.

ONLY return the email address, nothing else. If you cannot find or reasonably construct the email, return "NOT_FOUND".

Email:"""

            response = await gemini_client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.0
                )
            )

            email = response.text.strip().lower()

            # Validate the email looks reasonable
            if email and email != "not_found" and "@" in email and "." in email:
                # Filter out generic emails
                generic_prefixes = ['info', 'contact', 'support', 'hello', 'sales', 'admin', 'help', 'team']
                email_prefix = email.split('@')[0]
                if email_prefix not in generic_prefixes:
                    return email

        except Exception as e:
            print(f"    Email search error: {str(e)[:50]}")

        return ""


class IcebreakerGenerator:
    """Generate icebreakers using GPT"""

    def __init__(self):
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True

    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Fetch URL content"""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT), allow_redirects=True) as response:
                if response.status == 200:
                    return await response.text()
        except:
            pass
        return None

    def extract_links(self, html: str, base_url: str) -> List[str]:
        """Extract internal links from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('/'):
                links.append(href)
            elif href.startswith(base_url):
                from urllib.parse import urlparse
                parsed = urlparse(href)
                if parsed.path:
                    links.append(parsed.path)

        # Deduplicate and limit
        seen = set()
        result = []
        for link in links:
            if link not in seen and link != '/':
                seen.add(link)
                result.append(link)
                if len(result) >= MAX_LINKS_PER_SITE:
                    break

        return result

    async def summarize_page(self, markdown_content: str) -> str:
        """Summarize page using GPT-4.1-nano"""
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

                text = response.choices[0].message.content.strip()
                if text.startswith("```"):
                    text = text.split("```")[1]
                    if text.startswith("json"):
                        text = text[4:]
                    text = text.strip()

                result = json.loads(text)
                return result.get("abstract", "no content")
            except Exception as e:
                if "429" in str(e) or "rate limit" in str(e).lower():
                    await asyncio.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                    continue
                break

        return "no content"

    async def generate_icebreaker(self, first_name: str, abstracts: List[str]) -> str:
        """Generate icebreaker using GPT-4.1-mini"""
        website_content = "\n\n".join(abstracts)

        prompt = f"""You're a senior outbound copywriter specializing in hyper-personalized cold email icebreakers. You are given multiple summaries of a company's website. Your job is to generate a single icebreaker that clearly shows we studied the recipient's site.

Return ONLY valid JSON in this exact format (no markdown code blocks):
{{"icebreaker":"Hey {{name}} — I spent some time looking through {{ShortCompanyName}}’s site, and the way you {{specific_niche_detail}} stood out. The emphasis on {{core_value_or_theme}} really came through.}}

RULES:
- {{ShortCompanyName}}: shorten multi-word company names to one clean word (e.g., "Maki Agency" → "Maki", "Chartwell Agency" → "Chartwell").
- {{specific_niche_detail}}: choose ONE sharp, concrete detail from the summaries (a specific process, case study, philosophy, niche service, repeated phrase, or concept).
- {{core_value_or_theme}}: choose ONE recurring value or theme that appears multiple times across the summaries (e.g., empathy, clarity, storytelling, precision, long-term thinking, craftsmanship, community impact, rigor).
- Both variables MUST directly come from the summaries. No inventing or guessing.
- Tone: concise, calm, founder-to-founder.
- Avoid generic compliments ("love your site", "great work").
- Do not alter the template — only fill in the variables.

Profile: {first_name}

Website Summaries:
{website_content}"""

        for attempt in range(MAX_RETRIES):
            try:
                response = await openai_client.chat.completions.create(
                    model=ICEBREAKER_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7
                )

                text = response.choices[0].message.content.strip()
                if text.startswith("```"):
                    text = text.split("```")[1]
                    if text.startswith("json"):
                        text = text[4:]
                    text = text.strip()

                result = json.loads(text)
                return result.get("icebreaker", "")
            except Exception as e:
                if "429" in str(e) or "rate limit" in str(e).lower():
                    await asyncio.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                    continue
                break

        return ""

    async def process_website(self, session: aiohttp.ClientSession, website: str, first_name: str) -> str:
        """Scrape website and generate icebreaker"""
        if not website.startswith('http'):
            website = 'https://' + website

        # Fetch homepage
        home_html = await self.fetch_url(session, website)
        if not home_html:
            return ""

        abstracts = []

        # Extract and summarize sub-pages
        links = self.extract_links(home_html, website)

        if links:
            for path in links:
                full_url = urljoin(website, path)
                page_html = await self.fetch_url(session, full_url)
                if page_html:
                    markdown = self.html_converter.handle(page_html)
                    if markdown.strip():
                        abstract = await self.summarize_page(markdown)
                        if abstract and abstract != "no content":
                            abstracts.append(abstract)
        else:
            # Use homepage if no links
            markdown = self.html_converter.handle(home_html)
            if markdown.strip():
                abstract = await self.summarize_page(markdown)
                if abstract and abstract != "no content":
                    abstracts.append(abstract)

        if not abstracts:
            return ""

        return await self.generate_icebreaker(first_name, abstracts)


def categorize_by_size(company_size: str) -> str:
    """Map company size to category"""
    size_str = company_size.lower()

    if "unknown" in size_str:
        return "small"  # Default to small for demo

    numbers = re.findall(r'\d+', size_str.replace(',', ''))
    if numbers:
        upper_bound = int(numbers[-1]) if len(numbers) >= 2 else int(numbers[0])
        if upper_bound <= 100:
            return "small"
        elif upper_bound <= 250:
            return "medium"
        else:
            return "large"

    return "small"


def parse_name(full_name: str) -> Tuple[str, str]:
    """Parse 'First Last' into (first_name, last_name)"""
    if not full_name:
        return '', ''
    parts = str(full_name).strip().split(' ', 1)
    first_name = parts[0] if parts else ''
    last_name = parts[1] if len(parts) > 1 else ''
    return first_name, last_name


async def upload_to_vercel(leads: List[Dict], location: str = DEMO_LOCATION) -> bool:
    """Upload leads to Vercel Blob Storage"""
    if not LEADS_UPLOAD_API_KEY:
        print("LEADS_UPLOAD_API_KEY not set - skipping upload")
        return False

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{VERCEL_API_URL}/api/upload-leads",
                headers={
                    "Content-Type": "application/json",
                    "X-API-Key": LEADS_UPLOAD_API_KEY
                },
                json={
                    "location": location,
                    "leads": leads
                }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"Uploaded {len(leads)} leads to Vercel")
                    return True
                else:
                    error = await response.text()
                    print(f"Upload failed: {error}")
                    return False
    except Exception as e:
        print(f"Upload error: {str(e)}")
        return False


async def process_lead(
    gemini_enricher: GeminiEnricher,
    icebreaker_gen: IcebreakerGenerator,
    session: aiohttp.ClientSession,
    company_name: str,
    job_role: str,
    indeed_link: str,
    lead_num: int
) -> Optional[Dict]:
    """Process a single lead through the full pipeline"""
    print(f"\n{'='*60}")
    print(f"Lead {lead_num}: {company_name}")
    print(f"{'='*60}")
    print(f"  Role: {job_role}")
    print(f"  Indeed Link: {indeed_link[:80]}...")

    # Step 1: Enrich with Gemini (find contact info, website, location, etc.)
    print(f"\n  Step 1: Enriching with Gemini...")
    contact_data = await gemini_enricher.find_contact(company_name)

    contact_name = contact_data["contact_name"]
    contact_title = contact_data["contact_title"]
    email = contact_data["email"]
    website = contact_data["website"]
    company_size = contact_data["company_size"]
    location = contact_data["location"]

    print(f"    Contact: {contact_name} - {contact_title}")
    print(f"    Email: {email}")
    print(f"    Website: {website}")
    print(f"    Location: {location}")
    print(f"    Company Size: {company_size}")

    if not contact_name:
        print(f"    No contact found - skipping lead")
        return None

    # Parse name
    first_name, last_name = parse_name(contact_name)

    # Step 2: Generate icebreaker
    icebreaker = ""
    if website and first_name:
        print(f"\n  Step 2: Generating icebreaker...")
        icebreaker = await icebreaker_gen.process_website(session, website, first_name)
        if icebreaker:
            print(f"    Icebreaker generated!")
        else:
            print(f"    No icebreaker generated")
    else:
        print(f"\n  Step 2: Skipping icebreaker (no website or name)")

    # Create lead object
    lead = {
        "firstName": first_name,
        "lastName": last_name,
        "title": contact_title,
        "companyName": company_name,
        "jobRole": job_role,
        "email": email,
        "website": website,
        "location": location,
        "companySize": company_size,
        "category": categorize_by_size(company_size),
        "jobLink": indeed_link,
        "icebreaker": icebreaker
    }

    return lead


async def main():
    parser = argparse.ArgumentParser(description='Process demo leads from company names and Indeed links')
    parser.add_argument('--input', '-i', default='demo_input.csv', help='Input CSV file')
    parser.add_argument('--output', '-o', default='demo_leads.json', help='Output JSON file')
    parser.add_argument('--skip-upload', action='store_true', help='Skip uploading to Vercel')
    args = parser.parse_args()

    print("=" * 60)
    print("Demo Lead Processor")
    print("=" * 60)

    # Check API keys
    if not os.getenv("GOOGLE_GEMINI_API_KEY"):
        print("Error: GOOGLE_GEMINI_API_KEY not found in .env")
        return

    if not os.getenv("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY not found in .env")
        return

    # Read input CSV
    if not os.path.exists(args.input):
        print(f"Error: {args.input} not found")
        print("\nCreate demo_input.csv with columns: company_name, role, indeed_link")
        return

    df = pd.read_csv(args.input)
    print(f"\nLoaded {len(df)} leads from {args.input}")

    if 'company_name' not in df.columns or 'role' not in df.columns or 'indeed_link' not in df.columns:
        print("Error: CSV must have 'company_name', 'role', and 'indeed_link' columns")
        return

    # Initialize components
    gemini_enricher = GeminiEnricher()
    icebreaker_gen = IcebreakerGenerator()

    leads = []

    # Process leads
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        for idx, row in df.iterrows():
            company_name = str(row['company_name']).strip()
            role = str(row['role']).strip()
            indeed_link = str(row['indeed_link']).strip()

            if not company_name or company_name == 'nan':
                continue

            lead = await process_lead(
                gemini_enricher,
                icebreaker_gen,
                session,
                company_name,
                role,
                indeed_link,
                idx + 1
            )

            if lead:
                leads.append(lead)

            # Small delay between leads to avoid rate limits
            await asyncio.sleep(3)

    # Save locally
    print(f"\n{'='*60}")
    print(f"Results")
    print(f"{'='*60}")
    print(f"Processed {len(leads)} leads successfully")

    with open(args.output, 'w') as f:
        json.dump(leads, f, indent=2)
    print(f"Saved to {args.output}")

    # Upload to Vercel
    if not args.skip_upload:
        print(f"\nUploading to Vercel...")
        success = await upload_to_vercel(leads)
        if success:
            print("Upload complete! Check https://www.ishaangpta.com/ai-tools/lead-gen")
    else:
        print("\nSkipped Vercel upload (--skip-upload flag)")

    # Summary
    print(f"\n{'='*60}")
    print(f"Summary")
    print(f"{'='*60}")
    for lead in leads:
        icebreaker_status = "with icebreaker" if lead.get("icebreaker") else "no icebreaker"
        print(f"  {lead['companyName']}: {lead['firstName']} {lead['lastName']} ({icebreaker_status})")


if __name__ == "__main__":
    asyncio.run(main())
