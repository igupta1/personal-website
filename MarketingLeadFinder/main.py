#!/usr/bin/env python3
"""
SMB Marketing Lead Finder
Finds small and medium businesses actively looking for marketing help in a specified city.

Searches multiple sources:
- Job boards (Indeed, ZipRecruiter, Glassdoor) for marketing job postings
- Google Search for RFPs, rebrands, and companies hiring marketing roles
- News articles about companies launching campaigns or rebranding

Outputs a comprehensive CSV with company info, evidence, and contact details.
"""

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urlparse

import aiohttp
import pandas as pd
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from google import genai
from google.genai import types

# Load environment variables
load_dotenv()

# Initialize Gemini client
gemini_client = genai.Client(api_key=os.getenv("GOOGLE_GEMINI_API_KEY"))

# ============================================================================
# CONFIGURATION
# ============================================================================

# Output file
OUTPUT_CSV = "smb_marketing_leads.csv"

# Scraping settings
TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 5
WAIT_BETWEEN_REQUESTS = 8.0  # seconds - increased for safe testing and avoiding blocks

# Target number of leads
TARGET_LEADS = 3

# Wait time between job searches (to avoid rate limiting)
WAIT_BETWEEN_JOB_SEARCHES = 10.0  # seconds

# Employee size thresholds for SMBs
SMB_MAX_EMPLOYEES = 200

# Marketing-related job titles to search for
MARKETING_JOB_TITLES = [
    "Growth Marketer",
    "Marketing Director",
    "Demand Generation Manager",
    "Head of Growth",
    "Performance Marketing Manager",
    "Paid Media Manager",
]

# Exclude these types of companies (agencies, franchises, large enterprises)
EXCLUDED_COMPANY_PATTERNS = [
    r"marketing\s*(agency|firm|company)",
    r"advertising\s*(agency|firm)",
    r"digital\s*(agency|firm)",
    r"pr\s*(agency|firm)",
    r"media\s*(agency|buying)",
    r"creative\s*(agency|studio)",
    r"(mcdonalds|starbucks|subway|dunkin)",
    r"(netflix|disney|amazon|google|apple|meta|microsoft)",
    r"(walmart|target|costco|home\s*depot)",
    r"franchise",
]

# Sites to skip when extracting company URLs
BLOCKED_DOMAINS = [
    "indeed.com", "linkedin.com", "glassdoor.com", "ziprecruiter.com",
    "facebook.com", "twitter.com", "instagram.com", "youtube.com",
    "google.com", "yelp.com", "yellowpages.com", "bbb.org",
    "craigslist.org", "monster.com", "careerbuilder.com",
    "wikipedia.org", "amazon.com", "ebay.com", "reddit.com",
]

# User agent for requests
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Lead:
    """Represents a potential lead (company looking for marketing help)"""
    company_name: str
    website: str = ""
    city_neighborhood: str = ""
    company_size: str = "unknown but appears SMB"
    evidence: str = ""
    source_links: str = ""
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    contact_linkedin: str = ""
    category: str = ""  # Employee count category
    job_role: str = ""  # The marketing role they're hiring for
    job_link: str = ""  # Direct link to the job posting
    raw_data: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        # Clean up city/neighborhood field to remove newlines
        clean_city = self.city_neighborhood.replace('\n', ' ').replace('\r', ' ').strip() if self.city_neighborhood else ""

        return {
            "Category": self.category,
            "Contact Name": self.contact_name,
            "Contact Title": self.contact_title,
            "Contact Email": self.contact_email,
            "Contact LinkedIn": self.contact_linkedin,
            "Company Name": self.company_name,
            "Company Size": self.company_size,
            "Website": self.website,
            "City / Neighborhood": clean_city,
            "Evidence They Need Marketing Help": self.evidence,
            "Source Link(s)": self.source_links,
            "Job Role": self.job_role,
            "Job Link": self.job_link,
        }


# ============================================================================
# SCRAPER CLASSES
# ============================================================================

class BaseScraper:
    """Base class for all scrapers"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def fetch_url(self, url: str) -> Optional[str]:
        """Fetch URL content with error handling"""
        try:
            if self.session is None:
                return None
            async with self.session.get(
                url, 
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                headers={"User-Agent": USER_AGENT}
            ) as response:
                if response.status == 200:
                    return await response.text()
                return None
        except Exception as e:
            print(f"    ⚠️  Fetch error: {str(e)[:50]}")
            return None


class IndeedScraper(BaseScraper):
    """Scrape Indeed for marketing job postings"""

    def __init__(self):
        super().__init__()
    
    async def search_jobs(self, job_title: str, max_results: int = 10) -> List[Lead]:
        """Search Indeed for job postings without location filtering"""
        leads = []

        # Format search URL - no location parameter for nationwide search
        query = quote_plus(job_title)
        url = f"https://www.indeed.com/jobs?q={query}&fromage=14"  # Last 14 days, any location

        print(f"  📋 Indeed: Searching '{job_title}' (all locations)...")

        try:
            async with async_playwright() as p:
                # Launch browser with additional stealth options
                browser = await p.chromium.launch(
                    headless=False,
                    args=['--disable-blink-features=AutomationControlled']
                )
                context = await browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={'width': 1920, 'height': 1080},
                    locale='en-US',
                    timezone_id='America/New_York'
                )

                # Add stealth scripts to avoid detection
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)

                page = await context.new_page()

                # Navigate with random delay before action
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Longer wait with random component to appear more human
                await page.wait_for_timeout(4000 + (asyncio.get_event_loop().time() % 1000))

                # Check for captcha/block page
                if await page.query_selector('#challenge-form, .captcha, [data-ray-id]'):
                    print("    ⚠️  CAPTCHA DETECTED - Aborting to protect IP")
                    await browser.close()
                    return []

                # Get job cards with error handling
                try:
                    job_cards = await page.query_selector_all('div.job_seen_beacon, div.jobsearch-ResultsList > div')
                except Exception as e:
                    print(f"    ⚠️  Could not find job cards: {str(e)[:50]}")
                    await browser.close()
                    return []

                print(f"    📊 Found {len(job_cards)} job cards to process")

                for i, card in enumerate(job_cards[:max_results]):
                    try:
                        # Extract company name
                        company_elem = await card.query_selector('[data-testid="company-name"], .companyName, .company')
                        company_name = await company_elem.inner_text() if company_elem else None

                        if not company_name:
                            continue

                        # Check if it's an excluded company
                        if self._is_excluded_company(company_name):
                            continue

                        # Extract location (keep it, but don't filter by it)
                        location_elem = await card.query_selector('[data-testid="text-location"], .companyLocation')
                        location_text = await location_elem.inner_text() if location_elem else ""

                        # Extract job link
                        link_elem = await card.query_selector('a[data-jk], a.jcs-JobTitle')
                        job_link = await link_elem.get_attribute('href') if link_elem else ""
                        if job_link and not job_link.startswith('http'):
                            job_link = f"https://www.indeed.com{job_link}"

                        # Create lead
                        lead = Lead(
                            company_name=company_name.strip(),
                            city_neighborhood=location_text.strip() if location_text else "Remote/Unspecified",
                            evidence=f"Indeed posting for {job_title} (last 14 days)",
                            source_links=job_link,
                            job_role=job_title.strip() if job_title else "",
                            job_link=job_link,
                        )
                        leads.append(lead)

                        # Small delay between processing cards
                        if i % 5 == 0:
                            await page.wait_for_timeout(500)

                    except Exception as e:
                        continue

                # Wait before closing to appear more natural
                await page.wait_for_timeout(1000)
                await browser.close()

        except Exception as e:
            print(f"    ⚠️  Indeed error: {str(e)[:100]}")

        print(f"    ✅ Found {len(leads)} leads from Indeed for '{job_title}'")
        return leads
    
    def _is_excluded_company(self, company_name: str) -> bool:
        """Check if company should be excluded"""
        name_lower = company_name.lower()
        for pattern in EXCLUDED_COMPANY_PATTERNS:
            if re.search(pattern, name_lower):
                return True
        return False


class CompanyEnricher:
    """Enrich lead data with company info, size, and contact details"""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def enrich_lead(self, lead: Lead) -> Lead:
        """Enrich a lead with contact information and website"""
        print(f"  📊 Enriching: {lead.company_name}")

        # Find contact person, website, email, LinkedIn, and company size using Gemini with Google Search
        if not lead.contact_name:
            contact_name, contact_title, website, email, linkedin, company_size = await self.find_contact(lead.company_name, lead.website)
            if contact_name:
                lead.contact_name = contact_name
                lead.contact_title = contact_title
                lead.contact_email = email
                lead.contact_linkedin = linkedin
                lead.company_size = company_size
                print(f"    ✅ Found contact: {contact_name} - {contact_title}")
            else:
                print(f"    ⚠️  No contact found")
                # Still store company size even if no contact found
                if company_size and company_size != "unknown":
                    lead.company_size = company_size

            # Update website if Gemini found one and we don't already have one
            if website and not lead.website:
                lead.website = website

        return lead
    
    async def _find_company_website(self, company_name: str) -> str:
        """Find company website via DuckDuckGo (more reliable than Google for automation)"""
        try:
            # Try DuckDuckGo first (less likely to block)
            website = await self._search_duckduckgo(company_name)
            if website:
                return website

            # Fallback to direct domain guessing
            website = await self._guess_domain(company_name)
            if website:
                return website

        except Exception as e:
            print(f"    ⚠️  Website search error: {str(e)[:80]}")

        return ""
    
    async def _search_duckduckgo(self, company_name: str) -> str:
        """Search DuckDuckGo for company website (more automation-friendly)"""
        try:
            await self.initialize_browser()
            page = await self.browser_context.new_page()
            
            # Clean company name for search
            clean_name = re.sub(r'[^\w\s]', '', company_name).strip()
            encoded_query = quote_plus(f'{clean_name} official website')
            url = f"https://duckduckgo.com/?q={encoded_query}"
            
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            await page.wait_for_timeout(2000)
            
            # DuckDuckGo result selectors
            links = await page.query_selector_all('a[data-testid="result-title-a"], article a[href^="http"]')
            
            for link in links[:5]:
                try:
                    href = await link.get_attribute('href')
                    if href and href.startswith('http'):
                        if not any(domain in href.lower() for domain in BLOCKED_DOMAINS):
                            parsed = urlparse(href)
                            website = f"{parsed.scheme}://{parsed.netloc}"
                            await page.close()
                            return website
                except:
                    continue
            
            await page.close()
        except Exception as e:
            pass
        
        return ""
    
    async def _guess_domain(self, company_name: str) -> str:
        """Try to guess the company domain directly"""
        # Clean company name
        clean_name = company_name.lower()
        clean_name = re.sub(r'[^\w\s]', '', clean_name)
        clean_name = re.sub(r'\s+(inc|llc|corp|co|ltd|company|group|holdings)$', '', clean_name)
        clean_name = clean_name.strip().replace(' ', '')
        
        if len(clean_name) < 2:
            return ""
        
        # Try common TLDs
        domains_to_try = [
            f"https://www.{clean_name}.com",
            f"https://{clean_name}.com",
            f"https://www.{clean_name}.co",
        ]
        
        headers = {"User-Agent": USER_AGENT}
        
        for domain in domains_to_try:
            try:
                response = requests.head(domain, headers=headers, timeout=5, allow_redirects=True)
                if response.status_code < 400:
                    return domain
            except:
                continue
        
        return ""

    async def find_contact(self, company_name: str, website: str = "") -> Tuple[str, str, str, str, str, str]:
        """Find a key contact, website, email, LinkedIn, and company size using Gemini with Google Search grounding"""
        try:
            prompt = f"""Find the current key decision-maker, contact information, official website, and company size for:
Company: {company_name}
Current Website: {website}

Target roles (in strict order):
1. CEO / Founder / Owner
2. President
3. CMO / VP of Marketing

INSTRUCTIONS:
- Use Google Search to verify the CURRENT person in this role.
- Also find the official company website (the main homepage URL).
- Try to find the person's work email address (not generic support@).
- Try to find the person's LinkedIn profile URL.
- Estimate the approximate number of employees at this company (e.g., "10-50", "50-100", "100-250", "unknown").
- If you find a generic email (support@) or generic name, return empty strings.
- Return valid JSON only.

Return JSON format:
{{
  "name": "Full Name",
  "title": "Exact Job Title",
  "website": "https://www.company.com",
  "email": "person@company.com",
  "linkedin": "https://www.linkedin.com/in/username",
  "company_size": "10-50 employees"
}}"""

            # Use Gemini with Google Search grounding (ASYNC + correct tool)
            response = await gemini_client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],  # Correct tool for web search
                    temperature=0.0
                )
            )

            # Parse JSON from response (may be wrapped in code blocks)
            response_text = response.text.strip()
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()

            result = json.loads(response_text)
            name = result.get('name', '').strip()
            title = result.get('title', '').strip()
            found_website = result.get('website', '').strip()
            email = result.get('email', '').strip()
            linkedin = result.get('linkedin', '').strip()
            company_size = result.get('company_size', 'unknown').strip()

            # Verify grounding was actually used
            has_grounding = False
            if response.candidates and response.candidates[0].grounding_metadata:
                if response.candidates[0].grounding_metadata.search_entry_point:
                    has_grounding = True

            # If we got a result from Gemini, return it
            if name and len(name) > 2 and len(name) < 50:
                source = "Gemini Search" if has_grounding else "Gemini Knowledge"
                print(f"    🎯 Found via {source}: {name} ({title})")
                if found_website:
                    print(f"    🌐 Found website: {found_website}")
                if email:
                    print(f"    📧 Found email: {email}")
                if linkedin:
                    print(f"    💼 Found LinkedIn: {linkedin}")
                if company_size and company_size != 'unknown':
                    print(f"    🏢 Company size: {company_size}")
                return name, title, found_website, email, linkedin, company_size

        except Exception as e:
            print(f"    ⚠️  Gemini error: {str(e)[:80]}")

        return "", "", "", "", "", "unknown"


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

class MarketingLeadFinder:
    """Main orchestrator for finding SMB marketing leads"""

    def __init__(self):
        self.leads: List[Lead] = []
        self.seen_companies: Set[str] = set()

        # Categorized leads by employee count
        self.leads_small: List[Lead] = []  # <= 100 employees
        self.leads_medium: List[Lead] = []  # 101-250 employees
        self.leads_large: List[Lead] = []  # 251+ employees

        self.indeed_scraper = IndeedScraper()
        self.enricher = CompanyEnricher()
    
    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name for deduplication"""
        return re.sub(r'[^a-z0-9]', '', name.lower())
    
    def _add_lead(self, lead: Lead) -> bool:
        """Add lead if not duplicate, return True if added"""
        normalized = self._normalize_company_name(lead.company_name)
        if normalized in self.seen_companies:
            return False
        self.seen_companies.add(normalized)
        self.leads.append(lead)
        return True
    
    def _get_employee_count(self, lead: Lead) -> Optional[int]:
        """Extract employee count from company size string"""
        size_str = lead.company_size.lower()

        # Skip unknowns
        if "unknown" in size_str:
            return None

        # Parse different company size formats
        # Try to extract numbers
        numbers = re.findall(r'\d+', size_str.replace(',', ''))
        if numbers:
            # If there's a range (e.g., "50-100"), use the upper bound
            if len(numbers) >= 2:
                return int(numbers[1])
            # Otherwise use the single number
            return int(numbers[0])

        return None

    def _categorize_and_output_lead(self, lead: Lead) -> bool:
        """Categorize lead by employee count and output if it has a contact name AND email. Returns True if lead was outputted."""
        # Only output leads with contact names AND emails
        if not lead.contact_name or not lead.contact_email:
            return False

        employee_count = self._get_employee_count(lead)

        # Skip leads without valid employee count
        if employee_count is None:
            return False

        # Categorize and output
        if employee_count <= 100:
            self.leads_small.append(lead)
            category = "≤ 100 Employees"
            lead.category = "≤100"
        elif employee_count <= 250:
            self.leads_medium.append(lead)
            category = "101-250 Employees"
            lead.category = "101-250"
        else:
            self.leads_large.append(lead)
            category = "251+ Employees"
            lead.category = "251+"

        # Output the lead immediately
        print(f"\n{'='*70}")
        print(f"✨ NEW LEAD - {category}")
        print(f"{'='*70}")
        print(f"Company: {lead.company_name}")
        print(f"Contact: {lead.contact_name} - {lead.contact_title}")
        if lead.contact_email:
            print(f"Email: {lead.contact_email}")
        if lead.contact_linkedin:
            print(f"LinkedIn: {lead.contact_linkedin}")
        print(f"Company Size: {lead.company_size}")
        print(f"Website: {lead.website}")
        print(f"Location: {lead.city_neighborhood}")
        print(f"Evidence: {lead.evidence}")
        print(f"{'='*70}\n")

        return True

    def _is_qualifying_lead(self, lead: Lead) -> bool:
        """Check if lead meets qualification criteria: <200 employees AND valid contact"""
        # Must have valid contact name and title
        if not lead.contact_name or not lead.contact_title:
            return False

        # Check company size
        size_str = lead.company_size.lower()

        # Skip unknowns
        if "unknown" in size_str:
            return False

        # Parse different company size formats and check if <200 employees
        # Check for various patterns indicating <200 employees
        if any(x in size_str for x in ["1-10", "10-50", "50-100", "100-150", "150-200"]):
            return True

        if "employees" in size_str:
            # Try to extract numbers
            numbers = re.findall(r'\d+', size_str.replace(',', ''))
            if numbers:
                # Get the first number (lower bound)
                first_num = int(numbers[0])
                if first_num < SMB_MAX_EMPLOYEES:
                    return True

        return False

    async def run(self) -> List[Lead]:
        """Run the full lead finding process"""
        print("=" * 70)
        print(f"🎯 SMB Marketing Lead Finder - Location Independent")
        print(f"🌎 Searching: All locations nationwide")
        print(f"🎯 Goal: Find {TARGET_LEADS} companies in ≤100 and 101-250 employee categories (combined)")
        print("=" * 70)

        # Step 1: Search Indeed for marketing jobs
        print("\n" + "=" * 70)
        print("📋 STEP 1: Searching Indeed for marketing job postings...")
        print("=" * 70)

        job_titles_to_search = [
            "Marketing Manager",
            "Digital Marketing Specialist",
        ]

        # Track qualifying leads count (small + medium only)
        max_iterations = 20  # Safety limit to prevent infinite loops
        iteration = 0

        for job_title in job_titles_to_search:
            target_count = len(self.leads_small) + len(self.leads_medium)
            if target_count >= TARGET_LEADS or iteration >= max_iterations:
                break

            try:
                iteration += 1
                print(f"\n  🔍 Searching for: {job_title}")
                indeed_leads = await self.indeed_scraper.search_jobs(job_title, max_results=15)
                added = 0
                for lead in indeed_leads:
                    if self._add_lead(lead):
                        added += 1
                if added > 0:
                    print(f"    ➕ Added {added} new unique leads (total: {len(self.leads)})")

                # Wait between job title searches to avoid rate limiting
                print(f"    ⏳ Waiting {WAIT_BETWEEN_JOB_SEARCHES}s before next search...")
                await asyncio.sleep(WAIT_BETWEEN_JOB_SEARCHES)

            except Exception as e:
                print(f"  ⚠️  Indeed search error: {str(e)[:50]}")

            # Enrich and categorize leads
            # Process the newly added leads
            unenriched_leads = [l for l in self.leads if not l.contact_name]
            if unenriched_leads:
                print(f"\n  📊 Enriching {len(unenriched_leads)} new leads...")
                for lead in unenriched_leads:
                    try:
                        await self.enricher.enrich_lead(lead)
                        # Categorize and output if lead has contact name
                        self._categorize_and_output_lead(lead)
                    except Exception as e:
                        print(f"    ⚠️  Enrichment error for {lead.company_name}: {str(e)[:50]}")
                    await asyncio.sleep(0.5)  # Increased delay for API rate limiting

                    # Check if we've reached our goal
                    target_count = len(self.leads_small) + len(self.leads_medium)
                    if target_count >= TARGET_LEADS:
                        break

                # Show progress
                target_count = len(self.leads_small) + len(self.leads_medium)
                print(f"\n  🎯 Progress: {target_count}/{TARGET_LEADS} target leads found (≤100: {len(self.leads_small)}, 101-250: {len(self.leads_medium)})")

        target_count = len(self.leads_small) + len(self.leads_medium)
        print(f"\n  📊 Total unique leads collected: {len(self.leads)}")
        print(f"  🎯 Target leads found (≤250 employees with contact): {target_count}")

        # Step 2: Final enrichment if we haven't reached target
        if target_count < TARGET_LEADS:
            print("\n" + "=" * 70)
            print("📊 STEP 2: Final enrichment check...")
            print("=" * 70)

            unenriched_leads = [l for l in self.leads if not l.contact_name]
            if unenriched_leads:
                print(f"  Enriching {len(unenriched_leads)} remaining leads...")
                for i, lead in enumerate(unenriched_leads):
                    print(f"\n[{i+1}/{len(unenriched_leads)}]", end="")
                    try:
                        enriched = await self.enricher.enrich_lead(lead)
                        # Categorize and output if lead has contact name
                        self._categorize_and_output_lead(enriched)
                    except Exception as e:
                        print(f"  ⚠️  Enrichment error: {str(e)[:50]}")
                    await asyncio.sleep(0.5)

                    # Check if we've reached our goal
                    target_count = len(self.leads_small) + len(self.leads_medium)
                    if target_count >= TARGET_LEADS:
                        break
        else:
            print("\n  ✅ Target reached! Skipping final enrichment.")

        # Summary of enrichment
        websites_found = sum(1 for l in self.leads if l.website)
        contacts_found = sum(1 for l in self.leads if l.contact_name)
        print(f"\n  📊 Enrichment summary: {websites_found} websites found, {contacts_found} contacts found")

        # Step 3: Save results
        print("\n" + "=" * 70)
        print("💾 STEP 3: Saving results...")
        print("=" * 70)

        self._save_to_csv()

        # Step 4: Show statistics by category
        print("\n" + "=" * 70)
        print("📈 STATISTICS BY CATEGORY")
        print("=" * 70)

        print(f"\n🏢 COMPANIES WITH ≤ 100 EMPLOYEES: {len(self.leads_small)}")
        print("=" * 70)
        if self.leads_small:
            for i, lead in enumerate(self.leads_small, 1):
                print(f"\n{i}. {lead.company_name}")
                print(f"   Contact: {lead.contact_name} - {lead.contact_title}")
                if lead.contact_email:
                    print(f"   Email: {lead.contact_email}")
                if lead.contact_linkedin:
                    print(f"   LinkedIn: {lead.contact_linkedin}")
                print(f"   Company Size: {lead.company_size}")
                print(f"   Website: {lead.website}")
                print(f"   Location: {lead.city_neighborhood}")

        print(f"\n\n🏢 COMPANIES WITH 101-250 EMPLOYEES: {len(self.leads_medium)}")
        print("=" * 70)
        if self.leads_medium:
            for i, lead in enumerate(self.leads_medium, 1):
                print(f"\n{i}. {lead.company_name}")
                print(f"   Contact: {lead.contact_name} - {lead.contact_title}")
                if lead.contact_email:
                    print(f"   Email: {lead.contact_email}")
                if lead.contact_linkedin:
                    print(f"   LinkedIn: {lead.contact_linkedin}")
                print(f"   Company Size: {lead.company_size}")
                print(f"   Website: {lead.website}")
                print(f"   Location: {lead.city_neighborhood}")

        print(f"\n\n🏢 COMPANIES WITH 251+ EMPLOYEES: {len(self.leads_large)}")
        print("=" * 70)
        if self.leads_large:
            for i, lead in enumerate(self.leads_large, 1):
                print(f"\n{i}. {lead.company_name}")
                print(f"   Contact: {lead.contact_name} - {lead.contact_title}")
                if lead.contact_email:
                    print(f"   Email: {lead.contact_email}")
                if lead.contact_linkedin:
                    print(f"   LinkedIn: {lead.contact_linkedin}")
                print(f"   Company Size: {lead.company_size}")
                print(f"   Website: {lead.website}")
                print(f"   Location: {lead.city_neighborhood}")

        target_count = len(self.leads_small) + len(self.leads_medium)
        print(f"\n✅ Complete!")
        print(f"   Total leads found: {len(self.leads)}")
        print(f"   ≤100 employees: {len(self.leads_small)}")
        print(f"   101-250 employees: {len(self.leads_medium)}")
        print(f"   251+ employees: {len(self.leads_large)}")
        print(f"   Target category total (≤100 + 101-250): {target_count}")
        print(f"📄 Results saved to: {OUTPUT_CSV}")

        return self.leads
    
    def _save_to_csv(self):
        """Save categorized leads to CSV file"""
        # Combine all categorized leads in order: small, medium, large
        categorized_leads = self.leads_small + self.leads_medium + self.leads_large

        if not categorized_leads:
            print("  ⚠️  No categorized leads to save")
            return

        rows = [lead.to_dict() for lead in categorized_leads]
        df = pd.DataFrame(rows)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"  📊 Saved {len(categorized_leads)} categorized leads to {OUTPUT_CSV}")
        print(f"       ≤100: {len(self.leads_small)}, 101-250: {len(self.leads_medium)}, 251+: {len(self.leads_large)}")


# ============================================================================
# WEBSITE UPLOAD
# ============================================================================

async def upload_leads_to_api(leads: List[Lead], api_key: str, api_url: str) -> dict:
    """Upload leads to Vercel Blob via the website API"""
    formatted_leads = []
    for lead in leads:
        # Split contact name into first/last
        name_parts = lead.contact_name.split() if lead.contact_name else []
        first_name = name_parts[0] if name_parts else ""
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Map category to expected format
        category = lead.category.lower() if lead.category else "small"
        if "100" in category and "101" not in category:
            category = "small"
        elif "101" in category or "250" in category:
            category = "medium"
        elif "251" in category or "+" in category:
            category = "large"

        formatted_leads.append({
            "firstName": first_name,
            "lastName": last_name,
            "title": lead.contact_title or "",
            "companyName": lead.company_name or "",
            "email": lead.contact_email or "",
            "website": lead.website or "",
            "location": lead.city_neighborhood or "",
            "companySize": lead.company_size or "",
            "category": category,
            "jobRole": lead.job_role or "",
            "jobLink": lead.job_link or "",
            "icebreaker": ""  # Can be generated separately if needed
        })

    print(f"\n📤 Uploading {len(formatted_leads)} leads to {api_url}...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{api_url}/api/upload-leads",
                json={"location": "demo", "leads": formatted_leads},
                headers={
                    "X-API-Key": api_key,
                    "Content-Type": "application/json"
                },
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"✅ Upload successful!")
                    print(f"   Stats: {result.get('stats', {})}")
                    return result
                else:
                    error_text = await response.text()
                    print(f"❌ Upload failed: HTTP {response.status}")
                    print(f"   Error: {error_text[:200]}")
                    return {"error": error_text, "status": response.status}
    except Exception as e:
        print(f"❌ Upload error: {str(e)}")
        return {"error": str(e)}


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main(upload_to_website: bool = False):
    """Main entry point"""
    finder = MarketingLeadFinder()
    leads = await finder.run()

    # Upload to website if requested
    if upload_to_website:
        api_key = os.getenv("LEADS_UPLOAD_API_KEY")
        api_url = os.getenv("VERCEL_API_URL", "https://www.ishaangpta.com")

        if api_key:
            # Filter to only leads with valid emails
            valid_leads = [l for l in leads if l.contact_email and "@" in l.contact_email]
            if valid_leads:
                await upload_leads_to_api(valid_leads, api_key, api_url)
                print(f"\n📊 Uploaded {len(valid_leads)} leads with valid emails to website")
            else:
                print("\n⚠️  No leads with valid emails to upload")
        else:
            print("\n⚠️  LEADS_UPLOAD_API_KEY not set, skipping upload")

    return leads


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SMB Marketing Lead Finder")
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to website (requires LEADS_UPLOAD_API_KEY env var)"
    )
    args = parser.parse_args()

    print(f"\n🚀 Starting SMB Marketing Lead Finder (Location Independent)\n")
    asyncio.run(main(upload_to_website=args.upload))

