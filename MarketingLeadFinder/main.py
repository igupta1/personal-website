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
import csv
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import aiohttp
import pandas as pd
import requests
from dotenv import load_dotenv
from openai import AsyncOpenAI
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from google import genai
from google.genai import types

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Initialize Gemini client
gemini_client = genai.Client(api_key=os.getenv("GOOGLE_GEMINI_API_KEY"))

# ============================================================================
# CONFIGURATION
# ============================================================================

# City to search (can be parameterized later)
DEFAULT_CITY = "Greater Los Angeles area"

# Output file
OUTPUT_CSV = "smb_marketing_leads.csv"

# Location validation - cities/areas that count as "Greater Los Angeles area"
# NOTE: These are used with WORD BOUNDARY matching to avoid false positives like "Atlanta" matching "la"
LA_AREA_CITIES = [
    # Core LA
    "los angeles",
    # LA neighborhoods and cities
    "hollywood", "west hollywood", "beverly hills",
    "santa monica", "venice", "culver city", "marina del rey",
    "pasadena", "glendale", "burbank", "north hollywood", "studio city",
    "sherman oaks", "encino", "van nuys", "woodland hills", "calabasas",
    "malibu", "brentwood", "westwood", "century city", "koreatown",
    "silver lake", "echo park", "los feliz", "eagle rock", "highland park",
    "atwater village", "boyle heights", "watts",
    "compton", "inglewood", "hawthorne", "el segundo", "manhattan beach",
    "hermosa beach", "redondo beach", "torrance", "carson", "long beach",
    "lakewood", "downey", "whittier", "cerritos", "norwalk",
    "la mirada", "fullerton", "anaheim", "irvine", "costa mesa",
    "newport beach", "huntington beach", "orange", "tustin", "santa ana",
    # Valleys and outlying areas
    "pomona", "covina", "west covina", "diamond bar", "claremont", 
    "azusa", "monrovia", "arcadia", "alhambra", "monterey park", 
    "el monte", "baldwin park", "san dimas", "glendora", "duarte",
    "rancho cucamonga", "ontario", "upland", "fontana",
    # Ventura County (close to LA)
    "ventura", "oxnard", "thousand oaks", "simi valley", "camarillo",
]

# LA area zip code prefixes (900xx, 901xx, 902xx, 903xx, 904xx, 905xx, 906xx, 907xx, 908xx, 909xx, 910xx-919xx, 90xxx, 91xxx, 92xxx for OC)
LA_ZIP_PREFIXES = ["900", "901", "902", "903", "904", "905", "906", "907", "908", "909", 
                   "910", "911", "912", "913", "914", "915", "916", "917", "918", "919",
                   "920", "921", "922", "923", "924", "925", "926", "927", "928"]

# Scraping settings
TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 5
WAIT_BETWEEN_REQUESTS = 1.0  # seconds

# Target number of leads
TARGET_LEADS = 5

# Employee size thresholds for SMBs
SMB_MAX_EMPLOYEES = 100

# Marketing-related job titles to search for
MARKETING_JOB_TITLES = [
    "Marketing Manager",
    "Digital Marketing Specialist",
    "Social Media Manager",
    "Content Marketer",
    "Growth Marketer",
    "Marketing Coordinator",
    "Brand Manager",
    "Marketing Director",
    "SEO Specialist",
    "PPC Specialist",
    "Email Marketing Manager",
    "Marketing Analyst",
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
    raw_data: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        # Clean up city/neighborhood field to remove newlines
        clean_city = self.city_neighborhood.replace('\n', ' ').replace('\r', ' ').strip() if self.city_neighborhood else ""

        return {
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
    
    def _is_in_target_area(self, location: str, target_city: str) -> bool:
        """Check if the location is within the target city/area using word boundary matching"""
        if not location:
            return False
        
        location_lower = location.lower().strip()
        
        # For Greater Los Angeles area, use strict matching
        if "los angeles" in target_city.lower():
            # Check for LA area cities using word boundary matching
            for city in LA_AREA_CITIES:
                # Use word boundary regex to avoid "la" matching "atlanta"
                pattern = r'\b' + re.escape(city) + r'\b'
                if re.search(pattern, location_lower):
                    return True
            
            # Check for LA area zip codes
            zip_match = re.search(r'\b(\d{5})\b', location)
            if zip_match:
                zip_code = zip_match.group(1)
                if any(zip_code.startswith(prefix) for prefix in LA_ZIP_PREFIXES):
                    return True
            
            # Check for ", CA" or ", California" at the end (but only after city check failed)
            # This catches LA area cities we might have missed
            if re.search(r',\s*ca\b', location_lower) or 'california' in location_lower:
                # Additional check: make sure it's not in other CA cities we don't want
                non_la_ca_cities = ['san francisco', 'san jose', 'oakland', 'sacramento', 
                                     'san diego', 'fresno', 'bakersfield', 'palo alto',
                                     'mountain view', 'sunnyvale', 'cupertino', 'berkeley',
                                     'walnut creek', 'concord', 'fremont', 'hayward']
                for non_la_city in non_la_ca_cities:
                    if non_la_city in location_lower:
                        return False
                # If it's CA and not a known non-LA city, include it
                return True
            
            return False
        
        # For other cities, use word boundary matching
        pattern = r'\b' + re.escape(target_city.lower()) + r'\b'
        return bool(re.search(pattern, location_lower))
    
    async def search_jobs(self, city: str, job_title: str) -> List[Lead]:
        """Search Indeed for job postings"""
        leads = []
        
        # Format search URL
        query = quote_plus(job_title)
        location = quote_plus(city)
        url = f"https://www.indeed.com/jobs?q={query}&l={location}&fromage=14"  # Last 14 days
        
        print(f"  📋 Indeed: Searching '{job_title}' in {city}...")
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=USER_AGENT)
                page = await context.new_page()
                
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Get job cards
                job_cards = await page.query_selector_all('div.job_seen_beacon, div.jobsearch-ResultsList > div')
                
                for card in job_cards[:15]:  # Limit to first 15 per search
                    try:
                        # Extract company name
                        company_elem = await card.query_selector('[data-testid="company-name"], .companyName, .company')
                        company_name = await company_elem.inner_text() if company_elem else None
                        
                        if not company_name:
                            continue
                        
                        # Check if it's an excluded company
                        if self._is_excluded_company(company_name):
                            continue
                        
                        # Extract location
                        location_elem = await card.query_selector('[data-testid="text-location"], .companyLocation')
                        location_text = await location_elem.inner_text() if location_elem else ""
                        
                        # Filter by location - only include if in target area
                        if not self._is_in_target_area(location_text, city):
                            continue
                        
                        # Extract job link
                        link_elem = await card.query_selector('a[data-jk], a.jcs-JobTitle')
                        job_link = await link_elem.get_attribute('href') if link_elem else ""
                        if job_link and not job_link.startswith('http'):
                            job_link = f"https://www.indeed.com{job_link}"
                        
                        # Create lead
                        lead = Lead(
                            company_name=company_name.strip(),
                            city_neighborhood=location_text.strip() if location_text else city,
                            evidence=f"Indeed posting for {job_title} (last 14 days)",
                            source_links=job_link,
                        )
                        leads.append(lead)
                        
                    except Exception as e:
                        continue
                
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


class GoogleSearchScraper(BaseScraper):
    """Scrape Google Search for various marketing-related signals"""
    
    def __init__(self):
        super().__init__()
        self.browser = None
        self.browser_context = None
    
    async def initialize_browser(self):
        """Initialize a reusable browser instance"""
        if self.browser is None:
            p = await async_playwright().start()
            self.browser = await p.chromium.launch(headless=True)
            self.browser_context = await self.browser.new_context(user_agent=USER_AGENT)
    
    async def close_browser(self):
        """Close the browser when done"""
        if self.browser:
            await self.browser.close()
            self.browser = None
            self.browser_context = None
    
    async def search(self, query: str, num_results: int = 20) -> List[Dict]:
        """Perform a search using DuckDuckGo (more reliable for automation)"""
        results = []
        
        print(f"  🔍 Searching: '{query[:55]}...'")
        
        try:
            await self.initialize_browser()
            page = await self.browser_context.new_page()
            
            # Use DuckDuckGo instead of Google (more automation-friendly)
            formatted_query = quote_plus(query)
            url = f"https://duckduckgo.com/?q={formatted_query}"
            
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2500)
            
            # Scroll to load more results
            for _ in range(2):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(800)
            
            # DuckDuckGo result selectors
            result_elements = await page.query_selector_all('article[data-testid="result"]')
            
            for elem in result_elements:
                try:
                    # Get title
                    title_elem = await elem.query_selector('h2, a[data-testid="result-title-a"]')
                    title = await title_elem.inner_text() if title_elem else ""
                    
                    # Get URL
                    link_elem = await elem.query_selector('a[data-testid="result-title-a"], a[href^="http"]')
                    link = await link_elem.get_attribute('href') if link_elem else ""
                    
                    # Get snippet
                    snippet_elem = await elem.query_selector('div[data-result="snippet"], span')
                    snippet = await snippet_elem.inner_text() if snippet_elem else ""
                    
                    if title and link:
                        results.append({
                            "title": title,
                            "url": link,
                            "snippet": snippet,
                        })
                except Exception as e:
                    continue
            
            # If DuckDuckGo didn't work, try Bing as fallback
            if len(results) == 0:
                await page.goto(f"https://www.bing.com/search?q={formatted_query}", wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)
                
                bing_results = await page.query_selector_all('li.b_algo')
                for elem in bing_results:
                    try:
                        title_elem = await elem.query_selector('h2 a')
                        title = await title_elem.inner_text() if title_elem else ""
                        link = await title_elem.get_attribute('href') if title_elem else ""
                        snippet_elem = await elem.query_selector('p')
                        snippet = await snippet_elem.inner_text() if snippet_elem else ""
                        
                        if title and link:
                            results.append({
                                "title": title,
                                "url": link,
                                "snippet": snippet,
                            })
                    except:
                        continue
            
            await page.close()
            print(f"    📊 Found {len(results)} search results")
                
        except Exception as e:
            print(f"    ⚠️  Search error: {str(e)[:100]}")
        
        return results
    
    async def search_job_postings(self, city: str) -> List[Lead]:
        """Search for marketing job postings via web search"""
        leads = []
        
        # Use city-specific queries for LA
        city_short = "Los Angeles" if "los angeles" in city.lower() else city
        
        queries = [
            f'hiring marketing manager {city_short}',
            f'marketing job opening {city_short}',
            f'digital marketing position {city_short}',
            f'marketing coordinator careers {city_short}',
        ]
        
        print(f"\n  🔍 Searching for job postings ({len(queries)} queries)...")
        
        for query in queries:
            results = await self.search(query)
            
            added_from_query = 0
            for result in results:
                # Skip blocked domains
                if any(domain in result['url'].lower() for domain in BLOCKED_DOMAINS):
                    continue
                
                # Extract company name from title
                company_name = self._extract_company_from_title(result['title'])
                if not company_name:
                    # Try from URL
                    company_name = self._extract_company_from_url(result['url'])
                
                if not company_name:
                    continue
                
                # Check if excluded
                if self._is_excluded_company(company_name):
                    continue
                
                lead = Lead(
                    company_name=company_name,
                    website=self._get_base_url(result['url']),
                    city_neighborhood=city,
                    evidence=f"Google: Job posting - {result['title'][:80]}",
                    source_links=result['url'],
                )
                leads.append(lead)
                added_from_query += 1
            
            if added_from_query > 0:
                print(f"    ✅ Added {added_from_query} leads from query")
            
            await asyncio.sleep(WAIT_BETWEEN_REQUESTS)
        
        print(f"  📊 Total leads from Google job search: {len(leads)}")
        return leads
    
    async def search_rfps(self, city: str) -> List[Lead]:
        """Search for RFPs and RFQs for marketing services"""
        leads = []
        
        city_short = "Los Angeles" if "los angeles" in city.lower() else city
        
        queries = [
            f'RFP marketing services {city_short}',
            f'marketing RFQ {city_short}',
            f'seeking marketing agency {city_short}',
        ]
        
        print(f"\n  🔍 Searching for RFPs ({len(queries)} queries)...")
        
        for query in queries:
            results = await self.search(query)
            
            added_from_query = 0
            for result in results:
                if any(domain in result['url'].lower() for domain in BLOCKED_DOMAINS):
                    continue
                
                company_name = self._extract_company_from_title(result['title'])
                if not company_name:
                    company_name = self._extract_company_from_url(result['url'])
                
                if not company_name or self._is_excluded_company(company_name):
                    continue
                
                snippet_text = result['snippet'][:120] if result['snippet'] else result['title'][:80]
                lead = Lead(
                    company_name=company_name,
                    website=self._get_base_url(result['url']),
                    city_neighborhood=city,
                    evidence=f"Google: RFP/RFQ - {snippet_text}",
                    source_links=result['url'],
                )
                leads.append(lead)
                added_from_query += 1
            
            if added_from_query > 0:
                print(f"    ✅ Added {added_from_query} leads from RFP query")
            
            await asyncio.sleep(WAIT_BETWEEN_REQUESTS)
        
        print(f"  📊 Total leads from Google RFP search: {len(leads)}")
        return leads
    
    async def search_rebrands_campaigns(self, city: str) -> List[Lead]:
        """Search for companies announcing rebrands or major campaigns"""
        leads = []
        
        city_short = "Los Angeles" if "los angeles" in city.lower() else city
        
        queries = [
            f'company rebrand {city_short} 2024',
            f'new marketing campaign launch {city_short}',
            f'brand refresh {city_short}',
        ]
        
        print(f"\n  🔍 Searching for rebrands/campaigns ({len(queries)} queries)...")
        
        for query in queries:
            results = await self.search(query)
            
            added_from_query = 0
            for result in results:
                if any(domain in result['url'].lower() for domain in BLOCKED_DOMAINS):
                    continue
                
                company_name = self._extract_company_from_title(result['title'])
                if not company_name:
                    company_name = self._extract_company_from_url(result['url'])
                
                if not company_name or self._is_excluded_company(company_name):
                    continue
                
                snippet_text = result['snippet'][:120] if result['snippet'] else result['title'][:80]
                lead = Lead(
                    company_name=company_name,
                    website=self._get_base_url(result['url']),
                    city_neighborhood=city,
                    evidence=f"Google: Rebrand/Campaign - {snippet_text}",
                    source_links=result['url'],
                )
                leads.append(lead)
                added_from_query += 1
            
            if added_from_query > 0:
                print(f"    ✅ Added {added_from_query} leads from rebrand query")
            
            await asyncio.sleep(WAIT_BETWEEN_REQUESTS)
        
        print(f"  📊 Total leads from Google rebrand search: {len(leads)}")
        return leads
    
    def _extract_company_from_title(self, title: str) -> Optional[str]:
        """Extract company name from search result title"""
        # Common patterns: "Company Name | Job Title" or "Job Title at Company Name"
        patterns = [
            r'^([^|]+)\s*\|',  # Before pipe
            r'at\s+([A-Z][^-]+)',  # "at Company Name"
            r'^([A-Z][A-Za-z0-9\s&]+)(?:\s*-|\s*\|)',  # Start with caps before dash/pipe
            r'([A-Z][A-Za-z0-9\s&]{2,30})\s+(?:is\s+)?(?:hiring|seeking|looking)',  # Company is hiring
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                name = match.group(1).strip()
                # Clean up
                name = re.sub(r'\s*[-|].*$', '', name)
                name = name.strip()
                if len(name) > 2 and len(name) < 50:
                    return name
        
        return None
    
    def _extract_company_from_url(self, url: str) -> Optional[str]:
        """Extract company name from URL domain"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            # Remove TLD
            name = domain.split('.')[0]
            # Convert to title case
            name = name.replace('-', ' ').replace('_', ' ').title()
            if len(name) > 2:
                return name
        except:
            pass
        return None
    
    def _get_base_url(self, url: str) -> str:
        """Extract base URL from full URL"""
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except:
            return url
    
    def _is_excluded_company(self, company_name: str) -> bool:
        """Check if company should be excluded"""
        if not company_name:
            return True
        name_lower = company_name.lower()
        for pattern in EXCLUDED_COMPANY_PATTERNS:
            if re.search(pattern, name_lower):
                return True
        return False


class CompanyEnricher:
    """Enrich lead data with company info, size, and contact details"""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def enrich_lead(self, lead: Lead, city: str) -> Lead:
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
    
    async def _find_company_website(self, company_name: str, city: str) -> str:
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
    
    def __init__(self, city: str = DEFAULT_CITY):
        self.city = city
        self.leads: List[Lead] = []
        self.seen_companies: Set[str] = set()

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
    
    async def run(self) -> List[Lead]:
        """Run the full lead finding process"""
        print("=" * 70)
        print(f"🎯 SMB Marketing Lead Finder")
        print(f"📍 City: {self.city}")
        print("=" * 70)
        
        # Check OpenAI API key
        if not os.getenv("OPENAI_API_KEY"):
            print("⚠️  Warning: OPENAI_API_KEY not set. Some features will be limited.")
        else:
            print("✅ OpenAI API key found")
        
        # Step 1: Search Indeed for marketing jobs
        print("\n" + "=" * 70)
        print("📋 STEP 1: Searching Indeed for marketing job postings...")
        print("=" * 70)
        
        job_titles_to_search = [
            "Marketing Manager",
            "Social Media Manager", 
            "Digital Marketing Specialist",
            "Content Marketer",
            "Growth Marketer",
            "Brand Manager",
            "Marketing Coordinator",
        ]
        
        # For LA area, search with specific CA locations
        search_locations = ["Los Angeles, CA"]
        if "los angeles" in self.city.lower():
            search_locations.extend(["Santa Monica, CA", "Burbank, CA", "Pasadena, CA", "Irvine, CA"])
        
        for location in search_locations:
            print(f"\n  📍 Searching in: {location}")
            for job_title in job_titles_to_search:
                try:
                    indeed_leads = await self.indeed_scraper.search_jobs(location, job_title)
                    added = 0
                    for lead in indeed_leads:
                        if self._add_lead(lead):
                            added += 1
                    if added > 0:
                        print(f"    ➕ Added {added} new unique leads")
                    await asyncio.sleep(WAIT_BETWEEN_REQUESTS * 2)
                except Exception as e:
                    print(f"  ⚠️  Indeed search error: {str(e)[:50]}")
                
                # Stop if we have enough leads from Indeed
                if len(self.leads) >= TARGET_LEADS:
                    break
            if len(self.leads) >= TARGET_LEADS:
                break
        
        print(f"\n  📊 Total unique leads after Indeed: {len(self.leads)}")

        # Step 2: Enrich leads with company info and contacts
        print("\n" + "=" * 70)
        print("📊 STEP 2: Enriching leads with company info and contacts...")
        print("=" * 70)
        
        enriched_leads = []
        leads_to_process = self.leads[:TARGET_LEADS]

        for i, lead in enumerate(leads_to_process):
            print(f"\n[{i+1}/{len(leads_to_process)}]", end="")
            try:
                enriched = await self.enricher.enrich_lead(lead, self.city)
                enriched_leads.append(enriched)
            except Exception as e:
                print(f"  ⚠️  Enrichment error: {str(e)[:50]}")
                enriched_leads.append(lead)

            await asyncio.sleep(0.3)

        self.leads = enriched_leads
        
        # Summary of enrichment
        websites_found = sum(1 for l in enriched_leads if l.website)
        contacts_found = sum(1 for l in enriched_leads if l.contact_name)
        print(f"\n  📊 Enrichment summary: {websites_found} websites found, {contacts_found} contacts found")

        # Save all enriched leads (no filtering)
        self.leads = enriched_leads

        # Step 3: Save results
        print("\n" + "=" * 70)
        print("💾 STEP 3: Saving results...")
        print("=" * 70)
        
        self._save_to_csv()

        # Step 4: Show statistics
        print("\n" + "=" * 70)
        print("📈 STATISTICS")
        print("=" * 70)

        total = len(self.leads)
        if total > 0:
            # Calculate fill rates for each column
            contact_names = sum(1 for l in self.leads if l.contact_name)
            contact_titles = sum(1 for l in self.leads if l.contact_title)
            contact_emails = sum(1 for l in self.leads if l.contact_email)
            contact_linkedins = sum(1 for l in self.leads if l.contact_linkedin)
            websites = sum(1 for l in self.leads if l.website)
            company_sizes = sum(1 for l in self.leads if l.company_size and l.company_size != "unknown" and l.company_size != "unknown but appears SMB")

            print(f"\n📊 Column Fill Rates (out of {total} total leads):")
            print(f"  • Contact Names:    {contact_names}/{total} ({contact_names/total*100:.1f}%)")
            print(f"  • Contact Titles:   {contact_titles}/{total} ({contact_titles/total*100:.1f}%)")
            print(f"  • Contact Emails:   {contact_emails}/{total} ({contact_emails/total*100:.1f}%)")
            print(f"  • Contact LinkedIn: {contact_linkedins}/{total} ({contact_linkedins/total*100:.1f}%)")
            print(f"  • Websites:         {websites}/{total} ({websites/total*100:.1f}%)")
            print(f"  • Company Sizes:    {company_sizes}/{total} ({company_sizes/total*100:.1f}%)")

            # Filter for businesses with <100 employees AND valid contact name and title
            smb_with_contacts = []
            for lead in self.leads:
                # Check if has valid contact
                if not lead.contact_name or not lead.contact_title:
                    continue

                # Check company size
                size_str = lead.company_size.lower()
                is_small = False

                # Parse different company size formats
                if "unknown" in size_str:
                    continue  # Skip unknowns

                # Check for various patterns indicating <100 employees
                if any(x in size_str for x in ["1-10", "10-50", "50-100"]):
                    is_small = True
                elif "employees" in size_str:
                    # Try to extract numbers
                    import re
                    numbers = re.findall(r'\d+', size_str.replace(',', ''))
                    if numbers:
                        # Get the first number (lower bound)
                        first_num = int(numbers[0])
                        if first_num < 100:
                            is_small = True

                if is_small:
                    smb_with_contacts.append(lead)

            print(f"\n🎯 Businesses with <100 employees AND valid contact: {len(smb_with_contacts)}/{total} ({len(smb_with_contacts)/total*100:.1f}%)")

            if smb_with_contacts:
                print("\n📋 List of qualifying businesses:")
                print("-" * 70)
                for i, lead in enumerate(smb_with_contacts, 1):
                    print(f"\n{i}. {lead.company_name}")
                    print(f"   Contact: {lead.contact_name} - {lead.contact_title}")
                    if lead.contact_email:
                        print(f"   Email: {lead.contact_email}")
                    if lead.contact_linkedin:
                        print(f"   LinkedIn: {lead.contact_linkedin}")
                    print(f"   Company Size: {lead.company_size}")
                    print(f"   Website: {lead.website}")
                    print(f"   Location: {lead.city_neighborhood}")

        print(f"\n✅ Complete! Found {len(self.leads)} qualified leads.")
        print(f"📄 Results saved to: {OUTPUT_CSV}")

        return self.leads
    
    def _save_to_csv(self):
        """Save leads to CSV file"""
        if not self.leads:
            print("  ⚠️  No leads to save")
            return
        
        rows = [lead.to_dict() for lead in self.leads]
        df = pd.DataFrame(rows)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"  📊 Saved {len(self.leads)} leads to {OUTPUT_CSV}")


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main(city: str = DEFAULT_CITY):
    """Main entry point"""
    finder = MarketingLeadFinder(city)
    leads = await finder.run()
    return leads


if __name__ == "__main__":
    import sys
    
    # Allow city to be passed as command line argument
    city = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CITY
    
    print(f"\n🚀 Starting SMB Marketing Lead Finder for: {city}\n")
    asyncio.run(main(city))

