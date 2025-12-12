#!/usr/bin/env python3
"""
Google Maps + Google Search Lead Scraper
Scrapes both Google Maps AND Google Search for business URLs,
then scrapes those sites for email addresses.

Perfect for finding agencies/service providers that may not have strong local presence.
"""

import re
import time
import requests
from typing import List, Set, Dict, Tuple
from urllib.parse import urljoin, urlparse, quote_plus
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# Configuration
# Full list of queries (uncomment to use all)
SEARCH_QUERIES_FULL = [
    # Direct dental marketing searches
    "dental marketing agency",
    "dental practice marketing company",
    "dentist marketing agency",
    "dental SEO agency",
    "dental practice growth consultant",
    
    # City-specific (agencies cluster in major metros)
    "dental marketing agency los angeles",
    "dental marketing agency new york",
    "dental marketing agency chicago",
    "dental marketing agency dallas",
    "dental marketing agency miami",
    "dental marketing agency houston",
    "dental marketing agency phoenix",
    "dental marketing agency atlanta",
    
    # Related services
    "dental website design agency",
    "dental lead generation company",
    "dental patient acquisition",
    "dental social media marketing agency",
    "dentist advertising company",
]

# Use full list for comprehensive results
SEARCH_QUERIES = SEARCH_QUERIES_FULL

MAX_URLS_PER_QUERY = 30  # Per source (Maps + Search)
WAIT_BETWEEN_REQUESTS = 0.5  # Wait time in seconds
SCRAPE_CONTACT_PAGES = True  # Also scrape /contact, /about pages
MAX_EMAILS_PER_DOMAIN = 2  # Keep top 2 emails per company
PREFER_PERSONAL_EMAILS = True  # Prioritize personal emails

# Enable/disable sources
USE_GOOGLE_MAPS = True
USE_GOOGLE_SEARCH = True

# Generic email prefixes to deprioritize
GENERIC_EMAIL_PREFIXES = [
    "info", "contact", "hello", "hi", "support", "help", "sales", "admin",
    "office", "team", "mail", "email", "enquiry", "enquiries", "inquiry",
    "service", "services", "billing", "accounts", "general", "webmaster",
    "postmaster", "noreply", "no-reply", "donotreply", "feedback",
    "marketing", "pr", "press", "media", "news", "jobs", "careers", "hr",
    "legal", "privacy", "abuse", "spam", "security", "customerservice",
    "customer-service", "customersupport", "customer-support"
]

# Sites to filter out
BLOCKED_SITES = [
    # Aggregators
    "zocdoc", "yelp", "facebook", "instagram", "twitter", "linkedin",
    "yellowpages", "bbb", "mapquest", "superpages", "citysearch",
    "foursquare", "tripadvisor", "angieslist", "thumbtack", "homeadvisor",
    "nextdoor", "patch", "manta", "dexknows", "chamberofcommerce",
    # Platforms
    "wix.com", "godaddy", "squarespace", "weebly", "wordpress.com",
    "hubspot.com", "mailchimp.com", "constantcontact.com",
    # Google properties
    "google", "gstatic", "youtube", "blogger",
    # Other
    "schema", "gg", "wikipedia", "amazon", "ebay", "pinterest",
    "reddit", "quora", "medium.com", "forbes.com", "inc.com",
    "clutch.co", "upcity.com", "expertise.com", "bark.com",
]


def scrape_google_maps(query: str) -> List[str]:
    """
    Scrape Google Maps for business URLs
    """
    formatted_query = query.replace(" ", "+")
    url = f"https://www.google.com/maps/search/{formatted_query}"
    
    print(f"  [Google Maps] Fetching: {url}")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)
            
            try:
                results_selector = 'div[role="feed"]'
                page.wait_for_selector(results_selector, timeout=15000)
                
                # Scroll to load more results
                for i in range(10):
                    page.evaluate('''
                        const feed = document.querySelector('div[role="feed"]');
                        if (feed) feed.scrollTop += 1500;
                    ''')
                    page.wait_for_timeout(1000)
                    
            except PlaywrightTimeout:
                print("    Warning: Results feed not found")
            
            html = page.content()
            browser.close()
            
            # Extract URLs
            regex = r'https?://[^/\s"\'>]+'
            urls = re.findall(regex, html)
            
            # Filter URLs
            filtered = filter_urls(urls)
            print(f"    Found {len(filtered)} business URLs from Maps")
            return filtered
            
    except Exception as e:
        print(f"    Error: {e}")
        return []


def scrape_google_search(query: str) -> List[str]:
    """
    Scrape Google Search results for business URLs
    Great for finding agencies that don't have strong Maps presence
    """
    formatted_query = quote_plus(query)
    url = f"https://www.google.com/search?q={formatted_query}&num=30"
    
    print(f"  [Google Search] Fetching results...")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)
            
            # Scroll to load more results
            for i in range(3):
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                page.wait_for_timeout(1000)
            
            html = page.content()
            
            # Extract URLs using multiple patterns
            urls = []
            
            # Pattern 1: URLs in cite elements (Google shows the URL there)
            cite_pattern = r'<cite[^>]*>([^<]+)</cite>'
            cite_matches = re.findall(cite_pattern, html)
            for match in cite_matches:
                # Clean up the URL
                clean_url = match.replace(' › ', '/').replace('›', '/').strip()
                if not clean_url.startswith('http'):
                    clean_url = 'https://' + clean_url
                urls.append(clean_url)
            
            # Pattern 2: Direct href links to external sites
            href_pattern = r'href="(https?://(?!google|gstatic|youtube|schema)[^"]+)"'
            href_urls = re.findall(href_pattern, html)
            urls.extend(href_urls)
            
            # Pattern 3: data-url attributes
            data_pattern = r'data-url="(https?://[^"]+)"'
            data_urls = re.findall(data_pattern, html)
            urls.extend(data_urls)
            
            browser.close()
            
            # Extract base domains
            base_urls = []
            for u in urls:
                try:
                    # Clean up URL
                    u = u.split('?')[0].split('#')[0]  # Remove query params
                    parsed = urlparse(u)
                    if parsed.netloc:
                        base_url = f"{parsed.scheme}://{parsed.netloc}"
                        base_urls.append(base_url)
                except:
                    continue
            
            # Filter URLs
            filtered = filter_urls(base_urls)
            print(f"    Found {len(filtered)} business URLs from Search")
            return filtered
            
    except Exception as e:
        print(f"    Error: {e}")
        return []


def filter_urls(urls: List[str]) -> List[str]:
    """
    Filter out unwanted URLs (Google, aggregators, etc.)
    """
    filtered = []
    seen = set()
    
    for url in urls:
        url_lower = url.lower().rstrip('/')
        
        # Skip if already seen
        if url_lower in seen:
            continue
        seen.add(url_lower)
        
        # Skip blocked sites
        if any(blocked in url_lower for blocked in BLOCKED_SITES):
            continue
        
        # Must be a valid domain
        if not url_lower.startswith('http'):
            continue
            
        filtered.append(url)
    
    return filtered


def remove_duplicates(items: List[str]) -> List[str]:
    """Remove duplicates while preserving order"""
    seen: Set[str] = set()
    unique = []
    for item in items:
        normalized = item.rstrip('/').lower()
        # Extract domain for comparison
        try:
            parsed = urlparse(normalized)
            domain = parsed.netloc.replace('www.', '')
        except:
            domain = normalized
            
        if domain not in seen:
            seen.add(domain)
            unique.append(item)
    return unique


def scrape_site(url: str, follow_redirects: bool = True) -> str:
    """Scrape individual website"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15, allow_redirects=follow_redirects)
        return response.text
    except Exception as e:
        return ""


def scrape_site_with_contact_pages(base_url: str) -> str:
    """Scrape homepage + contact pages"""
    print(f"[Scrape] {base_url}")
    
    all_content = []
    
    homepage_html = scrape_site(base_url)
    if homepage_html:
        all_content.append(homepage_html)
    
    if SCRAPE_CONTACT_PAGES:
        contact_paths = ["/contact", "/contact-us", "/about", "/about-us", "/team", "/our-team"]
        
        for path in contact_paths:
            try:
                contact_url = urljoin(base_url + "/", path.lstrip("/"))
                contact_html = scrape_site(contact_url)
                if contact_html and len(contact_html) > 500:
                    all_content.append(contact_html)
                    break
            except:
                continue
    
    return "\n".join(all_content)


def is_personal_email(email: str) -> bool:
    """Check if email looks personal (not generic)"""
    prefix = email.split('@')[0].lower()
    
    if prefix in GENERIC_EMAIL_PREFIXES:
        return False
    
    if '.' in prefix and len(prefix) > 3:
        return True
    
    if len(prefix) >= 3 and len(prefix) <= 15 and prefix.isalpha():
        return True
    
    return False


def score_email(email: str) -> int:
    """Score email quality. Higher = better lead."""
    score = 0
    prefix = email.split('@')[0].lower()
    
    if prefix in GENERIC_EMAIL_PREFIXES:
        score -= 100
    
    if '.' in prefix and len(prefix) > 5:
        parts = prefix.split('.')
        if len(parts) == 2 and all(len(p) >= 2 for p in parts):
            score += 50
    
    if prefix.isalpha() and 3 <= len(prefix) <= 12:
        score += 30
    
    if len(prefix) < 3:
        score -= 20
    if len(prefix) > 25:
        score -= 10
    
    return score


def get_email_domain(email: str) -> str:
    """Extract domain from email"""
    return email.split('@')[1].lower()


def extract_emails(html_content: str) -> List[str]:
    """Extract and validate emails from HTML"""
    regex = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6}'
    emails = re.findall(regex, html_content)
    
    filtered_emails = []
    
    # Domains that are definitely fake/placeholder
    fake_domains = [
        "example.com", "example.org", "example.net",
        "domain.com", "email.com", "yoursite.com", "mysite.com",
        "test.com", "sample.com", "placeholder.com", "fake.com",
        "mail.com", "business.com", "company.com", "website.com",
        "sentry.io", "wixpress.com", "w3.org", "schema.org", 
        "googleapis.com", "gstatic.com", "cloudflare.com",
        "wpengine.com", "wpenginepowered.com",
    ]
    
    # Email prefixes that are placeholders
    fake_prefixes = [
        "example", "your", "you", "user", "name", "email", "test",
        "sample", "demo", "placeholder", "fake", "myemail", "my-email",
        "youremail", "your-email", "mailto", "webon", "someone",
    ]
    
    # File extensions that look like emails
    file_extensions = [".png", ".jpg", ".gif", ".svg", ".css", ".js", ".woff", ".ico"]
    
    valid_tlds = {
        "com", "net", "org", "edu", "gov", "io", "co", "us", "uk", "ca",
        "agency", "marketing", "digital", "media", "consulting",
        "biz", "info", "me", "tv", "cc", "la", "nyc", "health", "dental"
    }
    
    for email in emails:
        email_lower = email.lower()
        
        # Skip file extensions
        if any(ext in email_lower for ext in file_extensions):
            continue
        
        # Skip social media
        if any(x in email_lower for x in ["@facebook", "@twitter", "@instagram", "@2x", "@3x"]):
            continue
        
        # Check domain
        try:
            prefix, domain = email_lower.split('@')
        except:
            continue
        
        # Skip fake domains
        if any(fd in domain for fd in fake_domains):
            continue
        
        # Skip fake prefixes
        if prefix in fake_prefixes:
            continue
        
        # Validate TLD
        try:
            tld = domain.split('.')[-1]
            if tld not in valid_tlds:
                continue
        except:
            continue
        
        # Basic sanity checks
        if len(email) < 6 or len(email) > 100:
            continue
        if email.count('@') != 1:
            continue
        if '..' in email:
            continue
        
        # Skip ALL CAPS emails (usually placeholders)
        if email.isupper():
            continue
            
        filtered_emails.append(email)
    
    return filtered_emails


def select_best_emails_per_domain(emails: List[str], max_per_domain: int = 1) -> List[str]:
    """Select best email(s) from each domain"""
    domain_emails: Dict[str, List[str]] = {}
    for email in emails:
        domain = get_email_domain(email)
        if domain not in domain_emails:
            domain_emails[domain] = []
        domain_emails[domain].append(email)
    
    best_emails = []
    for domain, email_list in domain_emails.items():
        sorted_emails = sorted(email_list, key=score_email, reverse=True)
        best_emails.extend(sorted_emails[:max_per_domain])
    
    return best_emails


def main():
    """Main workflow"""
    print("=" * 70)
    print("🔍 Lead Scraper - Google Maps + Google Search")
    print("=" * 70)
    print(f"Queries: {len(SEARCH_QUERIES)}")
    print(f"Sources: Maps={USE_GOOGLE_MAPS}, Search={USE_GOOGLE_SEARCH}")
    print(f"Max URLs/query: {MAX_URLS_PER_QUERY}, Emails/domain: {MAX_EMAILS_PER_DOMAIN}")
    print("=" * 70)
    
    all_urls: List[str] = []
    
    # Step 1: Collect URLs from all queries
    for i, query in enumerate(SEARCH_QUERIES):
        print(f"\n[{i+1}/{len(SEARCH_QUERIES)}] Query: \"{query}\"")
        
        query_urls = []
        
        if USE_GOOGLE_MAPS:
            maps_urls = scrape_google_maps(query)
            query_urls.extend(maps_urls)
        
        if USE_GOOGLE_SEARCH:
            search_urls = scrape_google_search(query)
            query_urls.extend(search_urls)
        
        all_urls.extend(query_urls)
        
        # Small delay between queries
        time.sleep(1)
    
    # Step 2: Deduplicate URLs
    print(f"\n{'=' * 70}")
    print("Processing URLs")
    print("=" * 70)
    print(f"Total URLs collected: {len(all_urls)}")
    
    unique_urls = remove_duplicates(all_urls)
    print(f"Unique domains: {len(unique_urls)}")
    
    # Limit total URLs to scrape
    max_total = MAX_URLS_PER_QUERY * 3  # Reasonable limit
    if len(unique_urls) > max_total:
        unique_urls = unique_urls[:max_total]
        print(f"Limited to: {len(unique_urls)}")
    
    # Step 3: Scrape each site for emails
    print(f"\n{'=' * 70}")
    print("Scraping Websites for Emails")
    print("=" * 70)
    
    all_emails: List[str] = []
    sites_with_emails = 0
    
    for i, url in enumerate(unique_urls):
        print(f"\n[{i+1}/{len(unique_urls)}] ", end="")
        
        site_html = scrape_site_with_contact_pages(url)
        
        time.sleep(WAIT_BETWEEN_REQUESTS)
        
        emails = extract_emails(site_html)
        if emails:
            sites_with_emails += 1
            print(f"  → Found {len(emails)} email(s)")
            all_emails.extend(emails)
        else:
            print(f"  → No emails")
    
    # Step 4: Process and deduplicate emails
    print(f"\n{'=' * 70}")
    print("Final Processing")
    print("=" * 70)
    print(f"Sites with emails: {sites_with_emails}/{len(unique_urls)}")
    print(f"Total emails found: {len(all_emails)}")
    
    # Remove exact duplicates
    unique_emails = list(set(all_emails))
    print(f"Unique emails: {len(unique_emails)}")
    
    # Select best per domain
    final_emails = select_best_emails_per_domain(unique_emails, MAX_EMAILS_PER_DOMAIN)
    print(f"After best-per-domain: {len(final_emails)}")
    
    # Categorize
    personal_emails = sorted([e for e in final_emails if is_personal_email(e)], key=score_email, reverse=True)
    generic_emails = sorted([e for e in final_emails if not is_personal_email(e)], key=score_email, reverse=True)
    
    # Output results
    print(f"\n{'=' * 70}")
    print("🎯 RESULTS - Dental Marketing Agency Leads")
    print("=" * 70)
    print(f"Total unique leads: {len(final_emails)}")
    print(f"  → Personal emails (best): {len(personal_emails)}")
    print(f"  → Generic emails: {len(generic_emails)}")
    print("-" * 70)
    
    if personal_emails:
        print("\n📧 PERSONAL EMAILS (Decision Makers):")
        for email in personal_emails:
            print(f"  ★ {email}")
    
    if generic_emails:
        print("\n📬 GENERIC EMAILS (Company Contacts):")
        for email in generic_emails:
            print(f"  • {email}")
    
    if not final_emails:
        print("  No emails found.")
    
    print("=" * 70)
    
    # Save to file
    with open("dental_marketing_leads.txt", "w") as f:
        f.write("DENTAL MARKETING AGENCY LEADS\n")
        f.write("=" * 50 + "\n\n")
        
        if personal_emails:
            f.write("PERSONAL EMAILS (Decision Makers):\n")
            for email in personal_emails:
                f.write(f"  {email}\n")
            f.write("\n")
        
        if generic_emails:
            f.write("GENERIC EMAILS (Company Contacts):\n")
            for email in generic_emails:
                f.write(f"  {email}\n")
    
    print(f"\n💾 Results saved to: dental_marketing_leads.txt")
    
    return final_emails


if __name__ == "__main__":
    emails = main()
