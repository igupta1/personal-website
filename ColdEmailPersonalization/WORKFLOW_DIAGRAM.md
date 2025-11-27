# Deep Multiline Icebreaker System - Workflow Diagram

```
╔═══════════════════════════════════════════════════════════════════════════╗
║                    DEEP MULTILINE ICEBREAKER SYSTEM                       ║
║                     n8n Workflow → Python Script                          ║
╚═══════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────┐
│ INPUT: apollo-contacts-export (21).csv (995 rows)                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 1: Load & Filter Leads                                             │
│ • Load CSV with pandas                                                   │
│ • Filter: Keep only rows where Website != empty AND Email != empty      │
│ • Map columns: First Name → first_name, Website → website_url, etc.     │
│ • Result: ~450 valid leads                                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 2: Loop Over Each Lead (Async Processing)                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┴───────────────────────────┐
        │ For Lead: John Doe, https://example.com               │
        └───────────────────────────┬───────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 2A: Scrape Home Page                                               │
│ • Fetch https://example.com                                             │
│ • Parse HTML with BeautifulSoup                                         │
│ • Extract all <a href="..."> links                                      │
│ • Result: ["https://example.com/about", "/services", "/contact", ...]  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 2B: Filter & Normalize Links (Replicates n8n JS Logic)            │
│                                                                          │
│ For each link:                                                          │
│   1. If starts with '/':                → Keep as-is                    │
│   2. If starts with 'http(s)://':       → Parse URL                     │
│      - Check if same domain             → Extract pathname              │
│      - Remove trailing slash (unless /) → Convert to relative           │
│   3. Other links (external/anchors):    → Skip                          │
│                                                                          │
│ Then:                                                                   │
│   4. Deduplicate paths                                                  │
│   5. Limit to top 3                                                     │
│                                                                          │
│ Result: ["/about", "/services", "/contact"]                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 2C: Scrape & Summarize Sub-pages (Parallel)                       │
│                                                                          │
│ For each path (/about, /services, /contact):                           │
│                                                                          │
│   1. Construct full URL: https://example.com + /about                   │
│   2. Fetch page HTML                                                    │
│   3. Convert HTML → Markdown (html2text)                                │
│   4. Call GPT-4o to summarize:                                          │
│      ┌──────────────────────────────────────────────────┐              │
│      │ System: "You're a website scraping assistant"    │              │
│      │ User: "Provide 2-paragraph abstract..."          │              │
│      │ Input: [Markdown content]                         │              │
│      │ Output: {"abstract": "This page describes..."}   │              │
│      └──────────────────────────────────────────────────┘              │
│                                                                          │
│ Result: 3 abstracts (one per sub-page)                                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 2D: Aggregate & Generate Icebreaker                               │
│                                                                          │
│ Combine all 3 abstracts into single text block                         │
│                                                                          │
│ Call GPT-4o to generate icebreaker:                                    │
│   ┌─────────────────────────────────────────────────────┐              │
│   │ System: "You're a sales assistant"                  │              │
│   │ User: [Detailed instructions for icebreaker format]│              │
│   │ Example: [Maki Agency example from n8n]            │              │
│   │ Input: Profile + Website abstracts                  │              │
│   │ Output: {"icebreaker": "Hey John, Love..."}        │              │
│   └─────────────────────────────────────────────────────┘              │
│                                                                          │
│ Temperature: 0.5 (balanced creativity)                                  │
│ Result: Personalized icebreaker for John Doe                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 3: Save Results                                                    │
│ • Append icebreaker to lead data                                        │
│ • Repeat for all 450 leads                                              │
│ • Save to CSV: leads_with_icebreakers.csv                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ OUTPUT: leads_with_icebreakers.csv                                      │
│                                                                          │
│ Columns:                                                                │
│ • first_name, last_name, email, website_url                            │
│ • headline, location, phone_number                                      │
│ • multiline_icebreaker ← THE MAGIC ✨                                  │
└─────────────────────────────────────────────────────────────────────────┘


╔═══════════════════════════════════════════════════════════════════════════╗
║                         TECHNICAL ARCHITECTURE                            ║
╚═══════════════════════════════════════════════════════════════════════════╝

┌──────────────────────────┐     ┌──────────────────────────┐
│   Async HTTP Client      │     │    OpenAI API Client     │
│   (aiohttp)              │     │    (openai library)      │
│                          │     │                          │
│ • Concurrent requests    │     │ • GPT-4o model           │
│ • Rate limiting (5 max)  │     │ • JSON output format     │
│ • Timeout handling       │     │ • Temperature control    │
│ • Retry logic            │     │ • Cost optimization      │
└────────────┬─────────────┘     └────────────┬─────────────┘
             │                                 │
             ▼                                 ▼
┌────────────────────────────────────────────────────────────┐
│              LeadProcessor Class                           │
│                                                            │
│  Methods:                                                  │
│  • fetch_url()            → Async HTTP requests           │
│  • extract_links()        → BeautifulSoup parsing         │
│  • filter_normalize()     → n8n logic replication         │
│  • html_to_markdown()     → Content conversion            │
│  • summarize_page()       → GPT-4o API call               │
│  • generate_icebreaker()  → GPT-4o API call               │
│  • process_lead()         → Orchestrates entire pipeline  │
└────────────────────────────────────────────────────────────┘


╔═══════════════════════════════════════════════════════════════════════════╗
║                         ERROR HANDLING FLOW                               ║
╚═══════════════════════════════════════════════════════════════════════════╝

Every step has error handling:

HTTP Request Fails
    ├─ Timeout (30s)        → Log error, skip page, continue
    ├─ 404/403/500          → Log error, skip page, continue
    └─ Network error        → Log error, skip page, continue

HTML Parsing Fails
    └─ Invalid HTML         → Log error, use empty content

Link Filtering Yields 0
    └─ No internal links    → Log warning, skip lead

GPT-4o API Fails
    ├─ Rate limit           → Automatic retry (SDK handles)
    ├─ Timeout              → Log error, return empty
    └─ Invalid response     → Log error, return empty

Final Result:
    • All leads processed (even if some fail)
    • Empty icebreaker = processing failed
    • Script completes successfully


╔═══════════════════════════════════════════════════════════════════════════╗
║                      PERFORMANCE CHARACTERISTICS                          ║
╚═══════════════════════════════════════════════════════════════════════════╝

Concurrency:
    • Max 5 simultaneous HTTP requests (semaphore)
    • Sequential lead processing (respectful rate limiting)
    • Async I/O for all network operations

Timing per Lead:
    • Home page scrape:     2-5 seconds
    • Sub-page scraping:    6-15 seconds (3 pages × 2-5s)
    • Summarization:        10-20 seconds (3 summaries × 3-7s)
    • Icebreaker gen:       5-10 seconds
    • TOTAL:                ~30-60 seconds per lead

Cost per Lead:
    • 3 summary calls:      ~$0.015 (GPT-4o, ~1K tokens each)
    • 1 icebreaker call:    ~$0.025 (GPT-4o, ~2K tokens)
    • TOTAL:                ~$0.04 per lead

Full Dataset (450 leads):
    • Time:                 3-4 hours
    • Cost:                 ~$18


╔═══════════════════════════════════════════════════════════════════════════╗
║                    KEY DIFFERENCES FROM N8N WORKFLOW                      ║
╚═══════════════════════════════════════════════════════════════════════════╝

┌─────────────────────┬─────────────────────┬──────────────────────┐
│ Component           │ n8n Workflow        │ Python Script        │
├─────────────────────┼─────────────────────┼──────────────────────┤
│ Input Source        │ Google Sheets       │ Local CSV            │
│ Lead Discovery      │ Apify scraper       │ Direct from CSV      │
│ HTTP Requests       │ Sequential          │ Async/Concurrent     │
│ HTML Parsing        │ n8n HTML node       │ BeautifulSoup        │
│ Link Filtering      │ Filter + Code nodes │ Python function      │
│ Content Format      │ Raw HTML            │ Markdown             │
│ AI Model            │ GPT-4.1             │ GPT-4o               │
│ Output              │ Google Sheets       │ Local CSV            │
│ Error Handling      │ Continue on error   │ Try/except + logging │
│ Progress Tracking   │ n8n UI              │ Console output       │
│ Deployment          │ n8n server          │ Local/CLI            │
└─────────────────────┴─────────────────────┴──────────────────────┘

Advantages of Python Version:
    ✅ Faster (async/concurrent)
    ✅ More portable (no n8n needed)
    ✅ Better error visibility
    ✅ Easier to debug & modify
    ✅ Version control friendly
    ✅ No external dependencies (Apify, Google Sheets)

Kept from n8n:
    ✅ Exact link filtering logic
    ✅ Same AI prompts & examples
    ✅ Same output format
    ✅ Same error handling approach


╔═══════════════════════════════════════════════════════════════════════════╗
║                              EXAMPLE OUTPUT                               ║
╚═══════════════════════════════════════════════════════════════════════════╝

Generated Icebreaker Example:

"Hey Junior,

Love what you're doing at Grio. Also doing some brand storytelling right now, 
wanted to run something by you.

I hope you'll forgive me, but I creeped you/Grio quite a bit, and know that 
empathy-led marketing is important to you guys (or at least I'm assuming this 
given the focus on value-driven branding). I put something together a few 
months ago that I think could help. To make a long story short, it's an 
outreach system that uses AI to find people hiring website devs. Then pitches 
them with templates (actually makes them a demo website). Costs just a few 
cents to run, very high converting, and I think it's in line with Grio's 
emphasis on purpose-led work."

Features:
    • Personalized name
    • Company shortening (Grio vs The Grio Agency)
    • Specific insight from website (empathy-led marketing)
    • Evidence (value-driven branding)
    • Maintains template structure
    • Spartan/laconic tone
```

