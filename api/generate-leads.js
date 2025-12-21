/**
 * Vercel Serverless Function for Lead Generation
 * Scrapes Indeed + Gemini API enrichment with 2-day caching
 */

const { chromium } = require('playwright-chromium');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const { put, list } = require('@vercel/blob');

// Configuration
const TWO_DAYS_MS = 172800000; // 2 days in milliseconds
const TARGET_LEADS = 5;
const MAX_DURATION = 55000; // 55 seconds (leave 5s buffer for Vercel's 60s limit)

// Excluded company patterns (from Python script lines 113-124)
const EXCLUDED_PATTERNS = [
  /\bagency\b/i,
  /\bagencies\b/i,
  /\bstaffing\b/i,
  /\brecruiting\b/i,
  /\bfranchise\b/i,
  /\bconsulting group\b/i,
  /\bworldwide\b/i,
  /\bglobal\b/i,
  /\binternational\b/i,
  /\bnetwork\b/i
];

// LA area cities (from Python script lines 55-83)
const LA_AREA_CITIES = new Set([
  'los angeles', 'santa monica', 'beverly hills', 'west hollywood',
  'culver city', 'venice', 'marina del rey', 'el segundo',
  'manhattan beach', 'hermosa beach', 'redondo beach', 'torrance',
  'pasadena', 'glendale', 'burbank', 'studio city', 'sherman oaks',
  'encino', 'north hollywood', 'hollywood', 'west los angeles',
  'brentwood', 'pacific palisades', 'malibu', 'calabasas',
  'irvine', 'newport beach', 'costa mesa', 'huntington beach',
  'santa ana', 'anaheim', 'long beach', 'downey', 'inglewood'
]);

// LA area zip codes (from Python script lines 55-83)
const LA_AREA_ZIPS = new Set([
  '90001', '90002', '90003', '90004', '90005', '90006', '90007', '90008',
  '90010', '90011', '90012', '90013', '90014', '90015', '90016', '90017',
  '90018', '90019', '90020', '90021', '90022', '90023', '90024', '90025',
  '90026', '90027', '90028', '90029', '90031', '90032', '90033', '90034',
  '90035', '90036', '90037', '90038', '90039', '90040', '90041', '90042',
  '90043', '90044', '90045', '90046', '90047', '90048', '90049', '90056',
  '90057', '90058', '90059', '90061', '90062', '90063', '90064', '90065',
  '90066', '90067', '90068', '90069', '90071', '90077', '90094', '90095',
  '90210', '90211', '90212', '90230', '90245', '90247', '90248', '90249',
  '90254', '90255', '90260', '90266', '90272', '90275', '90277', '90278',
  '90290', '90291', '90292', '90293', '90301', '90302', '90303', '90304',
  '90305', '90401', '90402', '90403', '90404', '90405', '91001', '91006',
  '91101', '91102', '91103', '91104', '91105', '91106', '91107', '91108',
  '91201', '91202', '91203', '91204', '91205', '91206', '91207', '91208',
  '91301', '91302', '91303', '91304', '91306', '91307', '91311', '91316',
  '91324', '91325', '91326', '91330', '91331', '91335', '91340', '91342',
  '91343', '91344', '91345', '91356', '91364', '91367', '91401', '91402',
  '91403', '91405', '91406', '91411', '91423', '91436', '91501', '91502',
  '91504', '91505', '91506', '91601', '91602', '91604', '91605', '91606',
  '91607', '91608', '92602', '92603', '92604', '92606', '92612', '92614',
  '92617', '92618', '92620', '92625', '92626', '92627', '92646', '92647',
  '92648', '92649', '92651', '92660', '92661', '92662', '92663', '92677',
  '92683', '92701', '92702', '92703', '92704', '92705', '92706', '92707',
  '92801', '92802', '92804', '92805', '90710', '90715', '90716', '90720',
  '90731', '90732', '90740', '90742', '90743', '90744', '90745', '90746',
  '90755', '90802', '90803', '90804', '90805', '90806', '90807', '90808',
  '90810', '90813', '90814', '90815'
]);

/**
 * Cache Manager
 */
async function getCachedLeads(location) {
  try {
    // List blobs to find cache file
    const { blobs } = await list({ prefix: `leads-cache-${location}` });

    if (blobs.length === 0) {
      return { expired: true };
    }

    // Get the latest blob
    const latestBlob = blobs[0];
    const response = await fetch(latestBlob.url);
    const data = await response.json();

    // Check if expired
    const age = Date.now() - new Date(data.timestamp).getTime();
    if (age >= TWO_DAYS_MS) {
      return { ...data, expired: true };
    }

    return { ...data, expired: false };
  } catch (error) {
    console.error('Cache read error:', error.message);
    return { expired: true };
  }
}

async function setCachedLeads(location, leadsData) {
  try {
    const cacheData = {
      timestamp: new Date().toISOString(),
      expiresAt: new Date(Date.now() + TWO_DAYS_MS).toISOString(),
      location,
      ...leadsData
    };

    await put(
      `leads-cache-${location}.json`,
      JSON.stringify(cacheData),
      { access: 'public' }
    );

    console.log(`Cache updated for location: ${location}`);
  } catch (error) {
    console.error('Cache write error:', error.message);
  }
}

/**
 * Indeed Scraper
 */
class IndeedScraper {
  constructor() {
    this.seenCompanies = new Set();
  }

  /**
   * Check if location is in LA area
   */
  isInLAArea(locationStr) {
    if (!locationStr) return false;

    const lower = locationStr.toLowerCase();

    // Check cities
    for (const city of LA_AREA_CITIES) {
      if (lower.includes(city)) return true;
    }

    // Check zip codes
    const zipMatch = locationStr.match(/\b\d{5}\b/);
    if (zipMatch && LA_AREA_ZIPS.has(zipMatch[0])) return true;

    return false;
  }

  /**
   * Check if company should be excluded
   */
  isExcludedCompany(companyName) {
    if (!companyName) return true;

    for (const pattern of EXCLUDED_PATTERNS) {
      if (pattern.test(companyName)) return true;
    }

    return false;
  }

  /**
   * Normalize company name for deduplication
   */
  normalizeCompanyName(name) {
    return name.toLowerCase().replace(/[^a-z0-9]/g, '');
  }

  /**
   * Scrape Indeed for job postings
   */
  async searchJobs(location, jobTitle, maxResults = 15) {
    const browser = await chromium.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    try {
      const page = await browser.newPage();
      page.setDefaultTimeout(10000);

      const query = encodeURIComponent(jobTitle);
      const loc = encodeURIComponent(location);
      const url = `https://www.indeed.com/jobs?q=${query}&l=${loc}&fromage=14`;

      console.log(`Scraping Indeed: ${url}`);
      await page.goto(url, { waitUntil: 'domcontentloaded' });

      // Wait for job cards
      await page.waitForSelector('div.job_seen_beacon, div.jobsearch-ResultsList > div', { timeout: 5000 });

      // Extract job cards
      const leads = await page.evaluate(() => {
        const cards = Array.from(document.querySelectorAll('div.job_seen_beacon, div.jobsearch-ResultsList > div'));
        const results = [];

        for (const card of cards.slice(0, 15)) {
          try {
            // Company name
            const companyElem = card.querySelector('[data-testid="company-name"], .companyName, .company');
            const companyName = companyElem ? companyElem.textContent.trim() : null;

            // Location
            const locationElem = card.querySelector('[data-testid="text-location"], .companyLocation');
            const location = locationElem ? locationElem.textContent.trim() : null;

            // Job link
            const linkElem = card.querySelector('a[id^="job_"], h2 a');
            const jobLink = linkElem ? linkElem.href : null;

            if (companyName && location) {
              results.push({
                companyName,
                location,
                jobLink,
                evidence: `Indeed posting for marketing role (last 14 days)`
              });
            }
          } catch (err) {
            // Skip this card
          }
        }

        return results;
      });

      await browser.close();

      // Filter leads
      const filteredLeads = leads.filter(lead => {
        // Check if in LA area
        if (!this.isInLAArea(lead.location)) return false;

        // Check if excluded company
        if (this.isExcludedCompany(lead.companyName)) return false;

        // Check for duplicates
        const normalized = this.normalizeCompanyName(lead.companyName);
        if (this.seenCompanies.has(normalized)) return false;

        this.seenCompanies.add(normalized);
        return true;
      });

      console.log(`  Found ${filteredLeads.length} unique LA area leads`);
      return filteredLeads;
    } catch (error) {
      console.error(`Indeed scraping error: ${error.message}`);
      await browser.close();
      return [];
    }
  }
}

/**
 * Contact Enricher using Gemini API
 */
class ContactEnricher {
  constructor(apiKey) {
    this.genAI = new GoogleGenerativeAI(apiKey);
  }

  /**
   * Extract employee count from company size string
   */
  getEmployeeCount(sizeStr) {
    if (!sizeStr || sizeStr.toLowerCase().includes('unknown')) {
      return null;
    }

    const numbers = sizeStr.match(/\d+/g);
    if (numbers && numbers.length > 0) {
      // If range (e.g., "50-100"), use upper bound
      if (numbers.length >= 2) {
        return parseInt(numbers[1]);
      }
      return parseInt(numbers[0]);
    }

    return null;
  }

  /**
   * Categorize lead by employee count
   */
  categorizeBySize(lead) {
    const employeeCount = this.getEmployeeCount(lead.companySize);

    if (employeeCount === null) {
      return null; // Skip unknown sizes
    }

    if (employeeCount <= 100) {
      return 'small';
    } else if (employeeCount <= 250) {
      return 'medium';
    } else {
      return 'large';
    }
  }

  /**
   * Enrich lead with contact information using Gemini API
   */
  async enrichLead(lead) {
    try {
      const model = this.genAI.getGenerativeModel({
        model: 'gemini-2.0-flash-exp',
        tools: [{ googleSearch: {} }] // Enable Google Search grounding
      });

      const prompt = `Find the current key decision-maker, contact information, official website, and company size for:
Company: ${lead.companyName}

Target roles (in strict order):
1. CEO / Founder / Owner
2. President
3. CMO / VP of Marketing

INSTRUCTIONS:
- Use Google Search to verify the CURRENT person in this role.
- Try to find the person's work email address (not generic support@).
- Estimate the approximate number of employees at this company (e.g., "10-50", "50-100", "100-250", "unknown").

Return ONLY valid JSON in this exact format:
{
  "contact_name": "First Last",
  "contact_title": "CEO",
  "contact_email": "email@company.com",
  "contact_linkedin": "https://linkedin.com/in/...",
  "website": "https://company.com",
  "company_size": "50-100 employees"
}

If you cannot find the person, return:
{
  "contact_name": "",
  "contact_title": "",
  "contact_email": "",
  "contact_linkedin": "",
  "website": "",
  "company_size": "unknown"
}`;

      const result = await model.generateContent({
        contents: [{ role: 'user', parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.0,
          responseMimeType: 'application/json'
        }
      });

      const response = await result.response;
      const text = response.text();

      // Parse JSON (handle code blocks if present)
      let jsonText = text.trim();
      if (jsonText.startsWith('```json')) {
        jsonText = jsonText.replace(/```json\n?/g, '').replace(/```\n?/g, '');
      }

      const data = JSON.parse(jsonText);

      // Only return enriched lead if we found a contact name
      if (!data.contact_name) {
        return null;
      }

      const enrichedLead = {
        firstName: data.contact_name.split(' ')[0] || '',
        lastName: data.contact_name.split(' ').slice(1).join(' ') || '',
        title: data.contact_title || '',
        companyName: lead.companyName,
        email: data.contact_email || '',
        website: data.website || '',
        location: lead.location,
        companySize: data.company_size || 'unknown',
        evidence: lead.evidence
      };

      // Categorize by size
      const category = this.categorizeBySize(enrichedLead);
      if (category === null) {
        return null; // Skip unknown sizes
      }

      enrichedLead.category = category;
      return enrichedLead;
    } catch (error) {
      console.error(`  Enrichment error for ${lead.companyName}: ${error.message}`);
      return null;
    }
  }
}

/**
 * Main orchestrator - generate leads until timeout or target reached
 */
async function generateLeads(location, maxDuration = MAX_DURATION) {
  const startTime = Date.now();

  console.log(`Starting lead generation for: ${location}`);
  console.log(`Max duration: ${maxDuration}ms`);

  // Initialize
  const scraper = new IndeedScraper();
  const enricher = new ContactEnricher(process.env.GOOGLE_GEMINI_API_KEY);

  const leadsSmall = [];
  const leadsMedium = [];
  const leadsLarge = [];

  // Job titles to search (from Python script)
  const jobTitles = [
    'Marketing Manager',
    'Digital Marketing Specialist',
    'Marketing Coordinator'
  ];

  // Search locations for LA area
  const searchLocations = [
    'Los Angeles, CA',
    'Santa Monica, CA',
    'Burbank, CA',
    'Pasadena, CA',
    'Irvine, CA'
  ];

  // Phase 1: Scrape Indeed (30 seconds max)
  console.log('\n=== PHASE 1: Scraping Indeed ===');
  const scrapedLeads = [];

  for (const searchLoc of searchLocations) {
    if (Date.now() - startTime > 30000) break; // 30s limit for scraping

    for (const jobTitle of jobTitles) {
      if (Date.now() - startTime > 30000) break;

      try {
        const leads = await scraper.searchJobs(searchLoc, jobTitle, 10);
        scrapedLeads.push(...leads);

        // Small delay between requests
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        console.error(`Scraping error: ${error.message}`);
      }
    }
  }

  console.log(`Scraped ${scrapedLeads.length} unique leads`);

  // Phase 2: Enrich with Gemini (remaining time)
  console.log('\n=== PHASE 2: Enriching with Gemini ===');

  for (const lead of scrapedLeads) {
    // Check timeout
    if (Date.now() - startTime > maxDuration) {
      console.log('Timeout approaching - stopping enrichment');
      break;
    }

    // Check if we've reached target (5 in small + medium combined)
    const targetCount = leadsSmall.length + leadsMedium.length;
    if (targetCount >= TARGET_LEADS) {
      console.log(`Reached target of ${TARGET_LEADS} leads`);
      break;
    }

    try {
      const enrichedLead = await enricher.enrichLead(lead);

      if (enrichedLead) {
        // Add to appropriate category
        if (enrichedLead.category === 'small') {
          leadsSmall.push(enrichedLead);
          console.log(`  ✓ Small company: ${enrichedLead.companyName} (${enrichedLead.companySize})`);
        } else if (enrichedLead.category === 'medium') {
          leadsMedium.push(enrichedLead);
          console.log(`  ✓ Medium company: ${enrichedLead.companyName} (${enrichedLead.companySize})`);
        } else if (enrichedLead.category === 'large') {
          leadsLarge.push(enrichedLead);
          console.log(`  ✓ Large company: ${enrichedLead.companyName} (${enrichedLead.companySize})`);
        }
      }

      // Small delay between API calls
      await new Promise(resolve => setTimeout(resolve, 300));
    } catch (error) {
      console.error(`Enrichment error: ${error.message}`);
    }
  }

  const allLeads = [...leadsSmall, ...leadsMedium, ...leadsLarge];
  const targetCount = leadsSmall.length + leadsMedium.length;
  const isPartial = targetCount < TARGET_LEADS;

  console.log('\n=== RESULTS ===');
  console.log(`Total leads: ${allLeads.length}`);
  console.log(`Small (≤100): ${leadsSmall.length}`);
  console.log(`Medium (101-250): ${leadsMedium.length}`);
  console.log(`Large (251+): ${leadsLarge.length}`);
  console.log(`Target count: ${targetCount}/${TARGET_LEADS}`);
  console.log(`Partial result: ${isPartial}`);

  return {
    success: true,
    leads: allLeads,
    stats: {
      small: leadsSmall.length,
      medium: leadsMedium.length,
      large: leadsLarge.length,
      targetCount,
      isPartial
    }
  };
}

/**
 * Vercel Serverless Handler
 */
module.exports = async function handler(req, res) {
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Credentials', true);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS,PATCH,DELETE,POST,PUT');
  res.setHeader('Access-Control-Allow-Headers', 'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version');

  // Handle preflight request
  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ success: false, error: 'Method not allowed' });
  }

  // Check for API key
  if (!process.env.GOOGLE_GEMINI_API_KEY) {
    console.error('GOOGLE_GEMINI_API_KEY is not set');
    return res.status(500).json({ success: false, error: 'Server configuration error: Missing API key' });
  }

  try {
    const { location } = req.body;

    if (!location) {
      return res.status(400).json({
        success: false,
        error: 'Missing required field: location'
      });
    }

    console.log(`\n=== NEW REQUEST ===`);
    console.log(`Location: ${location}`);

    // Check cache first
    const cached = await getCachedLeads(location);
    if (!cached.expired) {
      console.log('Cache hit - returning cached results');
      return res.status(200).json({
        success: true,
        leads: cached.leads,
        stats: cached.stats,
        cached: true
      });
    }

    console.log('Cache miss or expired - generating fresh leads');

    // Generate fresh leads
    const result = await generateLeads(location);

    // Cache the results
    await setCachedLeads(location, result);

    return res.status(200).json({
      ...result,
      cached: false
    });
  } catch (error) {
    console.error('Error processing request:', error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Internal server error'
    });
  }
};
