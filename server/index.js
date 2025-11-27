/**
 * Minimal Backend Proxy for Cold Email Icebreaker Generation
 * Keeps OpenAI API key secure on the server side
 */

const express = require('express');
const cors = require('cors');
const { OpenAI } = require('openai');
const axios = require('axios');
const cheerio = require('cheerio');
const TurndownService = require('turndown');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// Initialize Turndown for HTML to Markdown conversion
const turndownService = new TurndownService({
  headingStyle: 'atx',
  codeBlockStyle: 'fenced'
});

// Middleware
app.use(cors());
app.use(express.json());

// Configuration
const MAX_LINKS_PER_SITE = 3;
const TIMEOUT = 30000; // 30 seconds

/**
 * Fetch URL content with error handling
 */
async function fetchUrl(url) {
  try {
    const response = await axios.get(url, {
      timeout: TIMEOUT,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
      },
      maxRedirects: 5
    });
    return response.data;
  } catch (error) {
    console.log(`    ❌ Failed to fetch: ${url}`);
    return null;
  }
}

/**
 * Extract all href attributes from anchor tags
 */
function extractLinks(html) {
  const $ = cheerio.load(html);
  const links = [];
  $('a[href]').each((_, el) => {
    links.push($(el).attr('href'));
  });
  return links;
}

/**
 * Normalize domain by removing www. prefix
 */
function normalizeDomain(domain) {
  return domain.toLowerCase().replace(/^www\./, '');
}

/**
 * Filter and normalize links (replicate Python logic)
 */
function filterAndNormalizeLinks(links, baseUrl) {
  const normalized = [];
  let baseParsed;
  
  try {
    baseParsed = new URL(baseUrl);
  } catch {
    return [];
  }

  for (const link of links) {
    if (!link) continue;

    // Case 1: Starts with '/' - already relative
    if (link.startsWith('/')) {
      normalized.push(link);
    }
    // Case 2: Absolute URL
    else if (link.startsWith('http://') || link.startsWith('https://')) {
      try {
        const parsed = new URL(link);
        // Only include if same domain
        if (normalizeDomain(parsed.hostname) === normalizeDomain(baseParsed.hostname)) {
          let path = parsed.pathname;
          // Strip trailing slash unless root
          if (path !== '/' && path.endsWith('/')) {
            path = path.slice(0, -1);
          }
          if (path) {
            normalized.push(path);
          }
        }
      } catch {
        continue;
      }
    }
  }

  // Filter only those starting with '/'
  const filtered = normalized.filter(link => link.startsWith('/'));

  // Deduplicate while preserving order
  const seen = new Set();
  const deduplicated = [];
  for (const link of filtered) {
    if (!seen.has(link)) {
      seen.add(link);
      deduplicated.push(link);
    }
  }

  return deduplicated.slice(0, MAX_LINKS_PER_SITE);
}

/**
 * Convert HTML to Markdown
 */
function htmlToMarkdown(html) {
  try {
    const $ = cheerio.load(html);
    // Remove scripts, styles, and other non-content elements
    $('script, style, nav, footer, header, aside').remove();
    const bodyHtml = $('body').html() || html;
    return turndownService.turndown(bodyHtml);
  } catch {
    return '';
  }
}

/**
 * Summarize page content using GPT
 */
async function summarizePage(markdownContent) {
  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content: "You're a helpful, intelligent website scraping assistant."
        },
        {
          role: 'user',
          content: `You're provided a Markdown scrape of a website page. Your task is to provide a two-paragraph abstract of what this page is about.

Return in this JSON format:

{"abstract":"your abstract goes here"}

Rules:
- Your extract should be comprehensive—similar level of detail as an abstract to a published paper.
- Use a straightforward, spartan tone of voice.
- If it's empty, just say "no content".`
        },
        {
          role: 'user',
          content: markdownContent.slice(0, 10000) // Limit content length
        }
      ],
      response_format: { type: 'json_object' },
      temperature: 1.0
    });

    const result = JSON.parse(response.choices[0].message.content);
    return result.abstract || 'no content';
  } catch (error) {
    console.log(`    ⚠️ Summarize error: ${error.message}`);
    return 'no content';
  }
}

/**
 * Generate icebreaker using GPT
 */
async function generateIcebreaker(firstName, lastName, headline, abstracts) {
  try {
    const websiteContent = abstracts.join('\n\n');

    const response = await openai.chat.completions.create({
      model: 'gpt-4o',
      messages: [
        {
          role: 'system',
          content: `You're a senior outbound copywriter specializing in hyper-personalized cold email icebreakers. You are given multiple summaries of a company's website. Your job is to generate a single icebreaker that clearly shows we studied the recipient's site.

Return ONLY valid JSON in this exact format:

{"icebreaker":"Hey {name} — went down a rabbit hole on {ShortCompanyName}'s site. The part about {specific_niche_detail} caught my eye. Your focus on {core_value_or_theme} stuck with me."}

RULES:
- {ShortCompanyName}: shorten multi-word company names to one clean word (e.g., "Maki Agency" → "Maki", "Chartwell Agency" → "Chartwell").
- {specific_niche_detail}: choose ONE sharp, concrete detail from the summaries (a specific process, case study, philosophy, niche service, repeated phrase, or concept).
- {core_value_or_theme}: choose ONE recurring value or theme that appears multiple times across the summaries (e.g., empathy, clarity, storytelling, precision, long-term thinking, craftsmanship, community impact, rigor).
- Both variables MUST directly come from the summaries. No inventing or guessing.
- Tone: concise, calm, founder-to-founder.
- Avoid generic compliments ("love your site", "great work").
- Do not alter the template — only fill in the variables.`
        },
        {
          role: 'user',
          content: `=Profile: ${firstName} ${lastName} ${headline}\n\nWebsite Summaries:\n${websiteContent}`
        }
      ],
      response_format: { type: 'json_object' },
      temperature: 0.5
    });

    const result = JSON.parse(response.choices[0].message.content);
    return result.icebreaker || '';
  } catch (error) {
    console.log(`    ⚠️ Icebreaker error: ${error.message}`);
    return '';
  }
}

/**
 * Process a single lead through the entire pipeline
 */
async function processLead(lead) {
  let websiteUrl = lead.website;
  const firstName = lead.firstName;
  const lastName = lead.lastName;
  const headline = lead.title || '';
  const companyName = lead.companyName || '';

  console.log(`  🔄 Processing: ${firstName} ${lastName} - ${websiteUrl}`);

  // Ensure URL has scheme
  if (!websiteUrl.startsWith('http')) {
    websiteUrl = 'https://' + websiteUrl;
  }

  // Step 1: Scrape home page
  const homeHtml = await fetchUrl(websiteUrl);
  if (!homeHtml) {
    console.log(`    ❌ Failed to fetch ${websiteUrl}`);
    return { success: false, error: 'Failed to fetch website' };
  }

  console.log(`    ✅ Fetched homepage (${homeHtml.length} chars)`);

  // Step 2: Extract and filter links
  const allLinks = extractLinks(homeHtml);
  const filteredLinks = filterAndNormalizeLinks(allLinks, websiteUrl);

  const abstracts = [];

  if (filteredLinks.length === 0) {
    console.log(`    🔄 Using fallback (no internal links)`);
    // FALLBACK: Use homepage content
    const markdown = htmlToMarkdown(homeHtml);
    if (markdown.trim()) {
      const abstract = await summarizePage(markdown);
      if (abstract && abstract !== 'no content') {
        abstracts.push(abstract);
        console.log(`    ✅ Got homepage abstract`);
      }
    }
  } else {
    console.log(`    🔄 Scraping ${filteredLinks.length} sub-pages`);
    // Step 3: Scrape and summarize sub-pages
    for (const path of filteredLinks) {
      const fullUrl = new URL(path, websiteUrl).href;
      const pageHtml = await fetchUrl(fullUrl);
      if (pageHtml) {
        const markdown = htmlToMarkdown(pageHtml);
        if (markdown.trim()) {
          const abstract = await summarizePage(markdown);
          if (abstract && abstract !== 'no content') {
            abstracts.push(abstract);
          }
        }
      }
    }
    console.log(`    ✅ Got ${abstracts.length} abstracts`);
  }

  if (abstracts.length === 0) {
    console.log(`    ❌ No content to summarize`);
    return { success: false, error: 'No content found on website' };
  }

  // Step 4: Generate icebreaker
  console.log(`    🔄 Generating icebreaker...`);
  const icebreaker = await generateIcebreaker(firstName, lastName, headline, abstracts);

  if (icebreaker) {
    console.log(`    ✅ Generated icebreaker`);
    return {
      success: true,
      icebreaker,
      lead: {
        firstName,
        lastName,
        email: lead.email,
        website: websiteUrl,
        title: headline,
        companyName
      }
    };
  } else {
    console.log(`    ❌ Failed to generate icebreaker`);
    return { success: false, error: 'Failed to generate icebreaker' };
  }
}

// API Endpoints

/**
 * POST /api/generate-icebreaker
 * Generate icebreaker for a single lead
 */
app.post('/api/generate-icebreaker', async (req, res) => {
  try {
    const { lead } = req.body;

    if (!lead || !lead.firstName || !lead.email || !lead.website) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: firstName, email, website'
      });
    }

    console.log(`\n${'='.repeat(60)}`);
    console.log(`📧 New icebreaker request: ${lead.firstName} ${lead.lastName}`);
    console.log(`${'='.repeat(60)}`);

    const result = await processLead(lead);
    res.json(result);
  } catch (error) {
    console.error('Error processing lead:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/health
 * Health check endpoint
 */
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', message: 'Icebreaker API is running' });
});

// Start server
app.listen(PORT, () => {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`🚀 Icebreaker API Server running on http://localhost:${PORT}`);
  console.log(`${'='.repeat(60)}\n`);
});

