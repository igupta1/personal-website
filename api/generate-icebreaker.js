/**
 * Vercel Serverless Function for Cold Email Icebreaker Generation
 */

import OpenAI from 'openai';
import axios from 'axios';
import * as cheerio from 'cheerio';
import TurndownService from 'turndown';

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// Initialize Turndown for HTML to Markdown conversion
const turndownService = new TurndownService({
  headingStyle: 'atx',
  codeBlockStyle: 'fenced'
});

// Configuration
const MAX_LINKS_PER_SITE = 3;
const TIMEOUT = 25000; // 25 seconds (Vercel has 30s limit)

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
    console.log(`Failed to fetch: ${url}`);
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
 * Filter and normalize links
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

    if (link.startsWith('/')) {
      normalized.push(link);
    } else if (link.startsWith('http://') || link.startsWith('https://')) {
      try {
        const parsed = new URL(link);
        if (normalizeDomain(parsed.hostname) === normalizeDomain(baseParsed.hostname)) {
          let path = parsed.pathname;
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

  const filtered = normalized.filter(link => link.startsWith('/'));
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
          content: markdownContent.slice(0, 10000)
        }
      ],
      response_format: { type: 'json_object' },
      temperature: 1.0
    });

    const result = JSON.parse(response.choices[0].message.content);
    return result.abstract || 'no content';
  } catch (error) {
    console.log(`Summarize error: ${error.message}`);
    return 'no content';
  }
}

/**
 * Generate icebreaker using GPT
 */
async function generateIcebreakerText(firstName, lastName, headline, abstracts) {
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
    console.log(`Icebreaker error: ${error.message}`);
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

  console.log(`Processing: ${firstName} ${lastName} - ${websiteUrl}`);

  if (!websiteUrl.startsWith('http')) {
    websiteUrl = 'https://' + websiteUrl;
  }

  const homeHtml = await fetchUrl(websiteUrl);
  if (!homeHtml) {
    return { success: false, error: 'Failed to fetch website' };
  }

  const allLinks = extractLinks(homeHtml);
  const filteredLinks = filterAndNormalizeLinks(allLinks, websiteUrl);
  const abstracts = [];

  if (filteredLinks.length === 0) {
    const markdown = htmlToMarkdown(homeHtml);
    if (markdown.trim()) {
      const abstract = await summarizePage(markdown);
      if (abstract && abstract !== 'no content') {
        abstracts.push(abstract);
      }
    }
  } else {
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
  }

  if (abstracts.length === 0) {
    return { success: false, error: 'No content found on website' };
  }

  const icebreaker = await generateIcebreakerText(firstName, lastName, headline, abstracts);

  if (icebreaker) {
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
    return { success: false, error: 'Failed to generate icebreaker' };
  }
}

/**
 * Vercel Serverless Handler
 */
export default async function handler(req, res) {
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

  try {
    const { lead } = req.body;

    if (!lead || !lead.firstName || !lead.email || !lead.website) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: firstName, email, website'
      });
    }

    console.log(`New icebreaker request: ${lead.firstName} ${lead.lastName}`);

    const result = await processLead(lead);
    res.json(result);
  } catch (error) {
    console.error('Error processing lead:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

