/**
 * API endpoint for Marketing Readiness Scorecard
 * Runs 7 marketing checks on a domain and generates a Gemini summary
 */

const cheerio = require('cheerio');

// Rate limiting (in-memory, approximate on Vercel)
const rateLimitMap = new Map();

function checkRateLimit(ip) {
  const now = Date.now();
  const entry = rateLimitMap.get(ip);
  if (!entry || now > entry.resetAt) {
    rateLimitMap.set(ip, { count: 1, resetAt: now + 3600000 });
    return { allowed: true, remaining: 9 };
  }
  if (entry.count >= 10) {
    const retryAfter = Math.ceil((entry.resetAt - now) / 1000);
    return { allowed: false, retryAfter };
  }
  entry.count++;
  return { allowed: true, remaining: 10 - entry.count };
}

function validateAndCleanDomain(input) {
  if (!input || typeof input !== 'string') return null;
  let domain = input.trim().toLowerCase();
  domain = domain.replace(/^https?:\/\//, '');
  domain = domain.replace(/^www\./, '');
  domain = domain.replace(/\/.*$/, '');
  domain = domain.replace(/:.*$/, '');
  if (!/^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$/.test(domain)) {
    return null;
  }
  return domain;
}

// ─── HTML Fetcher ───

const CHROME_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

async function fetchHtml(domain) {
  const url = `https://${domain}`;

  // First attempt with Chrome UA
  try {
    const response = await fetch(url, {
      headers: { 'User-Agent': CHROME_UA, 'Accept': 'text/html,application/xhtml+xml' },
      redirect: 'follow',
      signal: AbortSignal.timeout(10000),
    });

    if (response.ok) {
      const html = await response.text();
      const headers = Object.fromEntries(response.headers.entries());
      return { html, headers, finalUrl: response.url, error: null };
    }

    // If blocked, try with Googlebot UA
    if (response.status === 403 || response.status === 429) {
      const retryResponse = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)' },
        redirect: 'follow',
        signal: AbortSignal.timeout(10000),
      });
      if (retryResponse.ok) {
        const html = await retryResponse.text();
        const headers = Object.fromEntries(retryResponse.headers.entries());
        return { html, headers, finalUrl: retryResponse.url, error: null };
      }
    }

    return { html: null, headers: null, finalUrl: null, error: `HTTP ${response.status}` };
  } catch (error) {
    // Try HTTP fallback
    try {
      const httpResponse = await fetch(`http://${domain}`, {
        headers: { 'User-Agent': CHROME_UA },
        redirect: 'follow',
        signal: AbortSignal.timeout(8000),
      });
      if (httpResponse.ok) {
        const html = await httpResponse.text();
        const headers = Object.fromEntries(httpResponse.headers.entries());
        return { html, headers, finalUrl: httpResponse.url, error: null };
      }
    } catch (e) { /* HTTP fallback also failed */ }

    return { html: null, headers: null, finalUrl: null, error: error.message };
  }
}

// ─── Check 1: Page Speed & Core Web Vitals ───

async function checkPageSpeed(domain) {
  const apiKey = process.env.PAGESPEED_API_KEY;
  const baseUrl = 'https://www.googleapis.com/pagespeedonline/v5/runPagespeed';

  async function runStrategy(strategy) {
    try {
      let url = `${baseUrl}?url=https://${encodeURIComponent(domain)}&strategy=${strategy}&category=performance`;
      if (apiKey) url += `&key=${apiKey}`;

      const response = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!response.ok) {
        return { error: `PageSpeed API returned ${response.status}` };
      }

      const data = await response.json();
      const lhr = data.lighthouseResult;
      if (!lhr) return { error: 'No Lighthouse data returned' };

      const audits = lhr.audits || {};
      const score = Math.round((lhr.categories?.performance?.score || 0) * 100);

      const metrics = {
        performanceScore: score,
        fcp: audits['first-contentful-paint']?.displayValue || null,
        lcp: audits['largest-contentful-paint']?.displayValue || null,
        cls: audits['cumulative-layout-shift']?.displayValue || null,
        tbt: audits['total-blocking-time']?.displayValue || null,
        speedIndex: audits['speed-index']?.displayValue || null,
        fcpMs: audits['first-contentful-paint']?.numericValue || null,
        lcpMs: audits['largest-contentful-paint']?.numericValue || null,
        clsValue: audits['cumulative-layout-shift']?.numericValue || null,
        tbtMs: audits['total-blocking-time']?.numericValue || null,
      };

      // Extract top opportunities
      const opportunities = [];
      const opportunityAudits = Object.values(audits).filter(
        a => a.details?.type === 'opportunity' && a.details?.overallSavingsMs > 0
      );
      opportunityAudits
        .sort((a, b) => (b.details.overallSavingsMs || 0) - (a.details.overallSavingsMs || 0))
        .slice(0, 5)
        .forEach(a => {
          opportunities.push({
            title: a.title,
            savings: `${(a.details.overallSavingsMs / 1000).toFixed(1)}s`,
          });
        });

      return { ...metrics, opportunities };
    } catch (error) {
      return { error: error.message };
    }
  }

  try {
    const [mobile, desktop] = await Promise.allSettled([
      runStrategy('mobile'),
      runStrategy('desktop'),
    ]);

    const mobileResult = mobile.status === 'fulfilled' ? mobile.value : { error: 'Check failed' };
    const desktopResult = desktop.status === 'fulfilled' ? desktop.value : { error: 'Check failed' };

    const issues = [];
    if (mobileResult.performanceScore !== undefined) {
      if (mobileResult.performanceScore < 50) {
        issues.push(`Your mobile performance score is ${mobileResult.performanceScore} out of 100 — most competitive sites aim for 80+`);
      }
      if (mobileResult.lcpMs && mobileResult.lcpMs > 4000) {
        issues.push(`Your main content takes ${mobileResult.lcp} to appear on mobile — visitors expect this in under 2.5 seconds`);
      }
    }
    if (desktopResult.performanceScore !== undefined && desktopResult.performanceScore < 50) {
      issues.push(`Desktop performance score is ${desktopResult.performanceScore} out of 100`);
    }

    const plainSummary = issues.length > 0
      ? issues.join('. ') + '. Slow load times directly correlate with higher bounce rates and lost leads.'
      : mobileResult.performanceScore !== undefined
        ? `Your website loads well with a mobile score of ${mobileResult.performanceScore} and desktop score of ${desktopResult.performanceScore || 'N/A'}. Fast sites convert more visitors into customers.`
        : 'Unable to analyze page speed at this time.';

    return {
      status: mobileResult.error && desktopResult.error ? 'error' : 'success',
      mobile: mobileResult,
      desktop: desktopResult,
      plainSummary,
      errorMessage: mobileResult.error && desktopResult.error ? mobileResult.error : undefined,
    };
  } catch (error) {
    return { status: 'error', errorMessage: `Page speed check failed: ${error.message}` };
  }
}

// ─── Check 2: Mobile Responsiveness ───

function checkMobileResponsiveness(pagespeedResult, $) {
  try {
    const results = {
      viewportMeta: false,
      tapTargets: { passed: null, details: null },
      fontSize: { passed: null, details: null },
      contentWidth: { passed: null, details: null },
    };

    // Try to extract from PageSpeed mobile data
    const mobile = pagespeedResult?.mobile;
    if (mobile && !mobile.error) {
      // These come from Lighthouse audits if available in the raw data
      // Viewport is also checkable via HTML
    }

    // Check viewport meta from HTML (always do this as fallback/confirmation)
    if ($) {
      const viewport = $('meta[name="viewport"]').attr('content');
      results.viewportMeta = !!viewport;
      results.viewportContent = viewport || null;
    }

    // Extract mobile-specific audits from PageSpeed if available
    if (mobile && !mobile.error) {
      results.mobileScore = mobile.performanceScore;
    }

    const issues = [];
    if (!results.viewportMeta) {
      issues.push('No viewport meta tag found — the site may not be configured for mobile devices');
    }
    if (mobile && mobile.performanceScore !== undefined && mobile.performanceScore < 40) {
      issues.push(`Mobile performance is very low (${mobile.performanceScore}/100), suggesting the site is not optimized for mobile users`);
    }

    results.plainSummary = issues.length === 0
      ? 'The site appears to be configured for mobile devices with a viewport meta tag present.' + (mobile?.performanceScore ? ` Mobile performance score is ${mobile.performanceScore}/100.` : '')
      : `${issues.join('. ')}. Over 60% of web traffic is mobile — poor mobile experience directly costs leads.`;

    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `Mobile check failed: ${error.message}` };
  }
}

// ─── Check 3: SEO Fundamentals ───

async function checkSeoFundamentals(domain, $) {
  try {
    const results = {
      title: { found: false, value: null, length: 0 },
      metaDescription: { found: false, value: null, length: 0 },
      h1: { found: false, count: 0, values: [] },
      headingHierarchy: [],
      canonical: { found: false, value: null },
      robotsMeta: { found: false, value: null, blocking: false },
      lang: null,
      robotsTxt: { found: false, error: null },
      sitemap: { found: false, urlCount: null, error: null },
      schemaMarkup: { found: false, types: [] },
    };

    if ($) {
      // Title
      const title = $('title').first().text().trim();
      if (title) {
        results.title = { found: true, value: title, length: title.length };
      }

      // Meta description
      const metaDesc = $('meta[name="description"]').attr('content');
      if (metaDesc) {
        results.metaDescription = { found: true, value: metaDesc.trim(), length: metaDesc.trim().length };
      }

      // H1 tags
      const h1s = $('h1');
      results.h1 = {
        found: h1s.length > 0,
        count: h1s.length,
        values: h1s.map((_, el) => $(el).text().trim()).get().slice(0, 5),
      };

      // Heading hierarchy
      ['h1', 'h2', 'h3', 'h4', 'h5', 'h6'].forEach(tag => {
        const count = $(tag).length;
        if (count > 0) results.headingHierarchy.push({ tag, count });
      });

      // Canonical
      const canonical = $('link[rel="canonical"]').attr('href');
      if (canonical) {
        results.canonical = { found: true, value: canonical };
      }

      // Robots meta
      const robotsMeta = $('meta[name="robots"]').attr('content');
      if (robotsMeta) {
        results.robotsMeta = {
          found: true,
          value: robotsMeta,
          blocking: robotsMeta.includes('noindex') || robotsMeta.includes('nofollow'),
        };
      }

      // Lang
      results.lang = $('html').attr('lang') || null;

      // Schema markup (JSON-LD)
      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const json = JSON.parse($(el).html());
          const type = json['@type'];
          if (type) {
            const types = Array.isArray(type) ? type : [type];
            types.forEach(t => {
              if (!results.schemaMarkup.types.includes(t)) {
                results.schemaMarkup.types.push(t);
              }
            });
          }
        } catch (e) { /* invalid JSON-LD */ }
      });
      results.schemaMarkup.found = results.schemaMarkup.types.length > 0;
    }

    // Check robots.txt
    try {
      const robotsResponse = await fetch(`https://${domain}/robots.txt`, {
        signal: AbortSignal.timeout(5000),
        headers: { 'User-Agent': CHROME_UA },
      });
      results.robotsTxt.found = robotsResponse.ok && robotsResponse.headers.get('content-type')?.includes('text');
    } catch (e) {
      results.robotsTxt.error = e.message;
    }

    // Check sitemap.xml
    try {
      const sitemapResponse = await fetch(`https://${domain}/sitemap.xml`, {
        signal: AbortSignal.timeout(5000),
        headers: { 'User-Agent': CHROME_UA },
      });
      if (sitemapResponse.ok) {
        const sitemapText = await sitemapResponse.text();
        results.sitemap.found = sitemapText.includes('<urlset') || sitemapText.includes('<sitemapindex');
        if (results.sitemap.found) {
          const urlMatches = sitemapText.match(/<loc>/g);
          results.sitemap.urlCount = urlMatches ? urlMatches.length : 0;
        }
      }
    } catch (e) {
      results.sitemap.error = e.message;
    }

    // Plain-English summary
    const issues = [];
    if (!results.title.found) issues.push('Missing a title tag');
    else if (results.title.length < 30 || results.title.length > 70) issues.push(`Title tag length is ${results.title.length} characters (ideal: 50-60)`);
    if (!results.metaDescription.found) issues.push('Missing a meta description — Google will guess what to show in search results');
    else if (results.metaDescription.length < 120 || results.metaDescription.length > 170) issues.push(`Meta description length is ${results.metaDescription.length} characters (ideal: 150-160)`);
    if (!results.h1.found) issues.push('No H1 heading found on the homepage');
    else if (results.h1.count > 1) issues.push(`${results.h1.count} H1 tags found (should be exactly 1)`);
    if (!results.sitemap.found) issues.push('No sitemap.xml found — makes it harder for search engines to discover all pages');
    if (!results.schemaMarkup.found) issues.push('No structured data (schema markup) found — missing an opportunity to enhance search result appearance');
    if (results.robotsMeta.blocking) issues.push('The robots meta tag is blocking search engine indexing');

    results.plainSummary = issues.length === 0
      ? 'SEO fundamentals look solid. The homepage has proper title, meta description, heading structure, and sitemap.'
      : `${issues.join('. ')}. These are foundational SEO fixes that any marketing agency would address in the first week.`;

    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `SEO check failed: ${error.message}` };
  }
}

// ─── Check 4: Social & Open Graph ───

function checkSocialOpenGraph($) {
  try {
    const ogTags = ['og:title', 'og:description', 'og:image', 'og:url', 'og:type'];
    const twitterTags = ['twitter:card', 'twitter:title', 'twitter:description', 'twitter:image'];

    const results = { og: {}, twitter: {} };

    if ($) {
      ogTags.forEach(tag => {
        const value = $(`meta[property="${tag}"]`).attr('content') || null;
        results.og[tag] = { present: !!value, value };
      });

      twitterTags.forEach(tag => {
        const value = $(`meta[name="${tag}"]`).attr('content') || null;
        results.twitter[tag] = { present: !!value, value };
      });
    }

    const ogPresent = Object.values(results.og).filter(t => t.present).length;
    const twitterPresent = Object.values(results.twitter).filter(t => t.present).length;
    const totalPresent = ogPresent + twitterPresent;
    const totalTags = ogTags.length + twitterTags.length;

    const issues = [];
    if (!results.og['og:title']?.present) issues.push('Missing og:title');
    if (!results.og['og:description']?.present) issues.push('Missing og:description');
    if (!results.og['og:image']?.present) issues.push('Missing og:image — social shares will have no preview image');
    if (twitterPresent === 0) issues.push('No Twitter Card tags configured');

    results.plainSummary = issues.length === 0
      ? `Social sharing is well configured with ${totalPresent} of ${totalTags} tags present. Links shared on social media will display with rich previews.`
      : `${issues.join('. ')}. When someone shares this website on LinkedIn or Facebook, the share will look like a bare URL with no context — this drastically reduces click-through rates from social media.`;

    results.ogPresent = ogPresent;
    results.twitterPresent = twitterPresent;
    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `Social/OG check failed: ${error.message}` };
  }
}

// ─── Check 5: Google Business Profile Detection ───

function checkGoogleBusinessProfile($) {
  try {
    const results = {
      mapsEmbed: false,
      mapsLink: false,
      localBusinessSchema: false,
      napDetected: false,
      schemaTypes: [],
    };

    if ($) {
      // Check for Google Maps iframe
      const iframes = $('iframe').map((_, el) => $(el).attr('src') || '').get();
      results.mapsEmbed = iframes.some(src =>
        src.includes('google.com/maps') || src.includes('maps.google.com')
      );

      // Check for links to Google Maps
      const links = $('a').map((_, el) => $(el).attr('href') || '').get();
      results.mapsLink = links.some(href =>
        href.includes('google.com/maps') || href.includes('maps.google.com') || href.includes('goo.gl/maps')
      );

      // Check for LocalBusiness schema
      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const json = JSON.parse($(el).html());
          const type = json['@type'];
          if (type) {
            const types = Array.isArray(type) ? type : [type];
            types.forEach(t => {
              if (!results.schemaTypes.includes(t)) results.schemaTypes.push(t);
              if (t === 'LocalBusiness' || t.includes('LocalBusiness') ||
                  t === 'Organization' || t === 'Store' || t === 'Restaurant' ||
                  t === 'MedicalBusiness' || t === 'LegalService' ||
                  t === 'FinancialService' || t === 'RealEstateAgent') {
                results.localBusinessSchema = true;
              }
            });
          }
        } catch (e) { /* invalid JSON */ }
      });

      // Basic NAP detection — look for address-like patterns
      const bodyText = $('body').text();
      const hasPhone = /(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/.test(bodyText);
      const hasAddress = /\d+\s+[\w\s]+(?:street|st|avenue|ave|road|rd|boulevard|blvd|drive|dr|lane|ln|way|court|ct|place|pl)\b/i.test(bodyText);
      results.napDetected = hasPhone && hasAddress;
    }

    const signals = [];
    if (results.mapsEmbed) signals.push('Google Maps embed found on the site');
    if (results.mapsLink) signals.push('Link to Google Maps found');
    if (results.localBusinessSchema) signals.push('LocalBusiness or similar schema markup found');
    if (results.napDetected) signals.push('Business name, address, and phone number detected on the page');

    results.plainSummary = signals.length > 0
      ? `${signals.join('. ')}. ${results.localBusinessSchema ? 'The structured data helps Google connect the website to the business listing.' : 'However, no LocalBusiness schema markup was found — adding this would help Google connect the website to the business listing for local search.'}`
      : "We couldn't confirm whether this business has a Google Business Profile set up. There's no Google Maps embed, no map links, and no LocalBusiness schema markup on the homepage. If the business serves local customers, this is a significant gap for local search visibility.";

    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `Google Business check failed: ${error.message}` };
  }
}

// ─── Check 6: Conversion & CTA Analysis ───

function checkConversionCta($) {
  try {
    const results = {
      forms: { count: 0 },
      ctas: [],
      phoneNumbers: [],
      emailAddresses: [],
      chatWidget: { found: false, provider: null },
      analytics: [],
    };

    if ($) {
      // Forms
      results.forms.count = $('form').length;

      // CTAs — buttons and links with action-oriented text
      const ctaPatterns = /\b(contact|get started|free quote|schedule|book|call|demo|sign up|subscribe|buy|get in touch|request|learn more|try free|start free|get pricing|talk to|reach out|let's talk)\b/i;
      $('a, button').each((_, el) => {
        const text = $(el).text().trim();
        if (text && ctaPatterns.test(text) && text.length < 60) {
          if (!results.ctas.some(c => c.text === text)) {
            results.ctas.push({ text, tag: el.tagName?.toLowerCase() || el.name });
          }
        }
      });
      results.ctas = results.ctas.slice(0, 15);

      // Phone numbers
      $('a[href^="tel:"]').each((_, el) => {
        const phone = $(el).attr('href').replace('tel:', '').trim();
        if (phone && !results.phoneNumbers.includes(phone)) {
          results.phoneNumbers.push(phone);
        }
      });
      // Also check body text for phone patterns
      const bodyHtml = $.html();
      const phoneRegex = /(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
      const phoneMatches = $('body').text().match(phoneRegex);
      if (phoneMatches) {
        phoneMatches.forEach(p => {
          const cleaned = p.trim();
          if (!results.phoneNumbers.includes(cleaned)) {
            results.phoneNumbers.push(cleaned);
          }
        });
      }
      results.phoneNumbers = results.phoneNumbers.slice(0, 5);

      // Email addresses
      $('a[href^="mailto:"]').each((_, el) => {
        const email = $(el).attr('href').replace('mailto:', '').split('?')[0].trim();
        if (email && !results.emailAddresses.includes(email)) {
          results.emailAddresses.push(email);
        }
      });
      results.emailAddresses = results.emailAddresses.slice(0, 5);

      // Chat widgets
      const chatProviders = [
        { name: 'Intercom', patterns: ['intercom.io', 'widget.intercom.io', 'Intercom'] },
        { name: 'Drift', patterns: ['drift.com', 'js.driftt.com'] },
        { name: 'HubSpot Chat', patterns: ['js.hs-scripts.com', 'js.usemessages.com'] },
        { name: 'Crisp', patterns: ['crisp.chat', 'client.crisp.chat'] },
        { name: 'tawk.to', patterns: ['tawk.to', 'embed.tawk.to'] },
        { name: 'Zendesk', patterns: ['zopim.com', 'zendesk.com/web_widget'] },
        { name: 'LiveChat', patterns: ['livechatinc.com', 'cdn.livechatinc.com'] },
      ];
      for (const provider of chatProviders) {
        if (provider.patterns.some(p => bodyHtml.includes(p))) {
          results.chatWidget = { found: true, provider: provider.name };
          break;
        }
      }

      // Analytics & tracking
      const analyticsProviders = [
        { name: 'Google Analytics (GA4)', patterns: ['gtag/js?id=G-', 'googletagmanager.com/gtag'] },
        { name: 'Google Analytics (UA)', patterns: ['google-analytics.com/analytics.js', 'google-analytics.com/ga.js'] },
        { name: 'Google Tag Manager', patterns: ['googletagmanager.com/gtm.js', 'googletagmanager.com/ns.html'] },
        { name: 'Facebook Pixel', patterns: ['connect.facebook.net/en_US/fbevents.js', 'facebook.com/tr?'] },
        { name: 'LinkedIn Insight Tag', patterns: ['snap.licdn.com/li.lms-analytics'] },
        { name: 'HubSpot Tracking', patterns: ['js.hs-analytics.net', 'js.hs-scripts.com'] },
        { name: 'Hotjar', patterns: ['static.hotjar.com', 'hotjar.com'] },
        { name: 'Mixpanel', patterns: ['cdn.mxpnl.com', 'mixpanel.com'] },
        { name: 'Segment', patterns: ['cdn.segment.com', 'segment.io'] },
      ];
      for (const provider of analyticsProviders) {
        if (provider.patterns.some(p => bodyHtml.includes(p))) {
          results.analytics.push(provider.name);
        }
      }
    }

    const issues = [];
    if (results.forms.count === 0) issues.push('No contact forms found on the homepage');
    if (results.ctas.length === 0) issues.push('No clear calls-to-action found');
    if (results.phoneNumbers.length === 0 && results.emailAddresses.length === 0) {
      issues.push('No visible contact information (phone or email)');
    }
    if (!results.chatWidget.found) issues.push('No live chat or chatbot widget detected');
    if (results.analytics.length === 0) issues.push('No analytics tracking detected — no visibility into who visits the site');

    results.plainSummary = issues.length === 0
      ? `The homepage has ${results.forms.count} form(s), ${results.ctas.length} call-to-action elements, ${results.chatWidget.found ? `a ${results.chatWidget.provider} chat widget` : 'chat support'}, and ${results.analytics.length} analytics tool(s). The site is set up to capture and track visitors.`
      : `${issues.join('. ')}. ${results.analytics.length === 0 ? "Without analytics, there's no way to know how many people visit or where they come from." : ''} Every missing conversion element is a potential lead that slips away.`;

    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `Conversion/CTA check failed: ${error.message}` };
  }
}

// ─── Check 7: Technology & CMS Detection ───

function checkTechCms($, headers) {
  try {
    const results = {
      cms: null,
      hosting: null,
      frameworks: [],
      marketingTools: [],
    };

    const html = $ ? $.html() : '';
    const headersLower = {};
    if (headers) {
      for (const [key, value] of Object.entries(headers)) {
        headersLower[key.toLowerCase()] = value;
      }
    }

    // CMS Detection
    const cmsChecks = [
      { name: 'WordPress', patterns: ['wp-content/', 'wp-includes/', 'wp-json', '/xmlrpc.php'], headerPatterns: ['WordPress'] },
      { name: 'Wix', patterns: ['static.wixstatic.com', 'wix.com', '_wix_browser_sess'], headerPatterns: ['x-wix-'] },
      { name: 'Squarespace', patterns: ['squarespace.com', 'static1.squarespace.com', 'squarespace-cdn.com'], headerPatterns: [] },
      { name: 'Shopify', patterns: ['cdn.shopify.com', 'myshopify.com', 'Shopify.theme'], headerPatterns: ['x-shopid', 'x-shopify'] },
      { name: 'Webflow', patterns: ['webflow.com', 'assets.website-files.com', 'wf-page'], headerPatterns: [] },
      { name: 'Drupal', patterns: ['sites/default/files', 'drupal.js', 'Drupal.settings'], headerPatterns: ['X-Drupal'] },
      { name: 'Joomla', patterns: ['/media/jui/', 'Joomla!', '/administrator/'], headerPatterns: [] },
      { name: 'HubSpot CMS', patterns: ['hs-scripts.com', '.hubspot.com', 'hbspt.forms'], headerPatterns: [] },
    ];

    for (const cms of cmsChecks) {
      if (cms.patterns.some(p => html.includes(p))) {
        results.cms = cms.name;
        break;
      }
      if (cms.headerPatterns.some(p => {
        return Object.entries(headersLower).some(([k, v]) =>
          k.includes(p.toLowerCase()) || (v && v.toString().toLowerCase().includes(p.toLowerCase()))
        );
      })) {
        results.cms = cms.name;
        break;
      }
    }

    // Hosting hints
    results.hosting = headersLower['x-powered-by'] || headersLower['server'] || null;

    // JavaScript frameworks
    const frameworkChecks = [
      { name: 'React', patterns: ['__react', 'react-root', 'data-reactroot', '_reactRootContainer'] },
      { name: 'Next.js', patterns: ['__next', '_next/static', '__NEXT_DATA__'] },
      { name: 'Vue.js', patterns: ['__vue', 'vue-app', 'data-v-', 'Vue.js'] },
      { name: 'Nuxt.js', patterns: ['__nuxt', '_nuxt/', '__NUXT__'] },
      { name: 'Angular', patterns: ['ng-version', 'ng-app', 'angular.js', 'angular.min.js'] },
      { name: 'jQuery', patterns: ['jquery.min.js', 'jquery.js', 'jQuery'] },
      { name: 'Bootstrap', patterns: ['bootstrap.min.css', 'bootstrap.min.js', 'bootstrap.css'] },
      { name: 'Tailwind CSS', patterns: ['tailwindcss', 'tailwind.min.css'] },
    ];

    for (const fw of frameworkChecks) {
      if (fw.patterns.some(p => html.includes(p))) {
        results.frameworks.push(fw.name);
      }
    }

    // Marketing tools
    const toolChecks = [
      { name: 'HubSpot', patterns: ['js.hs-scripts.com', 'hs-analytics.net', 'hubspot.com'] },
      { name: 'Mailchimp', patterns: ['mailchimp.com', 'chimpstatic.com', 'mc.us'] },
      { name: 'ActiveCampaign', patterns: ['activecampaign.com', 'trackcmp.net'] },
      { name: 'Klaviyo', patterns: ['klaviyo.com', 'static.klaviyo.com'] },
      { name: 'Marketo', patterns: ['marketo.net', 'munchkin.js', 'mktoForms'] },
      { name: 'Pardot', patterns: ['pardot.com', 'pi.pardot.com'] },
      { name: 'Salesforce', patterns: ['force.com', 'salesforce.com'] },
      { name: 'Google Ads', patterns: ['googleads.g.doubleclick.net', 'googlesyndication.com'] },
      { name: 'Optimizely', patterns: ['optimizely.com', 'cdn.optimizely.com'] },
    ];

    for (const tool of toolChecks) {
      if (tool.patterns.some(p => html.includes(p))) {
        results.marketingTools.push(tool.name);
      }
    }

    const parts = [];
    if (results.cms) parts.push(`This site runs on ${results.cms}`);
    else parts.push('The CMS could not be determined (may be custom-built)');

    if (results.frameworks.length > 0) parts.push(`uses ${results.frameworks.join(', ')}`);
    if (results.marketingTools.length > 0) {
      parts.push(`has ${results.marketingTools.join(', ')} installed — suggesting some marketing automation is in place`);
    } else {
      parts.push('no marketing automation tools were detected');
    }

    results.plainSummary = parts.join(', ') + '.';

    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `Technology check failed: ${error.message}` };
  }
}

// ─── Gemini Summary ───

async function generateGeminiSummary(domain, checks) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) {
    return { executiveSummary: null, quickWins: null, missedOpportunities: null, error: 'Gemini API key not configured' };
  }

  try {
    const prompt = `You are a senior marketing consultant writing a report for a non-technical small business owner. Based on the following marketing audit results for ${domain}, write:

1. **Executive Summary** (3-4 sentences): What is the overall marketing readiness of this website? What are the biggest opportunities being missed? Frame this around business impact — leads, revenue, competitive positioning — not technical jargon.

2. **3 Quick Wins**: Three specific, high-impact improvements that could be implemented within a week. For each one, explain: what the issue is, why it costs the business money or leads, and what the fix looks like in plain terms. Prioritize by impact — lead the list with whatever would move the needle most.

3. **Missed Opportunities** (2-3 sentences): Based on the full scan, what is this business leaving on the table? Think about it from the perspective of their competitors who ARE doing these things.

Do NOT use letter grades, numerical scores, or pass/fail language. Do NOT be condescending or use scare tactics. Write as if you are a trusted marketing advisor having a direct conversation with the business owner. Be specific — reference the actual findings from their scan. Use plain English throughout.

Respond in valid JSON format with this structure:
{
  "executiveSummary": "...",
  "quickWins": [
    { "title": "...", "description": "..." },
    { "title": "...", "description": "..." },
    { "title": "...", "description": "..." }
  ],
  "missedOpportunities": "..."
}

Scan results:
${JSON.stringify(checks, null, 2)}`;

    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: 0.7,
            maxOutputTokens: 2000,
            responseMimeType: 'application/json',
          },
        }),
        signal: AbortSignal.timeout(20000),
      }
    );

    if (!response.ok) {
      return { executiveSummary: null, quickWins: null, missedOpportunities: null, error: `Gemini API error: ${response.status}` };
    }

    const data = await response.json();
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!text) {
      return { executiveSummary: null, quickWins: null, missedOpportunities: null, error: 'Empty Gemini response' };
    }

    const parsed = JSON.parse(text);
    return {
      executiveSummary: parsed.executiveSummary || null,
      quickWins: parsed.quickWins || null,
      missedOpportunities: parsed.missedOpportunities || null,
    };
  } catch (error) {
    return { executiveSummary: null, quickWins: null, missedOpportunities: null, error: `Summary generation failed: ${error.message}` };
  }
}

// ─── Main Handler ───

module.exports = async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Credentials', true);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS,POST');
  res.setHeader('Access-Control-Allow-Headers', 'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ success: false, error: 'Method not allowed' });
  }

  try {
    const { domain: rawDomain } = req.body || {};
    const domain = validateAndCleanDomain(rawDomain);

    if (!domain) {
      return res.status(400).json({
        success: false,
        error: 'Invalid domain. Please enter a valid domain like "example.com"',
      });
    }

    // Rate limiting
    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || 'unknown';
    const rateCheck = checkRateLimit(ip);
    if (!rateCheck.allowed) {
      res.setHeader('Retry-After', rateCheck.retryAfter);
      return res.status(429).json({
        success: false,
        error: `Too many scans. Please try again in ${Math.ceil(rateCheck.retryAfter / 60)} minutes.`,
      });
    }

    const startTime = Date.now();

    // Phase 1: Fetch HTML and PageSpeed in parallel
    const [pageSpeedResult, htmlResult] = await Promise.allSettled([
      checkPageSpeed(domain),
      fetchHtml(domain),
    ]);

    const pageSpeed = pageSpeedResult.status === 'fulfilled' ? pageSpeedResult.value : { status: 'error', errorMessage: 'PageSpeed check crashed' };
    const htmlData = htmlResult.status === 'fulfilled' ? htmlResult.value : { html: null, headers: null, error: 'HTML fetch crashed' };

    // Parse HTML once
    const $ = htmlData.html ? cheerio.load(htmlData.html) : null;

    // Phase 2: Run HTML-dependent checks in parallel
    const [mobileResult, seoResult, socialResult, gbpResult, conversionResult, techResult] = await Promise.allSettled([
      Promise.resolve(checkMobileResponsiveness(pageSpeed, $)),
      checkSeoFundamentals(domain, $),
      Promise.resolve(checkSocialOpenGraph($)),
      Promise.resolve(checkGoogleBusinessProfile($)),
      Promise.resolve(checkConversionCta($)),
      Promise.resolve(checkTechCms($, htmlData.headers)),
    ]);

    const checks = {
      pageSpeed,
      mobile: mobileResult.status === 'fulfilled' ? mobileResult.value : { status: 'error', errorMessage: 'Check crashed' },
      seo: seoResult.status === 'fulfilled' ? seoResult.value : { status: 'error', errorMessage: 'Check crashed' },
      social: socialResult.status === 'fulfilled' ? socialResult.value : { status: 'error', errorMessage: 'Check crashed' },
      googleBusiness: gbpResult.status === 'fulfilled' ? gbpResult.value : { status: 'error', errorMessage: 'Check crashed' },
      conversion: conversionResult.status === 'fulfilled' ? conversionResult.value : { status: 'error', errorMessage: 'Check crashed' },
      technology: techResult.status === 'fulfilled' ? techResult.value : { status: 'error', errorMessage: 'Check crashed' },
    };

    // Add HTML fetch error note if applicable
    if (htmlData.error) {
      checks.htmlFetchError = htmlData.error;
    }

    // Generate Gemini summary
    const summary = await generateGeminiSummary(domain, checks);

    const scanDuration = Date.now() - startTime;

    return res.status(200).json({
      success: true,
      domain,
      scanDate: new Date().toISOString(),
      scanDuration,
      summary,
      checks,
    });
  } catch (error) {
    console.error('Marketing scan error:', error);
    return res.status(500).json({ success: false, error: 'Internal server error during marketing scan' });
  }
};
