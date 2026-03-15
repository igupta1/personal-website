/**
 * API endpoint for IT Security Scorecard
 * Runs 5 security checks on a domain and generates a Gemini summary
 */

const dns = require('dns').promises;
const tls = require('tls');

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

// ─── Check 1: Email Authentication ───

async function checkEmailAuth(domain) {
  try {
    const results = { spf: { found: false }, dkim: { found: false, selectors: [] }, dmarc: { found: false } };

    // SPF
    try {
      const txtRecords = await dns.resolveTxt(domain);
      const flat = txtRecords.map(r => r.join(''));
      const spfRecord = flat.find(r => r.startsWith('v=spf1'));
      if (spfRecord) {
        results.spf = { found: true, record: spfRecord };
      }
    } catch (e) { /* no TXT records */ }

    // DKIM — check common selectors
    const selectors = ['default', 'google', 'selector1', 'selector2', 'k1', 'mail', 'dkim', 'smtp', 's1', 's2'];
    const dkimChecks = await Promise.allSettled(
      selectors.map(async (sel) => {
        try {
          const records = await dns.resolveTxt(`${sel}._domainkey.${domain}`);
          const flat = records.map(r => r.join(''));
          const dkimRecord = flat.find(r => r.includes('v=DKIM1'));
          if (dkimRecord) return sel;
        } catch (e) { /* selector not found */ }
        return null;
      })
    );
    results.dkim.selectors = dkimChecks
      .filter(r => r.status === 'fulfilled' && r.value)
      .map(r => r.value);
    results.dkim.found = results.dkim.selectors.length > 0;

    // DMARC
    try {
      const dmarcRecords = await dns.resolveTxt(`_dmarc.${domain}`);
      const flat = dmarcRecords.map(r => r.join(''));
      const dmarcRecord = flat.find(r => r.startsWith('v=DMARC1'));
      if (dmarcRecord) {
        const policyMatch = dmarcRecord.match(/p=(\w+)/);
        results.dmarc = {
          found: true,
          record: dmarcRecord,
          policy: policyMatch ? policyMatch[1] : 'unknown',
        };
      }
    } catch (e) { /* no DMARC */ }

    // Plain-English summary
    const issues = [];
    if (!results.spf.found) issues.push('SPF record is missing');
    if (!results.dkim.found) issues.push('No DKIM records found on common selectors');
    if (!results.dmarc.found) issues.push('DMARC record is missing');
    else if (results.dmarc.policy === 'none') issues.push('DMARC policy is set to "none" (monitoring only, not enforcing)');

    results.plainSummary = issues.length === 0
      ? 'Email authentication is properly configured with SPF, DKIM, and DMARC records.'
      : `Email authentication issues found: ${issues.join('. ')}. This means your domain may be vulnerable to email spoofing and phishing attacks.`;

    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `Email auth check failed: ${error.message}` };
  }
}

// ─── Check 2: SSL/TLS Certificate ───

async function checkSslTls(domain) {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      resolve({ status: 'error', errorMessage: 'SSL/TLS connection timed out after 10 seconds' });
    }, 10000);

    try {
      const socket = tls.connect(443, domain, { servername: domain, rejectUnauthorized: false }, () => {
        try {
          const cert = socket.getPeerCertificate();
          const protocol = socket.getProtocol();
          const authorized = socket.authorized;

          const validFrom = new Date(cert.valid_from);
          const validTo = new Date(cert.valid_to);
          const now = new Date();
          const daysUntilExpiry = Math.floor((validTo - now) / (1000 * 60 * 60 * 24));

          const warnings = [];
          if (daysUntilExpiry < 0) warnings.push('Certificate has expired');
          else if (daysUntilExpiry <= 30) warnings.push(`Certificate expires in ${daysUntilExpiry} days`);
          if (protocol === 'TLSv1' || protocol === 'TLSv1.1') warnings.push(`Using deprecated ${protocol}`);
          if (!authorized) warnings.push('Certificate is not trusted (may be self-signed or have chain issues)');

          // Parse SAN
          const sanList = cert.subjectaltname
            ? cert.subjectaltname.split(',').map(s => s.trim().replace('DNS:', ''))
            : [];

          const result = {
            status: 'success',
            valid: daysUntilExpiry > 0 && authorized,
            issuer: cert.issuer ? (cert.issuer.O || cert.issuer.CN || 'Unknown') : 'Unknown',
            validFrom: validFrom.toISOString().split('T')[0],
            validTo: validTo.toISOString().split('T')[0],
            daysUntilExpiry,
            tlsVersion: protocol,
            subjectCN: cert.subject ? cert.subject.CN : 'Unknown',
            sanList: sanList.slice(0, 10),
            warnings,
          };

          result.plainSummary = warnings.length === 0
            ? `SSL/TLS certificate is valid and issued by ${result.issuer}. It expires on ${result.validTo} (${daysUntilExpiry} days from now). The connection uses ${protocol}.`
            : `SSL/TLS issues found: ${warnings.join('. ')}. Certificate is issued by ${result.issuer} and expires on ${result.validTo}.`;

          socket.end();
          clearTimeout(timeout);
          resolve(result);
        } catch (e) {
          socket.end();
          clearTimeout(timeout);
          resolve({ status: 'error', errorMessage: `Failed to parse certificate: ${e.message}` });
        }
      });

      socket.on('error', (err) => {
        clearTimeout(timeout);
        if (err.code === 'ECONNREFUSED') {
          resolve({
            status: 'success',
            valid: false,
            warnings: ['No HTTPS service found — the domain does not accept connections on port 443'],
            plainSummary: 'This domain does not appear to have HTTPS enabled. Visitors cannot connect securely, and browsers will show security warnings.',
          });
        } else {
          resolve({ status: 'error', errorMessage: `SSL/TLS connection failed: ${err.message}` });
        }
      });
    } catch (error) {
      clearTimeout(timeout);
      resolve({ status: 'error', errorMessage: `SSL/TLS check failed: ${error.message}` });
    }
  });
}

// ─── Check 3: Breach Exposure (HIBP) ───

async function checkBreachExposure(domain) {
  const apiKey = process.env.HIBP_API_KEY;
  if (!apiKey) {
    return {
      status: 'stubbed',
      plainSummary: 'Breach exposure checking is not yet configured. This feature will be available soon.',
    };
  }

  try {
    const response = await fetch(
      `https://haveibeenpwned.com/api/v3/breaches?domain=${encodeURIComponent(domain)}`,
      {
        headers: {
          'hibp-api-key': apiKey,
          'user-agent': 'PipelineGTM-SecurityScorecard',
        },
      }
    );

    if (response.status === 404) {
      return {
        status: 'success',
        breachCount: 0,
        breaches: [],
        plainSummary: 'No known data breaches were found associated with this domain. This is a good sign, but does not guarantee that credentials have not been compromised through other means.',
      };
    }

    if (response.status === 429) {
      return { status: 'error', errorMessage: 'Breach check rate limited. Please try again in a few minutes.' };
    }

    if (!response.ok) {
      return { status: 'error', errorMessage: `HIBP API returned status ${response.status}` };
    }

    const breaches = await response.json();
    const mapped = breaches.map(b => ({
      name: b.Name,
      date: b.BreachDate,
      dataClasses: b.DataClasses,
      pwnCount: b.PwnCount,
    }));

    const totalExposed = mapped.reduce((sum, b) => sum + (b.pwnCount || 0), 0);
    const dataTypes = [...new Set(mapped.flatMap(b => b.dataClasses || []))];

    return {
      status: 'success',
      breachCount: mapped.length,
      breaches: mapped,
      plainSummary: `Email addresses from this domain were found in ${mapped.length} known data breach${mapped.length !== 1 ? 'es' : ''}, affecting approximately ${totalExposed.toLocaleString()} accounts. Exposed data includes: ${dataTypes.slice(0, 5).join(', ')}. This means employee credentials may be circulating online and should be changed.`,
    };
  } catch (error) {
    return { status: 'error', errorMessage: `Breach check failed: ${error.message}` };
  }
}

// ─── Check 4: DNS Health ───

async function checkDnsHealth(domain) {
  try {
    const results = { nameservers: [], dnssecEnabled: false, mxRecords: [], mailProvider: 'Unknown' };

    // Nameservers
    try {
      results.nameservers = await dns.resolveNs(domain);
    } catch (e) { /* no NS records at this level, try parent */ }

    // MX records
    try {
      const mxRecords = await dns.resolveMx(domain);
      results.mxRecords = mxRecords.sort((a, b) => a.priority - b.priority).map(r => ({
        priority: r.priority,
        exchange: r.exchange,
      }));

      // Detect mail provider
      const exchanges = results.mxRecords.map(r => r.exchange.toLowerCase()).join(' ');
      if (exchanges.includes('google') || exchanges.includes('gmail')) {
        results.mailProvider = 'Google Workspace';
      } else if (exchanges.includes('outlook') || exchanges.includes('microsoft')) {
        results.mailProvider = 'Microsoft 365';
      } else if (exchanges.includes('proofpoint')) {
        results.mailProvider = 'Proofpoint';
      } else if (exchanges.includes('mimecast')) {
        results.mailProvider = 'Mimecast';
      } else if (exchanges.includes('barracuda')) {
        results.mailProvider = 'Barracuda';
      } else if (exchanges.includes('zoho')) {
        results.mailProvider = 'Zoho Mail';
      } else if (exchanges.includes('secureserver') || exchanges.includes('godaddy')) {
        results.mailProvider = 'GoDaddy';
      } else if (results.mxRecords.length > 0) {
        results.mailProvider = results.mxRecords[0].exchange;
      }
    } catch (e) { /* no MX records */ }

    // DNSSEC via Google DoH
    try {
      const dohResponse = await fetch(
        `https://dns.google/resolve?name=${encodeURIComponent(domain)}&type=A`,
        { signal: AbortSignal.timeout(5000) }
      );
      if (dohResponse.ok) {
        const dohData = await dohResponse.json();
        results.dnssecEnabled = dohData.AD === true;
      }
    } catch (e) { /* DoH check failed */ }

    // Plain-English summary
    const issues = [];
    if (results.mxRecords.length === 0) issues.push('No MX records found — email may not be configured');
    if (!results.dnssecEnabled) issues.push('DNSSEC is not enabled — the domain is vulnerable to DNS spoofing attacks');
    if (results.nameservers.length < 2) issues.push('Fewer than 2 nameservers found — limited DNS redundancy');

    results.plainSummary = issues.length === 0
      ? `DNS is healthy. The domain uses ${results.mailProvider} for email with ${results.nameservers.length} nameservers and DNSSEC enabled.`
      : `${results.mailProvider !== 'Unknown' ? `The domain uses ${results.mailProvider} for email. ` : ''}${issues.join('. ')}.`;

    return { status: 'success', ...results };
  } catch (error) {
    return { status: 'error', errorMessage: `DNS health check failed: ${error.message}` };
  }
}

// ─── Check 5: Security Headers ───

async function checkSecurityHeaders(domain) {
  try {
    const headersToCheck = [
      { key: 'strict-transport-security', name: 'Strict-Transport-Security (HSTS)' },
      { key: 'x-content-type-options', name: 'X-Content-Type-Options' },
      { key: 'x-frame-options', name: 'X-Frame-Options' },
      { key: 'content-security-policy', name: 'Content-Security-Policy' },
      { key: 'referrer-policy', name: 'Referrer-Policy' },
      { key: 'permissions-policy', name: 'Permissions-Policy' },
    ];

    let httpsRedirect = false;
    const headerResults = {};
    let fetchError = null;

    // Check HTTPS
    try {
      const response = await fetch(`https://${domain}`, {
        redirect: 'follow',
        signal: AbortSignal.timeout(8000),
      });

      for (const h of headersToCheck) {
        const value = response.headers.get(h.key);
        headerResults[h.key] = { name: h.name, present: !!value, value: value || null };
      }
    } catch (e) {
      fetchError = e.message;
      for (const h of headersToCheck) {
        headerResults[h.key] = { name: h.name, present: false, value: null };
      }
    }

    // Check HTTP → HTTPS redirect
    try {
      const httpResponse = await fetch(`http://${domain}`, {
        redirect: 'manual',
        signal: AbortSignal.timeout(5000),
      });
      const location = httpResponse.headers.get('location') || '';
      httpsRedirect = (httpResponse.status === 301 || httpResponse.status === 302) &&
        location.toLowerCase().startsWith('https');
    } catch (e) { /* HTTP check failed */ }

    const presentCount = Object.values(headerResults).filter(h => h.present).length;
    const totalCount = headersToCheck.length;

    const plainSummary = fetchError
      ? `Could not connect to the website over HTTPS to check security headers: ${fetchError}`
      : presentCount === totalCount
        ? `All ${totalCount} recommended security headers are present. ${httpsRedirect ? 'HTTP properly redirects to HTTPS.' : 'However, HTTP does not redirect to HTTPS.'}`
        : `The website is missing ${totalCount - presentCount} out of ${totalCount} recommended security headers. These are configurations that protect visitors from common attacks like clickjacking and data injection.${!httpsRedirect ? ' Additionally, HTTP does not redirect to HTTPS.' : ''}`;

    return {
      status: fetchError ? 'partial' : 'success',
      headers: headerResults,
      httpsRedirect,
      headersFound: presentCount,
      headersTotal: totalCount,
      plainSummary,
    };
  } catch (error) {
    return { status: 'error', errorMessage: `Security headers check failed: ${error.message}` };
  }
}

// ─── Gemini Summary ───

async function generateGeminiSummary(domain, checks) {
  const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_GEMINI_API_KEY;
  if (!apiKey) {
    return { executiveSummary: null, recommendations: null, error: 'Gemini API key not configured' };
  }

  try {
    const prompt = `You are an IT security advisor writing a report for a non-technical small business owner. Based on the following security scan results for ${domain}, write:

1. **Executive Summary** (3-4 sentences): What is the overall security posture of this domain? What should the business owner be most concerned about?

2. **Top 3 Recommendations**: The three most impactful things they should do immediately, written in plain English. Each recommendation should explain what the problem is, why it matters to their business (lost clients, ransomware risk, compliance, etc.), and what fixing it looks like in simple terms.

Do NOT use letter grades or scores. Do NOT be condescending. Write as if you are a trusted advisor having a conversation with the business owner. Be direct and specific — reference the actual findings from their scan.

Keep each recommendation description to 2-3 sentences maximum. Be concise.

Respond in valid JSON format with this structure:
{
  "executiveSummary": "...",
  "recommendations": [
    { "title": "...", "description": "..." },
    { "title": "...", "description": "..." },
    { "title": "...", "description": "..." }
  ]
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
            maxOutputTokens: 4000,
            responseMimeType: 'application/json',
          },
        }),
        signal: AbortSignal.timeout(30000),
      }
    );

    if (!response.ok) {
      const errText = await response.text();
      console.error('Gemini API error:', response.status, errText);
      return { executiveSummary: null, recommendations: null, error: `Gemini API error: ${response.status} - ${errText.slice(0, 200)}` };
    }

    const data = await response.json();
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!text) {
      return { executiveSummary: null, recommendations: null, error: 'Empty Gemini response' };
    }

    const parsed = JSON.parse(text);
    return {
      executiveSummary: parsed.executiveSummary || null,
      recommendations: parsed.recommendations || null,
    };
  } catch (error) {
    return { executiveSummary: null, recommendations: null, error: `Summary generation failed: ${error.message}` };
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
        error: 'Invalid domain. Please enter a valid domain like "example.com" (without http:// or www.)',
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

    // Run all checks in parallel
    const [emailAuth, sslTls, breachExposure, dnsHealth, securityHeaders] = await Promise.allSettled([
      checkEmailAuth(domain),
      checkSslTls(domain),
      checkBreachExposure(domain),
      checkDnsHealth(domain),
      checkSecurityHeaders(domain),
    ]);

    const checks = {
      emailAuth: emailAuth.status === 'fulfilled' ? emailAuth.value : { status: 'error', errorMessage: 'Check crashed unexpectedly' },
      sslTls: sslTls.status === 'fulfilled' ? sslTls.value : { status: 'error', errorMessage: 'Check crashed unexpectedly' },
      breachExposure: breachExposure.status === 'fulfilled' ? breachExposure.value : { status: 'error', errorMessage: 'Check crashed unexpectedly' },
      dnsHealth: dnsHealth.status === 'fulfilled' ? dnsHealth.value : { status: 'error', errorMessage: 'Check crashed unexpectedly' },
      securityHeaders: securityHeaders.status === 'fulfilled' ? securityHeaders.value : { status: 'error', errorMessage: 'Check crashed unexpectedly' },
    };

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
    console.error('Security scan error:', error);
    return res.status(500).json({ success: false, error: 'Internal server error during security scan' });
  }
};
