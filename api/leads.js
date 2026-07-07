// Queryable lead inventory for the outreach workflow (System B).
// GET /api/leads?industry=&niche=&city=&state=&signal_type=&freshness=&exclude_ids=&limit=
//
// Serves the full inventory blob (cfo-leads-inventory.json) filtered in
// memory. Public: it's aggregated public signals (job posts, SEC
// filings) with no contact info. Returns the spec lead shape:
//   { id, company, domain, city, state, industry, niche, signal_type,
//     freshness, signals: [{ type, date, plain_words_description }] }

const { list } = require('@vercel/blob');

const BLOB_KEY = 'cfo-leads-inventory.json';
const CACHE_HEADER = 'public, s-maxage=120, stale-while-revalidate=600';

let _cachedBlobUrl = null;

async function _resolveBlobUrl() {
  if (_cachedBlobUrl) return _cachedBlobUrl;
  const { blobs } = await list({ prefix: BLOB_KEY, limit: 1 });
  if (!blobs.length) return null;
  _cachedBlobUrl = blobs[0].url;
  return _cachedBlobUrl;
}

function norm(s) {
  return (s == null ? '' : String(s)).trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}
function enumVal(s) {
  return norm(s).replace(/ /g, '_'); // "CFO-wanted" -> "cfo_wanted"
}

const STATE_TO_CODE = {
  alabama:'al', alaska:'ak', arizona:'az', arkansas:'ar', california:'ca',
  colorado:'co', connecticut:'ct', delaware:'de', 'district of columbia':'dc',
  florida:'fl', georgia:'ga', hawaii:'hi', idaho:'id', illinois:'il',
  indiana:'in', iowa:'ia', kansas:'ks', kentucky:'ky', louisiana:'la',
  maine:'me', maryland:'md', massachusetts:'ma', michigan:'mi', minnesota:'mn',
  mississippi:'ms', missouri:'mo', montana:'mt', nebraska:'ne', nevada:'nv',
  'new hampshire':'nh', 'new jersey':'nj', 'new mexico':'nm', 'new york':'ny',
  'north carolina':'nc', 'north dakota':'nd', ohio:'oh', oklahoma:'ok',
  oregon:'or', pennsylvania:'pa', 'rhode island':'ri', 'south carolina':'sc',
  'south dakota':'sd', tennessee:'tn', texas:'tx', utah:'ut', vermont:'vt',
  virginia:'va', washington:'wa', 'west virginia':'wv', wisconsin:'wi',
  wyoming:'wy', 'puerto rico':'pr',
};
function stateKey(s) {
  const n = norm(s);
  if (!n) return '';
  if (n.length === 2) return n;
  return STATE_TO_CODE[n] || n;
}

function newestSignalMs(lead) {
  let best = 0;
  for (const sig of lead.signals || []) {
    const t = Date.parse(sig.date);
    if (!Number.isNaN(t) && t > best) best = t;
  }
  return best;
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'method not allowed' });
  }

  try {
    const blobUrl = await _resolveBlobUrl();
    if (!blobUrl) {
      res.setHeader('Cache-Control', 'public, s-maxage=30');
      return res.status(200).json({ generated_at: null, count: 0, leads: [], note: 'inventory not generated yet' });
    }
    const blobResp = await fetch(blobUrl, { cache: 'no-store' });
    if (!blobResp.ok) throw new Error(`blob fetch failed: ${blobResp.status}`);
    const payload = await blobResp.json();

    let leads = Array.isArray(payload.leads) ? payload.leads : [];
    const q = req.query || {};

    if (q.industry) { const v = enumVal(q.industry); leads = leads.filter(l => enumVal(l.industry) === v); }
    if (q.niche)    { const v = enumVal(q.niche);    leads = leads.filter(l => enumVal(l.niche) === v); }
    if (q.city)     { const v = norm(q.city);        leads = leads.filter(l => norm(l.city) === v); }
    if (q.state)    { const v = stateKey(q.state);   leads = leads.filter(l => stateKey(l.state) === v); }
    if (q.signal_type) { const v = enumVal(q.signal_type); leads = leads.filter(l => l.signal_type === v); }
    if (q.freshness)   { const v = norm(q.freshness);      leads = leads.filter(l => l.freshness === v); }
    if (q.exclude_ids) {
      const ex = new Set(String(q.exclude_ids).split(',').map(s => s.trim()).filter(Boolean));
      leads = leads.filter(l => !ex.has(l.id));
    }

    // Default order: freshest signal first (System B still applies its
    // own signal_type ranking per Step 3b).
    leads.sort((a, b) => newestSignalMs(b) - newestSignalMs(a));

    const limit = q.limit ? parseInt(q.limit, 10) : 0;
    if (limit && limit > 0) leads = leads.slice(0, limit);

    res.setHeader('Cache-Control', CACHE_HEADER);
    return res.status(200).json({
      generated_at: payload.generated_at,
      count: leads.length,
      leads,
    });
  } catch (err) {
    console.error('api/leads failed:', err);
    return res.status(502).json({ error: 'inventory query failed' });
  }
};
