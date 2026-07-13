// GET /api/niche-leads?niche=trucking|pc|bookkeeping
//
// One endpoint for the three geo-only outreach sub-inventories (consolidated to
// stay under Vercel's serverless-function limit):
//   trucking  -> live insurance blob, new-motor-carrier (FMCSA new-authority) leads
//   pc        -> live insurance blob, the growth/trigger (non-trucking) leads,
//                with trigger_type normalized to the recognized signal type
//   bookkeeping-> the bookkeeping blob (junior-finance-hire leads)
// Shape mirrors /api/leads: { generated_at, niche, count, leads:[...] }.

const { list } = require('@vercel/blob');

const AUTHORITY_SIGNAL = 'new_motor_carrier_authority';
const PC_TRIGGERS = new Set([
  'funding_raised', 'funding', 'new_business_filed', 'new_business',
  'osha_inspection_recorded', 'building_permit_issued',
]);
const BLOB_FOR = {
  trucking: 'insurance-leads-current.json',
  pc: 'insurance-leads-current.json',
  bookkeeping: 'bookkeeping-leads-current.json',
};
const VALID = Object.keys(BLOB_FOR);
const CACHE_HEADER = 'public, s-maxage=120, stale-while-revalidate=600';

const _cache = {};
async function _resolveBlobUrl(key) {
  if (_cache[key]) return _cache[key];
  const { blobs } = await list({ prefix: key, limit: 1 });
  if (!blobs.length) return null;
  _cache[key] = blobs[0].url;
  return _cache[key];
}

function norm(s) {
  return (s == null ? '' : String(s)).trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}
function isTrucking(lead) {
  return (lead.signals || []).some((s) => s && s.type === AUTHORITY_SIGNAL);
}
function normalizePc(lead) {
  const sig = (lead.signals || [])[0] || {};
  return PC_TRIGGERS.has(sig.type) ? { ...lead, trigger_type: sig.type } : lead;
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'method not allowed' });
  }
  const niche = req.query.niche;
  if (!VALID.includes(niche)) {
    return res.status(400).json({ error: `niche must be one of: ${VALID.join(', ')}` });
  }
  try {
    const blobUrl = await _resolveBlobUrl(BLOB_FOR[niche]);
    if (!blobUrl) {
      res.setHeader('Cache-Control', 'public, s-maxage=30');
      return res.status(200).json({ generated_at: null, niche, count: 0, leads: [], note: 'inventory not generated yet' });
    }
    const blobResp = await fetch(blobUrl, { cache: 'no-store' });
    if (!blobResp.ok) throw new Error(`blob fetch failed: ${blobResp.status}`);
    const payload = await blobResp.json();

    let leads = Array.isArray(payload.leads) ? payload.leads : [];
    if (niche === 'trucking') leads = leads.filter(isTrucking);
    else if (niche === 'pc') leads = leads.filter((l) => !isTrucking(l)).map(normalizePc);
    // bookkeeping: served as-is

    const q = req.query || {};
    if (q.state) { const v = norm(q.state); leads = leads.filter((l) => norm(l.state) === v); }
    if (q.limit) { const n = parseInt(q.limit, 10); if (n > 0) leads = leads.slice(0, n); }

    res.setHeader('Cache-Control', CACHE_HEADER);
    return res.status(200).json({ generated_at: payload.generated_at, niche, count: leads.length, leads });
  } catch (err) {
    console.error('niche-leads failed:', err);
    return res.status(502).json({ error: 'inventory fetch failed' });
  }
};
