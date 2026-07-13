// GET /api/trucking-leads — the trucking sub-inventory for the outreach engine.
//
// Reads the LIVE insurance inventory blob and returns only new-motor-carrier
// (FMCSA new-authority) leads. Serving off the insurance blob (rather than a
// separate trucking blob) keeps this endpoint fresh with every insurance
// scraper run and independent of the best-effort trucking upload.
// Shape mirrors /api/leads: { generated_at, count, leads:[...] }.

const { list } = require('@vercel/blob');

const BLOB_KEY = 'insurance-leads-current.json';
const AUTHORITY_SIGNAL = 'new_motor_carrier_authority';
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
function isTrucking(lead) {
  return (lead.signals || []).some((s) => s && s.type === AUTHORITY_SIGNAL);
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

    let leads = (Array.isArray(payload.leads) ? payload.leads : []).filter(isTrucking);
    const q = req.query || {};
    if (q.state) { const v = norm(q.state); leads = leads.filter((l) => norm(l.state) === v); }
    if (q.limit) { const n = parseInt(q.limit, 10); if (n > 0) leads = leads.slice(0, n); }

    res.setHeader('Cache-Control', CACHE_HEADER);
    return res.status(200).json({ generated_at: payload.generated_at, count: leads.length, leads });
  } catch (err) {
    console.error('trucking-leads failed:', err);
    return res.status(502).json({ error: 'inventory fetch failed' });
  }
};
