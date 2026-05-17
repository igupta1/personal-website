// Serves the cfo-pipeline blob. Decoupled from /api/generate-leads
// (MSP/MSSP/Cloud) and /api/generate-insurance-leads. Returns an empty
// list with a fresh generated_at timestamp until cfo_pipeline starts
// uploading to `cfo-leads-current.json` — the React page renders the
// empty state cleanly in the gap.

const { list } = require('@vercel/blob');

const BLOB_KEY = 'cfo-leads-current.json';
const CACHE_HEADER = 'public, s-maxage=300, stale-while-revalidate=3600';

let _cachedBlobUrl = null;

async function _resolveBlobUrl() {
  if (_cachedBlobUrl) return _cachedBlobUrl;
  const { blobs } = await list({ prefix: BLOB_KEY, limit: 1 });
  if (!blobs.length) return null;
  _cachedBlobUrl = blobs[0].url;
  return _cachedBlobUrl;
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'method not allowed' });
  }

  try {
    const blobUrl = await _resolveBlobUrl();
    if (!blobUrl) {
      res.setHeader('Cache-Control', 'public, s-maxage=60');
      return res.status(200).json({
        generated_at: null,
        niche: 'cfo',
        leads: [],
      });
    }

    const blobResp = await fetch(blobUrl, { cache: 'no-store' });
    if (!blobResp.ok) {
      throw new Error(`blob fetch failed: ${blobResp.status}`);
    }
    const payload = await blobResp.json();

    res.setHeader('Cache-Control', CACHE_HEADER);
    return res.status(200).json({
      generated_at: payload.generated_at,
      niche: 'cfo',
      leads: payload.leads || [],
    });
  } catch (err) {
    console.error('generate-cfo-leads failed:', err);
    return res.status(502).json({ error: 'upstream blob fetch failed' });
  }
};
