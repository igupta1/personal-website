// Reference endpoint: the niche taxonomy (parent industry -> child
// niches) so the outreach workflow (System B) knows how to map a
// prospect's stated niche to the values /api/leads filters on. Sourced
// from the inventory blob's `taxonomy` field (single source of truth in
// the pipeline's taxonomy.py), so it never drifts.

const { list } = require('@vercel/blob');

const BLOB_KEY = 'cfo-leads-inventory.json';

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
      return res.status(200).json({ taxonomy: {}, note: 'inventory not generated yet' });
    }
    const blobResp = await fetch(blobUrl, { cache: 'no-store' });
    if (!blobResp.ok) throw new Error(`blob fetch failed: ${blobResp.status}`);
    const payload = await blobResp.json();
    res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
    return res.status(200).json({ taxonomy: payload.taxonomy || {} });
  } catch (err) {
    console.error('api/niches failed:', err);
    return res.status(502).json({ error: 'taxonomy fetch failed' });
  }
};
