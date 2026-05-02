const { list } = require('@vercel/blob');

const BLOB_KEY = 'leads-current.json';
const VALID_NICHES = ['it_msp', 'mssp', 'cloud'];
const CACHE_HEADER = 'public, s-maxage=300, stale-while-revalidate=3600';

let _cachedBlobUrl = null;

async function _resolveBlobUrl() {
  if (_cachedBlobUrl) return _cachedBlobUrl;
  const { blobs } = await list({ prefix: BLOB_KEY, limit: 1 });
  if (!blobs.length) throw new Error('blob not found');
  _cachedBlobUrl = blobs[0].url;
  return _cachedBlobUrl;
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'method not allowed' });
  }

  const niche = req.query.niche;
  if (niche !== undefined && !VALID_NICHES.includes(niche)) {
    return res.status(400).json({
      error: `niche must be one of: ${VALID_NICHES.join(', ')}`,
    });
  }

  try {
    const blobUrl = await _resolveBlobUrl();
    const blobResp = await fetch(blobUrl, { cache: 'no-store' });
    if (!blobResp.ok) {
      throw new Error(`blob fetch failed: ${blobResp.status}`);
    }
    const payload = await blobResp.json();

    res.setHeader('Cache-Control', CACHE_HEADER);
    if (niche) {
      return res.status(200).json({
        generated_at: payload.generated_at,
        niche,
        leads: payload.niches[niche] || [],
      });
    }
    return res.status(200).json(payload);
  } catch (err) {
    console.error('generate-leads failed:', err);
    return res.status(502).json({ error: 'upstream blob fetch failed' });
  }
};
