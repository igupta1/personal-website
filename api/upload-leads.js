const { put } = require('@vercel/blob');

const BLOB_KEY = 'leads-current.json';
const MAX_BODY_BYTES = 5 * 1024 * 1024;
const REQUIRED_NICHES = ['it_msp', 'mssp', 'cloud'];

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'method not allowed' });
  }

  const expected = process.env.LEADS_UPLOAD_API_KEY;
  if (!expected) {
    return res.status(500).json({ error: 'server misconfigured: no API key' });
  }
  const auth = req.headers.authorization || '';
  if (auth !== `Bearer ${expected}`) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const body = req.body;
  if (!body || typeof body !== 'object') {
    return res.status(400).json({ error: 'body must be JSON object' });
  }
  if (!body.niches || typeof body.niches !== 'object') {
    return res.status(400).json({ error: 'missing niches object' });
  }
  for (const niche of REQUIRED_NICHES) {
    if (!Array.isArray(body.niches[niche])) {
      return res.status(400).json({ error: `niches.${niche} must be array` });
    }
  }

  try {
    const json = JSON.stringify(body);
    if (Buffer.byteLength(json, 'utf8') > MAX_BODY_BYTES) {
      return res.status(413).json({ error: 'payload too large' });
    }
    const blob = await put(BLOB_KEY, json, {
      access: 'public',
      contentType: 'application/json',
      addRandomSuffix: false,
      allowOverwrite: true,
    });
    return res.status(200).json({
      ok: true,
      url: blob.url,
      uploaded_at: new Date().toISOString(),
      total_leads: Object.values(body.niches).reduce(
        (acc, arr) => acc + arr.length,
        0,
      ),
    });
  } catch (err) {
    console.error('blob put failed:', err);
    return res.status(500).json({ error: 'blob upload failed' });
  }
};
