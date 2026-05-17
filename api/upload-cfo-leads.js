// Sibling of upload-insurance-leads.js, separate blob key. Receives
// cfo pipeline JSON shape `{generated_at, leads: [...]}` — same flat
// shape as the insurance pipeline. Auth reuses LEADS_UPLOAD_API_KEY
// (consistent with the insurance endpoint's choice) so the user
// doesn't have to manage a third Bearer secret.

const { put } = require('@vercel/blob');

const BLOB_KEY = 'cfo-leads-current.json';
const MAX_BODY_BYTES = 5 * 1024 * 1024;

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
  if (!Array.isArray(body.leads)) {
    return res.status(400).json({ error: 'body.leads must be an array' });
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
      total_leads: body.leads.length,
    });
  } catch (err) {
    console.error('cfo blob put failed:', err);
    return res.status(500).json({ error: 'blob upload failed' });
  }
};
