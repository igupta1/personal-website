/**
 * Vercel Serverless Function for Lead Generation
 * Returns cached leads that were uploaded via the upload-leads endpoint
 */

const { list } = require('@vercel/blob');

/**
 * Get cached leads from Vercel Blob Storage
 */
async function getCachedLeads(location) {
  try {
    const { blobs } = await list({ prefix: `leads-cache-${location}` });

    if (blobs.length === 0) {
      return null;
    }

    // Get the latest blob
    const latestBlob = blobs[0];
    const response = await fetch(latestBlob.url);
    const data = await response.json();

    return data;
  } catch (error) {
    console.error('Cache read error:', error.message);
    return null;
  }
}

/**
 * Vercel Serverless Handler
 */
module.exports = async function handler(req, res) {
  // Set CORS headers
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
    const { location } = req.body;

    if (!location) {
      return res.status(400).json({
        success: false,
        error: 'Missing required field: location'
      });
    }

    console.log(`Fetching cached leads for: ${location}`);

    // Get cached leads
    const cached = await getCachedLeads(location);

    if (!cached || !cached.leads || cached.leads.length === 0) {
      return res.status(200).json({
        success: false,
        error: 'No cached leads available. Please try again later.',
        leads: [],
        stats: { small: 0, medium: 0, large: 0 }
      });
    }

    console.log(`Returning ${cached.leads.length} cached leads`);
    console.log(`Cache timestamp: ${cached.timestamp}`);

    return res.status(200).json({
      success: true,
      leads: cached.leads,
      stats: cached.stats,
      timestamp: cached.timestamp
    });
  } catch (error) {
    console.error('Error processing request:', error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Internal server error'
    });
  }
};
