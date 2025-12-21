/**
 * Simple endpoint to clear the leads cache
 * Usage: POST /api/clear-leads-cache with { location: "Greater Los Angeles Area" }
 */

const { del, list } = require('@vercel/blob');

module.exports = async function handler(req, res) {
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Credentials', true);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS,PATCH,DELETE,POST,PUT');
  res.setHeader('Access-Control-Allow-Headers', 'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version');

  // Handle preflight request
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

    console.log(`Clearing cache for location: ${location}`);

    // List all blobs with the cache prefix for this location
    const { blobs } = await list({ prefix: `leads-cache-${location}` });

    if (blobs.length === 0) {
      return res.status(200).json({
        success: true,
        message: 'No cache found for this location',
        deleted: 0
      });
    }

    // Delete all matching cache blobs
    let deleted = 0;
    for (const blob of blobs) {
      await del(blob.url);
      deleted++;
      console.log(`Deleted cache blob: ${blob.pathname}`);
    }

    return res.status(200).json({
      success: true,
      message: `Cache cleared for ${location}`,
      deleted
    });
  } catch (error) {
    console.error('Error clearing cache:', error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Internal server error'
    });
  }
};
