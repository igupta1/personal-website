/**
 * API endpoint to upload leads from local Python script
 * Protected with a simple API key
 */

const { put } = require('@vercel/blob');

// Simple API key protection - set this in Vercel environment variables
const UPLOAD_API_KEY = process.env.LEADS_UPLOAD_API_KEY;

module.exports = async function handler(req, res) {
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Credentials', true);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS,POST');
  res.setHeader('Access-Control-Allow-Headers', 'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version, X-API-Key');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ success: false, error: 'Method not allowed' });
  }

  // Check API key
  const apiKey = req.headers['x-api-key'];
  if (!UPLOAD_API_KEY || apiKey !== UPLOAD_API_KEY) {
    return res.status(401).json({ success: false, error: 'Invalid API key' });
  }

  try {
    const { location, leads } = req.body;

    if (!location || !leads || !Array.isArray(leads)) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: location (string) and leads (array)'
      });
    }

    // Validate leads have required fields
    const validatedLeads = leads.map(lead => ({
      firstName: lead.firstName || '',
      lastName: lead.lastName || '',
      title: lead.title || '',
      companyName: lead.companyName || '',
      email: lead.email || '',
      website: lead.website || '',
      location: lead.location || '',
      companySize: lead.companySize || '',
      category: lead.category || 'small', // small, medium, large
      evidence: lead.evidence || '',
      jobRole: lead.jobRole || '',
      jobLink: lead.jobLink || '',
      icebreaker: lead.icebreaker || '',
      postingDate: lead.postingDate || '',
      mostRecentPostingDate: lead.mostRecentPostingDate || '',
      linkedinUrl: lead.linkedinUrl || '',
      sourceUrl: lead.sourceUrl || '',
      confidence: lead.confidence || ''
    }));

    // Count by category
    const stats = {
      small: validatedLeads.filter(l => l.category === 'small').length,
      medium: validatedLeads.filter(l => l.category === 'medium').length,
      large: validatedLeads.filter(l => l.category === 'large').length
    };

    const cacheData = {
      timestamp: new Date().toISOString(),
      location,
      leads: validatedLeads,
      stats,
      totalLeads: validatedLeads.length
    };

    // Store in Vercel Blob
    await put(
      `leads-cache-${location}.json`,
      JSON.stringify(cacheData, null, 2),
      { access: 'public' }
    );

    console.log(`Uploaded ${validatedLeads.length} leads for ${location}`);
    console.log(`Stats: small=${stats.small}, medium=${stats.medium}, large=${stats.large}`);

    return res.status(200).json({
      success: true,
      message: `Successfully uploaded ${validatedLeads.length} leads`,
      stats
    });
  } catch (error) {
    console.error('Error uploading leads:', error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Internal server error'
    });
  }
};
