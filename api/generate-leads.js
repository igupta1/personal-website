import { list, head } from '@vercel/blob';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { location } = req.body;

    if (!location) {
      return res.status(400).json({ error: 'Location is required' });
    }

    // Fetch leads from Vercel Blob Storage
    // Upload API stores as leads-cache-{location}.json
    const blobPath = `leads-cache-${location}.json`;

    try {
      // List all blobs to find the one we need
      const { blobs } = await list({
        prefix: `leads-cache-${location}`
      });

      if (!blobs || blobs.length === 0) {
        return res.status(404).json({
          success: false,
          error: 'No leads found for this location'
        });
      }

      // Get the most recent blob
      const latestBlob = blobs[0];

      // Fetch the blob content
      const response = await fetch(latestBlob.url);
      const cacheData = await response.json();

      // Extract leads from cached data structure and filter to only those with emails
      const allLeads = cacheData.leads || [];
      const leads = allLeads.filter(l => l.email && l.email.trim() !== '');

      // Categorize leads by size
      const leadsSmall = leads.filter(l => l.category === 'small');
      const leadsMedium = leads.filter(l => l.category === 'medium');
      const leadsLarge = leads.filter(l => l.category === 'large');

      return res.status(200).json({
        success: true,
        leads: leads,
        stats: {
          small: leadsSmall.length,
          medium: leadsMedium.length,
          large: leadsLarge.length,
          targetCount: leadsSmall.length + leadsMedium.length
        }
      });

    } catch (blobError) {
      console.error('Blob fetch error:', blobError);
      return res.status(404).json({
        success: false,
        error: 'No leads found for this location'
      });
    }

  } catch (error) {
    console.error('Error fetching leads:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to fetch leads'
    });
  }
}
