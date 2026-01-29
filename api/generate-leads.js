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

      // Sort by uploadedAt descending to get the most recent blob
      const sortedBlobs = blobs.sort((a, b) =>
        new Date(b.uploadedAt) - new Date(a.uploadedAt)
      );
      const latestBlob = sortedBlobs[0];

      // Fetch the blob content
      const response = await fetch(latestBlob.url);
      const cacheData = await response.json();

      // Extract leads from cached data structure
      const allLeads = cacheData.leads || [];
      // Filter out leads with invalid placeholder emails, but keep leads without emails
      const leads = allLeads.filter(l => {
        if (!l.email) return true; // Keep leads without emails
        const email = l.email.trim().toLowerCase();
        // Filter out placeholder values
        if (email.includes('n/a') || email.includes('not found') || email.includes('not available')) return false;
        return true;
      });

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
