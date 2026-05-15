import React from "react";
import LeadsPage from "./LeadsPage";

// Insurance reads from its own serverless endpoint
// (`api/generate-insurance-leads.js`) which serves a separate Vercel
// Blob populated by `insurance_pipeline/` — not from the MSP pipeline.
export default function Insurance() {
  return (
    <LeadsPage
      niche="insurance"
      title="Independent Insurance Agency Leads"
      subtitle="SMBs and newly-formed entities (≤250 employees) with recent buying triggers — new business registrations, blue-collar / fleet hiring, finance or HR leadership changes."
      apiPath="/api/generate-insurance-leads"
    />
  );
}
