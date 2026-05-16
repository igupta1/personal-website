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
      subtitle="US SMBs (≤250 employees) with recent commercial buying triggers — newly-issued motor carrier authorities (commercial auto), fresh private-securities offerings (D&O / EPLI). Refreshed nightly."
      apiPath="/api/generate-insurance-leads"
    />
  );
}
