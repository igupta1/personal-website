import React from "react";
import LeadsPage from "./LeadsPage";

// /cfo reads from its own serverless endpoint
// (`api/generate-cfo-leads.js`) which serves a separate Vercel Blob
// populated by `cfo_pipeline/` — independent of the MSP and insurance
// pipelines.
export default function CFO() {
  return (
    <LeadsPage
      niche="cfo"
      title="Fractional CFO Leads"
      subtitle="US SMBs (≤~75 employees) currently hiring finance leadership one rung below CFO — Controllers, VP / Director of Finance, Accounting Managers, FP&A. Companies with an open full-time CFO posting are excluded. Recent Form D filings and announced rounds boost urgency. Refreshed nightly."
      apiPath="/api/generate-cfo-leads"
    />
  );
}
