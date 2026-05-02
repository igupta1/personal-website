import React, { useState } from "react";

const INDUSTRY_LABELS = {
  software_saas: "Software / SaaS",
  fintech: "Fintech",
  healthcare: "Healthcare",
  ecommerce_retail: "E-commerce / Retail",
  manufacturing: "Manufacturing",
  logistics_transport: "Logistics / Transport",
  real_estate: "Real Estate",
  legal_professional: "Legal / Professional",
  education: "Education",
  media_entertainment: "Media / Entertainment",
  hospitality_food: "Hospitality / Food",
  government_nonprofit: "Government / Nonprofit",
  construction: "Construction",
  other: "Other",
};

function prettyIndustry(value) {
  if (!value) return null;
  return INDUSTRY_LABELS[value] || value;
}

function scoreColor(score) {
  if (score == null) return "bg-gray-200 text-gray-600";
  if (score >= 70) return "bg-green-100 text-green-800";
  if (score >= 40) return "bg-amber-100 text-amber-800";
  return "bg-gray-100 text-gray-600";
}

function ScoreBadge({ score }) {
  const display = score == null ? "—" : score.toFixed(0);
  return (
    <div className={`shrink-0 px-3 py-1 rounded-full font-semibold text-sm ${scoreColor(score)}`}>
      {display}
    </div>
  );
}

function CopyButton({ text }) {
  const [copied, setCopied] = useState(false);
  const onClick = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  };
  return (
    <button
      onClick={onClick}
      className="px-3 py-1 text-xs font-medium rounded border border-gray-300 hover:bg-gray-100 transition-colors"
    >
      {copied ? "Copied!" : "Copy outreach"}
    </button>
  );
}

export default function LeadCard({ lead }) {
  const [expanded, setExpanded] = useState(false);
  const industryLabel = prettyIndustry(lead.industry);
  const location = [lead.city, lead.state].filter(Boolean).join(", ");

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-5 shadow-sm hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <h3 className="text-lg font-semibold text-gray-900 truncate">{lead.name}</h3>
          <div className="mt-2 flex flex-wrap gap-2">
            {industryLabel && (
              <span className="px-2 py-0.5 text-xs rounded-full bg-blue-50 text-blue-700">
                {industryLabel}
              </span>
            )}
            {lead.headcount != null && (
              <span className="px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-700">
                ~{lead.headcount} emp
              </span>
            )}
            {location && (
              <span className="px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-700">
                {location}
              </span>
            )}
          </div>
        </div>
        <ScoreBadge score={lead.score} />
      </div>

      {lead.insight && (
        <p className="mt-3 text-sm text-gray-700 leading-relaxed">{lead.insight}</p>
      )}

      <button
        onClick={() => setExpanded(e => !e)}
        className="mt-3 text-sm text-blue-600 hover:text-blue-800 font-medium"
      >
        {expanded ? "Hide outreach ↑" : "Show outreach ↓"}
      </button>

      {expanded && (
        <div className="mt-4 space-y-4">
          <div>
            <div className="flex items-center justify-between mb-2">
              <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                Outreach draft
              </h4>
              {lead.outreach && <CopyButton text={lead.outreach} />}
            </div>
            <div className="bg-gray-50 border border-gray-200 rounded p-3 text-sm text-gray-800 whitespace-pre-wrap">
              {lead.outreach || "(no outreach copy)"}
            </div>
          </div>

          {lead.signals && lead.signals.length > 0 && (
            <details className="text-sm">
              <summary className="cursor-pointer text-gray-600 hover:text-gray-900 font-medium">
                Signals ({lead.signals.length})
              </summary>
              <ul className="mt-2 space-y-2">
                {lead.signals.map((s, i) => (
                  <li key={i} className="bg-gray-50 border border-gray-200 rounded p-2">
                    <div>
                      <strong className="text-gray-800">{s.type}</strong>
                      <span className="text-gray-500"> · {s.days_ago}d ago</span>
                    </div>
                    {s.payload && Object.keys(s.payload).length > 0 && (
                      <pre className="mt-1 text-xs text-gray-600 overflow-x-auto">
                        {JSON.stringify(s.payload, null, 2)}
                      </pre>
                    )}
                  </li>
                ))}
              </ul>
            </details>
          )}
        </div>
      )}
    </div>
  );
}
