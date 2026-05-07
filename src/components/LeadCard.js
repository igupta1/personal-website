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

const SIGNAL_KIND_META = {
  job_posted_it_support:    { label: "IT Support job",    pill: "bg-blue-50 text-blue-700 ring-blue-200" },
  job_posted_it_leadership: { label: "IT Leadership job", pill: "bg-indigo-50 text-indigo-700 ring-indigo-200" },
  job_posted_security:      { label: "Security job",      pill: "bg-red-50 text-red-700 ring-red-200" },
  job_posted_cloud_devops:  { label: "Cloud / DevOps job",pill: "bg-cyan-50 text-cyan-700 ring-cyan-200" },
  exec_hired:               { label: "Exec hire",         pill: "bg-purple-50 text-purple-700 ring-purple-200" },
  funding_raised:           { label: "Funding raised",    pill: "bg-emerald-50 text-emerald-700 ring-emerald-200" },
  breach_disclosed:         { label: "Breach disclosed",  pill: "bg-amber-50 text-amber-800 ring-amber-200" },
};

const AGENCY_LABELS = {
  ca_ag: "California AG",
  me_ag: "Maine AG",
  wa_ag: "Washington AG",
};

function prettyIndustry(value) {
  if (!value) return null;
  return INDUSTRY_LABELS[value] || value;
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
      {copied ? "Copied!" : "Copy"}
    </button>
  );
}

function SignalRow({ signal }) {
  const meta = SIGNAL_KIND_META[signal.type];
  if (!meta) return null;

  const days = signal.days_ago === 0 ? "today" : `${signal.days_ago}d ago`;
  const payload = signal.payload || {};

  let primary = null;
  let extra = null;
  let link = null;

  if (signal.type.startsWith("job_posted_") || signal.type === "exec_hired") {
    primary = payload.title || "(untitled posting)";
    if (payload.url) link = { href: payload.url, label: "View posting" };
  } else if (signal.type === "funding_raised") {
    primary = payload.feed_title || payload.title || "(no headline)";
    if (payload.link) link = { href: payload.link, label: "Read article" };
  } else if (signal.type === "breach_disclosed") {
    const agencyLabel = AGENCY_LABELS[payload.agency] || payload.agency || "state AG";
    primary = `Disclosed to ${agencyLabel}`;
    if (payload.reported_date) extra = `reported ${payload.reported_date}`;
  }

  return (
    <li className="flex flex-wrap items-center gap-x-3 gap-y-1 py-2 border-t border-gray-100 first:border-t-0">
      <span
        className={`shrink-0 px-2 py-0.5 text-xs font-medium rounded-full ring-1 ${meta.pill}`}
      >
        {meta.label}
      </span>
      <span className="text-sm text-gray-900 flex-1 min-w-0 truncate" title={primary}>
        {primary}
      </span>
      <span className="shrink-0 text-xs text-gray-500 tabular-nums">{days}</span>
      {extra && <span className="shrink-0 text-xs text-gray-400">· {extra}</span>}
      {link && (
        <a
          href={link.href}
          target="_blank"
          rel="noopener noreferrer"
          className="shrink-0 text-xs font-medium text-blue-600 hover:text-blue-800 hover:underline"
        >
          {link.label} ↗
        </a>
      )}
    </li>
  );
}

export default function LeadCard({ lead }) {
  const [outreachOpen, setOutreachOpen] = useState(false);

  const industryLabel = prettyIndustry(lead.industry);
  const location = [lead.city, lead.state].filter(Boolean).join(", ");
  const renderableSignals = (lead.signals || []).filter((s) => SIGNAL_KIND_META[s.type]);

  return (
    <div className="bg-white border border-gray-200 rounded-2xl p-5 shadow-sm hover:shadow-lg hover:-translate-y-0.5 transition-all duration-150">
      {/* Header: name + domain link, then chips */}
      <div className="min-w-0">
        <div className="flex items-baseline gap-2 flex-wrap">
          <h3 className="text-lg font-semibold text-gray-900 leading-snug">
            {lead.name}
          </h3>
          {lead.domain && (
            <a
              href={
                lead.domain.startsWith("http")
                  ? lead.domain
                  : `https://${lead.domain}`
              }
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs font-medium text-blue-600 hover:text-blue-800 hover:underline whitespace-nowrap"
            >
              {lead.domain} ↗
            </a>
          )}
        </div>
        <div className="mt-2 flex flex-wrap gap-1.5 text-xs">
          {industryLabel && (
            <span className="px-2 py-0.5 rounded-full bg-blue-50 text-blue-700 ring-1 ring-blue-200">
              {industryLabel}
            </span>
          )}
          {lead.headcount != null && (
            <span className="px-2 py-0.5 rounded-full bg-gray-100 text-gray-700 ring-1 ring-gray-200">
              ~{lead.headcount} emp
            </span>
          )}
          {location && (
            <span className="px-2 py-0.5 rounded-full bg-gray-100 text-gray-700 ring-1 ring-gray-200">
              {location}
            </span>
          )}
        </div>
      </div>

      {/* Insight */}
      {lead.insight && (
        <p className="mt-4 text-sm text-gray-800 leading-relaxed">{lead.insight}</p>
      )}

      {/* Signals — always visible, scannable */}
      {renderableSignals.length > 0 && (
        <div className="mt-4">
          <h4 className="text-[11px] font-semibold tracking-wide text-gray-500 uppercase mb-1">
            Signals ({renderableSignals.length})
          </h4>
          <ul>
            {renderableSignals.map((s, i) => (
              <SignalRow key={i} signal={s} />
            ))}
          </ul>
        </div>
      )}

      {/* Outreach — collapsed by default */}
      {lead.outreach && (
        <div className="mt-4 pt-3 border-t border-gray-100">
          <div className="flex items-center justify-between">
            <button
              onClick={() => setOutreachOpen((v) => !v)}
              className="text-sm font-medium text-blue-600 hover:text-blue-800"
            >
              {outreachOpen ? "Hide outreach draft ↑" : "Show outreach draft ↓"}
            </button>
            {outreachOpen && <CopyButton text={lead.outreach} />}
          </div>
          {outreachOpen && (
            <div className="mt-2 bg-gray-50 border border-gray-200 rounded-md p-3 text-sm text-gray-800 whitespace-pre-wrap leading-relaxed">
              {lead.outreach}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
