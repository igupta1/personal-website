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
  job_posted_it_support:    { label: "IT Support",    pill: "bg-blue-500/15 text-blue-300 ring-blue-400/30" },
  job_posted_it_leadership: { label: "IT Leadership", pill: "bg-indigo-500/15 text-indigo-300 ring-indigo-400/30" },
  job_posted_security:      { label: "Security",      pill: "bg-rose-500/15 text-rose-300 ring-rose-400/30" },
  job_posted_cloud_devops:  { label: "Cloud / DevOps",pill: "bg-cyan-500/15 text-cyan-300 ring-cyan-400/30" },
  exec_hired:               { label: "Exec hire",     pill: "bg-purple-500/15 text-purple-300 ring-purple-400/30" },
  funding_raised:           { label: "Funding",       pill: "bg-emerald-500/15 text-emerald-300 ring-emerald-400/30" },
  breach_disclosed:         { label: "Breach",        pill: "bg-amber-500/15 text-amber-300 ring-amber-400/30" },
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
      className="px-3 py-1 text-xs font-medium rounded-md bg-slate-700/60 text-gray-200 border border-slate-600 hover:bg-slate-600 transition-colors"
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
    <li className="flex flex-wrap items-center gap-x-3 gap-y-1 py-2 border-t border-slate-700/60 first:border-t-0">
      <span
        className={`shrink-0 px-2 py-0.5 text-[11px] font-medium rounded-full ring-1 ${meta.pill}`}
      >
        {meta.label}
      </span>
      <span className="text-sm text-gray-200 flex-1 min-w-0 truncate" title={primary}>
        {primary}
      </span>
      <span className="shrink-0 text-xs text-gray-500 tabular-nums">{days}</span>
      {extra && <span className="shrink-0 text-xs text-gray-500">· {extra}</span>}
      {link && (
        <a
          href={link.href}
          target="_blank"
          rel="noopener noreferrer"
          className="shrink-0 text-xs font-medium text-blue-400 hover:text-blue-300 hover:underline"
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
    <div className="bg-slate-800/70 backdrop-blur border border-slate-700/70 rounded-2xl p-5 shadow-lg shadow-black/10 hover:bg-slate-800 hover:border-slate-600 hover:-translate-y-0.5 transition-all duration-150">
      {/* Header: name + domain link, then chips */}
      <div className="min-w-0">
        <div className="flex items-baseline gap-2 flex-wrap">
          <h3 className="text-base font-semibold text-white leading-snug">
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
              className="text-xs font-medium text-blue-400 hover:text-blue-300 hover:underline whitespace-nowrap"
            >
              {lead.domain} ↗
            </a>
          )}
        </div>
        <div className="mt-2 flex flex-wrap gap-1.5 text-[11px]">
          {industryLabel && (
            <span className="px-2 py-0.5 rounded-full bg-blue-500/15 text-blue-300 ring-1 ring-blue-400/30">
              {industryLabel}
            </span>
          )}
          {lead.headcount != null && (
            <span className="px-2 py-0.5 rounded-full bg-slate-700/70 text-gray-300 ring-1 ring-slate-600/70">
              ~{lead.headcount} emp
            </span>
          )}
          {location && (
            <span className="px-2 py-0.5 rounded-full bg-slate-700/70 text-gray-300 ring-1 ring-slate-600/70">
              {location}
            </span>
          )}
        </div>
      </div>

      {/* Insight */}
      {lead.insight && (
        <p className="mt-4 text-sm text-gray-300 leading-relaxed">{lead.insight}</p>
      )}

      {/* Signals — always visible, scannable */}
      {renderableSignals.length > 0 && (
        <div className="mt-4">
          <h4 className="text-[10px] font-semibold tracking-widest text-gray-500 uppercase mb-1">
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
        <div className="mt-4 pt-3 border-t border-slate-700/60">
          <div className="flex items-center justify-between">
            <button
              onClick={() => setOutreachOpen((v) => !v)}
              className="text-xs font-medium text-blue-400 hover:text-blue-300 transition-colors"
            >
              {outreachOpen ? "Hide outreach draft ↑" : "Show outreach draft ↓"}
            </button>
            {outreachOpen && <CopyButton text={lead.outreach} />}
          </div>
          {outreachOpen && (
            <div className="mt-3 bg-slate-900/60 border border-slate-700/70 rounded-lg p-3 text-sm text-gray-200 whitespace-pre-wrap leading-relaxed">
              {lead.outreach}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
