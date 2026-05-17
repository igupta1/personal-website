import React from "react";

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
  job_posted_ops_role:      { label: "Ops hire",      pill: "bg-sky-500/15 text-sky-300 ring-sky-400/30" },
  job_posted_blue_collar:   { label: "Blue-collar",   pill: "bg-orange-500/15 text-orange-300 ring-orange-400/30" },
  job_posted_fleet_role:    { label: "Fleet",         pill: "bg-yellow-500/15 text-yellow-300 ring-yellow-400/30" },
  job_posted_finance_ops:   { label: "Finance / HR",  pill: "bg-teal-500/15 text-teal-300 ring-teal-400/30" },
  job_posted_finance_lead:  { label: "Finance lead",  pill: "bg-emerald-500/15 text-emerald-300 ring-emerald-400/30" },
  exec_hired:               { label: "Exec hire",     pill: "bg-purple-500/15 text-purple-300 ring-purple-400/30" },
  funding_raised:           { label: "Funding",       pill: "bg-emerald-500/15 text-emerald-300 ring-emerald-400/30" },
  breach_disclosed:         { label: "Breach",        pill: "bg-amber-500/15 text-amber-300 ring-amber-400/30" },
  new_business_filed:       { label: "New entity",    pill: "bg-lime-500/15 text-lime-300 ring-lime-400/30" },
  new_motor_carrier_authority: { label: "Motor carrier", pill: "bg-yellow-500/15 text-yellow-300 ring-yellow-400/30" },
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

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

// Accepts "YYYY-MM-DD", "MM/DD/YYYY", "M/D/YY", or anything Date can parse.
// Returns "May 5" when the year matches today, "May 5, 2025" otherwise.
function formatDate(raw) {
  if (!raw) return null;
  let d;
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) {
    const [y, m, day] = raw.split("-").map(Number);
    d = new Date(y, m - 1, day);
  } else if (/^\d{1,2}\/\d{1,2}\/\d{2,4}/.test(raw)) {
    let [m, day, y] = raw.split("/").map(Number);
    if (y < 100) y += 2000;
    d = new Date(y, m - 1, day);
  } else {
    d = new Date(raw);
  }
  if (isNaN(d.getTime())) return raw;
  const month = MONTHS[d.getMonth()];
  const day = d.getDate();
  const sameYear = d.getFullYear() === new Date().getFullYear();
  return sameYear ? `${month} ${day}` : `${month} ${day}, ${d.getFullYear()}`;
}

// Freshness dot color: green if today, amber if last few days, gray otherwise.
function freshnessClass(daysAgo) {
  if (daysAgo == null) return "bg-gray-500";
  if (daysAgo === 0) return "bg-emerald-400";
  if (daysAgo <= 2) return "bg-amber-400";
  if (daysAgo <= 7) return "bg-gray-400";
  return "bg-gray-600";
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
    if (payload.url) link = { href: payload.url, label: "View" };
  } else if (signal.type === "funding_raised") {
    primary = payload.feed_title || payload.title || "(no headline)";
    if (payload.link) link = { href: payload.link, label: "Read" };
    // USAspending contracts share NAICS-description titles across distinct
    // awards (e.g. two "ENGINEERING SERVICES" contracts to the same firm).
    // Surface amount + state so duplicate-looking rows are visibly different.
    if (payload.filing_type === "Federal contract") {
      const bits = [];
      const amt = Number(payload.amount_usd);
      if (Number.isFinite(amt) && amt > 0) {
        bits.push(amt >= 1_000_000 ? `$${(amt / 1_000_000).toFixed(1)}M` : `$${Math.round(amt / 1000)}K`);
      }
      if (payload.state) bits.push(payload.state);
      if (bits.length) extra = bits.join(" · ");
    }
  } else if (signal.type === "breach_disclosed") {
    const agencyLabel = AGENCY_LABELS[payload.agency] || payload.agency || "state AG";
    primary = agencyLabel;
    if (payload.reported_date) extra = `reported ${formatDate(payload.reported_date)}`;
  } else if (signal.type === "new_business_filed") {
    const filingType = payload.filing_type || "Entity";
    const state = payload.state || "?";
    primary = `${filingType} filed in ${state}`;
    if (payload.filed_on) extra = `filed ${formatDate(payload.filed_on)}`;
  } else if (signal.type === "new_motor_carrier_authority") {
    const fleet = payload.fleet_size_power_units;
    const drivers = payload.drivers;
    if (fleet) {
      const unit = fleet === 1 ? "truck" : "trucks";
      primary = drivers && drivers > 1
        ? `Motor carrier · ${fleet} ${unit} · ${drivers} drivers`
        : `Motor carrier · ${fleet} ${unit}`;
    } else {
      primary = "Motor carrier authority";
    }
    if (payload.issue_date) extra = `authority ${formatDate(payload.issue_date)}`;
    if (payload.usdot) {
      link = {
        href: `https://safer.fmcsa.dot.gov/CompanySnapshot.aspx?query=${payload.usdot}`,
        label: `USDOT ${payload.usdot}`,
      };
    }
  }

  return (
    <li className="flex items-center gap-2 py-1.5 border-t border-slate-700/60 first:border-t-0 first:pt-0">
      <span
        className={`shrink-0 inline-block h-1.5 w-1.5 rounded-full ${freshnessClass(signal.days_ago)}`}
        title={days}
      />
      <span
        className={`shrink-0 px-1.5 py-0.5 text-[10px] font-medium rounded ring-1 ${meta.pill}`}
      >
        {meta.label}
      </span>
      <span className="text-[13px] text-gray-200 flex-1 min-w-0 truncate" title={primary}>
        {primary}
      </span>
      <span className="shrink-0 text-[11px] text-gray-500 tabular-nums">{days}</span>
      {extra && <span className="shrink-0 text-[11px] text-gray-500">· {extra}</span>}
      {link && (
        <a
          href={link.href}
          target="_blank"
          rel="noopener noreferrer"
          className="shrink-0 text-[11px] font-medium text-blue-400 hover:text-blue-300 hover:underline"
        >
          {link.label} ↗
        </a>
      )}
    </li>
  );
}

function namesMatch(a, b) {
  if (!a || !b) return false;
  const norm = (s) => s.toUpperCase().replace(/[^A-Z\s]/g, "").replace(/\s+/g, " ").trim();
  return norm(a) === norm(b);
}

export default function LeadCard({ lead }) {
  const industryLabel = prettyIndustry(lead.industry);
  const location = [lead.city, lead.state].filter(Boolean).join(", ");
  const renderableSignals = (lead.signals || []).filter((s) => SIGNAL_KIND_META[s.type]);
  // FMCSA owner-operators often file under their own personal name —
  // the DM panel then duplicates the lead title verbatim. Hide it.
  const dmRedundantWithName = namesMatch(lead.name, lead.dm_name);
  const showDmPanel = lead.dm_name && !dmRedundantWithName;

  return (
    <div className="bg-slate-800/70 backdrop-blur border border-slate-700/70 rounded-2xl p-5 shadow-lg shadow-black/10 hover:bg-slate-800 hover:border-slate-600 hover:-translate-y-0.5 transition-all duration-150 flex flex-col">
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
              ~{lead.headcount} employees
            </span>
          )}
          {location && (
            <span className="px-2 py-0.5 rounded-full bg-slate-700/70 text-gray-300 ring-1 ring-slate-600/70">
              {location}
            </span>
          )}
        </div>
      </div>

      {/* Likely decision maker — featured panel. Hidden when the DM
          name duplicates the lead name (owner-operator carriers filed
          under the owner's own name). */}
      {showDmPanel && (
        <div className="mt-3 rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-3 py-2.5">
          <div className="text-[10px] font-semibold tracking-widest text-emerald-400 uppercase mb-1">
            Decision maker
          </div>
          <div className="text-sm text-white font-semibold leading-tight">
            {lead.dm_name}
          </div>
          {(() => {
            // Apollo sometimes returns a name with no title (or a
            // long "Founder | CEO | CTO" pipe-stuffed string). Hide
            // noisy strings; fall back to "Contact" when null so the
            // row keeps its two-line shape instead of a floating name.
            const t = lead.dm_title;
            const usable = t && t.length <= 80 && !t.includes("|");
            const label = usable ? t : "Contact";
            return (
              <div className="text-[12px] text-gray-300 mt-0.5">{label}</div>
            );
          })()}
          {(lead.dm_email || lead.dm_linkedin_url) && (
            <div className="mt-2 flex items-center gap-1.5 flex-wrap">
              {lead.dm_email && (
                <a
                  href={`mailto:${lead.dm_email}`}
                  title={lead.dm_email}
                  className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-blue-500/15 text-blue-300 ring-1 ring-blue-400/30 hover:bg-blue-500/25 hover:text-blue-200 transition-colors text-[11px] font-medium max-w-full"
                >
                  <svg viewBox="0 0 16 16" fill="currentColor" className="h-3 w-3 shrink-0" aria-hidden="true">
                    <path d="M2 3h12a1 1 0 0 1 1 1v8a1 1 0 0 1-1 1H2a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1zm0 1.5v.4l6 3.6 6-3.6v-.4H2zm12 1.66-5.62 3.37a1 1 0 0 1-1.04 0L2 6.16V12h12V6.16z" />
                  </svg>
                  <span className="truncate">{lead.dm_email}</span>
                </a>
              )}
              {lead.dm_linkedin_url && (
                <a
                  href={
                    lead.dm_linkedin_url.startsWith("http")
                      ? lead.dm_linkedin_url
                      : `https://${lead.dm_linkedin_url}`
                  }
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-blue-500/15 text-blue-300 ring-1 ring-blue-400/30 hover:bg-blue-500/25 hover:text-blue-200 transition-colors text-[11px] font-medium whitespace-nowrap shrink-0"
                >
                  <svg viewBox="0 0 24 24" fill="currentColor" className="h-3 w-3 shrink-0" aria-hidden="true">
                    <path d="M20.45 20.45h-3.55v-5.57c0-1.33-.02-3.04-1.85-3.04-1.85 0-2.13 1.45-2.13 2.94v5.67H9.36V9h3.41v1.56h.05c.47-.9 1.63-1.85 3.36-1.85 3.59 0 4.26 2.36 4.26 5.43v6.31zM5.34 7.43a2.06 2.06 0 1 1 0-4.12 2.06 2.06 0 0 1 0 4.12zM7.12 20.45H3.55V9h3.57v11.45zM22.22 0H1.77C.79 0 0 .77 0 1.72v20.56C0 23.23.79 24 1.77 24h20.45c.98 0 1.78-.77 1.78-1.72V1.72C24 .77 23.2 0 22.22 0z" />
                  </svg>
                  LinkedIn
                </a>
              )}
            </div>
          )}
        </div>
      )}

      {/* Insight */}
      {lead.insight && (
        <p className="mt-3 text-[13px] text-gray-300 leading-relaxed italic">
          {lead.insight}
        </p>
      )}

      {/* Signals — always visible, scannable, with freshness dots */}
      {renderableSignals.length > 0 && (
        <div className="mt-3">
          <h4 className="text-[10px] font-semibold tracking-widest text-gray-500 uppercase mb-1">
            Signals · {renderableSignals.length}
          </h4>
          <ul>
            {renderableSignals.map((s, i) => (
              <SignalRow key={i} signal={s} />
            ))}
          </ul>
        </div>
      )}

    </div>
  );
}
