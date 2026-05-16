import React, { useEffect, useMemo, useState } from "react";
import LeadCard from "../components/LeadCard";
import LeadFilters, { SIZE_BANDS } from "../components/LeadFilters";

function formatRelative(iso) {
  if (!iso) return null;
  const then = new Date(iso);
  const diffMs = Date.now() - then.getTime();
  const mins = Math.round(diffMs / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins} min ago`;
  const hours = Math.round(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

function bandFor(value) {
  return SIZE_BANDS.find((b) => b.value === value) || SIZE_BANDS[0];
}

export default function LeadsPage({ niche, title, subtitle, apiPath }) {
  const [leads, setLeads] = useState(null);
  const [error, setError] = useState(null);
  const [generatedAt, setGeneratedAt] = useState(null);

  const [industry, setIndustry] = useState("any");
  const [sizeBand, setSizeBand] = useState("any");
  const [state, setState] = useState("any");
  const [trigger, setTrigger] = useState("any");

  useEffect(() => {
    document.title = `${title} — Ishaan Gupta`;
  }, [title]);

  useEffect(() => {
    let cancelled = false;
    setLeads(null);
    setError(null);
    // Insurance reads from its own dedicated endpoint (no `niche` qs);
    // MSP/MSSP/Cloud share `/api/generate-leads?niche=<n>`.
    const url = apiPath ?? `/api/generate-leads?niche=${niche}`;
    fetch(url)
      .then((r) =>
        r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))
      )
      .then((d) => {
        if (cancelled) return;
        setLeads(d.leads);
        setGeneratedAt(d.generated_at);
      })
      .catch((e) => {
        if (!cancelled) setError(e.message);
      });
    return () => {
      cancelled = true;
    };
  }, [niche, apiPath]);

  // Insurance niche emits a trigger_type field; the MSP niches don't,
  // so the dropdown is only rendered when at least one lead carries
  // the field (the LeadFilters component gates rendering on
  // `setTrigger` being truthy, which we hand in only for insurance).
  const supportsTriggerFilter = !!(leads && leads.length && leads.some((l) => l.trigger_type != null));

  const visibleLeads = useMemo(() => {
    if (!leads) return [];
    let out = leads;
    if (industry !== "any") {
      out = out.filter((l) => l.industry === industry);
    }
    if (sizeBand !== "any") {
      const band = bandFor(sizeBand);
      if (sizeBand === "unknown") {
        out = out.filter((l) => l.headcount == null);
      } else if (band.min != null && band.max != null) {
        out = out.filter(
          (l) => l.headcount != null && l.headcount >= band.min && l.headcount <= band.max
        );
      }
    }
    if (state !== "any") {
      out = out.filter((l) => l.state === state);
    }
    if (supportsTriggerFilter && trigger !== "any") {
      out = out.filter((l) => l.trigger_type === trigger);
    }
    return out;
  }, [leads, industry, sizeBand, state, trigger, supportsTriggerFilter]);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
      <header className="mb-6">
        <div className="flex items-center gap-2">
          <span className="inline-flex h-2 w-2 rounded-full bg-emerald-400 ring-2 ring-emerald-400/25" />
          <span className="text-[10px] font-semibold tracking-widest uppercase text-emerald-300">
            Live feed
          </span>
        </div>
        <h1 className="mt-2 text-2xl sm:text-3xl font-semibold text-white tracking-tight">
          {title}
        </h1>
        {subtitle && (
          <p className="mt-2 text-sm text-gray-400 max-w-3xl leading-relaxed">
            {subtitle}
          </p>
        )}
        {generatedAt && (
          <p className="mt-1.5 text-xs text-gray-500">
            Updated {formatRelative(generatedAt)} · refreshed nightly
          </p>
        )}
      </header>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-800 rounded-xl p-4 shadow-sm">
          Failed to load leads: {error}
        </div>
      )}

      {leads === null && !error && (
        <div className="text-gray-300 py-12 text-center">Loading…</div>
      )}

      {leads !== null && (
        <>
          <LeadFilters
            leads={leads}
            industry={industry}
            setIndustry={setIndustry}
            sizeBand={sizeBand}
            setSizeBand={setSizeBand}
            state={state}
            setState={setState}
            trigger={supportsTriggerFilter ? trigger : undefined}
            setTrigger={supportsTriggerFilter ? setTrigger : undefined}
          />

          <div className="flex items-center justify-between mb-4">
            <span className="inline-flex items-center gap-1.5 text-xs text-gray-400">
              <span className="font-semibold text-gray-200">{visibleLeads.length}</span>
              <span>of {leads.length} leads</span>
            </span>
          </div>

          <div className="grid gap-4 grid-cols-1 md:grid-cols-2 lg:grid-cols-3">
            {visibleLeads.map((lead, i) => (
              <LeadCard key={`${lead.name}-${i}`} lead={lead} />
            ))}
          </div>

          {visibleLeads.length === 0 && (
            <div className="text-center text-gray-400 py-16">
              <div className="text-sm font-medium">No leads match the current filters.</div>
              <div className="text-xs text-gray-500 mt-1">Try widening the size or industry selector.</div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
