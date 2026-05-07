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

export default function LeadsPage({ niche, title, subtitle }) {
  const [leads, setLeads] = useState(null);
  const [error, setError] = useState(null);
  const [generatedAt, setGeneratedAt] = useState(null);

  const [industry, setIndustry] = useState("any");
  const [sizeBand, setSizeBand] = useState("any");
  const [sortBy, setSortBy] = useState("default");

  useEffect(() => {
    document.title = `${title} — Ishaan Gupta`;
  }, [title]);

  useEffect(() => {
    let cancelled = false;
    setLeads(null);
    setError(null);
    fetch(`/api/generate-leads?niche=${niche}`)
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
  }, [niche]);

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
    if (sortBy === "name") {
      out = [...out].sort((a, b) => a.name.localeCompare(b.name));
    }
    return out;
  }, [leads, industry, sizeBand, sortBy]);

  return (
    <div className="max-w-6xl mx-auto px-4 sm:px-6 py-10">
      <header className="mb-8">
        <div className="flex items-center gap-3">
          <span className="inline-flex h-2.5 w-2.5 rounded-full bg-emerald-400 ring-2 ring-emerald-400/30" />
          <span className="text-xs font-semibold tracking-widest uppercase text-emerald-300">
            Live lead feed
          </span>
        </div>
        <h1 className="mt-3 text-4xl sm:text-5xl font-bold text-white tracking-tight">
          {title}
        </h1>
        {subtitle && (
          <p className="mt-3 text-base sm:text-lg text-gray-300 max-w-3xl leading-relaxed">
            {subtitle}
          </p>
        )}
        {generatedAt && (
          <p className="mt-3 text-xs text-gray-400">
            Updated {formatRelative(generatedAt)} · refreshed automatically every night
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
            sortBy={sortBy}
            setSortBy={setSortBy}
          />

          <div className="flex items-center justify-between mb-4">
            <span className="inline-flex items-center gap-2 text-sm text-gray-300">
              <span className="font-semibold text-white">{visibleLeads.length}</span>
              <span className="text-gray-400">of {leads.length} leads</span>
            </span>
          </div>

          <div className="grid gap-5 grid-cols-1 lg:grid-cols-2">
            {visibleLeads.map((lead, i) => (
              <LeadCard key={`${lead.name}-${i}`} lead={lead} />
            ))}
          </div>

          {visibleLeads.length === 0 && (
            <div className="text-center text-gray-300 py-16">
              <div className="text-base font-medium">No leads match the current filters.</div>
              <div className="text-sm text-gray-400 mt-1">Try widening the size or industry selector.</div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
