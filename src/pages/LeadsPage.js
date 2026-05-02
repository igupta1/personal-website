import React, { useEffect, useMemo, useState } from "react";
import LeadCard from "../components/LeadCard";
import LeadFilters from "../components/LeadFilters";

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

export default function LeadsPage({ niche, title, subtitle }) {
  const [leads, setLeads] = useState(null);
  const [error, setError] = useState(null);
  const [generatedAt, setGeneratedAt] = useState(null);

  const [industries, setIndustries] = useState(new Set());
  const [headcountMax, setHeadcountMax] = useState(250);
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("score");

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
    if (industries.size > 0) {
      out = out.filter((l) => l.industry && industries.has(l.industry));
    }
    if (headcountMax < 250) {
      out = out.filter((l) => l.headcount == null || l.headcount <= headcountMax);
    }
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      out = out.filter((l) => l.name.toLowerCase().includes(q));
    }
    if (sortBy === "name") {
      out = [...out].sort((a, b) => a.name.localeCompare(b.name));
    }
    return out;
  }, [leads, industries, headcountMax, search, sortBy]);

  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      <header className="mb-6">
        <h1 className="text-3xl font-bold text-gray-900">{title}</h1>
        {subtitle && <p className="mt-2 text-gray-600">{subtitle}</p>}
        {generatedAt && (
          <p className="mt-2 text-xs text-gray-500">
            Updated {formatRelative(generatedAt)}
          </p>
        )}
      </header>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-800 rounded p-4">
          Failed to load leads: {error}
        </div>
      )}

      {leads === null && !error && (
        <div className="text-gray-500 py-8 text-center">Loading…</div>
      )}

      {leads !== null && (
        <>
          <LeadFilters
            leads={leads}
            industries={industries}
            setIndustries={setIndustries}
            headcountMax={headcountMax}
            setHeadcountMax={setHeadcountMax}
            search={search}
            setSearch={setSearch}
            sortBy={sortBy}
            setSortBy={setSortBy}
          />

          <div className="text-sm text-gray-500 mb-3">
            Showing {visibleLeads.length} of {leads.length} leads
          </div>

          <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
            {visibleLeads.map((lead, i) => (
              <LeadCard key={`${lead.name}-${i}`} lead={lead} />
            ))}
          </div>

          {visibleLeads.length === 0 && (
            <div className="text-center text-gray-500 py-12">
              No leads match the current filters.
            </div>
          )}
        </>
      )}
    </div>
  );
}
