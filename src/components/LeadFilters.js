import React, { useMemo } from "react";

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

// Each band: [label, predicate(headcount)]. Predicate accepts a number or null.
export const SIZE_BANDS = [
  { value: "any", label: "Any size" },
  { value: "1-10", label: "1–10 employees", min: 1, max: 10 },
  { value: "11-50", label: "11–50 employees", min: 11, max: 50 },
  { value: "51-200", label: "51–200 employees", min: 51, max: 200 },
  { value: "201-250", label: "201–250 employees", min: 201, max: 250 },
  { value: "unknown", label: "Size unknown" },
];

// Combined across niches — insurance + CFO pipelines emit different
// trigger_type values, but the dropdown is rendered once and gates
// individual options on whether the current page has any leads of
// that type (count > 0). Niche-specific entries with zero hits get
// suppressed before render below.
const TRIGGER_TYPES = [
  { value: "any", label: "All triggers" },
  // Insurance niche
  { value: "motor_carrier", label: "Motor carrier (commercial auto)" },
  { value: "federal_contract", label: "Federal contract (cyber / E&O / GL)" },
  { value: "new_entity", label: "New business filing" },
  // CFO niche
  { value: "finance_hire", label: "Finance lead hire (Controller / VP Finance)" },
  { value: "form_d", label: "Form D filing (post-funding board prep)" },
  // Shared
  { value: "funding_event", label: "Funding event (TechCrunch / PRNewswire)" },
];

export default function LeadFilters({
  leads,
  industry, setIndustry,
  sizeBand, setSizeBand,
  state, setState,
  trigger, setTrigger,
}) {
  const availableIndustries = useMemo(() => {
    const counts = {};
    for (const l of leads) {
      if (l.industry) counts[l.industry] = (counts[l.industry] || 0) + 1;
    }
    return Object.entries(counts).sort((a, b) => b[1] - a[1]);
  }, [leads]);

  const availableStates = useMemo(() => {
    const counts = {};
    for (const l of leads) {
      if (l.state) counts[l.state] = (counts[l.state] || 0) + 1;
    }
    return Object.entries(counts).sort((a, b) => a[0].localeCompare(b[0]));
  }, [leads]);

  const availableTriggers = useMemo(() => {
    const counts = {};
    for (const l of leads) {
      const t = l.trigger_type || "other";
      counts[t] = (counts[t] || 0) + 1;
    }
    return counts;
  }, [leads]);

  const clearAll = () => {
    setIndustry("any");
    setSizeBand("any");
    setState("any");
    if (setTrigger) setTrigger("any");
  };

  const hasActiveFilters =
    industry !== "any" || sizeBand !== "any" || state !== "any" ||
    (trigger && trigger !== "any");

  const selectClass =
    "w-full px-3 py-2.5 border border-slate-700 rounded-lg text-sm " +
    "bg-slate-900/60 text-gray-100 transition-colors " +
    "hover:bg-slate-900 hover:border-slate-600 " +
    "focus:outline-none focus:ring-2 focus:ring-blue-500/60 focus:border-blue-500 " +
    "appearance-none bg-no-repeat bg-right pr-10";

  // Inline SVG chevron tinted for dark backgrounds.
  const chevronStyle = {
    backgroundImage:
      "url(\"data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%239ca3af' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='m6 8 4 4 4-4'/%3e%3c/svg%3e\")",
    backgroundPosition: "right 0.7rem center",
    backgroundSize: "1.25rem",
  };

  return (
    <div className="bg-slate-800/70 backdrop-blur border border-slate-700/70 rounded-2xl shadow-lg shadow-black/10 mb-6 overflow-hidden">
      <div className={`px-5 py-4 grid grid-cols-1 gap-4 items-end ${setTrigger ? "md:grid-cols-4" : "md:grid-cols-3"}`}>
        {setTrigger && (
          <div>
            <label className="block text-[10px] font-semibold text-gray-400 uppercase tracking-widest mb-1.5">
              Trigger type
            </label>
            <select
              value={trigger || "any"}
              onChange={(e) => setTrigger(e.target.value)}
              className={selectClass}
              style={chevronStyle}
            >
              {TRIGGER_TYPES.map((t) => {
                const n = t.value === "any" ? leads.length : (availableTriggers[t.value] || 0);
                // Suppress trigger options the current niche has zero
                // hits for. Keeps the dropdown clean when an insurance
                // page renders next to a CFO page on the same shared
                // component.
                if (t.value !== "any" && n === 0) return null;
                return (
                  <option key={t.value} value={t.value}>
                    {t.label} ({n})
                  </option>
                );
              })}
            </select>
          </div>
        )}
        <div>
          <label className="block text-[10px] font-semibold text-gray-400 uppercase tracking-widest mb-1.5">
            Industry
          </label>
          <select
            value={industry}
            onChange={(e) => setIndustry(e.target.value)}
            className={selectClass}
            style={chevronStyle}
          >
            <option value="any">All industries</option>
            {availableIndustries.map(([ind, count]) => (
              <option key={ind} value={ind}>
                {INDUSTRY_LABELS[ind] || ind} ({count})
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-[10px] font-semibold text-gray-400 uppercase tracking-widest mb-1.5">
            Company size
          </label>
          <select
            value={sizeBand}
            onChange={(e) => setSizeBand(e.target.value)}
            className={selectClass}
            style={chevronStyle}
          >
            {SIZE_BANDS.map((b) => (
              <option key={b.value} value={b.value}>
                {b.label}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-[10px] font-semibold text-gray-400 uppercase tracking-widest mb-1.5">
            Location
          </label>
          <select
            value={state}
            onChange={(e) => setState(e.target.value)}
            className={selectClass}
            style={chevronStyle}
          >
            <option value="any">All locations</option>
            {availableStates.map(([st, count]) => (
              <option key={st} value={st}>
                {st} ({count})
              </option>
            ))}
          </select>
        </div>
      </div>

      {hasActiveFilters && (
        <div className="px-5 py-2 bg-slate-900/40 border-t border-slate-700/60 flex justify-end">
          <button
            onClick={clearAll}
            className="text-xs font-medium text-gray-400 hover:text-white transition-colors"
          >
            Reset filters →
          </button>
        </div>
      )}
    </div>
  );
}
