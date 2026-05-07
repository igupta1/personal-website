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

export default function LeadFilters({
  leads,
  industry, setIndustry,
  sizeBand, setSizeBand,
  sortBy, setSortBy,
}) {
  const availableIndustries = useMemo(() => {
    const counts = {};
    for (const l of leads) {
      if (l.industry) counts[l.industry] = (counts[l.industry] || 0) + 1;
    }
    return Object.entries(counts).sort((a, b) => b[1] - a[1]);
  }, [leads]);

  const clearAll = () => {
    setIndustry("any");
    setSizeBand("any");
    setSortBy("default");
  };

  const hasActiveFilters =
    industry !== "any" || sizeBand !== "any" || sortBy !== "default";

  return (
    <div className="bg-white border border-gray-200 rounded-xl shadow-sm mb-6 overflow-hidden">
      <div className="px-5 py-4 grid grid-cols-1 md:grid-cols-3 gap-4 items-end">
        <div>
          <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1.5">
            Industry
          </label>
          <select
            value={industry}
            onChange={(e) => setIndustry(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
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
          <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1.5">
            Company size
          </label>
          <select
            value={sizeBand}
            onChange={(e) => setSizeBand(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            {SIZE_BANDS.map((b) => (
              <option key={b.value} value={b.value}>
                {b.label}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1.5">
            Sort by
          </label>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="default">Best match</option>
            <option value="name">Name (A–Z)</option>
          </select>
        </div>
      </div>

      {hasActiveFilters && (
        <div className="px-5 py-2 bg-gray-50 border-t border-gray-200 flex justify-end">
          <button
            onClick={clearAll}
            className="text-xs font-medium text-gray-600 hover:text-gray-900 underline"
          >
            Reset filters
          </button>
        </div>
      )}
    </div>
  );
}
