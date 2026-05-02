import React, { useMemo } from "react";

export default function LeadFilters({
  leads,
  industries, setIndustries,
  headcountMax, setHeadcountMax,
  search, setSearch,
  sortBy, setSortBy,
}) {
  const availableIndustries = useMemo(() => {
    const counts = {};
    for (const l of leads) {
      if (l.industry) counts[l.industry] = (counts[l.industry] || 0) + 1;
    }
    return Object.entries(counts).sort((a, b) => b[1] - a[1]);
  }, [leads]);

  const toggleIndustry = (ind) => {
    const next = new Set(industries);
    if (next.has(ind)) next.delete(ind);
    else next.add(ind);
    setIndustries(next);
  };

  const clearAll = () => {
    setIndustries(new Set());
    setHeadcountMax(250);
    setSearch("");
    setSortBy("score");
  };

  const hasActiveFilters = industries.size > 0 || headcountMax < 250 || search || sortBy !== "score";

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4 mb-6 space-y-4">
      <div className="flex flex-wrap items-center gap-4">
        <div className="flex-1 min-w-[200px]">
          <label className="block text-xs font-semibold text-gray-500 uppercase mb-1">
            Search by name
          </label>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Acme..."
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div>
          <label className="block text-xs font-semibold text-gray-500 uppercase mb-1">
            Max headcount
          </label>
          <div className="flex items-center gap-2">
            <input
              type="range"
              min="10"
              max="250"
              step="10"
              value={headcountMax}
              onChange={(e) => setHeadcountMax(parseInt(e.target.value, 10))}
              className="w-32"
            />
            <span className="text-sm text-gray-700 w-10 tabular-nums">{headcountMax}</span>
          </div>
        </div>

        <div>
          <label className="block text-xs font-semibold text-gray-500 uppercase mb-1">
            Sort by
          </label>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="px-3 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="score">Score (highest first)</option>
            <option value="name">Name (A–Z)</option>
          </select>
        </div>

        {hasActiveFilters && (
          <button
            onClick={clearAll}
            className="self-end px-3 py-1.5 text-xs text-gray-600 hover:text-gray-900 underline"
          >
            Clear filters
          </button>
        )}
      </div>

      {availableIndustries.length > 0 && (
        <div>
          <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">
            Filter by industry
          </label>
          <div className="flex flex-wrap gap-2">
            {availableIndustries.map(([ind, count]) => {
              const active = industries.has(ind);
              return (
                <button
                  key={ind}
                  onClick={() => toggleIndustry(ind)}
                  className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                    active
                      ? "bg-blue-600 text-white border-blue-600"
                      : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
                  }`}
                >
                  {ind} ({count})
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
