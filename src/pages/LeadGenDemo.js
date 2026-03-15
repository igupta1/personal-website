//LeadGenDemo.js
import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";

const parseLocalDate = (dateStr) => {
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day);
};

const SIZE_RANGES = [
  { label: "1-10", min: 1, max: 10 },
  { label: "11-25", min: 11, max: 25 },
  { label: "26-50", min: 26, max: 50 },
  { label: "51-100", min: 51, max: 100 },
  { label: "101-250", min: 101, max: 250 },
];

const SPECIALTY_KEYWORDS = {
  "Social Media": ["social media", "community manager", "social content", "social strategist"],
  "Paid Ads": ["paid media", "paid search", "ppc", "ad operations", "advertising", "media planner", "media planning", "media buyer", "media buying"],
  "Content": ["content strategist", "content coordinator", "copywriter", "copy editor", "content marketing", "web content", "content writer", "content manager", "content specialist"],
  "SEO": ["seo", "search engine", "organic search", "organic growth"],
  "PR": ["public relations", "pr ", "communications", "comms ", "press", "media relations"],
  "Email Marketing": ["email marketing", "email", "crm", "lifecycle", "retention marketing", "marketing automation"],
  "Web/Ecommerce": ["ecommerce", "e-commerce", "web developer", "web designer", "webmaster", "digital merchandising", "shopify", "web content"],
  "Brand/Creative": ["brand ", "branding", "creative director", "creative manager", "graphic design", "art director", "visual design", "creative strategist"],
  "Growth/Demand Gen": ["growth marketing", "demand gen", "demand generation", "growth manager", "head of growth", "acquisition", "performance marketing", "gtm"],
  "General Marketing": ["marketing coordinator", "marketing specialist", "marketing manager", "marketing associate", "marketing assistant", "marketing director", "marketing analyst", "digital marketing"],
};

const getCompanySpecialties = (roles) => {
  const specialties = new Set();
  for (const role of roles) {
    const lower = role.title.toLowerCase();
    for (const [specialty, keywords] of Object.entries(SPECIALTY_KEYWORDS)) {
      if (keywords.some(kw => lower.includes(kw))) {
        specialties.add(specialty);
      }
    }
  }
  return specialties;
};

function LeadGenDemo() {
  const [companies, setCompanies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedCompanies, setExpandedCompanies] = useState({});
  const [selectedIndustries, setSelectedIndustries] = useState([]);
  const [selectedSizeRanges, setSelectedSizeRanges] = useState([]);
  const [filtersExpanded, setFiltersExpanded] = useState(false);
  const [selectedSpecialty, setSelectedSpecialty] = useState(null);

  const fetchLeads = async () => {
    const location = "marketing-discovery";

    try {
      const API_URL = process.env.REACT_APP_API_URL || '';
      const response = await fetch(`${API_URL}/api/generate-leads`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ location })
      });

      if (!response.ok) {
        throw new Error(`API request failed: ${response.status}`);
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.log('Using demo data (API not available locally)');
      const today = new Date();
      const formatDate = (daysAgo) => {
        const d = new Date(today);
        d.setDate(d.getDate() - daysAgo);
        return d.toISOString().split('T')[0];
      };
      return {
        success: true,
        leads: [
          { companyName: "Inspira Education", firstName: "Xavier", lastName: "Portillo", title: "Co-Founder Senior Advisor", jobRole: "Copy Editor", confidence: "Medium", website: "https://inspiraeducation.com", postingDate: formatDate(23), mostRecentPostingDate: formatDate(1), industry: "Education", employeeCount: 45 },
          { companyName: "Inspira Education", firstName: "Xavier", lastName: "Portillo", title: "Co-Founder Senior Advisor", jobRole: "Growth Marketing Manager", confidence: "Medium", website: "https://inspiraeducation.com", postingDate: formatDate(1), mostRecentPostingDate: formatDate(1), industry: "Education", employeeCount: 45 },
          { companyName: "Emergent", firstName: "Mukund", lastName: "Jha", title: "CEO", jobRole: "Head of Growth", confidence: "High", website: "https://emergent.com", postingDate: formatDate(8), mostRecentPostingDate: formatDate(8), industry: "SaaS / Technology", employeeCount: 120 },
          { companyName: "Emergent", firstName: "Mukund", lastName: "Jha", title: "CEO", jobRole: "Growth Manager", confidence: "High", website: "https://emergent.com", postingDate: formatDate(8), mostRecentPostingDate: formatDate(8), industry: "SaaS / Technology", employeeCount: 120 },
          { companyName: "Feastables", firstName: "Ryan", lastName: "Brisco", title: "Vice President Of Marketing", jobRole: "Shopper Marketing Manager", confidence: "High", website: "https://feastables.com", postingDate: formatDate(9), mostRecentPostingDate: formatDate(9), industry: "Food & Beverage", employeeCount: 80 },
        ]
      };
    }
  };

  const groupByCompany = (leads) => {
    const grouped = {};

    leads.forEach(lead => {
      const companyName = lead.companyName;
      if (!grouped[companyName]) {
        grouped[companyName] = {
          companyName,
          website: lead.website,
          category: lead.category || '',
          industry: lead.industry || '',
          employeeCount: lead.employeeCount || 0,
          mostRecentPostingDate: lead.mostRecentPostingDate || lead.postingDate || '',
          insight: lead.insight || '',
          priorityTier: lead.priorityTier || '',
          outreachDraft: lead.outreachDraft || '',
          decisionMaker: {
            firstName: lead.firstName,
            lastName: lead.lastName,
            title: lead.title,
            sourceUrl: lead.sourceUrl,
            confidence: lead.confidence
          },
          roles: []
        };
      }
      // Update mostRecentPostingDate - check both mostRecentPostingDate and postingDate
      const leadDate = lead.mostRecentPostingDate || lead.postingDate || '';
      if (leadDate && leadDate > grouped[companyName].mostRecentPostingDate) {
        grouped[companyName].mostRecentPostingDate = leadDate;
      }
      if (lead.jobRole && !grouped[companyName].roles.some(r => r.title === lead.jobRole)) {
        grouped[companyName].roles.push({
          title: lead.jobRole,
          jobLink: lead.jobLink,
          postingDate: lead.postingDate
        });
      }
    });

    // Filter out P5 companies, then sort by: 1) Priority tier, 2) Has decision maker, 3) Most recent posting date
    const priorityOrder = { 'P1': 1, 'P2': 2, 'P3': 3, 'P4': 4 };
    const result = Object.values(grouped).filter(c => c.priorityTier !== 'P5').sort((a, b) => {
      const aPriority = priorityOrder[a.priorityTier] || 6;
      const bPriority = priorityOrder[b.priorityTier] || 6;
      if (aPriority !== bPriority) return aPriority - bPriority;
      const aHasDM = a.decisionMaker && a.decisionMaker.firstName ? 1 : 0;
      const bHasDM = b.decisionMaker && b.decisionMaker.firstName ? 1 : 0;
      if (aHasDM !== bHasDM) return bHasDM - aHasDM;
      const aDate = a.mostRecentPostingDate ? parseLocalDate(a.mostRecentPostingDate) : new Date(0);
      const bDate = b.mostRecentPostingDate ? parseLocalDate(b.mostRecentPostingDate) : new Date(0);
      return bDate - aDate;
    });

    return result;
  };

  const getFilteredCompanies = () => {
    let filtered = companies;

    if (selectedSpecialty) {
      filtered = filtered.filter(c => getCompanySpecialties(c.roles).has(selectedSpecialty));
    }

    if (selectedIndustries.length > 0) {
      filtered = filtered.filter(c => selectedIndustries.includes(c.industry));
    }

    if (selectedSizeRanges.length > 0) {
      filtered = filtered.filter(c => {
        const count = c.employeeCount;
        if (!count || count === 0) return false;
        return selectedSizeRanges.some(range => count >= range.min && count <= range.max);
      });
    }

    return filtered;
  };

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      const result = await fetchLeads();

      if (result.success) {
        const groupedCompanies = groupByCompany(result.leads || []);
        setCompanies(groupedCompanies);
      } else {
        setError(result.error || 'Failed to load leads');
      }
      setLoading(false);
    };

    loadData();
  }, []);

  const formatDate = (dateStr) => {
    if (!dateStr || dateStr === 'Unknown') return null;
    try {
      const date = parseLocalDate(dateStr);
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    } catch {
      return dateStr;
    }
  };

  const getDaysAgo = (dateStr) => {
    if (!dateStr) return null;
    try {
      const posted = parseLocalDate(dateStr);
      // Check if date is valid
      if (isNaN(posted.getTime())) return null;
      const now = new Date();
      const diffTime = now - posted;
      const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
      // Return null for negative days (future dates) or very old dates
      if (diffDays < 0) return 0; // Treat future dates as "today"
      return diffDays;
    } catch {
      return null;
    }
  };

  const toggleCompanyExpanded = (companyName) => {
    setExpandedCompanies(prev => ({
      ...prev,
      [companyName]: !prev[companyName]
    }));
  };

  const filteredCompanies = getFilteredCompanies();
  const activeFilterCount = selectedIndustries.length + selectedSizeRanges.length + (selectedSpecialty ? 1 : 0);

  // Only show specialties that have at least one matching company
  const availableSpecialties = Object.keys(SPECIALTY_KEYWORDS).filter(specialty =>
    companies.some(c => getCompanySpecialties(c.roles).has(specialty))
  );

  // Derive available filter options from actual data
  const availableIndustries = [...new Set(companies.map(c => c.industry).filter(Boolean))].sort();
  const availableSizeRanges = SIZE_RANGES.filter(range =>
    companies.some(c => c.employeeCount && c.employeeCount >= range.min && c.employeeCount <= range.max)
  );
  const hasAnyFilters = availableIndustries.length > 0 || availableSizeRanges.length > 0;

  return (
    <div className="about-page-combined">
      <div className="about-container" style={{ gridTemplateColumns: '1fr' }}>
        <div className="about-right-column">
          <section className="demo-section">
            <div className="demo-top-row">
              <div className="demo-section-header">
                <Link to="/gtm" className="back-link-inline">Back to GTM Systems</Link>
                <h2>Find Companies Needing Marketing Help</h2>
              </div>
            </div>

            {error && (
              <div className="demo-error">
                <span className="demo-error-icon">Warning</span>
                <span>{error}</span>
                <button onClick={() => setError(null)} className="demo-error-close">x</button>
              </div>
            )}

            {loading ? (
              <div className="demo-loading">
                <div className="demo-loading-spinner"></div>
                <p>Loading leads...</p>
              </div>
            ) : (
              <div className="demo-results-stage">
                {/* Specialty Filter Pills */}
                {availableSpecialties.length > 0 && (
                  <div className="lead-gen-specialty-filters">
                    <button
                      className={`lead-gen-specialty-pill ${!selectedSpecialty ? 'active' : ''}`}
                      onClick={() => setSelectedSpecialty(null)}
                    >
                      All
                    </button>
                    {availableSpecialties.map(specialty => (
                      <button
                        key={specialty}
                        className={`lead-gen-specialty-pill ${selectedSpecialty === specialty ? 'active' : ''}`}
                        onClick={() => setSelectedSpecialty(selectedSpecialty === specialty ? null : specialty)}
                      >
                        {specialty}
                      </button>
                    ))}
                  </div>
                )}

                {/* Filter Panel - only show if data has filterable fields */}
                {hasAnyFilters && (
                <div className="lead-gen-filter-panel">
                  <button
                    className="lead-gen-filter-toggle"
                    onClick={() => setFiltersExpanded(!filtersExpanded)}
                  >
                    Filters
                    {activeFilterCount > 0 && (
                      <span className="lead-gen-filter-count">{activeFilterCount}</span>
                    )}
                    <span className={`lead-gen-filter-chevron ${filtersExpanded ? 'expanded' : ''}`}>
                      &#9660;
                    </span>
                  </button>

                  {filtersExpanded && (
                    <div className="lead-gen-filter-content">
                      {availableIndustries.length > 0 && (
                      <div className="lead-gen-filter-group">
                        <h5 className="lead-gen-filter-group-title">Industry</h5>
                        <div className="lead-gen-filter-options">
                          {availableIndustries.map(industry => (
                            <label key={industry} className="lead-gen-filter-checkbox">
                              <input
                                type="checkbox"
                                checked={selectedIndustries.includes(industry)}
                                onChange={() => {
                                  setSelectedIndustries(prev =>
                                    prev.includes(industry)
                                      ? prev.filter(i => i !== industry)
                                      : [...prev, industry]
                                  );
                                }}
                              />
                              <span>{industry}</span>
                            </label>
                          ))}
                        </div>
                      </div>
                      )}

                      {availableSizeRanges.length > 0 && (
                      <div className="lead-gen-filter-group">
                        <h5 className="lead-gen-filter-group-title">Company Size</h5>
                        <div className="lead-gen-filter-options">
                          {availableSizeRanges.map(range => (
                            <label key={range.label} className="lead-gen-filter-checkbox">
                              <input
                                type="checkbox"
                                checked={selectedSizeRanges.some(r => r.label === range.label)}
                                onChange={() => {
                                  setSelectedSizeRanges(prev =>
                                    prev.some(r => r.label === range.label)
                                      ? prev.filter(r => r.label !== range.label)
                                      : [...prev, range]
                                  );
                                }}
                              />
                              <span>{range.label} employees</span>
                            </label>
                          ))}
                        </div>
                      </div>
                      )}

                      {activeFilterCount > 0 && (
                        <button
                          className="lead-gen-filter-clear"
                          onClick={() => {
                            setSelectedIndustries([]);
                            setSelectedSizeRanges([]);
                            setSelectedSpecialty(null);
                          }}
                        >
                          Clear all filters
                        </button>
                      )}
                    </div>
                  )}
                </div>
                )}

                <div className="lead-gen-companies-list">
                  {filteredCompanies.map((company, index) => {
                    const daysAgo = getDaysAgo(company.mostRecentPostingDate);
                    const isExpanded = expandedCompanies[company.companyName];

                    return (
                      <div key={index} className="lead-gen-company-card">
                        {/* Company Header */}
                        <div className="lead-gen-company-header">
                          <div className="lead-gen-company-info">
                            <h3 className="lead-gen-company-name">
                              {company.companyName}
                            </h3>
                            {company.website && (
                              <a href={company.website} target="_blank" rel="noopener noreferrer" className="lead-gen-company-website">
                                {company.website.replace('https://', '')}
                              </a>
                            )}
                            {company.employeeCount > 0 && (
                              <span className="lead-gen-employee-count">
                                About {Math.round(company.employeeCount / 10) * 10} Employees
                              </span>
                            )}
                            {daysAgo !== null && (
                              <span className="lead-gen-posted-badge">
                                {daysAgo === 0 ? 'Role Posted Today' : `Role Posted ${daysAgo} ${daysAgo === 1 ? 'Day' : 'Days'} Ago`}
                                {daysAgo <= 1 && <span className="lead-gen-fire-emoji"> 🔥</span>}
                              </span>
                            )}
                          </div>
                        </div>

                        {/* Roles Section - First 3 visible, rest behind "See more" */}
                        {company.roles.length > 0 && (
                          <div className="lead-gen-roles-section">
                            <h4 className="lead-gen-section-title">
                              Open Positions
                            </h4>
                            <div className="lead-gen-roles-list">
                              {company.roles.slice(0, 3).map((role, roleIndex) => (
                                <div key={roleIndex} className="lead-gen-role-item">
                                  <span className="lead-gen-role-title">{role.title}</span>
                                  {role.postingDate && formatDate(role.postingDate) && (
                                    <span className="lead-gen-role-date">{formatDate(role.postingDate)}</span>
                                  )}
                                  {role.jobLink && (
                                    <a href={role.jobLink} target="_blank" rel="noopener noreferrer" className="lead-gen-role-link">
                                      View →
                                    </a>
                                  )}
                                </div>
                              ))}
                              {isExpanded && company.roles.slice(3).map((role, roleIndex) => (
                                <div key={roleIndex + 3} className="lead-gen-role-item">
                                  <span className="lead-gen-role-title">{role.title}</span>
                                  {role.postingDate && formatDate(role.postingDate) && (
                                    <span className="lead-gen-role-date">{formatDate(role.postingDate)}</span>
                                  )}
                                  {role.jobLink && (
                                    <a href={role.jobLink} target="_blank" rel="noopener noreferrer" className="lead-gen-role-link">
                                      View →
                                    </a>
                                  )}
                                </div>
                              ))}
                            </div>
                            {company.roles.length > 3 && (
                              <button
                                className="lead-gen-see-more"
                                onClick={() => toggleCompanyExpanded(company.companyName)}
                              >
                                {isExpanded ? 'Show less' : `See ${company.roles.length - 3} more role${company.roles.length - 3 === 1 ? '' : 's'}`}
                              </button>
                            )}
                          </div>
                        )}

                        {/* Decision Maker Section */}
                        {company.decisionMaker && company.decisionMaker.firstName && (
                          <div className="lead-gen-dm-section">
                            <h4 className="lead-gen-section-title">Decision Maker</h4>
                            <div className="lead-gen-dm-card">
                              <div className="lead-gen-dm-avatar">
                                {company.decisionMaker.firstName.charAt(0)}
                                {company.decisionMaker.lastName ? company.decisionMaker.lastName.charAt(0) : ''}
                              </div>
                              <div className="lead-gen-dm-info">
                                <div className="lead-gen-dm-name">
                                  {company.decisionMaker.firstName} {company.decisionMaker.lastName}
                                </div>
                                <div className="lead-gen-dm-title">{company.decisionMaker.title}</div>
                                <div className="lead-gen-dm-links">
                                </div>
                              </div>
                            </div>
                          </div>
                        )}

                        {/* AI Insight */}
                        {company.insight && (
                          <div className="lead-gen-insight-section">
                            <h4 className="lead-gen-section-title">How A Marketing Agency Can Help</h4>
                            <div className="lead-gen-insight-card">
                              <p className="lead-gen-insight-text">{company.insight}</p>
                            </div>
                          </div>
                        )}

                        {/* Outreach Draft */}
                        {company.outreachDraft && (
                          <div className="lead-gen-insight-section">
                            <h4 className="lead-gen-section-title">Personalized Introduction Copy</h4>
                            <div className="lead-gen-insight-card">
                              <p className="lead-gen-insight-text">{company.outreachDraft}</p>
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

export default LeadGenDemo;
