// MarketingCheck.js
import React, { useState, useEffect, useRef } from "react";
import { Link } from "react-router-dom";

const CHECK_LABELS = [
  "Analyzing page speed & Core Web Vitals",
  "Checking mobile responsiveness",
  "Reviewing SEO fundamentals",
  "Inspecting social & Open Graph tags",
  "Detecting Google Business Profile",
  "Analyzing conversion elements & CTAs",
  "Identifying technology & CMS",
];

function validateDomain(input) {
  if (!input) return null;
  let domain = input.trim().toLowerCase();
  domain = domain.replace(/^https?:\/\//, "");
  domain = domain.replace(/^www\./, "");
  domain = domain.replace(/\/.*$/, "");
  domain = domain.replace(/:.*$/, "");
  if (!/^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$/.test(domain)) {
    return null;
  }
  return domain;
}

function MarketingCheck() {
  const [domainInput, setDomainInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [report, setReport] = useState(null);
  const [visibleChecks, setVisibleChecks] = useState(0);
  const [expandedSections, setExpandedSections] = useState({});
  const intervalRef = useRef(null);

  useEffect(() => {
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, []);

  const handleScan = async (e) => {
    e.preventDefault();
    const domain = validateDomain(domainInput);
    if (!domain) {
      setError('Please enter a valid domain (e.g., "example.com")');
      return;
    }

    setError(null);
    setReport(null);
    setLoading(true);
    setVisibleChecks(0);
    setExpandedSections({});

    let count = 0;
    intervalRef.current = setInterval(() => {
      count++;
      setVisibleChecks(count);
      if (count >= CHECK_LABELS.length) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    }, 500);

    try {
      const apiUrl = process.env.REACT_APP_API_URL || "";
      const response = await fetch(`${apiUrl}/api/marketing-scan`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ domain }),
      });

      const data = await response.json();

      if (!response.ok) {
        setError(data.error || `Scan failed (status ${response.status})`);
        setLoading(false);
        return;
      }

      setReport(data);
    } catch (err) {
      setError("Failed to connect to the server. Please try again.");
    } finally {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
      setLoading(false);
    }
  };

  const handleNewScan = () => {
    setReport(null);
    setError(null);
    setDomainInput("");
    setVisibleChecks(0);
  };

  const toggleSection = (key) => {
    setExpandedSections((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const getSectionStatus = (checkKey, check) => {
    if (!check) return { label: "Could Not Check", className: "na" };
    if (check.status === "error") return { label: "Could Not Check", className: "na" };

    switch (checkKey) {
      case "pageSpeed": {
        const mScore = check.mobile?.performanceScore;
        if (mScore === undefined) return { label: "Could Not Check", className: "na" };
        return mScore >= 70
          ? { label: "Looking Good", className: "good" }
          : { label: "Opportunities Found", className: "opportunities" };
      }
      case "mobile": {
        if (!check.viewportMeta) return { label: "Opportunities Found", className: "opportunities" };
        return { label: "Looking Good", className: "good" };
      }
      case "seo": {
        const issues = [!check.title?.found, !check.metaDescription?.found, !check.h1?.found, !check.sitemap?.found, check.robotsMeta?.blocking].filter(Boolean).length;
        return issues > 0
          ? { label: "Opportunities Found", className: "opportunities" }
          : { label: "Looking Good", className: "good" };
      }
      case "social": {
        const ogMissing = check.og && Object.values(check.og).some(t => !t.present);
        return ogMissing
          ? { label: "Opportunities Found", className: "opportunities" }
          : { label: "Looking Good", className: "good" };
      }
      case "googleBusiness": {
        const hasSignals = check.mapsEmbed || check.mapsLink || check.localBusinessSchema;
        return hasSignals
          ? { label: "Looking Good", className: "good" }
          : { label: "Opportunities Found", className: "opportunities" };
      }
      case "conversion": {
        const issues = [check.forms?.count === 0, check.ctas?.length === 0, check.analytics?.length === 0].filter(Boolean).length;
        return issues > 0
          ? { label: "Opportunities Found", className: "opportunities" }
          : { label: "Looking Good", className: "good" };
      }
      case "technology": {
        return { label: "Detected", className: "good" };
      }
      default:
        return { label: "Checked", className: "good" };
    }
  };

  return (
    <div className="about-page-combined">
      <div className="about-container" style={{ gridTemplateColumns: "1fr" }}>
        <div className="about-right-column">
          <section className="demo-section">
            <div className="demo-top-row">
              <div className="demo-section-header">
                <Link to="/gtm" className="back-link-inline">
                  ← Back to Pipeline GTM
                </Link>
                <h2>Marketing Readiness Scorecard</h2>
                <p className="demo-section-subtitle">
                  Scan any business website for marketing gaps and opportunities in seconds
                </p>
              </div>
            </div>

            {/* Input Section */}
            {!loading && !report && (
              <div className="mkt-check-input-section">
                <p>
                  Enter a website URL to get a comprehensive marketing audit including page speed, SEO, social presence, conversion elements, and technology stack.
                </p>
                <form className="mkt-check-input-form" onSubmit={handleScan}>
                  <input
                    type="text"
                    className="mkt-check-input"
                    placeholder="Enter any business website URL (e.g., acmeplumbing.com)"
                    value={domainInput}
                    onChange={(e) => setDomainInput(e.target.value)}
                    autoFocus
                  />
                  <button type="submit" className="mkt-check-btn">
                    Run Marketing Scan
                  </button>
                </form>
              </div>
            )}

            {/* Error */}
            {error && (
              <div className="mkt-check-error">
                <span>⚠</span>
                <span>{error}</span>
              </div>
            )}

            {/* Loading State */}
            {loading && (
              <div className="mkt-check-loading">
                <h3>Scanning {validateDomain(domainInput)}...</h3>
                <p className="mkt-check-loading-note">This may take 15-30 seconds as we analyze the website</p>
                <div className="mkt-check-progress-list">
                  {CHECK_LABELS.map((label, i) => (
                    i < visibleChecks && (
                      <div
                        key={i}
                        className="mkt-check-progress-item active"
                        style={{ animationDelay: `${i * 0.1}s` }}
                      >
                        <div className="mkt-check-spinner" />
                        <span>{label}...</span>
                      </div>
                    )
                  ))}
                </div>
              </div>
            )}

            {/* Report */}
            {report && (
              <div className="mkt-check-report">
                {/* Header */}
                <div className="mkt-check-report-header">
                  <h2>{report.domain} Marketing Readiness Report</h2>
                  <span className="mkt-check-meta">
                    Scanned on {new Date(report.scanDate).toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric", hour: "2-digit", minute: "2-digit" })}
                    {report.scanDuration && ` · ${(report.scanDuration / 1000).toFixed(1)}s`}
                  </span>
                </div>

                {/* HTML Fetch Warning */}
                {report.checks?.htmlFetchError && (
                  <div className="mkt-check-warning">
                    Note: We had trouble fetching the website's HTML ({report.checks.htmlFetchError}). Some checks may have limited data.
                  </div>
                )}

                {/* Executive Summary */}
                {report.summary?.executiveSummary && (
                  <div className="mkt-check-card">
                    <h3>Executive Summary</h3>
                    <p>{report.summary.executiveSummary}</p>
                  </div>
                )}

                {/* 3 Quick Wins */}
                {report.summary?.quickWins?.length > 0 && (
                  <div className="mkt-check-card">
                    <h3>3 Quick Wins</h3>
                    <ul className="mkt-check-recs-list">
                      {report.summary.quickWins.map((win, i) => (
                        <li key={i} className="mkt-check-rec-item">
                          <h4>{i + 1}. {win.title}</h4>
                          <p>{win.description}</p>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* Missed Opportunities */}
                {report.summary?.missedOpportunities && (
                  <div className="mkt-check-card">
                    <h3>Missed Opportunities</h3>
                    <p>{report.summary.missedOpportunities}</p>
                  </div>
                )}

                {/* Detailed Findings */}
                <CollapsibleSection
                  title="Page Speed & Core Web Vitals"
                  sectionKey="pageSpeed"
                  check={report.checks.pageSpeed}
                  expanded={expandedSections.pageSpeed}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <PageSpeedDetails check={report.checks.pageSpeed} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="Mobile Responsiveness"
                  sectionKey="mobile"
                  check={report.checks.mobile}
                  expanded={expandedSections.mobile}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <MobileDetails check={report.checks.mobile} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="SEO Fundamentals"
                  sectionKey="seo"
                  check={report.checks.seo}
                  expanded={expandedSections.seo}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <SeoDetails check={report.checks.seo} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="Social & Open Graph"
                  sectionKey="social"
                  check={report.checks.social}
                  expanded={expandedSections.social}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <SocialOgDetails check={report.checks.social} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="Google Business Profile"
                  sectionKey="googleBusiness"
                  check={report.checks.googleBusiness}
                  expanded={expandedSections.googleBusiness}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <GbpDetails check={report.checks.googleBusiness} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="Conversion & CTAs"
                  sectionKey="conversion"
                  check={report.checks.conversion}
                  expanded={expandedSections.conversion}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <ConversionDetails check={report.checks.conversion} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="Technology & CMS"
                  sectionKey="technology"
                  check={report.checks.technology}
                  expanded={expandedSections.technology}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <TechCmsDetails check={report.checks.technology} />
                </CollapsibleSection>

                {/* New Scan */}
                <div className="mkt-check-new-scan">
                  <button onClick={handleNewScan}>Scan Another Domain</button>
                </div>

                {/* Footer */}
                <div className="mkt-check-footer">
                  <p>Report generated by Pipeline GTM — Powered by AI-driven marketing analysis</p>
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

// ─── Collapsible Section ───

function CollapsibleSection({ title, sectionKey, check, expanded, onToggle, getStatus, children }) {
  const status = getStatus(sectionKey, check);
  return (
    <div className="mkt-check-section">
      <div className="mkt-check-section-header" onClick={() => onToggle(sectionKey)}>
        <h3>
          {title}
          <span className={`mkt-check-status-tag ${status.className}`}>{status.label}</span>
        </h3>
        <span className={`mkt-check-chevron ${expanded ? "open" : ""}`}>▼</span>
      </div>
      {expanded && <div className="mkt-check-section-content">{children}</div>}
    </div>
  );
}

// ─── Page Speed Details ───

function PageSpeedDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  const renderMetrics = (label, data) => {
    if (!data || data.error) return (
      <div style={{ marginBottom: "1rem" }}>
        <div className="mkt-check-detail-label">{label}</div>
        <p style={{ color: "#999" }}>{data?.error || "Not available"}</p>
      </div>
    );

    return (
      <div style={{ marginBottom: "1rem" }}>
        <div className="mkt-check-detail-label">{label} — Score: {data.performanceScore}/100</div>
        <table className="mkt-check-metrics-table">
          <thead>
            <tr><th>Metric</th><th>Value</th></tr>
          </thead>
          <tbody>
            {data.fcp && <tr><td>First Contentful Paint</td><td>{data.fcp}</td></tr>}
            {data.lcp && <tr><td>Largest Contentful Paint</td><td>{data.lcp}</td></tr>}
            {data.cls && <tr><td>Cumulative Layout Shift</td><td>{data.cls}</td></tr>}
            {data.tbt && <tr><td>Total Blocking Time</td><td>{data.tbt}</td></tr>}
            {data.speedIndex && <tr><td>Speed Index</td><td>{data.speedIndex}</td></tr>}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <>
      {renderMetrics("Mobile", check.mobile)}
      {renderMetrics("Desktop", check.desktop)}

      {check.mobile?.opportunities?.length > 0 && (
        <div style={{ marginTop: "0.5rem" }}>
          <div className="mkt-check-detail-label">Top Opportunities</div>
          {check.mobile.opportunities.map((opp, i) => (
            <div key={i} className="mkt-check-detail-row">
              <span className="label">{opp.title}</span>
              <span className="value">~{opp.savings} savings</span>
            </div>
          ))}
        </div>
      )}

      {check.plainSummary && <p className="mkt-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── Mobile Details ───

function MobileDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      <div className="mkt-check-detail-row">
        <span className="label">Viewport Meta Tag</span>
        <span className="value" style={{ color: check.viewportMeta ? "#4ade80" : "#fbbf24" }}>
          {check.viewportMeta ? "Present" : "Missing"}
        </span>
      </div>
      {check.viewportContent && (
        <div className="mkt-check-detail-row">
          <span className="label">Viewport Content</span>
          <span className="value" style={{ fontSize: "0.8rem" }}>{check.viewportContent}</span>
        </div>
      )}
      {check.mobileScore !== undefined && (
        <div className="mkt-check-detail-row">
          <span className="label">Mobile Performance Score</span>
          <span className="value">{check.mobileScore}/100</span>
        </div>
      )}
      {check.plainSummary && <p className="mkt-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── SEO Details ───

function SeoDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      <div className="mkt-check-detail-row">
        <span className="label">Title Tag</span>
        <span className="value">
          {check.title?.found ? `"${check.title.value}" (${check.title.length} chars)` : "Missing"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">Meta Description</span>
        <span className="value">
          {check.metaDescription?.found
            ? `"${check.metaDescription.value.substring(0, 80)}${check.metaDescription.value.length > 80 ? '...' : ''}" (${check.metaDescription.length} chars)`
            : "Missing"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">H1 Tag</span>
        <span className="value">
          {check.h1?.found
            ? `${check.h1.count} found${check.h1.count === 1 ? '' : ' (should be 1)'}: "${check.h1.values[0]}"`
            : "Missing"}
        </span>
      </div>

      {check.headingHierarchy?.length > 0 && (
        <div className="mkt-check-detail-row">
          <span className="label">Heading Structure</span>
          <span className="value">
            {check.headingHierarchy.map(h => `${h.tag.toUpperCase()}: ${h.count}`).join(", ")}
          </span>
        </div>
      )}

      <div className="mkt-check-detail-row">
        <span className="label">Canonical Tag</span>
        <span className="value" style={{ color: check.canonical?.found ? "#4ade80" : "#fbbf24" }}>
          {check.canonical?.found ? check.canonical.value : "Not set"}
        </span>
      </div>

      {check.robotsMeta?.found && (
        <div className="mkt-check-detail-row">
          <span className="label">Robots Meta</span>
          <span className="value" style={{ color: check.robotsMeta.blocking ? "#f87171" : "#4ade80" }}>
            {check.robotsMeta.value} {check.robotsMeta.blocking && "⚠ Blocking indexing!"}
          </span>
        </div>
      )}

      <div className="mkt-check-detail-row">
        <span className="label">Language</span>
        <span className="value">{check.lang || "Not set"}</span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">robots.txt</span>
        <span className="value" style={{ color: check.robotsTxt?.found ? "#4ade80" : "#fbbf24" }}>
          {check.robotsTxt?.found ? "Found" : "Not found"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">Sitemap</span>
        <span className="value" style={{ color: check.sitemap?.found ? "#4ade80" : "#fbbf24" }}>
          {check.sitemap?.found ? `Found (${check.sitemap.urlCount} URLs)` : "Not found"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">Schema Markup</span>
        <span className="value" style={{ color: check.schemaMarkup?.found ? "#4ade80" : "#fbbf24" }}>
          {check.schemaMarkup?.found ? `Found: ${check.schemaMarkup.types.join(", ")}` : "None detected"}
        </span>
      </div>

      {check.plainSummary && <p className="mkt-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── Social / Open Graph Details ───

function SocialOgDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      {check.og && (
        <>
          <div className="mkt-check-detail-label">Open Graph Tags</div>
          <table className="mkt-check-metrics-table">
            <thead>
              <tr><th>Tag</th><th>Status</th><th>Value</th></tr>
            </thead>
            <tbody>
              {Object.entries(check.og).map(([tag, data]) => (
                <tr key={tag}>
                  <td>{tag}</td>
                  <td className={data.present ? "present" : "missing"}>
                    {data.present ? "Present" : "Missing"}
                  </td>
                  <td style={{ fontSize: "0.75rem", maxWidth: "250px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {data.value || "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {check.twitter && (
        <>
          <div className="mkt-check-detail-label" style={{ marginTop: "1rem" }}>Twitter Card Tags</div>
          <table className="mkt-check-metrics-table">
            <thead>
              <tr><th>Tag</th><th>Status</th><th>Value</th></tr>
            </thead>
            <tbody>
              {Object.entries(check.twitter).map(([tag, data]) => (
                <tr key={tag}>
                  <td>{tag}</td>
                  <td className={data.present ? "present" : "missing"}>
                    {data.present ? "Present" : "Missing"}
                  </td>
                  <td style={{ fontSize: "0.75rem", maxWidth: "250px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {data.value || "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {check.plainSummary && <p className="mkt-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── Google Business Profile Details ───

function GbpDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      <div className="mkt-check-detail-row">
        <span className="label">Google Maps Embed</span>
        <span className="value" style={{ color: check.mapsEmbed ? "#4ade80" : "#fbbf24" }}>
          {check.mapsEmbed ? "Found" : "Not found"}
        </span>
      </div>
      <div className="mkt-check-detail-row">
        <span className="label">Google Maps Link</span>
        <span className="value" style={{ color: check.mapsLink ? "#4ade80" : "#fbbf24" }}>
          {check.mapsLink ? "Found" : "Not found"}
        </span>
      </div>
      <div className="mkt-check-detail-row">
        <span className="label">LocalBusiness Schema</span>
        <span className="value" style={{ color: check.localBusinessSchema ? "#4ade80" : "#fbbf24" }}>
          {check.localBusinessSchema ? "Found" : "Not found"}
        </span>
      </div>
      <div className="mkt-check-detail-row">
        <span className="label">NAP (Name, Address, Phone)</span>
        <span className="value" style={{ color: check.napDetected ? "#4ade80" : "#fbbf24" }}>
          {check.napDetected ? "Detected" : "Not clearly detected"}
        </span>
      </div>
      {check.schemaTypes?.length > 0 && (
        <div className="mkt-check-detail-row">
          <span className="label">Schema Types Found</span>
          <span className="value">{check.schemaTypes.join(", ")}</span>
        </div>
      )}
      {check.plainSummary && <p className="mkt-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── Conversion & CTA Details ───

function ConversionDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      <div className="mkt-check-detail-row">
        <span className="label">Forms on Page</span>
        <span className="value">{check.forms?.count || 0}</span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">CTAs Found</span>
        <span className="value">{check.ctas?.length || 0}</span>
      </div>
      {check.ctas?.length > 0 && (
        <div style={{ paddingLeft: "0.5rem", marginBottom: "0.5rem" }}>
          {check.ctas.slice(0, 8).map((cta, i) => (
            <div key={i} style={{ fontSize: "0.8rem", color: "#ccc", margin: "0.2rem 0" }}>
              • "{cta.text}" ({cta.tag})
            </div>
          ))}
          {check.ctas.length > 8 && (
            <div style={{ fontSize: "0.8rem", color: "#999" }}>...and {check.ctas.length - 8} more</div>
          )}
        </div>
      )}

      <div className="mkt-check-detail-row">
        <span className="label">Phone Numbers</span>
        <span className="value">
          {check.phoneNumbers?.length > 0 ? check.phoneNumbers.join(", ") : "None found"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">Email Addresses</span>
        <span className="value">
          {check.emailAddresses?.length > 0 ? check.emailAddresses.join(", ") : "None found"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">Chat Widget</span>
        <span className="value" style={{ color: check.chatWidget?.found ? "#4ade80" : "#fbbf24" }}>
          {check.chatWidget?.found ? check.chatWidget.provider : "None detected"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">Analytics & Tracking</span>
        <span className="value" style={{ color: check.analytics?.length > 0 ? "#4ade80" : "#fbbf24" }}>
          {check.analytics?.length > 0 ? check.analytics.join(", ") : "None detected"}
        </span>
      </div>

      {check.plainSummary && <p className="mkt-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── Technology & CMS Details ───

function TechCmsDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      <div className="mkt-check-detail-row">
        <span className="label">CMS / Platform</span>
        <span className="value">{check.cms || "Could not determine"}</span>
      </div>

      {check.hosting && (
        <div className="mkt-check-detail-row">
          <span className="label">Hosting / Server</span>
          <span className="value">{check.hosting}</span>
        </div>
      )}

      <div className="mkt-check-detail-row">
        <span className="label">Frameworks & Libraries</span>
        <span className="value">
          {check.frameworks?.length > 0 ? check.frameworks.join(", ") : "None detected"}
        </span>
      </div>

      <div className="mkt-check-detail-row">
        <span className="label">Marketing Tools</span>
        <span className="value" style={{ color: check.marketingTools?.length > 0 ? "#4ade80" : "#fbbf24" }}>
          {check.marketingTools?.length > 0 ? check.marketingTools.join(", ") : "None detected"}
        </span>
      </div>

      {check.plainSummary && <p className="mkt-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

export default MarketingCheck;
