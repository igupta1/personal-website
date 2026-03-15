//SecurityCheck.js
import React, { useState, useEffect, useRef } from "react";
import { Link } from "react-router-dom";

const CHECK_LABELS = [
  "Checking email authentication (SPF, DKIM, DMARC)",
  "Inspecting SSL/TLS certificate",
  "Checking breach exposure",
  "Analyzing DNS health",
  "Scanning security headers",
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

function SecurityCheck() {
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

    // Stagger check labels appearance
    let count = 0;
    intervalRef.current = setInterval(() => {
      count++;
      setVisibleChecks(count);
      if (count >= CHECK_LABELS.length) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    }, 600);

    try {
      const apiUrl = process.env.REACT_APP_API_URL || "";
      const response = await fetch(`${apiUrl}/api/security-scan`, {
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

  const getSectionStatus = (check) => {
    if (!check) return { label: "Error", className: "error" };
    if (check.status === "error") return { label: "Error", className: "error" };
    if (check.status === "stubbed") return { label: "Coming Soon", className: "stubbed" };

    // Determine if there are issues
    switch (true) {
      // Email auth
      case check.spf !== undefined: {
        const dkimOk = check.dkim?.found || check.dkimAssessment === "likely_configured";
        const hasIssues = !check.spf?.found || !dkimOk || !check.dmarc?.found || check.dmarc?.policy === "none";
        return hasIssues ? { label: "Issues Found", className: "issues" } : { label: "Looks Good", className: "good" };
      }
      // SSL
      case check.valid !== undefined: {
        return (check.warnings?.length > 0) ? { label: "Issues Found", className: "issues" } : { label: "Looks Good", className: "good" };
      }
      // Breach
      case check.breachCount !== undefined: {
        return check.breachCount > 0 ? { label: "Breaches Found", className: "issues" } : { label: "No Breaches", className: "good" };
      }
      // DNS
      case check.nameservers !== undefined: {
        const hasIssues = check.mxRecords?.length === 0;
        return hasIssues ? { label: "Issues Found", className: "issues" } : { label: "Looks Good", className: "good" };
      }
      // Headers
      case check.headersFound !== undefined: {
        const serious = check.headersFound <= Math.floor(check.headersTotal / 2);
        return serious ? { label: `${check.headersFound}/${check.headersTotal} Present`, className: "issues" } : { label: "Looks Good", className: "good" };
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
                <h2>IT Security Scorecard</h2>
                <p className="demo-section-subtitle">
                  Scan any business domain for security vulnerabilities in seconds
                </p>
              </div>
            </div>

            {/* Input Section */}
            {!loading && !report && (
              <div className="sec-check-input-section">
                <p>
                  Enter a domain to get a comprehensive security assessment including email authentication, SSL certificates, breach exposure, DNS health, and security headers.
                </p>
                <form className="sec-check-input-form" onSubmit={handleScan}>
                  <input
                    type="text"
                    className="sec-check-input"
                    placeholder="Enter domain (e.g., example.com)"
                    value={domainInput}
                    onChange={(e) => setDomainInput(e.target.value)}
                    autoFocus
                  />
                  <button type="submit" className="sec-check-btn">
                    Run Security Scan
                  </button>
                </form>
              </div>
            )}

            {/* Error */}
            {error && (
              <div className="sec-check-error">
                <span>⚠</span>
                <span>{error}</span>
              </div>
            )}

            {/* Loading State */}
            {loading && (
              <div className="sec-check-loading">
                <h3>Scanning {validateDomain(domainInput)}...</h3>
                <div className="sec-check-progress-list">
                  {CHECK_LABELS.map((label, i) => (
                    i < visibleChecks && (
                      <div
                        key={i}
                        className={`sec-check-progress-item active`}
                        style={{ animationDelay: `${i * 0.1}s` }}
                      >
                        <div className="sec-check-spinner" />
                        <span>{label}...</span>
                      </div>
                    )
                  ))}
                </div>
              </div>
            )}

            {/* Report */}
            {report && (
              <div className="sec-check-report">
                {/* Header */}
                <div className="sec-check-report-header">
                  <h2>{report.domain} Security Report</h2>
                  <span className="sec-check-meta">
                    Scanned on {new Date(report.scanDate).toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric", hour: "2-digit", minute: "2-digit" })}
                    {report.scanDuration && ` · ${(report.scanDuration / 1000).toFixed(1)}s`}
                  </span>
                </div>

                {/* Scan Summary */}
                {report.summary?.scanSummary ? (
                  <div className="sec-check-card">
                    <h3>Scan Summary</h3>
                    <p>{report.summary.scanSummary}</p>
                  </div>
                ) : report.summary?.error && (
                  <div className="sec-check-error">
                    <span>⚠</span>
                    <span>AI Summary unavailable: {report.summary.error}</span>
                  </div>
                )}

                {/* Sales Opportunities */}
                {report.summary?.salesOpportunities?.length > 0 && (
                  <div className="sec-check-card">
                    <h3>Sales Opportunities</h3>
                    <ul className="sec-check-recs-list">
                      {report.summary.salesOpportunities.map((opp, i) => (
                        <li key={i} className="sec-check-rec-item">
                          <h4>{i + 1}. {opp.title}</h4>
                          <p>{opp.description}</p>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* Personalized Intro Opener */}
                {report.summary?.introOpener && (
                  <IntroOpener text={report.summary.introOpener} />
                )}

                {/* Detailed Findings */}
                <CollapsibleSection
                  title="Email Authentication"
                  sectionKey="emailAuth"
                  check={report.checks.emailAuth}
                  expanded={expandedSections.emailAuth}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <EmailAuthDetails check={report.checks.emailAuth} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="SSL/TLS Certificate"
                  sectionKey="sslTls"
                  check={report.checks.sslTls}
                  expanded={expandedSections.sslTls}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <SslTlsDetails check={report.checks.sslTls} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="DNS Health"
                  sectionKey="dnsHealth"
                  check={report.checks.dnsHealth}
                  expanded={expandedSections.dnsHealth}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <DnsHealthDetails check={report.checks.dnsHealth} />
                </CollapsibleSection>

                <CollapsibleSection
                  title="Security Headers"
                  sectionKey="securityHeaders"
                  check={report.checks.securityHeaders}
                  expanded={expandedSections.securityHeaders}
                  onToggle={toggleSection}
                  getStatus={getSectionStatus}
                >
                  <SecurityHeadersDetails check={report.checks.securityHeaders} />
                </CollapsibleSection>

                {/* New Scan */}
                <div className="sec-check-new-scan">
                  <button onClick={handleNewScan}>Scan Another Domain</button>
                </div>

                {/* Footer */}
                <div className="sec-check-footer">
                  <p>Report generated by Pipeline GTM — Powered by AI-driven security analysis</p>
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
  const status = getStatus(check);
  return (
    <div className="sec-check-section">
      <div className="sec-check-section-header" onClick={() => onToggle(sectionKey)}>
        <h3>
          {title}
          <span className={`sec-check-status-tag ${status.className}`}>{status.label}</span>
        </h3>
        <span className={`sec-check-chevron ${expanded ? "open" : ""}`}>▼</span>
      </div>
      {expanded && <div className="sec-check-section-content">{children}</div>}
    </div>
  );
}

// ─── Email Auth Details ───

function EmailAuthDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      <div className="sec-check-detail-label">SPF Record</div>
      <div className="sec-check-detail-value">
        {check.spf?.found ? check.spf.record : "Not found"}
      </div>

      <div className="sec-check-detail-label">DKIM</div>
      <div className="sec-check-detail-value">
        {check.dkim?.found
          ? `Found on selectors: ${check.dkim.selectors.join(", ")}`
          : check.dkimAssessment === "likely_configured"
            ? "Not found on common selectors, but DMARC enforcement suggests DKIM is configured with a custom selector"
            : "Not found on common selectors (checked: default, google, selector1, selector2, k1, mail, dkim, smtp, s1, s2)"}
      </div>

      <div className="sec-check-detail-label">DMARC Record</div>
      <div className="sec-check-detail-value">
        {check.dmarc?.found
          ? `${check.dmarc.record} (policy: ${check.dmarc.policy})`
          : "Not found"}
      </div>

      {check.plainSummary && <p className="sec-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── SSL/TLS Details ───

function SslTlsDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      {check.issuer && (
        <div className="sec-check-detail-row">
          <span className="label">Issuer</span>
          <span className="value">{check.issuer}</span>
        </div>
      )}
      {check.validFrom && (
        <div className="sec-check-detail-row">
          <span className="label">Valid From</span>
          <span className="value">{check.validFrom}</span>
        </div>
      )}
      {check.validTo && (
        <div className="sec-check-detail-row">
          <span className="label">Valid Until</span>
          <span className="value">{check.validTo}</span>
        </div>
      )}
      {check.daysUntilExpiry !== undefined && (
        <div className="sec-check-detail-row">
          <span className="label">Days Until Expiry</span>
          <span className="value">{check.daysUntilExpiry}</span>
        </div>
      )}
      {check.tlsVersion && (
        <div className="sec-check-detail-row">
          <span className="label">TLS Version</span>
          <span className="value">{check.tlsVersion}</span>
        </div>
      )}
      {check.subjectCN && (
        <div className="sec-check-detail-row">
          <span className="label">Subject CN</span>
          <span className="value">{check.subjectCN}</span>
        </div>
      )}
      {check.sanList?.length > 0 && (
        <div className="sec-check-detail-row">
          <span className="label">SANs</span>
          <span className="value">{check.sanList.join(", ")}</span>
        </div>
      )}
      {check.warnings?.length > 0 && (
        <>
          <div className="sec-check-detail-label" style={{ marginTop: "0.5rem" }}>Warnings</div>
          {check.warnings.map((w, i) => (
            <p key={i} style={{ color: "#fbbf24", margin: "0.25rem 0" }}>⚠ {w}</p>
          ))}
        </>
      )}
      {check.plainSummary && <p className="sec-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── Intro Opener ───

function IntroOpener({ text }) {
  return (
    <div className="sec-check-card">
      <h3>Personalized Intro Opener</h3>
      <div className="sec-check-opener-text">{text}</div>
    </div>
  );
}

// ─── DNS Health Details ───

function DnsHealthDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      {check.nameservers?.length > 0 && (
        <>
          <div className="sec-check-detail-label">Nameservers</div>
          <div className="sec-check-detail-value">{check.nameservers.join(", ")}</div>
        </>
      )}

      <div className="sec-check-detail-row">
        <span className="label">DNSSEC</span>
        <span className="value" style={{ color: check.dnssecEnabled ? "#4ade80" : "#fbbf24" }}>
          {check.dnssecEnabled ? "Enabled" : "Not Enabled (common for most domains)"}
        </span>
      </div>

      <div className="sec-check-detail-row">
        <span className="label">Mail Provider</span>
        <span className="value">{check.mailProvider || "Unknown"}</span>
      </div>

      {check.mxRecords?.length > 0 && (
        <>
          <div className="sec-check-detail-label">MX Records</div>
          <div className="sec-check-detail-value">
            {check.mxRecords.map((mx) => `${mx.priority} ${mx.exchange}`).join("\n")}
          </div>
        </>
      )}

      {check.plainSummary && <p className="sec-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

// ─── Security Headers Details ───

function SecurityHeadersDetails({ check }) {
  if (check.status === "error") return <p>{check.errorMessage}</p>;

  return (
    <>
      <div className="sec-check-detail-row">
        <span className="label">HTTPS Redirect</span>
        <span className="value" style={{ color: check.httpsRedirect ? "#4ade80" : "#fbbf24" }}>
          {check.httpsRedirect ? "Yes — HTTP redirects to HTTPS" : "No — HTTP does not redirect to HTTPS"}
        </span>
      </div>

      {check.headers && (
        <table className="sec-check-header-table">
          <thead>
            <tr>
              <th>Header</th>
              <th>Status</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {Object.values(check.headers).map((h, i) => (
              <tr key={i}>
                <td>{h.name}</td>
                <td className={h.present ? "present" : "missing"}>
                  {h.present ? "Present" : "Missing"}
                </td>
                <td style={{ fontSize: "0.75rem", maxWidth: "300px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {h.value || "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {check.plainSummary && <p className="sec-check-plain-summary">{check.plainSummary}</p>}
    </>
  );
}

export default SecurityCheck;
