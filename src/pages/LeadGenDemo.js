//LeadGenDemo.js
import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

const parseLocalDate = (dateStr) => {
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day);
};

function LeadGenDemo() {
  const [companies, setCompanies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedCompanies, setExpandedCompanies] = useState({});

  const experiences = [
    { title: "Software Engineer", company: "Google", description: "Built Generative AI Features for Gmail and Google Chat", logo: googlelogo },
    { title: "Software Engineer", company: "Amazon", description: "Developed AI Infrastructure for Amazon.com", logo: amazonlogo },
    { title: "Software Engineer", company: "Cisco", description: "Implemented Distributed System Architecture for Networking Solutions", logo: ciscologo }
  ];

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
      // Demo data matching the run output format
      // Use dynamic dates relative to today for demo
      const today = new Date();
      const formatDate = (daysAgo) => {
        const d = new Date(today);
        d.setDate(d.getDate() - daysAgo);
        return d.toISOString().split('T')[0];
      };
      return {
        success: true,
        leads: [
          { companyName: "Inspira Education", firstName: "Xavier", lastName: "Portillo", title: "Co-Founder Senior Advisor", jobRole: "Copy Editor", confidence: "Medium", website: "https://inspiraeducation.com", postingDate: formatDate(23), mostRecentPostingDate: formatDate(1) },
          { companyName: "Inspira Education", firstName: "Xavier", lastName: "Portillo", title: "Co-Founder Senior Advisor", jobRole: "Growth Marketing Manager", confidence: "Medium", website: "https://inspiraeducation.com", postingDate: formatDate(1), mostRecentPostingDate: formatDate(1) },
          { companyName: "Emergent", firstName: "Mukund", lastName: "Jha", title: "CEO", jobRole: "Head of Growth", confidence: "High", website: "https://emergent.com", postingDate: formatDate(8), mostRecentPostingDate: formatDate(8) },
          { companyName: "Emergent", firstName: "Mukund", lastName: "Jha", title: "CEO", jobRole: "Growth Manager", confidence: "High", website: "https://emergent.com", postingDate: formatDate(8), mostRecentPostingDate: formatDate(8) },
          { companyName: "Feastables", firstName: "Ryan", lastName: "Brisco", title: "Vice President Of Marketing", jobRole: "Shopper Marketing Manager", confidence: "High", website: "https://feastables.com", postingDate: formatDate(9), mostRecentPostingDate: formatDate(9) },
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
          mostRecentPostingDate: lead.mostRecentPostingDate || lead.postingDate || '',
          decisionMaker: {
            firstName: lead.firstName,
            lastName: lead.lastName,
            title: lead.title,
            email: lead.email,
            linkedinUrl: lead.linkedinUrl,
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

    // Convert to array and sort by most recent posting date (newest first)
    const result = Object.values(grouped).sort((a, b) => {
      const aDate = a.mostRecentPostingDate ? parseLocalDate(a.mostRecentPostingDate) : new Date(0);
      const bDate = b.mostRecentPostingDate ? parseLocalDate(b.mostRecentPostingDate) : new Date(0);
      return bDate - aDate;
    });

    // Filter out companies where the most recent role was posted more than 9 days ago
    const now = new Date();
    const filtered = result.filter(company => {
      if (!company.mostRecentPostingDate) return false;
      const postDate = parseLocalDate(company.mostRecentPostingDate);
      const diffDays = Math.floor((now - postDate) / (1000 * 60 * 60 * 24));
      return diffDays <= 9;
    });

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

  return (
    <div className="about-page-combined">
      <div className="about-container">
        <div className="about-left-column">
          <section id="home" className="profile-section">
            <div className="profile-header">
              <img src={pfpImage} alt="Ishaan Gupta" className="profile-image" />
              <div className="profile-info">
                <h1>Ishaan Gupta</h1>
                <p>Software Engineer - Problem Solver - Builder<br /><br />I focus on turning complex problems into clear, practical solutions — from large-scale systems at Gmail and Amazon.com to tools for everyday users.</p>
              </div>
            </div>
          </section>

          <section id="about" className="work-experience-section">
            <h2>Work Experience</h2>
            <div className="experience-list">
              {experiences.map((exp, index) => (
                <div key={index} className="experience-card">
                  <div className="experience-logo"><img src={exp.logo} alt={exp.company + ' logo'} /></div>
                  <div className="experience-details">
                    <h3>{exp.title}</h3>
                    <h4 className="company-name">{exp.company}</h4>
                    {exp.description && <p className="description">{exp.description}</p>}
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>

        <div className="about-right-column">
          <section className="demo-section">
            <div className="demo-section-header">
              <Link to="/ai-tools" className="back-link-inline">Back to AI Tools</Link>
              <h2>Marketing Lead Discovery</h2>
              <p className="demo-script-link">AI-powered ATS detection & decision maker identification</p>
            </div>

            {error && (
              <div className="demo-error">
                <span className="demo-error-icon">Warning</span>
                <span>{error}</span>
                <button onClick={() => setError(null)} className="demo-error-close">x</button>
              </div>
            )}

            {/* How It Works */}
            <div className="demo-how-it-works">
              <h3>How It Works</h3>
              <div className="demo-steps-grid">
                <div className="demo-step">
                  <span className="demo-step-num">1</span>
                  <span className="demo-step-text">Detect ATS & Find Marketing Jobs</span>
                </div>
                <div className="demo-step">
                  <span className="demo-step-num">2</span>
                  <span className="demo-step-text">Identify Decision Makers via AI</span>
                </div>
                <div className="demo-step">
                  <span className="demo-step-num">3</span>
                  <span className="demo-step-text">Enrich with Verified Emails</span>
                </div>
              </div>
            </div>

            {loading ? (
              <div className="demo-loading">
                <div className="demo-loading-spinner"></div>
                <p>Loading leads...</p>
              </div>
            ) : (
              <div className="demo-results-stage">
                <div className="demo-results-header">
                  <div className="demo-results-stats">
                    <span className="demo-stat">
                      <strong>{companies.length}</strong> Companies
                    </span>
                    <span className="demo-stat">
                      <strong>{companies.reduce((sum, c) => sum + c.roles.length, 0)}</strong> Open Roles
                    </span>
                  </div>
                </div>

                <div className="lead-gen-companies-list">
                  {companies.map((company, index) => {
                    const daysAgo = getDaysAgo(company.mostRecentPostingDate);
                    const isExpanded = expandedCompanies[company.companyName];

                    return (
                      <div key={index} className="lead-gen-company-card">
                        {/* Company Header */}
                        <div className="lead-gen-company-header">
                          <div className="lead-gen-company-info">
                            <h3 className="lead-gen-company-name">{company.companyName}</h3>
                            {company.website && (
                              <a href={company.website} target="_blank" rel="noopener noreferrer" className="lead-gen-company-website">
                                {company.website.replace('https://', '')}
                              </a>
                            )}
                            {daysAgo !== null && (
                              <span className="lead-gen-posted-badge">
                                {daysAgo === 0 ? 'Role Posted Today' : `Role Posted ${daysAgo} ${daysAgo === 1 ? 'Day' : 'Days'} Ago`}
                                {daysAgo <= 1 && <span className="lead-gen-fire-emoji"> 🔥</span>}
                              </span>
                            )}
                          </div>
                          <div className="lead-gen-roles-count">
                            <span className="lead-gen-roles-number">{company.roles.length}</span>
                            <span className="lead-gen-roles-label">roles</span>
                          </div>
                        </div>

                        {/* Roles Section - First 3 visible, rest behind "See more" */}
                        {company.roles.length > 0 && (
                          <div className="lead-gen-roles-section">
                            <h4 className="lead-gen-section-title">
                              Open Positions ({company.roles.length})
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
                                {company.decisionMaker.email && (
                                  <div className="lead-gen-dm-email">{company.decisionMaker.email}</div>
                                )}
                                <div className="lead-gen-dm-links">
                                  {company.decisionMaker.linkedinUrl && (
                                    <a href={company.decisionMaker.linkedinUrl} target="_blank" rel="noopener noreferrer">
                                      LinkedIn
                                    </a>
                                  )}
                                </div>
                              </div>
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
