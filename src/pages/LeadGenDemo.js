//LeadGenDemo.js
import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

function LeadGenDemo() {
  const [stage, setStage] = useState('processing');
  const [leads, setLeads] = useState([]);
  const [currentLead, setCurrentLead] = useState(null);
  const [processingPhase, setProcessingPhase] = useState('job');
  const [error, setError] = useState(null);
  const [hasStarted, setHasStarted] = useState(false);

  const experiences = [
    { title: "Software Engineer", company: "Google", description: "Built Generative AI Features for Gmail and Google Chat", logo: googlelogo },
    { title: "Software Engineer", company: "Amazon", description: "Developed AI Infrastructure for Amazon.com", logo: amazonlogo },
    { title: "Software Engineer", company: "Cisco", description: "Implemented Distributed System Architecture for Networking Solutions", logo: ciscologo }
  ];

  const generateLeads = async () => {
    const location = "marketing-discovery";
    // Demo data for local development / fallback
    const demoLeads = [
      {
        firstName: "Sarah",
        lastName: "Chen",
        title: "VP of Marketing",
        companyName: "TechStartup Inc",
        jobRole: "Marketing Director",
        email: "sarah.chen@techstartup.com",
        website: "https://techstartup.com",
        location: "San Francisco, CA",
        companySize: "50 employees",
        category: "small",
        jobLink: "https://boards.greenhouse.io/techstartup/jobs/123",
        postingDate: "2025-01-15",
        linkedinUrl: "https://linkedin.com/in/sarah-chen",
        confidence: "High"
      },
      {
        firstName: "Michael",
        lastName: "Rodriguez",
        title: "Head of Growth",
        companyName: "ScaleUp Labs",
        jobRole: "Growth Marketing Manager",
        email: "michael@scaleuplabs.io",
        website: "https://scaleuplabs.io",
        location: "Austin, TX",
        companySize: "75 employees",
        category: "small",
        jobLink: "https://jobs.lever.co/scaleuplabs/456",
        postingDate: "2025-01-20",
        linkedinUrl: "https://linkedin.com/in/michael-rodriguez",
        confidence: "High"
      },
      {
        firstName: "Emma",
        lastName: "Thompson",
        title: "CMO",
        companyName: "FinanceFlow",
        jobRole: "Senior Content Marketing Manager",
        email: "emma.t@financeflow.com",
        website: "https://financeflow.com",
        location: "New York, NY",
        companySize: "150 employees",
        category: "medium",
        jobLink: "https://boards.greenhouse.io/financeflow/jobs/789",
        postingDate: "2025-01-18",
        linkedinUrl: "https://linkedin.com/in/emma-thompson",
        confidence: "Medium"
      },
      {
        firstName: "David",
        lastName: "Kim",
        title: "Director of Marketing",
        companyName: "HealthTech Solutions",
        jobRole: "Digital Marketing Specialist",
        email: "dkim@healthtechsolutions.com",
        website: "https://healthtechsolutions.com",
        location: "Boston, MA",
        companySize: "200 employees",
        category: "medium",
        jobLink: "https://jobs.ashbyhq.com/healthtech/abc",
        postingDate: "2025-01-22",
        linkedinUrl: "https://linkedin.com/in/david-kim",
        confidence: "High"
      },
      {
        firstName: "Jennifer",
        lastName: "Patel",
        title: "VP of Growth",
        companyName: "Enterprise Corp",
        jobRole: "Marketing Operations Manager",
        email: "jpatel@enterprisecorp.com",
        website: "https://enterprisecorp.com",
        location: "Seattle, WA",
        companySize: "500 employees",
        category: "large",
        jobLink: "https://careers.smartrecruiters.com/enterprise/def",
        postingDate: "2025-01-25",
        linkedinUrl: "https://linkedin.com/in/jennifer-patel",
        confidence: "High"
      }
    ];

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
      return {
        success: true,
        leads: demoLeads,
        stats: {
          small: demoLeads.filter(l => l.category === 'small').length,
          medium: demoLeads.filter(l => l.category === 'medium').length,
          large: demoLeads.filter(l => l.category === 'large').length,
          targetCount: demoLeads.filter(l => l.category === 'small' || l.category === 'medium').length
        }
      };
    }
  };

  const processLeads = async () => {
    setStage('processing');
    setLeads([]);
    setError(null);
    setHasStarted(true);

    const result = await generateLeads();

    if (!result.success) {
      setError(result.error || 'Failed to generate leads');
      return;
    }

    const allLeads = result.leads || [];

    // Simulate streaming - add leads one at a time with delay
    for (let i = 0; i < allLeads.length; i++) {
      const lead = allLeads[i];

      // Phase 1: Show company and role
      setCurrentLead(lead);
      setProcessingPhase('job');
      await new Promise(resolve => setTimeout(resolve, 1500));

      // Phase 2: Finding decision maker
      setProcessingPhase('contact');
      await new Promise(resolve => setTimeout(resolve, 1500));

      // Add lead to list
      setLeads(prev => [...prev, lead]);
    }

    setCurrentLead(null);
    setProcessingPhase('job');
    setStage('results');
  };

  const resetDemo = () => {
    setStage('processing');
    setLeads([]);
    setCurrentLead(null);
    setProcessingPhase('job');
    setError(null);
    processLeads();
  };

  // Auto-start demo on page load
  useEffect(() => {
    if (!hasStarted) {
      processLeads();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const formatDate = (dateStr) => {
    if (!dateStr) return null;
    try {
      const date = new Date(dateStr);
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    } catch {
      return dateStr;
    }
  };

  const getCategoryLabel = (category) => {
    switch (category) {
      case 'small': return '≤100 employees';
      case 'medium': return '101-250 employees';
      case 'large': return '250+ employees';
      default: return category;
    }
  };

  const getCategoryColor = (category) => {
    switch (category) {
      case 'small': return '#4ade80'; // green
      case 'medium': return '#fbbf24'; // amber
      case 'large': return '#60a5fa'; // blue
      default: return '#94a3b8';
    }
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
              <p className="demo-script-link">Powered by AI-driven ATS detection & decision maker identification</p>
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

            {stage === 'processing' && (
              <div className="demo-processing-stage">
                {currentLead && (
                  <div className="demo-current-lead">
                    <div className="demo-processing-message">
                      <p className="demo-phase-text">
                        <span className="demo-company-highlight">{currentLead.companyName}</span> is hiring for <span className="demo-role-highlight">{currentLead.jobRole || 'Marketing'}</span>{processingPhase === 'job' && <span className="demo-animated-dots"><span>.</span><span>.</span><span>.</span></span>}
                      </p>
                      <p className="demo-phase-text">
                        {processingPhase === 'contact' ? 'Finding Decision Maker' : 'Analyzing Job Posting'}{processingPhase === 'contact' && <span className="demo-animated-dots"><span>.</span><span>.</span><span>.</span></span>}
                      </p>
                    </div>
                  </div>
                )}

                <div className="demo-category-section">
                  {leads.length > 0 ? (
                    <div className="demo-live-results-list">
                      {leads.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar" style={{ background: `linear-gradient(135deg, ${getCategoryColor(lead.category)}, #667eea)` }}>
                              {lead.firstName.charAt(0)}{lead.lastName ? lead.lastName.charAt(0) : ''}
                            </div>
                            <div className="demo-live-result-name">
                              <strong>{lead.firstName} {lead.lastName}</strong>
                              <span>{lead.title || 'Decision Maker'} at {lead.companyName}</span>
                            </div>
                            <span className="demo-live-result-check">✓</span>
                          </div>
                          <div className="demo-live-result-details">
                            {lead.email && <span>📧 {lead.email}</span>}
                            {lead.website && <span>🌐 {lead.website}</span>}
                            {lead.jobRole && <span>📋 Hiring: {lead.jobRole}</span>}
                            {lead.postingDate && <span>📅 Posted: {formatDate(lead.postingDate)}</span>}
                            {lead.confidence && <span style={{ color: lead.confidence === 'High' ? '#4ade80' : '#fbbf24' }}>⭐ {lead.confidence} Confidence</span>}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="demo-category-empty">Discovering leads...</p>
                  )}
                </div>

                <button className="demo-cancel-button" onClick={resetDemo}>Cancel</button>
              </div>
            )}

            {stage === 'results' && (
              <div className="demo-results-stage">
                <div className="demo-results-header">
                  <div className="demo-results-stats">
                    <span className="demo-stat">
                      <strong>{leads.length}</strong> Leads Found
                    </span>
                    <span className="demo-stat">
                      <strong>{leads.filter(l => l.email).length}</strong> with Emails
                    </span>
                  </div>
                  <button className="demo-reset-button" onClick={resetDemo}>Refresh Search</button>
                </div>

                <div className="demo-results-category">
                  <div className="demo-results-list">
                    {leads.map((lead, index) => (
                      <div key={index} className="demo-result-card">
                        <div className="demo-result-header">
                          <div className="demo-result-avatar" style={{ background: `linear-gradient(135deg, ${getCategoryColor(lead.category)}, #667eea)` }}>
                            {lead.firstName.charAt(0)}{lead.lastName ? lead.lastName.charAt(0) : ''}
                          </div>
                          <div className="demo-result-info">
                            <h4>{lead.firstName} {lead.lastName}</h4>
                            <span>{lead.title || 'Decision Maker'} at {lead.companyName}</span>
                            <span className="demo-result-category-tag" style={{ color: getCategoryColor(lead.category) }}>
                              {getCategoryLabel(lead.category)}
                            </span>
                          </div>
                          {lead.confidence && (
                            <span className="demo-confidence-badge" style={{
                              background: lead.confidence === 'High' ? 'rgba(74, 222, 128, 0.2)' : 'rgba(251, 191, 36, 0.2)',
                              color: lead.confidence === 'High' ? '#4ade80' : '#fbbf24'
                            }}>
                              {lead.confidence}
                            </span>
                          )}
                        </div>
                        <div className="demo-result-inputs">
                          {lead.email && (
                            <div className="demo-result-input">
                              <span className="demo-result-label">Email</span>
                              <span className="demo-result-value">{lead.email}</span>
                            </div>
                          )}
                          {lead.linkedinUrl && (
                            <div className="demo-result-input">
                              <span className="demo-result-label">LinkedIn</span>
                              <span className="demo-result-value">
                                <a href={lead.linkedinUrl} target="_blank" rel="noopener noreferrer">View Profile →</a>
                              </span>
                            </div>
                          )}
                          {lead.website && (
                            <div className="demo-result-input">
                              <span className="demo-result-label">Company</span>
                              <span className="demo-result-value">
                                <a href={lead.website} target="_blank" rel="noopener noreferrer">{lead.website.replace('https://', '')}</a>
                              </span>
                            </div>
                          )}
                          {lead.jobRole && (
                            <div className="demo-result-input">
                              <span className="demo-result-label">Hiring For</span>
                              <span className="demo-result-value">{lead.jobRole}</span>
                            </div>
                          )}
                          {lead.postingDate && (
                            <div className="demo-result-input">
                              <span className="demo-result-label">Posted</span>
                              <span className="demo-result-value">{formatDate(lead.postingDate)}</span>
                            </div>
                          )}
                          {lead.jobLink && (
                            <div className="demo-result-input">
                              <span className="demo-result-label">Job Posting</span>
                              <span className="demo-result-value">
                                <a href={lead.jobLink} target="_blank" rel="noopener noreferrer">View Job →</a>
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
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
