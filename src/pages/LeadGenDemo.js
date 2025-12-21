//LeadGenDemo.js
import React, { useState, useRef, useEffect } from "react";
import { Link } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

// Cycling status messages to show what the AI is doing
const STATUS_MESSAGES = [
  { icon: "🔍", text: "Searching for marketing agencies..." },
  { icon: "📍", text: "Finding locations..." },
  { icon: "💼", text: "Extracting job titles..." },
  { icon: "👤", text: "Identifying contact names..." },
  { icon: "📧", text: "Gathering contact information..." },
  { icon: "🌐", text: "Collecting website data..." },
  { icon: "✨", text: "Finalizing lead details..." }
];

function LeadGenDemo() {
  const [stage, setStage] = useState('upload');
  const [location, setLocation] = useState('Greater Los Angeles Area');
  const [leadsSmall, setLeadsSmall] = useState([]);
  const [leadsMedium, setLeadsMedium] = useState([]);
  const [leadsLarge, setLeadsLarge] = useState([]);
  const [currentLead, setCurrentLead] = useState(null);
  const [processingStatus, setProcessingStatus] = useState('');
  const [statusIndex, setStatusIndex] = useState(0);
  const [error, setError] = useState(null);

  // Cycle through status messages while processing
  useEffect(() => {
    let interval;
    if (stage === 'processing' && currentLead) {
      interval = setInterval(() => {
        setStatusIndex(prev => (prev + 1) % STATUS_MESSAGES.length);
      }, 2000); // Change message every 2 seconds
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [stage, currentLead]);

  // Reset status index when lead changes
  useEffect(() => {
    if (currentLead) {
      setStatusIndex(0);
    }
  }, [currentLead]);

  const experiences = [
    { title: "Software Engineer Intern", company: "Google", period: "Summer 2025", description: "Generative AI in Gmail and Google Chat", logo: googlelogo },
    { title: "Software Engineer Intern", company: "Amazon", period: "Summer 2024", description: "Developing Infrastructure for Amazon.com", logo: amazonlogo },
    { title: "Software Engineer Intern", company: "Cisco", period: "Spring 2024", description: "Distributed Systems Engineering", logo: ciscologo }
  ];

  const generateLeads = async (location) => {
    // Demo data for local development / fallback
    const demoLeads = [
      {
        firstName: "Sarah",
        lastName: "Mitchell",
        title: "CEO",
        companyName: "Coastal Marketing Co",
        email: "sarah@coastalmarketing.com",
        website: "https://coastalmarketing.com",
        location: "Santa Monica, CA",
        companySize: "45 employees",
        category: "small"
      },
      {
        firstName: "David",
        lastName: "Chen",
        title: "Founder",
        companyName: "Tech Solutions LA",
        email: "david@techsolutionsla.com",
        website: "https://techsolutionsla.com",
        location: "Los Angeles, CA",
        companySize: "85 employees",
        category: "small"
      },
      {
        firstName: "Maria",
        lastName: "Rodriguez",
        title: "President",
        companyName: "Urban Design Studio",
        email: "maria@urbandesignstudio.com",
        website: "https://urbandesignstudio.com",
        location: "Pasadena, CA",
        companySize: "120 employees",
        category: "medium"
      },
      {
        firstName: "James",
        lastName: "Thompson",
        title: "Owner",
        companyName: "Pacific Consulting Group",
        email: "james@pacificconsulting.com",
        website: "https://pacificconsulting.com",
        location: "Burbank, CA",
        companySize: "65 employees",
        category: "small"
      },
      {
        firstName: "Lisa",
        lastName: "Park",
        title: "Co-Founder & CEO",
        companyName: "Innovate Digital",
        email: "lisa@innovatedigital.com",
        website: "https://innovatedigital.com",
        location: "Irvine, CA",
        companySize: "180 employees",
        category: "medium"
      },
      {
        firstName: "Robert",
        lastName: "Williams",
        title: "Founder & CEO",
        companyName: "Summit Enterprises",
        email: "robert@summitenterprises.com",
        website: "https://summitenterprises.com",
        location: "Los Angeles, CA",
        companySize: "320 employees",
        category: "large"
      },
      {
        firstName: "Emily",
        lastName: "Johnson",
        title: "President & CEO",
        companyName: "Westside Media Group",
        email: "emily@westsidemedia.com",
        website: "https://westsidemedia.com",
        location: "Santa Monica, CA",
        companySize: "95 employees",
        category: "small"
      },
      {
        firstName: "Michael",
        lastName: "Brown",
        title: "CEO & Founder",
        companyName: "Digital Horizons",
        email: "michael@digitalhorizons.com",
        website: "https://digitalhorizons.com",
        location: "Pasadena, CA",
        companySize: "450 employees",
        category: "large"
      }
    ];

    try {
      // Try to call the real API endpoint (will work on Vercel)
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
      // Fallback to demo data (for local development)
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
    setLeadsSmall([]);
    setLeadsMedium([]);
    setLeadsLarge([]);
    setError(null);

    // Fetch all leads from API
    const result = await generateLeads(location);

    if (!result.success) {
      setError(result.error || 'Failed to generate leads');
      setStage('upload');
      return;
    }

    const allLeads = result.leads || [];

    // Simulate streaming - add leads one at a time with delay
    for (let i = 0; i < allLeads.length; i++) {
      const lead = allLeads[i];
      setCurrentLead(lead);
      setProcessingStatus('generating');

      // Simulate processing delay (3.5 seconds per lead)
      await new Promise(resolve => setTimeout(resolve, 3500));

      // Add lead to appropriate category
      if (lead.category === 'small') {
        setLeadsSmall(prev => [...prev, lead]);
      } else if (lead.category === 'medium') {
        setLeadsMedium(prev => [...prev, lead]);
      } else if (lead.category === 'large') {
        setLeadsLarge(prev => [...prev, lead]);
      }

      // Stop after we have 5 in small + medium combined
      const currentSmallCount = i < allLeads.length ? allLeads.slice(0, i + 1).filter(l => l.category === 'small').length : 0;
      const currentMediumCount = i < allLeads.length ? allLeads.slice(0, i + 1).filter(l => l.category === 'medium').length : 0;

      if (currentSmallCount + currentMediumCount >= 5 && lead.category !== 'large') {
        // Continue to show any large leads
        const remainingLargeLeads = allLeads.slice(i + 1).filter(l => l.category === 'large');
        for (const largeLead of remainingLargeLeads) {
          setCurrentLead(largeLead);
          await new Promise(resolve => setTimeout(resolve, 3500));
          setLeadsLarge(prev => [...prev, largeLead]);
        }
        break;
      }
    }

    setCurrentLead(null);
    setProcessingStatus('');
    setStage('results');
  };

  const useDemoData = () => {
    setError(null);
    processLeads();
  };

  const handleSearch = () => {
    processLeads();
  };

  const resetDemo = () => {
    setStage('upload');
    setLocation('Greater Los Angeles Area');
    setLeadsSmall([]);
    setLeadsMedium([]);
    setLeadsLarge([]);
    setCurrentLead(null);
    setProcessingStatus('');
    setError(null);
  };

  const getCurrentStatus = () => {
    return STATUS_MESSAGES[statusIndex];
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
                <p>Software Engineer - Problem Solver - Builder<br /><br />From large-scale systems at Gmail and Amazon.com to the tools I build for everyday users, I focus on transforming complex problems into clear, usable solutions.</p>
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
                    <p className="period">{exp.period}</p>
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
              <h2>Marketing Agency Lead Generation</h2>
              <p className="demo-section-subtitle">AI-Powered Lead Discovery for Marketing Agencies</p>
              <p className="demo-script-link">Check Out The Full Script <a href="https://github.com/igupta1/personal-website/blob/master/MarketingLeadFinder/main.py" target="_blank" rel="noopener noreferrer">Here</a></p>
            </div>

            {error && (
              <div className="demo-error">
                <span className="demo-error-icon">Warning</span>
                <span>{error}</span>
                <button onClick={() => setError(null)} className="demo-error-close">x</button>
              </div>
            )}

            {stage === 'upload' && (
              <div className="demo-upload-stage">
                <div className="demo-how-it-works">
                  <h3>How It Works</h3>
                  <div className="demo-steps-grid">
                    <div className="demo-step"><span className="demo-step-num">1</span><span className="demo-step-text">Choose Location</span></div>
                    <div className="demo-step"><span className="demo-step-num">2</span><span className="demo-step-text">AI Scrapes Indeed to Find Companies Looking for Marketing Help</span></div>
                    <div className="demo-step"><span className="demo-step-num">3</span><span className="demo-step-text">Extract Contact Information</span></div>
                    <div className="demo-step"><span className="demo-step-num">4</span><span className="demo-step-text">Generate Qualified Lead List</span></div>
                  </div>
                </div>

                <div className="demo-location-selector">
                  <label htmlFor="location-select">Select Location:</label>
                  <select
                    id="location-select"
                    value={location}
                    onChange={(e) => setLocation(e.target.value)}
                    className="demo-location-dropdown"
                  >
                    <option value="Greater Los Angeles Area">Greater Los Angeles Area</option>
                  </select>
                </div>

                <button className="demo-try-button" onClick={useDemoData}>Try a Demo with 5 Leads</button>
              </div>
            )}

            {stage === 'processing' && (
              <div className="demo-processing-stage">
                <div className="demo-progress-header">
                  <h3>Finding Leads</h3>
                  <div className="demo-progress-bar">
                    <div className="demo-progress-fill" style={{ width: ((leadsSmall.length + leadsMedium.length) / 5) * 100 + '%' }} />
                  </div>
                  <p className="demo-progress-text">{leadsSmall.length + leadsMedium.length} of 5 target leads found</p>
                </div>

                {currentLead && (
                  <div className="demo-current-lead">
                    <div className="demo-input-card">
                      <h4 className="demo-generating-title">Finding Lead<span className="demo-animated-dots"><span>.</span><span>.</span><span>.</span></span></h4>
                      <div className="demo-input-row"><span className="demo-input-label">Name:</span><span>{currentLead.firstName} {currentLead.lastName}</span></div>
                      <div className="demo-input-row"><span className="demo-input-label">Title:</span><span>{currentLead.title || 'N/A'}</span></div>
                      <div className="demo-input-row"><span className="demo-input-label">Company:</span><span>{currentLead.companyName || 'N/A'}</span></div>
                      <div className="demo-input-row"><span className="demo-input-label">Company Size:</span><span className="demo-highlight">{currentLead.companySize}</span></div>
                    </div>

                    <div className="demo-processing-indicator">
                      <div className="demo-status-animation">
                        <div className="demo-status-icon">{getCurrentStatus().icon}</div>
                        <div className="demo-status-text">{getCurrentStatus().text}</div>
                      </div>
                      <div className="demo-progress-steps">
                        {STATUS_MESSAGES.slice(0, 5).map((_, idx) => (
                          <div key={idx} className={`demo-progress-dot ${idx <= statusIndex % 5 ? 'active' : ''}`} />
                        ))}
                      </div>
                    </div>
                  </div>
                )}

                {/* Category: ≤100 Employees */}
                <div className="demo-category-section">
                  <h4 className="demo-category-header">Companies with ≤100 Employees ({leadsSmall.length})</h4>
                  {leadsSmall.length > 0 ? (
                    <div className="demo-live-results-list">
                      {leadsSmall.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-live-result-name"><strong>{lead.firstName} {lead.lastName}</strong><span>{lead.companyName}</span></div>
                            <span className="demo-live-result-check">✓</span>
                          </div>
                          <div className="demo-live-result-details">
                            <span>{lead.email}</span>
                            <span>{lead.companySize}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="demo-category-empty">No leads yet...</p>
                  )}
                </div>

                {/* Category: 101-250 Employees */}
                <div className="demo-category-section">
                  <h4 className="demo-category-header">Companies with 101-250 Employees ({leadsMedium.length})</h4>
                  {leadsMedium.length > 0 ? (
                    <div className="demo-live-results-list">
                      {leadsMedium.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-live-result-name"><strong>{lead.firstName} {lead.lastName}</strong><span>{lead.companyName}</span></div>
                            <span className="demo-live-result-check">✓</span>
                          </div>
                          <div className="demo-live-result-details">
                            <span>{lead.email}</span>
                            <span>{lead.companySize}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="demo-category-empty">No leads yet...</p>
                  )}
                </div>

                {/* Category: 251+ Employees */}
                <div className="demo-category-section">
                  <h4 className="demo-category-header">Companies with 251+ Employees ({leadsLarge.length})</h4>
                  {leadsLarge.length > 0 ? (
                    <div className="demo-live-results-list">
                      {leadsLarge.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-live-result-name"><strong>{lead.firstName} {lead.lastName}</strong><span>{lead.companyName}</span></div>
                            <span className="demo-live-result-check">✓</span>
                          </div>
                          <div className="demo-live-result-details">
                            <span>{lead.email}</span>
                            <span>{lead.companySize}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="demo-category-empty">No leads yet...</p>
                  )}
                </div>

                <button className="demo-cancel-button" onClick={resetDemo}>Cancel</button>
              </div>
            )}

            {stage === 'results' && (
              <div className="demo-results-stage">
                <div className="demo-results-header">
                  <h3>{leadsSmall.length + leadsMedium.length + leadsLarge.length} Leads Found!</h3>
                  <button className="demo-reset-button" onClick={resetDemo}>New Search</button>
                </div>

                {/* Category: ≤100 Employees */}
                <div className="demo-results-category">
                  <h4 className="demo-results-category-title">Companies with ≤100 Employees ({leadsSmall.length})</h4>
                  <div className="demo-results-list">
                    {leadsSmall.map((lead, index) => (
                      <div key={index} className="demo-result-card">
                        <div className="demo-result-header">
                          <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                          <div className="demo-result-info"><h4>{lead.firstName} {lead.lastName}</h4><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                          <span className="demo-result-num">#{index + 1}</span>
                        </div>
                        <div className="demo-result-inputs">
                          <div className="demo-result-input"><span className="demo-result-label">Email</span><span className="demo-result-value">{lead.email}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Website</span><span className="demo-result-value">{lead.website}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Company Size</span><span className="demo-result-value">{lead.companySize}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Location</span><span className="demo-result-value">{lead.location}</span></div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Category: 101-250 Employees */}
                <div className="demo-results-category">
                  <h4 className="demo-results-category-title">Companies with 101-250 Employees ({leadsMedium.length})</h4>
                  <div className="demo-results-list">
                    {leadsMedium.map((lead, index) => (
                      <div key={index} className="demo-result-card">
                        <div className="demo-result-header">
                          <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                          <div className="demo-result-info"><h4>{lead.firstName} {lead.lastName}</h4><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                          <span className="demo-result-num">#{index + 1}</span>
                        </div>
                        <div className="demo-result-inputs">
                          <div className="demo-result-input"><span className="demo-result-label">Email</span><span className="demo-result-value">{lead.email}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Website</span><span className="demo-result-value">{lead.website}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Company Size</span><span className="demo-result-value">{lead.companySize}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Location</span><span className="demo-result-value">{lead.location}</span></div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Category: 251+ Employees */}
                {leadsLarge.length > 0 && (
                  <div className="demo-results-category">
                    <h4 className="demo-results-category-title">Companies with 251+ Employees ({leadsLarge.length})</h4>
                    <div className="demo-results-list">
                      {leadsLarge.map((lead, index) => (
                        <div key={index} className="demo-result-card">
                          <div className="demo-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-result-info"><h4>{lead.firstName} {lead.lastName}</h4><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                            <span className="demo-result-num">#{index + 1}</span>
                          </div>
                          <div className="demo-result-inputs">
                            <div className="demo-result-input"><span className="demo-result-label">Email</span><span className="demo-result-value">{lead.email}</span></div>
                            <div className="demo-result-input"><span className="demo-result-label">Website</span><span className="demo-result-value">{lead.website}</span></div>
                            <div className="demo-result-input"><span className="demo-result-label">Company Size</span><span className="demo-result-value">{lead.companySize}</span></div>
                            <div className="demo-result-input"><span className="demo-result-label">Location</span><span className="demo-result-value">{lead.location}</span></div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

export default LeadGenDemo;
