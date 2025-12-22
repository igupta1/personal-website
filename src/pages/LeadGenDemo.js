//LeadGenDemo.js
import React, { useState } from "react";
import { Link } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

// Available metro areas for lead generation
const METRO_OPTIONS = [
  "Greater Atlanta Area",
  "Greater Austin Area",
  "Greater Bay Area",
  "Greater Boston Area",
  "Greater Chicago Area",
  "Greater Dallas Area",
  "Greater Houston Area",
  "Greater Los Angeles Area",
  "Greater Miami Area",
  "Greater New York Area",
  "Greater Philadelphia Area",
  "Greater Phoenix Area",
  "Greater San Diego Area",
  "Greater Seattle Area"
];

function LeadGenDemo() {
  const [stage, setStage] = useState('upload');
  const [location, setLocation] = useState('Greater Los Angeles Area');
  const [leadsSmall, setLeadsSmall] = useState([]);
  const [leadsMedium, setLeadsMedium] = useState([]);
  const [leadsLarge, setLeadsLarge] = useState([]);
  const [currentLead, setCurrentLead] = useState(null);
  const [processingPhase, setProcessingPhase] = useState('job'); // 'job' or 'contact'
  const [error, setError] = useState(null);

  const experiences = [
    { title: "Software Engineer", company: "Google", description: "Built Generative AI Features for Gmail and Google Chat", logo: googlelogo },
    { title: "Software Engineer", company: "Amazon", description: "Developed AI Infrastructure for Amazon.com", logo: amazonlogo },
    { title: "Software Engineer", company: "Cisco", description: "Implemented Distributed System Architecture for Networking Solutions", logo: ciscologo }
  ];

  const generateLeads = async (location) => {
    // Demo data for local development / fallback
    const demoLeads = [
      {
        firstName: "Sarah",
        lastName: "Mitchell",
        title: "CEO",
        companyName: "Coastal Marketing Co",
        jobRole: "Marketing Manager",
        email: "sarah@coastalmarketing.com",
        website: "https://coastalmarketing.com",
        location: "Santa Monica, CA",
        companySize: "45 employees",
        category: "small",
        jobLink: "" // Demo data - real leads will have actual Indeed links
      },
      {
        firstName: "David",
        lastName: "Chen",
        title: "Founder",
        companyName: "Tech Solutions LA",
        jobRole: "Social Media Manager",
        email: "david@techsolutionsla.com",
        website: "https://techsolutionsla.com",
        location: "Los Angeles, CA",
        companySize: "85 employees",
        category: "small",
        jobLink: ""
      },
      {
        firstName: "Maria",
        lastName: "Rodriguez",
        title: "President",
        companyName: "Urban Design Studio",
        jobRole: "Digital Marketing Specialist",
        email: "maria@urbandesignstudio.com",
        website: "https://urbandesignstudio.com",
        location: "Pasadena, CA",
        companySize: "120 employees",
        category: "medium",
        jobLink: ""
      },
      {
        firstName: "James",
        lastName: "Thompson",
        title: "Owner",
        companyName: "Pacific Consulting Group",
        jobRole: "Content Marketing Manager",
        email: "james@pacificconsulting.com",
        website: "https://pacificconsulting.com",
        location: "Burbank, CA",
        companySize: "65 employees",
        category: "small",
        jobLink: ""
      },
      {
        firstName: "Lisa",
        lastName: "Park",
        title: "Co-Founder & CEO",
        companyName: "Innovate Digital",
        jobRole: "Growth Marketing Lead",
        email: "lisa@innovatedigital.com",
        website: "https://innovatedigital.com",
        location: "Irvine, CA",
        companySize: "180 employees",
        category: "medium",
        jobLink: ""
      },
      {
        firstName: "Robert",
        lastName: "Williams",
        title: "Founder & CEO",
        companyName: "Summit Enterprises",
        jobRole: "Marketing Manager",
        email: "robert@summitenterprises.com",
        website: "https://summitenterprises.com",
        location: "Los Angeles, CA",
        companySize: "320 employees",
        category: "large",
        jobLink: ""
      },
      {
        firstName: "Emily",
        lastName: "Johnson",
        title: "President & CEO",
        companyName: "Westside Media Group",
        jobRole: "Social Media Manager",
        email: "emily@westsidemedia.com",
        website: "https://westsidemedia.com",
        location: "Santa Monica, CA",
        companySize: "95 employees",
        category: "small",
        jobLink: ""
      },
      {
        firstName: "Michael",
        lastName: "Brown",
        title: "CEO & Founder",
        companyName: "Digital Horizons",
        jobRole: "Digital Marketing Specialist",
        email: "michael@digitalhorizons.com",
        website: "https://digitalhorizons.com",
        location: "Pasadena, CA",
        companySize: "450 employees",
        category: "large",
        jobLink: ""
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

      // Phase 1: Show "[Company] is looking for a [Role]" for 2 seconds
      setCurrentLead(lead);
      setProcessingPhase('job');
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Phase 2: Show "Finding Contact Information" for 2 seconds
      setProcessingPhase('contact');
      await new Promise(resolve => setTimeout(resolve, 2000));

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
          setProcessingPhase('job');
          await new Promise(resolve => setTimeout(resolve, 2000));
          setProcessingPhase('contact');
          await new Promise(resolve => setTimeout(resolve, 2000));
          setLeadsLarge(prev => [...prev, largeLead]);
        }
        break;
      }
    }

    setCurrentLead(null);
    setProcessingPhase('job');
    setStage('results');
  };

  const useDemoData = () => {
    setError(null);
    processLeads();
  };

  const resetDemo = () => {
    setStage('upload');
    setLocation('Greater Los Angeles Area');
    setLeadsSmall([]);
    setLeadsMedium([]);
    setLeadsLarge([]);
    setCurrentLead(null);
    setProcessingPhase('job');
    setError(null);
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
                    <div className="demo-step"><span className="demo-step-num">1</span><span className="demo-step-text">Select Your Target Location</span></div>
                    <div className="demo-step"><span className="demo-step-num">2</span><span className="demo-step-text">AI Finds Companies Hiring Marketing Roles</span></div>
                    <div className="demo-step"><span className="demo-step-num">3</span><span className="demo-step-text">Extract Decision-Maker Contact Details</span></div>
                    <div className="demo-step"><span className="demo-step-num">4</span><span className="demo-step-text">Get Your Qualified Lead List</span></div>
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
                    {METRO_OPTIONS.map(metro => (
                      <option key={metro} value={metro}>{metro}</option>
                    ))}
                  </select>
                </div>

                <button className="demo-try-button" onClick={useDemoData}>Try Demo</button>
              </div>
            )}

            {stage === 'processing' && (
              <div className="demo-processing-stage">
                {currentLead && (
                  <div className="demo-current-lead">
                    <div className="demo-processing-message">
                      <p className="demo-phase-text">
                        <span className="demo-company-highlight">{currentLead.companyName}</span> is looking for a <span className="demo-role-highlight">{currentLead.jobRole || 'Marketing Manager'}</span>{processingPhase === 'job' && <span className="demo-animated-dots"><span>.</span><span>.</span><span>.</span></span>}
                      </p>
                      <p className="demo-phase-text">
                        Finding Contact Information{processingPhase === 'contact' && <span className="demo-animated-dots"><span>.</span><span>.</span><span>.</span></span>}
                      </p>
                    </div>
                  </div>
                )}

                {/* Category: Less Than 100 Employees */}
                <div className="demo-category-section">
                  <h4 className="demo-category-header">Companies With Less Than 100 Employees</h4>
                  {leadsSmall.length > 0 ? (
                    <div className="demo-live-results-list">
                      {leadsSmall.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-live-result-name"><strong>{lead.firstName} {lead.lastName}</strong><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                            <span className="demo-live-result-check">✓</span>
                          </div>
                          <div className="demo-live-result-details">
                            <span>{lead.email}</span>
                            <span>{lead.website}</span>
                            {lead.jobRole && <span>Hiring for: {lead.jobRole}</span>}
                            {lead.jobLink && <span><a href={lead.jobLink} target="_blank" rel="noopener noreferrer">View Job Posting →</a></span>}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="demo-category-empty">No leads yet...</p>
                  )}
                </div>

                {/* Category: Between 100 and 250 Employees */}
                <div className="demo-category-section">
                  <h4 className="demo-category-header">Companies Between 100 and 250 Employees</h4>
                  {leadsMedium.length > 0 ? (
                    <div className="demo-live-results-list">
                      {leadsMedium.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-live-result-name"><strong>{lead.firstName} {lead.lastName}</strong><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                            <span className="demo-live-result-check">✓</span>
                          </div>
                          <div className="demo-live-result-details">
                            <span>{lead.email}</span>
                            <span>{lead.website}</span>
                            {lead.jobRole && <span>Hiring for: {lead.jobRole}</span>}
                            {lead.jobLink && <span><a href={lead.jobLink} target="_blank" rel="noopener noreferrer">View Job Posting →</a></span>}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="demo-category-empty">No leads yet...</p>
                  )}
                </div>

                {/* Category: More Than 250 Employees */}
                <div className="demo-category-section">
                  <h4 className="demo-category-header">Companies With More Than 250 Employees</h4>
                  {leadsLarge.length > 0 ? (
                    <div className="demo-live-results-list">
                      {leadsLarge.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-live-result-name"><strong>{lead.firstName} {lead.lastName}</strong><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                            <span className="demo-live-result-check">✓</span>
                          </div>
                          <div className="demo-live-result-details">
                            <span>{lead.email}</span>
                            <span>{lead.website}</span>
                            {lead.jobRole && <span>Hiring for: {lead.jobRole}</span>}
                            {lead.jobLink && <span><a href={lead.jobLink} target="_blank" rel="noopener noreferrer">View Job Posting →</a></span>}
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
                  <button className="demo-reset-button" onClick={resetDemo}>New Search</button>
                </div>

                {/* Category: Less Than 100 Employees */}
                <div className="demo-results-category">
                  <h4 className="demo-results-category-title">Companies With Less Than 100 Employees</h4>
                  <div className="demo-results-list">
                    {leadsSmall.map((lead, index) => (
                      <div key={index} className="demo-result-card">
                        <div className="demo-result-header">
                          <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                          <div className="demo-result-info"><h4>{lead.firstName} {lead.lastName}</h4><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                        </div>
                        <div className="demo-result-inputs">
                          <div className="demo-result-input"><span className="demo-result-label">Email</span><span className="demo-result-value">{lead.email}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Website</span><span className="demo-result-value">{lead.website}</span></div>
                          {lead.jobRole && (
                            <div className="demo-result-input"><span className="demo-result-label">Hiring for</span><span className="demo-result-value">{lead.jobRole}</span></div>
                          )}
                          {lead.jobLink && (
                            <div className="demo-result-input"><span className="demo-result-label">Job Posting</span><span className="demo-result-value"><a href={lead.jobLink} target="_blank" rel="noopener noreferrer">View on Indeed →</a></span></div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Category: Between 100 and 250 Employees */}
                <div className="demo-results-category">
                  <h4 className="demo-results-category-title">Companies Between 100 and 250 Employees</h4>
                  <div className="demo-results-list">
                    {leadsMedium.map((lead, index) => (
                      <div key={index} className="demo-result-card">
                        <div className="demo-result-header">
                          <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                          <div className="demo-result-info"><h4>{lead.firstName} {lead.lastName}</h4><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                        </div>
                        <div className="demo-result-inputs">
                          <div className="demo-result-input"><span className="demo-result-label">Email</span><span className="demo-result-value">{lead.email}</span></div>
                          <div className="demo-result-input"><span className="demo-result-label">Website</span><span className="demo-result-value">{lead.website}</span></div>
                          {lead.jobRole && (
                            <div className="demo-result-input"><span className="demo-result-label">Hiring for</span><span className="demo-result-value">{lead.jobRole}</span></div>
                          )}
                          {lead.jobLink && (
                            <div className="demo-result-input"><span className="demo-result-label">Job Posting</span><span className="demo-result-value"><a href={lead.jobLink} target="_blank" rel="noopener noreferrer">View on Indeed →</a></span></div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Category: More Than 250 Employees */}
                {leadsLarge.length > 0 && (
                  <div className="demo-results-category">
                    <h4 className="demo-results-category-title">Companies With More Than 250 Employees</h4>
                    <div className="demo-results-list">
                      {leadsLarge.map((lead, index) => (
                        <div key={index} className="demo-result-card">
                          <div className="demo-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-result-info"><h4>{lead.firstName} {lead.lastName}</h4><span>{lead.title || 'Professional'} at {lead.companyName}</span></div>
                          </div>
                          <div className="demo-result-inputs">
                            <div className="demo-result-input"><span className="demo-result-label">Email</span><span className="demo-result-value">{lead.email}</span></div>
                            <div className="demo-result-input"><span className="demo-result-label">Website</span><span className="demo-result-value">{lead.website}</span></div>
                            {lead.jobRole && (
                              <div className="demo-result-input"><span className="demo-result-label">Hiring for</span><span className="demo-result-value">{lead.jobRole}</span></div>
                            )}
                            {lead.jobLink && (
                              <div className="demo-result-input"><span className="demo-result-label">Job Posting</span><span className="demo-result-value"><a href={lead.jobLink} target="_blank" rel="noopener noreferrer">View on Indeed →</a></span></div>
                            )}
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
