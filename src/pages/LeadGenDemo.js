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
  const [leadsSmall, setLeadsSmall] = useState([]);
  const [leadsMedium, setLeadsMedium] = useState([]);
  const [leadsLarge, setLeadsLarge] = useState([]);
  const [currentLead, setCurrentLead] = useState(null);
  const [processingPhase, setProcessingPhase] = useState('job'); // 'job' or 'contact'
  const [error, setError] = useState(null);
  const [hasStarted, setHasStarted] = useState(false);

  const experiences = [
    { title: "Software Engineer", company: "Google", description: "Built Generative AI Features for Gmail and Google Chat", logo: googlelogo },
    { title: "Software Engineer", company: "Amazon", description: "Developed AI Infrastructure for Amazon.com", logo: amazonlogo },
    { title: "Software Engineer", company: "Cisco", description: "Implemented Distributed System Architecture for Networking Solutions", logo: ciscologo }
  ];

  const generateLeads = async () => {
    const location = "demo"; // Hardcoded location for demo
    // Demo data for local development / fallback - matches smb_marketing_leads.csv exactly
    const demoLeads = [
      // ≤100 employees (3 leads)
      {
        firstName: "Lacey",
        lastName: "Clark",
        title: "Principal / Owner",
        companyName: "NW Recruiting Partners",
        jobRole: "Marketing Manager",
        email: "laceyc@nwrecruitingpartners.com",
        website: "https://nwrecruitingpartners.com",
        location: "Los Angeles, CA 90041",
        companySize: "11-50 employees",
        category: "small",
        jobLink: "https://www.indeed.com/viewjob?jk=example1",
        icebreaker: "Hey Lacey — went down a rabbit hole on NWRecruiting's site. The part about your contract-to-hire solution letting businesses trial candidates before permanent decisions caught my eye. Your focus on tailored recruitment and strong client fit stuck with me."
      },
      {
        firstName: "Jason Y.",
        lastName: "Lee",
        title: "Founder, CEO",
        companyName: "Jubilee Media",
        jobRole: "Social Media Manager",
        email: "jason@jubileemedia.com",
        website: "https://www.jubileemedia.com",
        location: "Los Angeles, CA 90045 (Westchester area)",
        companySize: "50-100 employees",
        category: "small",
        jobLink: "https://www.indeed.com/viewjob?jk=example2",
        icebreaker: "Hey Jason — went down a rabbit hole on Jubilee's site. The part about using discomfort and conflict in shows like Middle Ground to spark real dialogue caught my eye. Your focus on fostering empathy through challenging social norms stuck with me."
      },
      {
        firstName: "Ashley",
        lastName: "Carlson",
        title: "Founder and CEO",
        companyName: "Elevate Business Support",
        jobRole: "Social Media Manager",
        email: "ashley@elevatevbsolutions.com",
        website: "https://elevatevbsolutions.com/",
        location: "Hybrid work in Gardena, CA 90248",
        companySize: "10-50 employees",
        category: "small",
        jobLink: "https://www.indeed.com/viewjob?jk=example3",
        icebreaker: "Hey Ashley — went down a rabbit hole on Elevate's site. The part about your 'Get It Done' service for streamlining admin tasks caught my eye. Your focus on boosting productivity through delegation stuck with me."
      },
      // 101-250 employees (2 leads)
      {
        firstName: "Jaret",
        lastName: "Matthews",
        title: "Founder & CEO",
        companyName: "Astrolab",
        jobRole: "Marketing Manager",
        email: "jaret.matthews@venturiastrolab.com",
        website: "https://www.astrolab.com",
        location: "Hawthorne, CA 90250",
        companySize: "100-250 employees",
        category: "medium",
        jobLink: "https://www.indeed.com/viewjob?jk=example4"
      },
      {
        firstName: "Rohshann",
        lastName: "Pilla",
        title: "President, Aquent Talent",
        companyName: "Aquent Talent",
        jobRole: "Marketing Manager",
        email: "rpilla@aquent.com",
        website: "https://aquenttalent.com/",
        location: "Hybrid work in Glendale, CA 91222",
        companySize: "100-250 employees",
        category: "medium",
        jobLink: "https://www.indeed.com/viewjob?jk=example5",
        icebreaker: "Hey Rohshann — went down a rabbit hole on Aquent's site. The part about VFX artists evaluating AI-generated images for realism and accuracy caught my eye. Your focus on flexible, empowering work environments stuck with me."
      },
      // 251+ employees (2 leads)
      {
        firstName: "Dr. Ryan",
        lastName: "Cornner",
        title: "Superintendent/President",
        companyName: "Glendale Community College",
        jobRole: "Marketing Manager",
        email: "rcornner@glendale.edu",
        website: "https://www.glendale.edu",
        location: "Glendale, CA 91208 (Glendale area)",
        companySize: "250-500 employees",
        category: "large",
        jobLink: "https://www.indeed.com/viewjob?jk=example6"
      },
      {
        firstName: "Jay",
        lastName: "Penske",
        title: "Chairman, Founder & Chief Executive Officer",
        companyName: "Penske Media Corp.",
        jobRole: "Marketing Manager",
        email: "jpenske@pmc.com",
        website: "https://www.pmc.com",
        location: "Los Angeles, CA 90025 (Westwood area)",
        companySize: "1000-2500 employees",
        category: "large",
        jobLink: "https://www.indeed.com/viewjob?jk=example7"
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
    setHasStarted(true);

    // Fetch all leads from API
    const result = await generateLeads();

    if (!result.success) {
      setError(result.error || 'Failed to generate leads');
      // Stay in processing stage but show error
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

  const resetDemo = () => {
    setStage('processing');
    setLeadsSmall([]);
    setLeadsMedium([]);
    setLeadsLarge([]);
    setCurrentLead(null);
    setProcessingPhase('job');
    setError(null);
    // Restart the demo
    processLeads();
  };

  // Auto-start demo on page load
  useEffect(() => {
    if (!hasStarted) {
      processLeads();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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
              <h2>Lead Discovery for Marketing Agencies</h2>
              <p className="demo-script-link">Check Out The Full Script <a href="https://github.com/igupta1/personal-website/blob/master/MarketingLeadFinder/main.py" target="_blank" rel="noopener noreferrer">Here</a></p>
            </div>

            {error && (
              <div className="demo-error">
                <span className="demo-error-icon">Warning</span>
                <span>{error}</span>
                <button onClick={() => setError(null)} className="demo-error-close">x</button>
              </div>
            )}

            {/* How It Works - always visible */}
            <div className="demo-how-it-works">
              <h3>How It Works</h3>
              <div className="demo-steps-grid">
                <div className="demo-step">
                  <span className="demo-step-num">1</span>
                  <span className="demo-step-text">Find Companies Hiring Marketing Roles</span>
                </div>
                <div className="demo-step">
                  <span className="demo-step-num">2</span>
                  <span className="demo-step-text">Extract Decision-Maker Contact Details</span>
                </div>
                <div className="demo-step">
                  <span className="demo-step-num">3</span>
                  <span className="demo-step-text">Obtain Qualified Lead List</span>
                </div>
              </div>
            </div>

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

                {/* All Leads Combined */}
                <div className="demo-category-section">
                  {[...leadsSmall, ...leadsMedium, ...leadsLarge].length > 0 ? (
                    <div className="demo-live-results-list">
                      {[...leadsSmall, ...leadsMedium, ...leadsLarge].map((lead, index) => (
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
                          {lead.icebreaker && (
                            <div className="demo-icebreaker">
                              <span className="demo-icebreaker-label">📝 Icebreaker:</span>
                              <p className="demo-icebreaker-text">{lead.icebreaker}</p>
                            </div>
                          )}
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
                  <button className="demo-reset-button" onClick={resetDemo}>Refresh Search</button>
                </div>

                {/* All Leads Combined */}
                <div className="demo-results-category">
                  <div className="demo-results-list">
                    {[...leadsSmall, ...leadsMedium, ...leadsLarge].map((lead, index) => (
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
                          {lead.icebreaker && (
                            <div className="demo-result-input demo-result-icebreaker"><span className="demo-result-label">📝 Icebreaker</span><span className="demo-result-value demo-icebreaker-value">{lead.icebreaker}</span></div>
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
