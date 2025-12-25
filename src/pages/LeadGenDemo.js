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
    // Demo data for local development / fallback - using real data from smb_marketing_leads.csv
    const demoLeads = [
      {
        firstName: "Lacey",
        lastName: "Clark",
        title: "Principal / Owner",
        companyName: "NW Recruiting Partners",
        jobRole: "Marketing Manager",
        email: "laceyc@nwrecruitingpartners.com",
        website: "https://nwrecruitingpartners.com",
        location: "Los Angeles, CA 90041",
        companySize: "10-50 employees",
        category: "small",
        jobLink: "https://www.indeed.com/pagead/clk?mo=r&ad=-6NYlbfkN0AnVjGjnSyNF8IBfNb--AMl867kMIwBSscSrglcDFQnJb3bL0IVYEu78LtHLo_qqr0jhBZACcxNSAfa2x95ORqSwMgjJJ8kgbhSa5KY7hBgMMTV5WdS6xakW6L3VNu457k-hQJ9-hAaVtk4ghcYIebXfS0goO_lP2YSZijQT8J89zzrt0h1hWXb5NAPOhKKK43qverLSiT-MJrOF1ES7W43_uRUN-jRtm5MepvQ7fgfn0TF6hDzlhFkIzDEUHzqMohj8_XRrduUjNP1TALVD6EowYU7RLOas9H_UFFrUnYb3D8-0gBBK6LkjtTtq7MdQnVZiggFp7qGqz6-Bui-g-BfvwktWL6sQ1os1c45T-vm_N2hegf44KgpbGV3ufUzag0m1t2qSd4-hVoAcqixhq9-SDvdgOj-D9PunmfTmD5jDF-juIKUqUQgW8ue3Tul5gaAh0Iii3wbExUEJIHMdpvC4KY6HDNKWopSVv5kaDrMgLGJPahouDvgOc_-CpE1pwgveWzyKbPWn7s5BDb8J39yg1KKEal9nP8lVWf1v3NjquQymbLH-TuwsBbgGq62J7YJQJwZI90FDXcFRG5_wu4iFjT8rABMu9WqJ3HaegH6Es5ijCQpUMmBBsPgQseRr3le7xMikAC3T_6W6v8lLEpfxHvUvGt4rz0slwlf2K2pKtAmMMNTUvyYe-pPXOhEcG9t4iKbXJvYjJPYxQN1XRjEMCBjAOQfBPmXEAnv9gJ7ZA=="
      },
      {
        firstName: "Vaheh",
        lastName: "Manoukian",
        title: "Founder & Lead Attorney",
        companyName: "MANOUKIAN LAW FIRM, P.C.",
        jobRole: "Social Media Manager",
        email: "contact@manoukianlaw.com",
        website: "https://www.manoukianlaw.com",
        location: "Hybrid work in Chatsworth, CA 91311",
        companySize: "1-10 employees",
        category: "small",
        jobLink: "https://www.indeed.com/rc/clk?jk=1b149c322be5b9c8&bb=8-_eo_ZYQj5o3FMGyBD5GXZcQ59GCTBcRu1uG_XD-2-DSUUZNzxIkHht7iQoIoeAwlAs2JOQvMtfaUNgh3Hh0ul5pTjpqDxFeJlX1lLlgAFdySEag6wPyZjo1nMmH9RqFCH9aid-SNO0qoJyDJedy0TzzhVvz3Ed"
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
        companySize: "10-19 employees",
        category: "small",
        jobLink: "https://www.indeed.com/rc/clk?jk=840c67222ae628a4&bb=8-_eo_ZYQj5o3FMGyBD5Gb3lBojWPq39dK8E5OLSVAdwp30oZgKrfx97RP8IzL_KLJXtgyBwp1iHluugCvdi-Y1Z17j974Rl8d4gdyf-BupHXCj2uYSHXmgUBxCIjzi13Voqex3piChD2XgaeRIDjvHp7lDODmT0"
      },
      {
        firstName: "Roya",
        lastName: "Ghafouri, MD",
        title: "Owner / Oculoplastic & Reconstructive Surgeon",
        companyName: "Roya Ghafouri, MD",
        jobRole: "Marketing Coordinator",
        email: "roya@royamd.com",
        website: "https://www.royamd.com",
        location: "Beverly Hills, CA",
        companySize: "1-10 employees",
        category: "small",
        jobLink: "https://www.indeed.com/rc/clk?jk=d1423a36d623e435&bb=JQZt2D0517W_ThEfR5RSA1iAGhgLAu6NJ_oL8-A-MZEh4637GD7ZKKB8jsYV70_HufoQjmPZF9yPzXkHlxD8BQDJZl_ljivlQebE26rHwqCAX62_aAe-6qIFioir6dK6uv58N5kuO8va47GKOj0mXM3Ylum7ZH1O"
      },
      {
        firstName: "David",
        lastName: "Yerushalmi",
        title: "Attorney / Founder",
        companyName: "Yerushalmi Law Firm",
        jobRole: "Growth Marketer",
        email: "info@injuryneeds.com",
        website: "http://www.injuryneeds.com/",
        location: "Beverly Hills, CA 90212",
        companySize: "1-10 employees",
        category: "small",
        jobLink: "https://www.indeed.com/pagead/clk?mo=r&ad=-6NYlbfkN0DmbvpZmwu_8ru03qfVhJtao4Kc_AjKK-lMi5y9y6L-5A8oIa_bhL6uDoTZN4-MH0G0XZIUv0CEUFsnMmjeIHMbWDsoEEIeJako8Iku7Ynysd--8VRWJ6hkN9XuqESN3_ZyH82EZF2S0cRpEXj4Gts65qBeAUxrlHBID7N0AgwDmvt4prr8h9aVGR4-CzII43-rHLHJ4Wc0Z0yybUddmSotQnl4eRGwSwaTlTbWZTeHgJcmWjlaHDtjWXDAfEkh1Bzr2AOE6qvO5ZcnXmop8oJX-XCtyNx-P3eFJ0KXPnS63iTWy7O7n06pEGmj7abkzZn0WD8qyXud4AIPOFRx6AXWJjce1YJ4a3hZK8jFETfegaFNSgDkO-Gvds1XibWzBmltaeO7Gd1MA6s_x9nD--A-5rGb-lg_pbgq3CuayR0X9XkJmJ5OtCv00CJnLZ67dEJHKVq7Im33G40YElK7OkqKG5uX8xUM5RbwF81kMupmsD7en_w3OThmbr_Rt9776ieu0WEDpSDH5aR2-6PWp77s5XxE0lLcti5KKU5GsQsuOIiteDMra5pPRk8kHfrE-ZgoiGgz9JeLtJYg8HL40NNoWZsxtsCZlAiZf63IwF3agTKoY_FAzg9-2xAslMJ76C4gEJWtqg5e5-x66QNMYdV5YIEqJa4tPb0j__pdphGcSCrNYHKJaVyXS4dpgrxgptO_ZxbDEMfPBMK7d4y5duGrLvDS67uJn3VWjifaK6Yrww=="
      },
      {
        firstName: "Carl",
        lastName: "McLarand",
        title: "Founder and Chairman Emeritus",
        companyName: "MVE & Partners, INC.",
        jobRole: "Marketing Manager",
        email: "cmclarand@mve-architects.com",
        website: "https://www.mve-architects.com",
        location: "Irvine, CA 92614",
        companySize: "51-200 employees",
        category: "medium",
        jobLink: "https://www.indeed.com/rc/clk?jk=87dbca53130ce15c&bb=DSI1fiAZBHUkwJDckxuEZSgc6aV2rlrz3GywFFWM_QIO6OqOUe5sAxVrOeRfN4sx-ywlkVzItNXvL345RRcVUzTi5BeW0VcUfkGtBr7Rp83__KS3g4Tz54UwEHZsnPBKK3xnhazAxYKC_C5Wr3Lj1a9y7sssQyD4"
      },
      {
        firstName: "Jerry",
        lastName: "Battipaglia",
        title: "Co-Founder & CEO",
        companyName: "Whitecap Search",
        jobRole: "Digital Marketing Specialist",
        email: "jbattipaglia@whitecapsearch.com",
        website: "https://www.whitecapsearch.com",
        location: "Remote in Century City, CA",
        companySize: "100-250 employees",
        category: "medium",
        jobLink: "https://www.indeed.com/pagead/clk?mo=r&ad=-6NYlbfkN0Cp3za1qLcVC1e2ICPInAVbA_8ws4pGmEjrc9xvG5TXP7JVVOg79MvQCUOYoV66uFWYOPbwL-W33ghmoxknRLaDgavntE3bNY1UxWTEzMqUQWSqUKzFlQ2Qufn-c692efN_sA_0FCuGQYnGyxpRglDhh_rAd0Z9BVd-INJsm3lOO5XWT1LGQRsCrWUlBStKJjrmxX58nO0XNLGbRNf3hGuhZ62gcGmsZqeMhNNwuP8-s-Bv94oHpHjEVpK_gU9Ox24rF3rYbrrtFDPZ1y7pIa_fHJTePlBgf9UZVbqkoupCUnJwarGO2WwaG-OllTEjO5M8QIdlVO4kmA_wTIC8psieOpjDhExgEYtU8k9esTvyg5cgHIjcLDzbxYNYQx5mWAWAN8IgDRBHRvDqFu4sKYOseqfnqoIAJL1vdIkBg-Krl_Zlb3hjTI7PbqtYWm3c870NqitjV-Zn4a6BppbShaE3UAN6zND0LBqqz1DN7_jhe3psMRC5JNA_dqAQFmsnTcrpZbio_OQrCTL7RRAZnfnqkxyc3LofYC6c3umTF4AITL8GjX7BKkjMYd3zphihCRgN_7JQMn6Z23OeDluRuHyMPFjXB6koR5SwLV_RCMR0akGpo-twExzoSE-yWzgauVMt1VOS6ZUvuDxO7XHvl265QylPLxRAzZJ-NRz6x-YiI46H9GrQ4fML1GpaGYHGIvE="
      },
      {
        firstName: "Courtney",
        lastName: "Siegel",
        title: "President & Chief Executive Officer",
        companyName: "Oakmont Senior Living",
        jobRole: "Social Media Manager",
        email: "courtney.siegel@oakmontmg.com",
        website: "https://www.oakmontseniorliving.com",
        location: "Irvine, CA 92614",
        companySize: "1000-5000 employees",
        category: "large",
        jobLink: "https://www.indeed.com/pagead/clk?mo=r&ad=-6NYlbfkN0DGUq6kWQMojRqnS_MHd4LuVjA-lKl7scDdGGR-PUueRJC2hOe1qjdAMohwHEI1Syj6QMwEHQaz50_N3yEMmfs96TDnFoUJ8P61kQncGpZ1Gs1QymjRUkSguE-jgylcpns-Ij_NuMzlUVo_ZkDUwjxfBUK0_OW_d3pmBB5wUvr4PdZJtUnJ9RuBzAH5mstY_ZLQhfFyhDMIoa1BeZyD5679a2yQNuq3BqVOYj7UnYs6ytTaKNAoVQzsft11ZU4uxNGUkqLT7b1N0KogQpo2_dt3oqgifnLiN4ignzt_7KIc69VxOtctkGnDp63nBm4ZJBfWK4aML9iBifipuEpIbwXDRY_QFbu44D_l567gnpDWYWV9LVnD8-3DSCOURarmS2sEjp8rM63ZahSizs6__VhqwlorjT90flqYbk2SB-xhj6RtmGr0carn2kmR3ikaF4ukJr1kfNkWP7kMyi_0UvPVEm4YIrFVSCrh0G_4BppxGVq7SM0Bmzp0JUQ15QYn4uGYshZCwVXVVvDpUNVdDiKfgH6gf-LlT0JGNgZIcbCDNkDsfXeM316TM2BGnhNIGpVrpGwFayC5Iycj2Mfvhsfz4qWzDprDEO23Kl_SdQq21LP7hEE-ggTqaktqjIpEiNNpl3HcYBtVh5eVMrI9H3dSqOEb8hGHGhGxU-bBzlmBBCsC5ZeY_yFz3IUevadfCkJlge7heOtHbVk6vgJN5XMMpvQQSzYqL3KgSK_P17Y2RzgwxW3Ok7QVr0eW90LXFloApCifPBodP1Quroq15U9nM2QBp7vZoNYXY9hN5Ri0Dhal9PHR0Ckrn8AjFGVoaNc="
      },
      {
        firstName: "Jack",
        lastName: "Haldrup",
        title: "Founder & CEO",
        companyName: "Dr. Squatch",
        jobRole: "Marketing Manager",
        email: "jack.haldrup@drsquatch.com",
        website: "https://www.drsquatch.com",
        location: "Hybrid work in Marina del Rey, CA",
        companySize: "300-500 employees",
        category: "large",
        jobLink: "https://www.indeed.com/rc/clk?jk=0898ef95c1ec1729&bb=wj9HCgwE828SBHn1hutHmQ3emGH-H-2P_aU22iVdA7Gy3DQQrYAxP0JYmZuzDmrSGuHU6LAW9OBws-t9Yfxr7aqp9PXwwMiVPAXxfhx3O-LP3blJvSS4b2tZ_8waI8FwnpwS2aQi7L9vKAjqghylVqEWZ252V3Q2"
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
                          {lead.icebreaker && (
                            <div className="demo-result-input demo-result-icebreaker"><span className="demo-result-label">📝 Icebreaker</span><span className="demo-result-value demo-icebreaker-value">{lead.icebreaker}</span></div>
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
                          {lead.icebreaker && (
                            <div className="demo-result-input demo-result-icebreaker"><span className="demo-result-label">📝 Icebreaker</span><span className="demo-result-value demo-icebreaker-value">{lead.icebreaker}</span></div>
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
