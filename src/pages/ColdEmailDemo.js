//ColdEmailDemo.js
import React, { useState, useRef, useEffect } from "react";
import { Link } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

// API endpoint - uses relative path for Vercel deployment
const API_URL = '';

// Cycling status messages to show what the AI is doing
const STATUS_MESSAGES = [
  { icon: "🌐", text: "Connecting to website..." },
  { icon: "📄", text: "Fetching homepage content..." },
  { icon: "🔗", text: "Discovering internal pages..." },
  { icon: "📖", text: "Reading about services..." },
  { icon: "🔍", text: "Analyzing company values..." },
  { icon: "🧠", text: "Understanding brand voice..." },
  { icon: "💡", text: "Identifying unique differentiators..." },
  { icon: "✍️", text: "Crafting personalized opener..." },
  { icon: "🎯", text: "Finding specific details..." },
  { icon: "✨", text: "Polishing the icebreaker..." }
];

// Demo data from marketing_agencies.csv (first 20 rows)
const DEMO_LEADS = [
  { firstName: "Junior", lastName: "Nyemb", title: "Founder, CEO", companyName: "the grio agency", email: "junior@grioagency.com", website: "https://grioagency.com" },
  { firstName: "Rebecca", lastName: "Epperson", title: "Founder/CEO", companyName: "Chartwell Agency", email: "repperson@chartwellagency.com", website: "https://chartwellagency.com" },
  { firstName: "Nicole", lastName: "Greige", title: "Founder", companyName: "Glyff Agency", email: "nicolegreige@itsglyff.com", website: "https://itsglyff.com" },
  { firstName: "Alonso", lastName: "Arias", title: "Founder - CCO", companyName: "the non agency", email: "alonso@thenonagency.com", website: "https://thenonagency.com" },
  { firstName: "Parker", lastName: "Warren", title: "Agency Owner", companyName: "PWA Media", email: "parker@pwa-media.org", website: "https://pwa-media.org" },
  { firstName: "Rachel", lastName: "Svoboda", title: "CEO", companyName: "Sunday Brunch Agency", email: "rachel@sundaybrunchagency.com", website: "https://sundaybrunchagency.com" },
  { firstName: "Steven", lastName: "Jumper", title: "Co-founder and CEO", companyName: "Ghost Note Agency", email: "steven@ghostnoteagency.com", website: "https://ghostnoteagency.com" },
  { firstName: "Newton", lastName: "Agency", title: "CEO", companyName: "Newton Agency", email: "robby@newtonagency.be", website: "https://newtonagency.be" },
  { firstName: "Josh", lastName: "Daunt", title: "Owner", companyName: "Undaunted Agency", email: "josh@undauntedvisuals.com", website: "https://undauntedagency.com" },
  { firstName: "Marty", lastName: "Engel", title: "Partner / CRO", companyName: "HH Agency", email: "martye@hhagency.co", website: "https://hhagency.co" },
  { firstName: "Steve", lastName: "Gray", title: "Co-Owner", companyName: "Spire Agency", email: "steve.gray@spireagency.com", website: "https://spireagency.com" },
  { firstName: "Seth", lastName: "Gruen", title: "Co-Founder", companyName: "The Branded Agency", email: "seth.gruen@thebranded.agency", website: "https://thebranded.agency" },
  { firstName: "Guillermo", lastName: "Plehn", title: "CEO", companyName: "M4 Agency Group", email: "gplehn@m4agency.com", website: "https://m4agency.com" },
  { firstName: "Aaron", lastName: "Gobidas", title: "CEO", companyName: "GoBeRewarded", email: "aaron@goberewarded.com", website: "https://goberewarded.com" },
  { firstName: "James", lastName: "Toon", title: "Co-Founder + CCO", companyName: "Solution Agency", email: "jt@yoursolutiondesign.com", website: "https://solutionagency.com" },
  { firstName: "Shane", lastName: "Bell", title: "CIO", companyName: "HotSpot Agency", email: "shane@urgentfury.com", website: "https://hotspots.agency" },
  { firstName: "Ashley", lastName: "Kohama", title: "Agency Owner", companyName: "Kohama Marketing", email: "ashley@kohamamarketing.com", website: "https://kohamamarketing.com" },
  { firstName: "Cheryl", lastName: "Russell", title: "Agency Owner", companyName: "Dot2", email: "cheryl@dot2.studio", website: "https://dot2.studio" },
  { firstName: "Gerry", lastName: "McGillvray", title: "President and Founder", companyName: "Blackiron Agency", email: "gerry@blackiron.agency", website: "https://blackiron.agency" },
  { firstName: "Chris", lastName: "Nelson", title: "Founder", companyName: "InCampaign Agency", email: "chris@incampaignagency.com", website: "https://incampaignagency.com" }
];

const REQUIRED_COLUMNS = ['First Name', 'Last Name', 'Title', 'Company Name', 'Email', 'Website'];

function ColdEmailDemo() {
  const [stage, setStage] = useState('upload');
  const [leads, setLeads] = useState([]);
  const [processedLeads, setProcessedLeads] = useState([]);
  const [currentLead, setCurrentLead] = useState(null);
  const [processingStatus, setProcessingStatus] = useState('');
  const [statusIndex, setStatusIndex] = useState(0);
  const [skippedCount, setSkippedCount] = useState(0);
  const [dragActive, setDragActive] = useState(false);
  const [error, setError] = useState(null);
  const fileInputRef = useRef(null);

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

  const parseCSV = (text) => {
    const lines = text.trim().split('\n');
    if (lines.length < 2) return { error: 'CSV file is empty or has no data rows' };

    const parseRow = (row) => {
      const result = [];
      let current = '';
      let inQuotes = false;
      for (let i = 0; i < row.length; i++) {
        const char = row[i];
        if (char === '"') inQuotes = !inQuotes;
        else if (char === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
        else current += char;
      }
      result.push(current.trim());
      return result;
    };

    const headers = parseRow(lines[0]);
    const findColumn = (name) => headers.findIndex(h => h.toLowerCase().replace(/['"]/g, '').trim() === name.toLowerCase());

    const columnIndices = {
      firstName: findColumn('First Name'),
      lastName: findColumn('Last Name'),
      title: findColumn('Title'),
      companyName: findColumn('Company Name'),
      email: findColumn('Email'),
      website: findColumn('Website')
    };

    const missingColumns = [];
    if (columnIndices.firstName === -1) missingColumns.push('First Name');
    if (columnIndices.lastName === -1) missingColumns.push('Last Name');
    if (columnIndices.title === -1) missingColumns.push('Title');
    if (columnIndices.companyName === -1) missingColumns.push('Company Name');
    if (columnIndices.email === -1) missingColumns.push('Email');
    if (columnIndices.website === -1) missingColumns.push('Website');

    if (missingColumns.length > 0) {
      return { error: 'Missing required columns: ' + missingColumns.join(', ') + '. Your CSV must have these columns: ' + REQUIRED_COLUMNS.join(', ') };
    }

    const parsed = [];
    for (let i = 1; i < lines.length && parsed.length < 20; i++) {
      const values = parseRow(lines[i]);
      const firstName = values[columnIndices.firstName]?.replace(/['"]/g, '').trim();
      const lastName = values[columnIndices.lastName]?.replace(/['"]/g, '').trim();
      const email = values[columnIndices.email]?.replace(/['"]/g, '').trim();
      const website = values[columnIndices.website]?.replace(/['"]/g, '').trim();
      
      if (firstName && email && website) {
        parsed.push({
          firstName,
          lastName: lastName || '',
          title: values[columnIndices.title]?.replace(/['"]/g, '').trim() || '',
          companyName: values[columnIndices.companyName]?.replace(/['"]/g, '').trim() || '',
          email,
          website
        });
      }
    }

    if (parsed.length === 0) return { error: 'No valid rows found.' };
    return { data: parsed };
  };

  const generateIcebreaker = async (lead) => {
    try {
      const response = await fetch(API_URL + '/api/generate-icebreaker', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ lead })
      });
      if (!response.ok) throw new Error('HTTP error! status: ' + response.status);
      return await response.json();
    } catch (err) {
      console.error('Error generating icebreaker:', err);
      return { success: false, error: err.message };
    }
  };

  const processLeads = async (leadsToProcess) => {
    setStage('processing');
    setProcessedLeads([]);
    setSkippedCount(0);
    setError(null);
    
    let successCount = 0;
    const maxLeads = Math.min(leadsToProcess.length, 20);
    const targetSuccess = 5;

    for (let i = 0; i < maxLeads && successCount < targetSuccess; i++) {
      const lead = leadsToProcess[i];
      setCurrentLead(lead);
      setProcessingStatus('generating');

      const result = await generateIcebreaker(lead);

      if (result.success && result.icebreaker) {
        successCount++;
        setProcessedLeads(prev => [...prev, { ...lead, icebreaker: result.icebreaker }]);
      } else {
        setSkippedCount(prev => prev + 1);
      }
    }

    setCurrentLead(null);
    setProcessingStatus('');
    setStage('results');
  };

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") setDragActive(true);
    else if (e.type === "dragleave") setDragActive(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]);
  };

  const handleFileInput = (e) => {
    if (e.target.files && e.target.files[0]) handleFile(e.target.files[0]);
  };

  const handleFile = (file) => {
    setError(null);
    if (!file.name.endsWith('.csv')) { setError('Please upload a CSV file'); return; }
    const reader = new FileReader();
    reader.onload = (e) => {
      const result = parseCSV(e.target.result);
      if (result.error) { setError(result.error); return; }
      setLeads(result.data);
      processLeads(result.data);
    };
    reader.readAsText(file);
  };

  const useDemoData = () => {
    setError(null);
    setLeads(DEMO_LEADS);
    processLeads(DEMO_LEADS);
  };

  const resetDemo = () => {
    setStage('upload');
    setLeads([]);
    setProcessedLeads([]);
    setCurrentLead(null);
    setProcessingStatus('');
    setSkippedCount(0);
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
              <h2>Cold Email Deep Personalization</h2>
              <p className="demo-section-subtitle">Watch AI Analyze Websites and Generate Hyper-Personalized Icebreakers</p>
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
                    <div className="demo-step"><span className="demo-step-num">1</span><span className="demo-step-text">Upload Leads as a CSV File</span></div>
                    <div className="demo-step"><span className="demo-step-num">2</span><span className="demo-step-text">AI Scrapes Web for Deep Insights</span></div>
                    <div className="demo-step"><span className="demo-step-num">3</span><span className="demo-step-text">Analyze Themes and Metrics</span></div>
                    <div className="demo-step"><span className="demo-step-num">4</span><span className="demo-step-text">Generate AI Personalized Icebreaker</span></div>
                  </div>
                </div>

                <button className="demo-try-button" onClick={useDemoData}>Try a Demo with 5 Leads</button>

                <div className="demo-or-divider"><span>OR</span></div>

                <div className={'demo-upload-zone' + (dragActive ? ' drag-active' : '')} onDragEnter={handleDrag} onDragLeave={handleDrag} onDragOver={handleDrag} onDrop={handleDrop} onClick={() => fileInputRef.current?.click()}>
                  <input ref={fileInputRef} type="file" accept=".csv" onChange={handleFileInput} style={{ display: 'none' }} />
                  <div className="demo-upload-icon">📁</div>
                  <h4>Upload Your CSV</h4>
                  <p>Drag and drop or click to browse</p>
                  <span className="demo-upload-hint">Required columns: {REQUIRED_COLUMNS.join(', ')}</span>
                </div>
              </div>
            )}

            {stage === 'processing' && (
              <div className="demo-processing-stage">
                <div className="demo-progress-header">
                  <h3>Generating Icebreakers</h3>
                  <div className="demo-progress-bar">
                    <div className="demo-progress-fill" style={{ width: ((processedLeads.length + skippedCount) / Math.min(leads.length, 20)) * 100 + '%' }} />
                  </div>
                  <p className="demo-progress-text">{processedLeads.length} of 5 icebreakers generated</p>
                </div>

                {currentLead && (
                  <div className="demo-current-lead">
                    <div className="demo-input-card">
                      <h4 className="demo-generating-title">Generating Icebreaker<span className="demo-animated-dots"><span>.</span><span>.</span><span>.</span></span></h4>
                      <div className="demo-input-row"><span className="demo-input-label">Name:</span><span>{currentLead.firstName} {currentLead.lastName}</span></div>
                      <div className="demo-input-row"><span className="demo-input-label">Title:</span><span>{currentLead.title || 'N/A'}</span></div>
                      <div className="demo-input-row"><span className="demo-input-label">Company:</span><span>{currentLead.companyName || 'N/A'}</span></div>
                      <div className="demo-input-row"><span className="demo-input-label">Website:</span><span className="demo-highlight">{currentLead.website}</span></div>
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

                {processedLeads.length > 0 && (
                  <div className="demo-live-results">
                    <h4>Completed Icebreakers ({processedLeads.length})</h4>
                    <div className="demo-live-results-list">
                      {processedLeads.map((lead, index) => (
                        <div key={index} className="demo-live-result-card">
                          <div className="demo-live-result-header">
                            <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                            <div className="demo-live-result-name"><strong>{lead.firstName} {lead.lastName}</strong><span>{lead.companyName}</span></div>
                            <span className="demo-live-result-check">Done</span>
                          </div>
                          <p className="demo-live-result-icebreaker">{lead.icebreaker}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <button className="demo-cancel-button" onClick={resetDemo}>Cancel</button>
              </div>
            )}

            {stage === 'results' && (
              <div className="demo-results-stage">
                <div className="demo-results-header">
                  <h3>{processedLeads.length} Icebreakers Generated!</h3>
                </div>

                <div className="demo-results-list">
                  {processedLeads.map((lead, index) => (
                    <div key={index} className="demo-result-card">
                      <div className="demo-result-header">
                        <div className="demo-result-avatar">{lead.firstName.charAt(0)}{lead.lastName.charAt(0) || ''}</div>
                        <div className="demo-result-info"><h4>{lead.firstName} {lead.lastName}</h4><span>{lead.title || 'Professional'} of {lead.companyName}</span></div>
                        <span className="demo-result-num">#{index + 1}</span>
                      </div>
                      <div className="demo-result-inputs">
                        <div className="demo-result-input"><span className="demo-result-label">Email</span><span className="demo-result-value">{lead.email}</span></div>
                        <div className="demo-result-input"><span className="demo-result-label">Website</span><span className="demo-result-value">{lead.website}</span></div>
                      </div>
                      <div className="demo-result-icebreaker">
                        <div className="demo-icebreaker-label">AI Personalized Icebreaker</div>
                        <p>{lead.icebreaker}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

export default ColdEmailDemo;
