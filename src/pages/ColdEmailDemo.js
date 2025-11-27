//ColdEmailDemo.js
import React, { useState, useRef } from "react";
import { Link } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

// Sample data from leads_with_icebreakers.csv (first 10)
const SAMPLE_DATA = [
  {
    first_name: "Junior",
    last_name: "Nyemb",
    email: "junior@grioagency.com",
    website_url: "https://grioagency.com",
    headline: "Founder, CEO",
    icebreaker: `Hey Junior — went down a rabbit hole on Grio's site. The part about blending data and empathy in "the Grio way" to understand what employees, customers, and investors value caught my eye. Your focus on empathy-led, mission-driven branding stuck with me.`
  },
  {
    first_name: "Rebecca",
    last_name: "Epperson",
    email: "repperson@chartwellagency.com",
    website_url: "https://chartwellagency.com",
    headline: "Founder/CEO",
    icebreaker: `Hey Rebecca — went down a rabbit hole on Chartwell's site. The part about using qualitative and quantitative research to shape sector-specific branding really caught my eye. Your focus on outcome‑driven, integrated strategy stuck with me.`
  },
  {
    first_name: "Parker",
    last_name: "Warren",
    email: "parker@pwa-media.org",
    website_url: "https://pwa-media.org",
    headline: "Agency Owner",
    icebreaker: `Hey Parker — went down a rabbit hole on PWA's site. The part about driving Utah businesses' growth with data-driven PPC case studies like the 200% sales lift and 45% CPA drop caught my eye. Your focus on transparent, ROI-focused reporting stuck with me.`
  },
  {
    first_name: "Rachel",
    last_name: "Svoboda",
    email: "rachel@sundaybrunchagency.com",
    website_url: "https://sundaybrunchagency.com",
    headline: "CEO",
    icebreaker: `Hey Rachel — went down a rabbit hole on SundayBrunch's site. The part about offering AI optimization from stacking and prompt engineering through custom workflows and training caught my eye. Your focus on white-glove, bespoke support for independently owned businesses stuck with me.`
  },
  {
    first_name: "Steven",
    last_name: "Jumper",
    email: "steven@ghostnoteagency.com",
    website_url: "https://ghostnoteagency.com",
    headline: "Co-founder and Chief Executive Officer",
    icebreaker: `Hey Steven — went down a rabbit hole on Ghost Note's site. The part about co-building platforms with culture and reducing the translation burden for tastemakers caught my eye. Your focus on cultural fluency that actually earns trust stuck with me.`
  },
  {
    first_name: "Newton",
    last_name: "Agency",
    email: "robby@newtonagency.be",
    website_url: "https://newtonagency.be",
    headline: "CEO",
    icebreaker: `Hey Newton — went down a rabbit hole on Newton's site. The part about plugging into a flexible freelancer network to pair a Usability Expert with Stef Boey's rebranding really caught my eye. Your focus on transparent, no-nonsense communication stuck with me.`
  },
  {
    first_name: "Josh",
    last_name: "Daunt",
    email: "josh@undauntedvisuals.com",
    website_url: "https://undauntedagency.com",
    headline: "Owner, Video Editor, Motion Graphics Artist",
    icebreaker: `Hey Josh — went down a rabbit hole on Undaunted's site. The part about building audience-driven websites that inform, guide, and convert really caught my eye. Your focus on advertising with purpose stuck with me.`
  },
  {
    first_name: "Marty",
    last_name: "Engel",
    email: "martye@hhagency.co",
    website_url: "https://hhagency.co",
    headline: "Partner / Chief Revenue Officer",
    icebreaker: `Hey Marty — went down a rabbit hole on HH's site. The part about tying Digital, Analog, and Experiential into one end-to-end brand-experiences stack caught my eye. Your focus on turning creativity into tangible, measurable outcomes stuck with me.`
  },
  {
    first_name: "Steve",
    last_name: "Gray",
    email: "steve.gray@spireagency.com",
    website_url: "https://spireagency.com",
    headline: "Co-Owner",
    icebreaker: `Hey Steve — went down a rabbit hole on Spire's site. The part about using customer interviews and audience segmentation to sharpen B2B messaging caught my eye. Your focus on aligning every brand expression to a strategic foundation stuck with me.`
  },
  {
    first_name: "Seth",
    last_name: "Gruen",
    email: "seth.gruen@thebranded.agency",
    website_url: "https://thebranded.agency",
    headline: "Co-Founder",
    icebreaker: `Hey Seth — went down a rabbit hole on Branded's site. The way you frame content as "journalism-level" and position clients as news outlets themselves caught my eye. Your focus on becoming "experts at becoming experts" through rapid immersion really stuck with me.`
  }
];

function ColdEmailDemo() {
  const [stage, setStage] = useState('upload'); // 'upload', 'processing', 'results'
  const [leads, setLeads] = useState([]);
  const [processedLeads, setProcessedLeads] = useState([]);
  const [currentProcessingIndex, setCurrentProcessingIndex] = useState(-1);
  const [dragActive, setDragActive] = useState(false);
  const fileInputRef = useRef(null);

  const experiences = [
    {
      title: "Software Engineer Intern",
      company: "Google",
      period: "Summer 2025",
      description: "Generative AI in Gmail and Google Chat",
      logo: googlelogo,
    },
    {
      title: "Software Engineer Intern",
      company: "Amazon",
      period: "Summer 2024",
      description: "Developing Infrastructure for Amazon.com",
      logo: amazonlogo,
    },
    {
      title: "Software Engineer Intern",
      company: "Cisco",
      period: "Spring 2024",
      description: "Distributed Systems Engineering",
      logo: ciscologo,
    },
  ];

  const parseCSV = (text) => {
    const lines = text.trim().split('\n');
    const headers = lines[0].split(',').map(h => h.trim().toLowerCase().replace(/['"]/g, ''));
    
    const nameIndex = headers.findIndex(h => (h.includes('name') && h.includes('first')) || h === 'first_name');
    const lastNameIndex = headers.findIndex(h => (h.includes('name') && h.includes('last')) || h === 'last_name');
    const emailIndex = headers.findIndex(h => h.includes('email'));
    const websiteIndex = headers.findIndex(h => h.includes('website') || h.includes('url'));
    
    if (emailIndex === -1 || websiteIndex === -1) {
      alert('CSV must contain email and website columns');
      return [];
    }

    const parsed = [];
    for (let i = 1; i < Math.min(lines.length, 11); i++) {
      const values = lines[i].split(',').map(v => v.trim().replace(/['"]/g, ''));
      if (values[emailIndex] && values[websiteIndex]) {
        parsed.push({
          first_name: values[nameIndex] || 'Unknown',
          last_name: values[lastNameIndex] || '',
          email: values[emailIndex],
          website_url: values[websiteIndex],
          headline: '',
          icebreaker: null
        });
      }
    }
    return parsed;
  };

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  const handleFileInput = (e) => {
    if (e.target.files && e.target.files[0]) {
      handleFile(e.target.files[0]);
    }
  };

  const handleFile = (file) => {
    if (!file.name.endsWith('.csv')) {
      alert('Please upload a CSV file');
      return;
    }
    const reader = new FileReader();
    reader.onload = (e) => {
      const parsed = parseCSV(e.target.result);
      if (parsed.length > 0) {
        setLeads(parsed);
        startProcessing(parsed, true);
      }
    };
    reader.readAsText(file);
  };

  const useSampleData = () => {
    setLeads(SAMPLE_DATA);
    startProcessing(SAMPLE_DATA, false);
  };

  const startProcessing = (dataToProcess, isUserData) => {
    setStage('processing');
    setProcessedLeads([]);
    setCurrentProcessingIndex(0);
    
    dataToProcess.forEach((lead, index) => {
      setTimeout(() => {
        setCurrentProcessingIndex(index);
        
        setTimeout(() => {
          setProcessedLeads(prev => [...prev, {
            ...lead,
            icebreaker: isUserData 
              ? generateDemoIcebreaker(lead)
              : lead.icebreaker
          }]);
          
          if (index === dataToProcess.length - 1) {
            setTimeout(() => {
              setStage('results');
            }, 500);
          }
        }, 800);
      }, index * 1200);
    });
  };

  const generateDemoIcebreaker = (lead) => {
    const domain = lead.website_url.replace(/https?:\/\//, '').replace('www.', '').split('/')[0];
    const companyName = domain.split('.')[0].charAt(0).toUpperCase() + domain.split('.')[0].slice(1);
    return `Hey ${lead.first_name} — went down a rabbit hole on ${companyName}'s site. Your approach to serving clients with personalized solutions caught my eye. Your focus on delivering real value stuck with me.`;
  };

  const resetDemo = () => {
    setStage('upload');
    setLeads([]);
    setProcessedLeads([]);
    setCurrentProcessingIndex(-1);
  };

  return (
    <div className="about-page-combined">
      <div className="about-container">
        {/* Left Column - Profile & Experience */}
        <div className="about-left-column">
          {/* Profile Section */}
          <section id="home" className="profile-section">
            <div className="profile-header">
              <img src={pfpImage} alt="Ishaan Gupta" className="profile-image" />
              <div className="profile-info">
                <h1>Ishaan Gupta</h1>
                <p>Software Engineer • Problem Solver • Builder
                <br /> <br />
                From large-scale systems at Gmail and Amazon.com to the tools I build for everyday users, I focus on transforming complex problems into clear, usable solutions.</p>
              </div>
            </div>
          </section>

          {/* Work Experience Section */}
          <section id="about" className="work-experience-section">
            <h2>Work Experience</h2>
            <div className="experience-list">
              {experiences.map((exp, index) => (
                <div key={index} className="experience-card">
                  <div className="experience-logo">
                    <img src={exp.logo} alt={`${exp.company} logo`} />
                  </div>
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

        {/* Right Column - Demo Content */}
        <div className="about-right-column">
          <section className="demo-section">
            {/* Demo Header */}
            <div className="demo-section-header">
              <Link to="/ai-tools" className="back-link-inline">
                ← Back to AI Tools
              </Link>
              <h2>Cold Email Deep Personalization</h2>
              <p className="demo-section-subtitle">
                Watch AI Analyze Websites and Generate Hyper-Personalized Icebreakers
              </p>
            </div>

            {/* Upload Stage */}
            {stage === 'upload' && (
              <div className="demo-upload-stage">
                {/* How It Works */}
                <div className="demo-how-it-works">
                  <h3>How It Works</h3>
                  <div className="demo-steps-grid">
                    <div className="demo-step">
                      <span className="demo-step-num">1</span>
                      <span className="demo-step-icon">📋</span>
                      <span className="demo-step-text">Upload Leads</span>
                    </div>
                    <div className="demo-step">
                      <span className="demo-step-num">2</span>
                      <span className="demo-step-icon">🔍</span>
                      <span className="demo-step-text">AI Scrapes</span>
                    </div>
                    <div className="demo-step">
                      <span className="demo-step-num">3</span>
                      <span className="demo-step-icon">🧠</span>
                      <span className="demo-step-text">Analyze</span>
                    </div>
                    <div className="demo-step">
                      <span className="demo-step-num">4</span>
                      <span className="demo-step-icon">✨</span>
                      <span className="demo-step-text">Icebreaker</span>
                    </div>
                  </div>
                </div>

                {/* Demo Button First */}
                <button className="demo-try-button" onClick={useSampleData}>
                  <span>🚀</span>
                  Try Demo with 10 Real Leads
                </button>

                <div className="demo-or-divider">
                  <span>OR</span>
                </div>

                {/* Upload Zone */}
                <div 
                  className={`demo-upload-zone ${dragActive ? 'drag-active' : ''}`}
                  onDragEnter={handleDrag}
                  onDragLeave={handleDrag}
                  onDragOver={handleDrag}
                  onDrop={handleDrop}
                  onClick={() => fileInputRef.current?.click()}
                >
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".csv"
                    onChange={handleFileInput}
                    style={{ display: 'none' }}
                  />
                  <div className="demo-upload-icon">📁</div>
                  <h4>Upload Your CSV</h4>
                  <p>Drag & drop or click to browse</p>
                  <span className="demo-upload-hint">Requires: name, email, website</span>
                </div>
              </div>
            )}

            {/* Processing Stage */}
            {stage === 'processing' && (
              <div className="demo-processing-stage">
                <div className="demo-progress-header">
                  <h3>Generating Icebreakers</h3>
                  <div className="demo-progress-bar">
                    <div 
                      className="demo-progress-fill" 
                      style={{ width: `${((currentProcessingIndex + 1) / leads.length) * 100}%` }}
                    />
                  </div>
                  <p className="demo-progress-text">
                    Processing {currentProcessingIndex + 1} of {leads.length}...
                  </p>
                </div>

                {currentProcessingIndex >= 0 && leads[currentProcessingIndex] && (
                  <div className="demo-current-lead">
                    <div className="demo-input-card">
                      <h4>📊 Input</h4>
                      <div className="demo-input-row">
                        <span className="demo-input-label">Name:</span>
                        <span>{leads[currentProcessingIndex].first_name} {leads[currentProcessingIndex].last_name}</span>
                      </div>
                      <div className="demo-input-row">
                        <span className="demo-input-label">Email:</span>
                        <span>{leads[currentProcessingIndex].email}</span>
                      </div>
                      <div className="demo-input-row">
                        <span className="demo-input-label">Website:</span>
                        <span className="demo-highlight">{leads[currentProcessingIndex].website_url}</span>
                      </div>
                    </div>

                    <div className="demo-processing-indicator">
                      <span className="demo-ai-badge">🤖 AI Processing</span>
                      <div className="demo-dots">
                        <span></span><span></span><span></span>
                      </div>
                    </div>

                    <div className="demo-output-card">
                      <h4>✨ Output</h4>
                      <div className="demo-skeleton">
                        <div className="demo-skeleton-line"></div>
                        <div className="demo-skeleton-line"></div>
                        <div className="demo-skeleton-line short"></div>
                      </div>
                    </div>
                  </div>
                )}

                <div className="demo-completed-list">
                  <h4>Completed ({processedLeads.length}/{leads.length})</h4>
                  <div className="demo-completed-items">
                    {processedLeads.map((lead, index) => (
                      <span key={index} className="demo-completed-item">
                        ✓ {lead.first_name}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Results Stage */}
            {stage === 'results' && (
              <div className="demo-results-stage">
                <div className="demo-results-header">
                  <h3>🎉 All Icebreakers Generated!</h3>
                  <button className="demo-reset-button" onClick={resetDemo}>
                    ← Run Again
                  </button>
                </div>

                <div className="demo-results-list">
                  {processedLeads.map((lead, index) => (
                    <div key={index} className="demo-result-card">
                      <div className="demo-result-header">
                        <div className="demo-result-avatar">
                          {lead.first_name.charAt(0)}{lead.last_name.charAt(0)}
                        </div>
                        <div className="demo-result-info">
                          <h4>{lead.first_name} {lead.last_name}</h4>
                          <span>{lead.headline || 'Professional'}</span>
                        </div>
                        <span className="demo-result-num">#{index + 1}</span>
                      </div>

                      <div className="demo-result-inputs">
                        <div className="demo-result-input">
                          <span className="demo-result-label">Email</span>
                          <span className="demo-result-value">{lead.email}</span>
                        </div>
                        <div className="demo-result-input">
                          <span className="demo-result-label">Website</span>
                          <span className="demo-result-value">{lead.website_url}</span>
                        </div>
                      </div>

                      <div className="demo-result-arrow">↓ AI Personalization</div>

                      <div className="demo-result-icebreaker">
                        <div className="demo-icebreaker-label">✨ Generated Icebreaker</div>
                        <p>{lead.icebreaker}</p>
                      </div>
                    </div>
                  ))}
                </div>

                <div className="demo-cta">
                  <h4>Ready to personalize at scale?</h4>
                  <a href="mailto:ishaangup@g.ucla.edu" className="demo-cta-button">
                    Get Access →
                  </a>
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
