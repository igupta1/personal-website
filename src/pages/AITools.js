//AITools.js
import React from "react";
import { useNavigate } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

function AITools() {
  const navigate = useNavigate();

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

  const aiTools = [
    {
      id: 1,
      title: "Cold Email Deep Personalization",
      description: "Mass Email Leads While Using AI to Personalize Each Email",
      icon: "✉️",
      link: "/ai-tools/cold-email-deep-personalization",
    },
    {
      id: 2,
      title: "LinkedIn Cold Message Personalization Tool",
      description: "Automatically Find Emails and Send a Personalized Message Instantly.",
      icon: "💼",
    },
    {
      id: 3,
      title: "Marketing Agency Lead Generation",
      description: "Find Marketing Agencies and Their Contact Information Instantly",
      icon: "🎯",
      link: "/ai-tools/lead-gen",
    },
    {
      id: 4,
      title: "More Coming Soon...",
      description: "Stay tuned for more AI-powered tools",
      icon: "🚀",
    },
  ];

  const handleToolClick = (link) => {
    if (link) {
      navigate(link);
    }
  };

  return (
    <div className="about-page-combined">
      <div className="about-container">
        {/* Left Column */}
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

        {/* Right Column - AI Tools */}
        <div className="about-right-column">
          <section id="ai-tools" className="projects-section">
            <h2>AI Tools</h2>
            <div className="projects-list">
              {aiTools.map((tool) => (
                <div
                  key={tool.id}
                  className={`project-card ${tool.link ? 'clickable' : 'non-clickable'}`}
                  onClick={() => handleToolClick(tool.link)}
                  style={{ cursor: tool.link ? 'pointer' : 'default' }}
                >
                  <div className="project-icon">
                    {tool.icon}
                  </div>
                  <div className="project-content">
                    <h3>{tool.title}</h3>
                    <p>{tool.description}</p>
                  </div>
                  {tool.link && <span className="try-it-badge">Try It →</span>}
                </div>
              ))}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

export default AITools;

