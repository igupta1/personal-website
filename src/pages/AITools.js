//AITools.js
import React from "react";
import { useNavigate } from "react-router-dom";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

// Tool logos
import linkmailLogo from '../assets/linkmail-logo.png';

// Icons
import { HiOutlineUserGroup } from "react-icons/hi";
import { HiOutlineMail } from "react-icons/hi";
import { HiOutlineSparkles } from "react-icons/hi";

function AITools() {
  const navigate = useNavigate();

  const experiences = [
    {
      title: "Software Engineer",
      company: "Google",
      description: "Built Generative AI Features for Gmail and Google Chat",
      logo: googlelogo,
    },
    {
      title: "Software Engineer",
      company: "Amazon",
      description: "Developed AI Infrastructure for Amazon.com",
      logo: amazonlogo,
    },
    {
      title: "Software Engineer",
      company: "Cisco",
      description: "Implemented Distributed System Architecture for Networking Solutions",
      logo: ciscologo,
    },
  ];

  const aiTools = [
    {
      id: 1,
      title: "Marketing Agency Lead Generation",
      description: "Find Marketing Agencies and Their Contact Information Instantly",
      icon: <HiOutlineUserGroup size={34} color="#f5f5f5" />,
      isReactIcon: true,
      link: "/ai-tools/lead-gen",
    },
    {
      id: 2,
      title: "Cold Email Deep Personalization",
      description: "Mass Email Leads While Using AI to Personalize Each Email",
      icon: <HiOutlineMail size={34} color="#f5f5f5" />,
      isReactIcon: true,
      link: "/ai-tools/cold-email-deep-personalization",
    },
    {
      id: 3,
      title: "LinkedIn Cold Message Personalization Tool",
      description: "Automatically Find Emails and Send a Personalized Message Instantly.",
      icon: linkmailLogo,
      isImage: true,
      link: "https://www.linkmail.dev/",
      external: true,
    },
    {
      id: 4,
      title: "More Coming Soon...",
      description: "Stay tuned for more AI-powered tools",
      icon: <HiOutlineSparkles size={34} color="#f5f5f5" />,
      isReactIcon: true,
    },
  ];

  const handleToolClick = (link, external) => {
    if (link) {
      if (external) {
        window.open(link, '_blank', 'noopener,noreferrer');
      } else {
        navigate(link);
      }
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
                <p>Software Engineer - Problem Solver - Builder
                <br /> <br />
                I focus on turning complex problems into clear, practical solutions — from large-scale systems at Gmail and Amazon.com to tools for everyday users.</p>
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
            <h2>AI Tools For Businesses</h2>
            <div className="projects-list">
              {aiTools.map((tool) => (
                <div
                  key={tool.id}
                  className={`project-card ${tool.link ? 'clickable' : 'non-clickable'} ${tool.id === 3 ? 'linkedin-tool' : ''}`}
                  onClick={() => handleToolClick(tool.link, tool.external)}
                  style={{ cursor: tool.link ? 'pointer' : 'default' }}
                >
                  <div className="project-icon">
                    {tool.isImage ? (
                      <img src={tool.icon} alt={tool.title} className="project-icon-img" />
                    ) : (
                      tool.icon
                    )}
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

