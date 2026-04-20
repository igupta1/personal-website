//AITools.js
import React, { useEffect, useRef } from "react";
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

const SCHEDULING_STYLESHEET_HREF = "https://calendar.google.com/calendar/scheduling-button-script.css";
const SCHEDULING_SCRIPT_SRC = "https://calendar.google.com/calendar/scheduling-button-script.js";
const SCHEDULING_URL = "https://calendar.google.com/calendar/appointments/schedules/AcZssZ0DBh4IrfhTXa3MeWJiWrKSR0h3nDlddqmRQ75mhjzv_3iJrPcH1irAOFYFcx4Z7Z2pKYkHNTTm?gv=true";

function AITools() {
  const navigate = useNavigate();
  const bookingButtonRef = useRef(null);

  useEffect(() => {
    document.title = "Example Automations — Ishaan Gupta";
  }, []);

  useEffect(() => {
    if (!document.querySelector(`link[href="${SCHEDULING_STYLESHEET_HREF}"]`)) {
      const link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = SCHEDULING_STYLESHEET_HREF;
      document.head.appendChild(link);
    }

    const mountButton = () => {
      if (!bookingButtonRef.current) return;
      if (bookingButtonRef.current.childElementCount > 0) return;
      if (!window.calendar || !window.calendar.schedulingButton) return;
      window.calendar.schedulingButton.load({
        url: SCHEDULING_URL,
        color: "#039BE5",
        label: "Book a call",
        target: bookingButtonRef.current,
      });
    };

    const existingScript = document.querySelector(`script[src="${SCHEDULING_SCRIPT_SRC}"]`);
    if (existingScript) {
      if (window.calendar && window.calendar.schedulingButton) {
        mountButton();
      } else {
        existingScript.addEventListener("load", mountButton, { once: true });
      }
    } else {
      const script = document.createElement("script");
      script.src = SCHEDULING_SCRIPT_SRC;
      script.async = true;
      script.addEventListener("load", mountButton, { once: true });
      document.body.appendChild(script);
    }
  }, []);

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
      title: "Live Example: SMBs Hiring Tech Support",
      description: "Surfaces SMBs hiring for internal tech support, identifies the decision maker, and drafts personalized outreach. One example of what I build for clients — tuned to their specific buying signals.",
      icon: <HiOutlineUserGroup size={34} color="#f5f5f5" />,
      isReactIcon: true,
      link: "/gtm/it-msp",
    },
    {
      id: 2,
      title: "LinkedIn Cold Message Personalization Tool",
      description: "Automatically Find Emails and Send a Personalized Message Instantly.",
      icon: linkmailLogo,
      isImage: true,
      link: "https://www.linkmail.dev/",
      external: true,
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
                <p>I build outbound systems for technical consultancies.
                <br /> <br />
                Software engineer and builder. Previously at Google, Amazon, and Cisco. I work with founder-led technical consultancies — cloud, data, and security — to turn buying signals into qualified meetings.</p>
                <div ref={bookingButtonRef} className="booking-button-container"></div>
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
            <h2>Example Automations</h2>
            <div className="projects-list">
              {aiTools.map((tool) => (
                <div
                  key={tool.id}
                  className={`project-card ${tool.link ? 'clickable' : 'non-clickable'} ${tool.id === 2 ? 'linkedin-tool' : ''}`}
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
