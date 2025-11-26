//About.js
import React from "react";
import pfpImage from '../assets/headshot.jpg';

// Company logos
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';

// Project logos
import linkmailLogo from '../assets/linkmail-logo.png';
import cscoLogo from '../assets/csco-logo.png';
import jellypodLogo from '../assets/jellypod-logo.png';

function About() {

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
      description: "Promise Evaluation Team",
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

  const projects = [
    {
      id: 1,
      title: "Linkmail",
      description: "Bridging LinkedIn and Gmail with AI Email Generation",
      icon: linkmailLogo,
      isImage: true,
      link: "https://github.com/Jaysontian/linkmail",
    },
    {
      id: 2,
      title: "CSCO",
      description: "Your Personal Media Board Meets AI",
      icon: cscoLogo,
      isImage: true,
      link: "https://github.com/cs35l-group/csco",
    },
    {
      id: 3,
      title: "UCLA Swipes",
      description: "Helps UCLA Kids Stay On Track With Their Meal Plan",
      icon: "🍔",
      link: "https://github.com/nitinsubz/ucla-swipes",
    },
    {
      id: 4,
      title: "Jellypod x UCLA",
      description: "UCLA Hub for Jellypod Podcasts For Classes",
      icon: jellypodLogo,
      isImage: true,
      link: "https://github.com/3eif/jelly",
    },
    {
      id: 5,
      title: "Visual Advertisement Detection",
      description: "Web Crawler that Locates Visual Ads and Reads Their Contents",
      icon: "🔍",
      link: "https://github.com/igupta1/VisualAdvertisementDetection",
    },
    {
      id: 6,
      title: "Drowsiness Detection",
      description: "Real-time detection for Alerting Drowsy Drivers",
      icon: "👁️",
      link: "https://github.com/igupta1/DrowsinessDetection",
    },
    {
      id: 7,
      title: "Sensor Fusion",
      description: "Combined Computer Vision, Ultrasonic, Infrared, and Light Sensors",
      icon: "🔬",
      link: "https://github.com/igupta1/SensorFusion",
    },
  ];

  const handleProjectClick = (link) => {
    window.open(link, "_blank", "noopener,noreferrer");
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
                <p>Software engineer. Problem-solver. Builder.
                <br /> <br />
                I love both ends of the spectrum — from contributing to large-scale systems at Gmail and Amazon.com, to building products that help people in their everyday workflows. I love turning complex problems into clean, intuitive tools.</p>
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

        {/* Right Column - Projects */}
        <div className="about-right-column">
          <section id="projects" className="projects-section">
            <h2>Projects</h2>
            <div className="projects-list">
              {projects.map((project) => (
                <div
                  key={project.id}
                  className="project-card"
                  onClick={() => handleProjectClick(project.link)}
                >
                  <div className="project-icon">
                    {project.isImage ? (
                      <img src={project.icon} alt={project.title} className="project-icon-img" />
                    ) : (
                      project.icon
                    )}
                  </div>
                  <div className="project-content">
                    <h3>{project.title}</h3>
                    <p>{project.description}</p>
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

export default About;
