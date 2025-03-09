//About.js
import React from "react";
import ExperienceItem from "../components/ExperienceItem";

// You can import company logos here
// import vestLogo from '../assets/vest-logo.png';
// import lookbkLogo from '../assets/lookbk-logo.png';
// etc.
import googlelogo from '../assets/googlelogo.jpg';
import amazonlogo from '../assets/amazonlogo.webp';
import ciscologo from '../assets/ciscologo.png';
import wonderlogo from '../assets/wonderlogo.png';
import skalablelogo from '../assets/skalablelogo.png';

function About() {
  const experiences = [
    {
      title: "Incoming Software Engineer Intern",
      company: "Google",
      period: "Summer 2025",
      description: "",
      logo: googlelogo, // Add logo path when available
    },
    {
      title: "Software Engineer Intern",
      company: "Amazon",
      period: "Summer 2024",
      description: "Promise Evaluation Team",
      logo: amazonlogo, // Add logo path when available
    },
    {
      title: "Software Engineer Intern",
      company: "Cisco",
      period: "Spring 2024",
      description: "Distributed Systems Engineering Team",
      logo: ciscologo, // Add logo path when available
    },
    {
      title: "Software Engineer Intern",
      company: "Wonder Dynamics",
      period: "Summer 2023",
      description:
        "Computer Vision for CGI & VFX Automation",
      logo: wonderlogo, // Add logo path when available
    },
    {
      title: "Software Engineer Intern",
      company: "Skalable Technologies",
      period: "Summer 2022",
      description:
        "Automated Invoice Processing for Workplace Efficiency",
      logo: skalablelogo, // Add logo path when available
    },
  ];

  return (
    <div className="about-page">
      <section className="about-intro">
        <h1>Hey I'm Ishaan!</h1>
        <p>
          A software engineer, a builder, and an escape room fanatic. I'm
          currently a junior at UCLA studying Computer Science. Feel free to
          reach out at{" "}
          <a href="mailto:ishaangpta@g.ucla.edu">ishaangpta@g.ucla.edu</a>!
        </p>
      </section>

      <section className="experience-section">
        <h2>Work Experience</h2>
        <div className="experience-timeline">
          {experiences.map((exp, index) => (
            <ExperienceItem
              key={index}
              title={exp.title}
              company={exp.company}
              period={exp.period}
              description={exp.description}
              logo={exp.logo}
            />
          ))}
        </div>
      </section>
    </div>
  );
}

export default About;
