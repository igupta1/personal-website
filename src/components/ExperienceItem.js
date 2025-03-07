import React from "react";

function ExperienceItem({ title, company, period, description, logo }) {
  return (
    <div className="experience-item">
      <div className="timeline-marker">
        <div className="timeline-dot"></div>
        <div className="timeline-line"></div>
      </div>
      
      {/* Logo now comes before content */}
      {logo && (
        <div className="company-logo">
          <img src={logo} alt={`${company} logo`} />
        </div>
      )}
      
      <div className="experience-content">
        <div className="experience-header">
          <h3>{title}</h3>
          <h4>{company}</h4>
          <p className="experience-period">{period}</p>
        </div>
        <p className="experience-description">{description}</p>
      </div>
    </div>
  );
}

export default ExperienceItem;
