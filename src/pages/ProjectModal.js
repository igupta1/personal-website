import React from 'react';
import './ProjectModal.css';

const ProjectModal = ({ project, onClose }) => {
  if (!project) return null;
  
  // Function to determine if the project has one or two images
  const getImagesCount = () => {
    // Count available images (not counting mainImage)
    let count = 0;
    if (project.sampleImage1) count++;
    if (project.sampleImage2) count++;
    return count;
  };
  
  const imageCount = getImagesCount();
  
  return (
    <div className="project-modal-overlay" onClick={onClose}>
      <div className="project-modal-content" onClick={e => e.stopPropagation()}>
        {/* Header with close button */}
        <div className="project-modal-header">
          <h3>{project.title}</h3>
          <button onClick={onClose} className="close-button">
            <span>×</span>
          </button>
        </div>
        
        {/* Content */}
        <div className="project-modal-body">
          {/* Project Description */}
          <div className="modal-section">
            <h4>Project Description</h4>
            <p>{project.description}</p>
          </div>
          
          {/* Skills Learned */}
          <div className="modal-section">
            <h4>Skills You'll Learn</h4>
            <div className="skills-grid">
              {project.skills.map((skill, index) => (
                <div className="skill-card" key={index}>
                  <span className="skill-title">{skill.title}</span>
                  <p className="skill-desc">{skill.desc}</p>
                </div>
              ))}
            </div>
          </div>
          
          {/* Project Highlights */}
          <div className="modal-section">
            <h4>Project Highlights</h4>
            <div className="highlights-container">
              <ul>
                {project.highlights.map((highlight, index) => (
                  <li key={index}>{highlight}</li>
                ))}
              </ul>
            </div>
          </div>
          
          {/* Code Snippet */}
          {project.codeSnippet && (
            <div className="modal-section">
              <h4>Code Sample</h4>
              <div className="code-snippet">
                <pre>{project.codeSnippet}</pre>
              </div>
            </div>
          )}
          
          {/* Example Results - Adaptive Layout */}
          <div className="modal-section">
            <h4>Example Results</h4>
            
            {/* Adaptive layout based on number of images */}
            {imageCount === 0 && (
              <div className="single-image-container">
                <img 
                  src={project.mainImage || "https://via.placeholder.com/800x400?text=Example+Result"} 
                  alt={`${project.title} example`}
                  className="full-width-image"
                />
              </div>
            )}
            
            {imageCount === 1 && (
              <div className="dual-image-container">
                <div className="result-image">
                  <img 
                    src={project.sampleImage1} 
                    alt={`${project.title} example`}
                    className="full-width-image"
                  />
                </div>
              </div>
            )}
            
            {imageCount === 2 && (
              <div className="dual-image-container">
                <div className="result-image">
                  <img 
                    src={project.sampleImage1} 
                    alt={`${project.title} example 1`} 
                    className="full-width-image"
                  />
                </div>
                <div className="result-image">
                  <img 
                    src={project.sampleImage2}
                    alt={`${project.title} example 2`} 
                    className="full-width-image"
                  />
                </div>
              </div>
            )}
          </div>
        </div>
        
        {/* Footer with action button */}
        <div className="project-modal-footer">
          <button onClick={onClose} className="action-button">
            Close
          </button>
        </div>
      </div>
    </div>
  );
};

export default ProjectModal;