import React, { useState } from "react";
import "./Portfolio.css"; // Make sure this points to your CSS file

// Import your project images
import linkmailss from "../assets/linkmailss.png";
import jellyss from "../assets/jellyss.png";
import uclaswipesss from "../assets/uclaswipesss.png";
import vadss from "../assets/vadss.png";
import ddss from "../assets/ddss.png";
import sfss from "../assets/sfss.png";
import cscoImage from "../assets/csco.png"; // Make sure to add this image to your assets folder
// Import other project images as needed

function Portfolio() {
  const projects = [
    {
      id: 1,
      title: "Linkmail",
      caption: "Coming soon: Bridging LinkedIn and Gmail with AI Email Generation",
      image: linkmailss,
      link: "https://github.com/Jaysontian/linkmail",
    },
    {
      id: 2,
      title: "CSCO",
      caption: "Your Personal Media Board Meets AI",
      image: cscoImage, // Add image path when available
      link: "https://github.com/cs35l-group/csco", // Update with actual link
    },
    {
      id: 3,
      title: "UCLA Swipes",
      caption: "Helps UCLA Kids Stay On Track With Their Meal Plan",
      image: uclaswipesss, // Add image path when available
      link: "https://github.com/nitinsubz/ucla-swipes", // Update with actual link
    },
    {
      id: 4,
      title: "Jellypod x UCLA",
      caption: "UCLA Hub for Jellypod Podcasts For Classes",
      image: jellyss, // Add image path when available
      link: "https://github.com/3eif/jelly", // Update with actual link
    },
    {
      id: 5,
      title: "Visual Advertisement Detection",
      caption: "Web Crawler that Locates Visual Ads and Reads Their Contents",
      image: vadss, // Add image path when available
      link: "https://github.com/igupta1/VisualAdvertisementDetection", // Update with actual link
    },
    {
      id: 6,
      title: "Drowsiness Detection",
      caption: "Real-time detection for Alerting Drowsy Drivers",
      image: ddss, // Add image path when available
      link: "https://github.com/igupta1/DrowsinessDetection", // Update with actual link
    },
    {
      id: 7,
      title: "Sensor Fusion",
      caption: "Combined Computer Vision, Ultrasonic, Infrared, and Light Sensors for Sensor Fusion",
      image: sfss, // Add image path when available
      link: "https://github.com/igupta1/SensorFusion", // Update with actual link
    },
  ];

  const [hoveredProject, setHoveredProject] = useState(null);

  const handleProjectClick = (link) => {
    window.open(link, "_blank", "noopener,noreferrer");
  };

  return (
    <div className="portfolio-page">
      <h1>Portfolio</h1>
      <div className="projects-grid">
        {projects.map((project) => (
          <div
            key={project.id}
            className={`project-card ${hoveredProject === project.id ? "hovered" : ""}`}
            onMouseEnter={() => setHoveredProject(project.id)}
            onMouseLeave={() => setHoveredProject(null)}
            onClick={() => handleProjectClick(project.link)}
          >
            <div className="project-image-container">
              <div className="project-image">
                {project.image ? (
                  <img src={project.image} alt={project.title} />
                ) : (
                  <div className="placeholder-image">{project.title[0]}</div>
                )}
                <div className="project-overlay">
                  <p>{project.caption}</p>
                </div>
              </div>
            </div>
            <div className="project-info">
              <h3>{project.title}</h3>
              <p>{project.caption}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default Portfolio;
