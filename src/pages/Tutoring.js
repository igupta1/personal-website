import React, { useRef } from "react";
import "./Tutoring.css";
import profilePicture from '../assets/pfp.png'; // Add your profile picture
import uclaLogo from "../assets/ucla-logo.jpg"; // Add UCLA logo
import googleLogo from '../assets/googlelogo.jpg';
import amazonLogo from '../assets/amazonlogo.webp';
import ciscoLogo from '../assets/ciscologo.png';

function Tutoring() {
  // Create refs for each section
  const aboutRef = useRef(null);
  const servicesRef = useRef(null);
  const contactRef = useRef(null);

  // Function to scroll to a section
  const scrollToSection = (ref) => {
    ref.current.scrollIntoView({ behavior: "smooth" });
  };

  return (
    <div className="tutoring-page">
      {/* Navigation Banner */}
      <div className="tutoring-nav">
        <div className="nav-container">
          <div className="nav-links">
            <button onClick={() => scrollToSection(aboutRef)}>About</button>
            <button onClick={() => scrollToSection(servicesRef)}>Services</button>
            <button onClick={() => scrollToSection(contactRef)}>Contact</button>
          </div>
        </div>
      </div>

      {/* About Section - Removed yellow background */}
      <section ref={aboutRef} className="about-section">
        <div className="about-container">
          <div className="about-content">
            <div className="about-text">
              <h1>Online Tutor</h1>
              <h2>Ishaan Gupta</h2>
              <p>
                Hey I'm Ishaan! I've built software for tech companies 
                and startups - notably Google, Amazon, and Cisco. I'm graduating 
                next year with a UCLA Computer Science degree.
              </p>
              <p>
                Outside of work I love to tutor and teach Computer Science, Math, 
                and General Coding!
              </p>
            </div>
            <div className="about-images">
              <div className="profile-image">
                <img src={profilePicture} alt="Ishaan Gupta" />
              </div>
              <div className="logo-container">
                <div className="university-logo">
                  <img src={uclaLogo} alt="UCLA" />
                </div>
                <div className="company-logos">
                  <img src={googleLogo} alt="Google" />
                  <img src={amazonLogo} alt="Amazon" />
                  <img src={ciscoLogo} alt="Cisco" />
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Services Section */}
      <section ref={servicesRef} className="services-section">
        <div className="section-container">
          <h2>Services</h2>
          
          {/* What We Offer Section */}
          <div className="what-we-offer">
            <h3>What We Offer</h3>
            <div className="offer-items">
              <div className="offer-item">
                <div className="offer-image">
                  {/* Replace with your actual image */}
                  <img src={require("../assets/knowledgeable-teaching.png")} alt="Knowledgeable Teaching" />
                </div>
                <h4>Knowledgeable Teaching</h4>
                <p>
                  {/* Add your description here */}
                  I've worked at top tech companies and am currently a UCLA CS student.
                </p>
              </div>
              
              <div className="offer-item">
                <div className="offer-image">
                  {/* Replace with your actual image */}
                  <img src={require("../assets/improved-results.png")} alt="Improved Results" />
                </div>
                <h4>Improved Results</h4>
                <p>
                  {/* Add your description here */}
                  Personalized tutoring tailored to your needs and academic goals.
                </p>
              </div>
              
              <div className="offer-item">
                <div className="offer-image">
                  {/* Replace with your actual image */}
                  <img src={require("../assets/flexible-hours.png")} alt="Flexible Hours" />
                </div>
                <h4>Flexible Sessions</h4>
                <p>
                  {/* Add your description here */}
                  Hours that are flexible and scheduled per your need.
                </p>
              </div>
            </div>
          </div>
          
        {/* Subjects Section */}
        <div className="subjects-section">
        <h3>Subjects</h3>
        <div className="subjects-container">
            <div className="subject-item computer-science">
            <h3>Computer Science</h3>
            <p>
                Fundamental and advanced Computer Science concepts with a focus on applications.
            </p>
            <ul className="subject-topics">
                <li>Data Structures & Algorithms</li>
                <li>Object-Oriented Programming</li>
                <li>Web Development</li>
                <li>Database Systems</li>
                <li>Operating Systems</li>
            </ul>
            </div>
            
            <div className="subject-item general-coding">
            <h3>General Coding</h3>
            <p>
                Learn to code from scratch or improve your existing skills with personalized guidance.
            </p>
            <ul className="subject-topics">
                <li>Python Programming</li>
                <li>JavaScript & Web Technologies</li>
                <li>Java Development</li>
                <li>C/C++ Programming</li>
                <li>Version Control with Git</li>
            </ul>
            </div>
            
            <div className="subject-item math">
            <h3>Math</h3>
            <p>
                Build a strong foundation in mathematics with clear explanations and problem-solving techniques.
            </p>
            <ul className="subject-topics">
                <li>Algebra</li>
                <li>Geometry</li>
                <li>Pre-Calculus</li>
                <li>AP Calculus</li>
                <li>AP Statistics</li>
            </ul>
            </div>
        </div>
        </div>

        </div>
      </section>

      {/* Contact Section */}
      <section ref={contactRef} className="contact-section">
        <div className="section-container">
          <h2>Contact</h2>
          <div className="contact-info">
            <div className="contact-item">
              <h3>Phone</h3>
              <p>(408) 892-7641</p>
            </div>
            <div className="contact-item">
              <h3>Email</h3>
              <p>
                <a href="mailto:ishaangpta@g.ucla.edu">ishaangpta@g.ucla.edu</a>
              </p>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export default Tutoring;
