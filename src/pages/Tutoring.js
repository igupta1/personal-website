import React, { useState, useRef } from "react";
import "./Tutoring.css";
import profilePicture from '../assets/pfp.png';
import uclaLogo from "../assets/ucla-logo.jpg";
import googleLogo from '../assets/googlelogo.jpg';
import amazonLogo from '../assets/amazonlogo.webp';
import ciscoLogo from '../assets/ciscologo.png';

// Import icons for services
import knowledgeIcon from '../assets/knowledgeable-teaching.png';
import resultsIcon from '../assets/improved-results.png';
import flexibleIcon from '../assets/flexible-hours.png';

function Tutoring() {
  // State for active subject tab
  const [activeSubject, setActiveSubject] = useState('apCSA');
  
  // Create refs for each section
  const aboutRef = useRef(null);
  const servicesRef = useRef(null);
  const subjectsRef = useRef(null);
  const testimonialsRef = useRef(null);
  const contactRef = useRef(null);

  // Function to scroll to a section
  const scrollToSection = (ref) => {
    ref.current.scrollIntoView({ behavior: "smooth" });
  };

  // Subject content based on active tab
  const subjectContent = {
    apCSA: {
      title: "AP Computer Science A",
      description: "Prepare for success in the AP Computer Science A exam with comprehensive coverage of all exam topics. This course focuses on Java programming fundamentals, problem-solving strategies, and test-taking techniques to help you earn a top score.",
      topics: [
        "Java Programming Basics",
        "Object-Oriented Programming",
        "Arrays and ArrayList",
        "Inheritance and Polymorphism",
        "Recursion and Sorting Algorithms",
        "AP Exam Practice Questions",
        "Free Response Question Strategies"
      ]
    },
    python: {
      title: "Python Programming",
      description: "From beginner to advanced levels, learn Python programming with personalized instruction. Whether you're just starting out or looking to master advanced concepts, I'll guide you through practical projects and real-world applications.",
      topics: [
        "Python Fundamentals (Introductory)",
        "Data Structures in Python",
        "Object-Oriented Python",
        "File I/O and Exception Handling",
        "Advanced Python Features (Decorators, Generators)",
        "Python Libraries (NumPy, Pandas, Matplotlib)",
        "Web Scraping and Automation"
      ]
    },
    java: {
      title: "Java Development",
      description: "Master Java programming at both introductory and advanced levels. Learn core concepts, object-oriented design principles, and advanced topics with hands-on projects tailored to your skill level.",
      topics: [
        "Java Fundamentals (Introductory)",
        "Object-Oriented Programming",
        "Java Collections Framework",
        "Multithreading and Concurrency",
        "Advanced Java Topics (Reflection, Annotations)",
        "Design Patterns",
        "Enterprise Java Development"
      ]
    },
    machineLearning: {
      title: "Machine Learning",
      description: "Demystify the world of AI and machine learning with practical, hands-on instruction. Learn to build and train models, analyze data, and implement machine learning solutions for real-world problems.",
      topics: [
        "Machine Learning Fundamentals",
        "Supervised Learning Algorithms",
        "Unsupervised Learning Techniques",
        "Neural Networks and Deep Learning",
        "Natural Language Processing",
        "Computer Vision",
        "Practical ML Project Development"
      ]
    },
    webTech: {
      title: "JavaScript & Web Technologies",
      description: "Build dynamic, interactive websites and web applications with comprehensive instruction in modern web development. From front-end frameworks to back-end systems, learn the full stack of technologies needed in today's web ecosystem.",
      topics: [
        "HTML and CSS Fundamentals",
        "JavaScript Essentials",
        "Front-end Frameworks",
        "Back-end Development",
        "RESTful API Design",
        "Database Integration",
        "Responsive and Mobile-First Design"
      ]
    }
  };

  return (
    <div className="tutoring-page">
      {/* Hero Section */}
      <section className="hero-section">
        <div className="hero-container">
          <div className="hero-content">
            <h1>Expert Tutoring for Computer Science & Programming</h1>
            <h2>Personalized learning with a UCLA CS student and Google/Amazon engineer</h2>
          </div>
        </div>
      </section>

      {/* About Section */}
      <section ref={aboutRef} className="about-section">
        <div className="about-container">
          <div className="about-image">
            <div className="profile-card">
              <img src={profilePicture} alt="Ishaan Gupta" className="profile-image" />
              <div className="profile-details">
                <h3 className="profile-name">Ishaan Gupta</h3>
                <p className="profile-title">Tutor</p>
                <div className="credentials">
                  <img src={uclaLogo} alt="UCLA" className="credential-logo" />
                  <img src={googleLogo} alt="Google" className="credential-logo" />
                  <img src={amazonLogo} alt="Amazon" className="credential-logo" />
                  <img src={ciscoLogo} alt="Cisco" className="credential-logo" />
                </div>
              </div>
            </div>
          </div>
          
          <div className="about-content">
            <h2 className="section-title">About Me</h2>
            <p>
              I'm a <span className="highlight">Computer Science student at UCLA</span> with experience working at top tech companies including <span className="highlight">Google, Amazon, and Cisco</span>. My passion for teaching stems from my belief that complex concepts can be made accessible to everyone with the right approach.
            </p>
            <p>
              With over 3 years of tutoring experience, I've helped students of all levels achieve their goals, from mastering basic programming concepts to developing full-scale applications.
            </p>
            <a href="#contact" className="cta-button" onClick={(e) => {
              e.preventDefault();
              scrollToSection(contactRef);
            }}>Get Started Today</a>
          </div>
        </div>
      </section>

      {/* Services Section */}
      <section ref={servicesRef} className="services-section">
        <div className="services-container">
          <div className="services-header">
            <h2>My Tutoring Services</h2>
            <p>Comprehensive support tailored to your learning needs</p>
          </div>
          
          <div className="services-grid">
            <div className="service-card">
              <div className="service-icon">
                <img src={knowledgeIcon} alt="Knowledgeable Teaching" />
              </div>
              <div className="service-content">
                <h3>Expert Instruction</h3>
                <p>
                  Learn from someone who has worked at top tech companies and is pursuing a Computer Science degree at UCLA. Get industry-relevant knowledge and academic excellence.
                </p>
              </div>
            </div>
            
            <div className="service-card">
              <div className="service-icon">
                <img src={resultsIcon} alt="Improved Results" />
              </div>
              <div className="service-content">
                <h3>Personalized Learning</h3>
                <p>
                  Customized lesson plans designed around your specific goals, learning style, and pace. Track your progress with regular assessments and feedback.
                </p>
              </div>
            </div>
            
            <div className="service-card">
              <div className="service-icon">
                <img src={flexibleIcon} alt="Flexible Hours" />
              </div>
              <div className="service-content">
                <h3>Flexible Sessions</h3>
                <p>
                  Online tutoring with flexible scheduling to fit your busy life. Choose between one-time help, regular weekly sessions, or intensive exam preparation.
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Subjects Section */}
      <section ref={subjectsRef} className="subjects-section">
        <div className="subjects-container">
          <div className="subjects-header">
            <h2>What I Teach</h2>
            <p>Some Example Courses I've Taught Previously</p>
          </div>
          
          <div className="subjects-tabs">
            <button 
              className={`subject-tab ${activeSubject === 'python' ? 'active' : ''}`}
              onClick={() => setActiveSubject('python')}
            >
              Python
            </button>
            <button 
              className={`subject-tab ${activeSubject === 'java' ? 'active' : ''}`}
              onClick={() => setActiveSubject('java')}
            >
              Java
            </button>
            <button 
              className={`subject-tab ${activeSubject === 'apCSA' ? 'active' : ''}`}
              onClick={() => setActiveSubject('apCSA')}
            >
              AP Computer Science A
            </button>
            <button 
              className={`subject-tab ${activeSubject === 'machineLearning' ? 'active' : ''}`}
              onClick={() => setActiveSubject('machineLearning')}
            >
              Machine Learning
            </button>
            <button 
              className={`subject-tab ${activeSubject === 'webTech' ? 'active' : ''}`}
              onClick={() => setActiveSubject('webTech')}
            >
              JavaScript & Web Technologies
            </button>
          </div>
          
          <div className="subject-content">
            <div className="subject-details">
              <div className="subject-info">
                <h3>{subjectContent[activeSubject].title}</h3>
                <p>{subjectContent[activeSubject].description}</p>
                <a href="#contact" className="cta-button" onClick={(e) => {
                  e.preventDefault();
                  scrollToSection(contactRef);
                }}>Inquire About {subjectContent[activeSubject].title}</a>
              </div>
              
              <div className="subject-topics">
                <h4>Topics Covered:</h4>
                <ul className="topics-list">
                  {subjectContent[activeSubject].topics.map((topic, index) => (
                    <li key={index}>{topic}</li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Testimonials Section */}
      <section ref={testimonialsRef} className="testimonials-section">
        <div className="testimonials-container">
          <div className="testimonials-header">
            <h2>Student Success Stories</h2>
            <p>Hear what my students have to say</p>
          </div>
          
          <div className="testimonials-grid">
            <div className="testimonial-card">
              <div className="testimonial-content">
                <p>
                  "Ishaan helped me understand data structures and algorithms in a way my professors couldn't. His real-world examples and patient teaching style made complex concepts click for me. I went from struggling to acing my CS classes!"
                </p>
              </div>
              <div className="testimonial-author">
                <div className="author-info">
                  <h4>Samay K.</h4>
                  <p>Computer Science Student</p>
                </div>
              </div>
            </div>
            
            <div className="testimonial-card">
              <div className="testimonial-content">
                <p>
                  "I started with zero coding knowledge, and after just a few months of working with Ishaan, I was able to build my own web applications. His step-by-step approach and homework assignments really helped me progress quickly."
                </p>
              </div>
              <div className="testimonial-author">
                <div className="author-info">
                  <h4>Manya R.</h4>
                  <p>Web Development Student</p>
                </div>
              </div>
            </div>
            
            <div className="testimonial-card">
              <div className="testimonial-content">
                <p>
                  "Ishaan's tutoring was instrumental in helping me prepare for my AP Computer Science Exam. He has a gift for explaining concepts clearly and making them interesting. I scored a 5 thanks to his help!"
                </p>
              </div>
              <div className="testimonial-author">
                <div className="author-info">
                  <h4>Jason L.</h4>
                  <p>High School Student</p>
                </div>
              </div>
            </div>
            
            <div className="testimonial-card">
              <div className="testimonial-content">
                <p>
                  "Working with Ishaan on my programming projects was a game-changer. He not only helped me debug my code but also taught me best practices and efficient approaches that I continue to use in my work today."
                </p>
              </div>
              <div className="testimonial-author">
                <div className="author-info">
                  <h4>Sophia L.</h4>
                  <p>Computer Science Student</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Contact Section - Centered with removed form and availability */}
      <section ref={contactRef} className="contact-section">
        <div className="contact-container-centered">
          <div className="contact-info-centered">
            <h2>Get in Touch</h2>
            <p>
              Ready to boost your skills in Computer Science or Programming? Contact me today.
            </p>
            
            <div className="contact-methods">
              <div className="contact-method">
                <div className="contact-icon">
                  <i className="fas fa-phone"></i>
                </div>
                <div className="contact-text">
                  <h4>Phone</h4>
                  <p>(408) 892-7641</p>
                </div>
              </div>
              
              <div className="contact-method">
                <div className="contact-icon">
                  <i className="fas fa-envelope"></i>
                </div>
                <div className="contact-text">
                  <h4>Email</h4>
                  <a href="mailto:ishaangpta@g.ucla.edu">ishaangpta@g.ucla.edu</a>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export default Tutoring;