import React, { useState, useRef } from "react";
import "./Tutoring.css";
import ProjectModal from "./ProjectModal"; // Import the modal component
import profilePicture from '../assets/pfp.png';
import uclaLogo from "../assets/ucla-logo.jpg";
import googleLogo from '../assets/googlelogo.jpg';
import amazonLogo from '../assets/amazonlogo.webp';
import ciscoLogo from '../assets/ciscologo.png';

// Import icons for services
import knowledgeIcon from '../assets/knowledgeable-teaching.png';
import resultsIcon from '../assets/improved-results.png';
import flexibleIcon from '../assets/flexible-hours.png';

// Import project images
import comicStrip1 from '../assets/projects/comic-strip-1.png';
import comicStrip2 from '../assets/projects/comic-strip-2.png';
import emailGenerator1 from '../assets/projects/email-generator-1.png';
import flashcards1 from '../assets/projects/flashcards-1.png';
import flashcards2 from '../assets/projects/flashcards-2.png';
import historicalScene1 from '../assets/projects/historical-scene-1.png';
import historicalScene2 from '../assets/projects/historical-scene-2.png';
import moodPlaylist1 from '../assets/projects/mood-playlist-1.png';
import textSimplifier1 from '../assets/projects/text-simplifier-1.png';
import textSimplifier2 from '../assets/projects/text-simplifier-2.png';

function Tutoring() {
  // State for active subject tab
  const [activeSubject, setActiveSubject] = useState('genAI');
  
  // State for project modal
  const [selectedProject, setSelectedProject] = useState(null);
  
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
  
  // Function to open a project modal
  const openProjectModal = (projectKey) => {
    setSelectedProject(projectKey);
  };
  
  // Function to close the project modal
  const closeProjectModal = () => {
    setSelectedProject(null);
  };

  // Project details data
  const projectDetails = {
    comicStrip: {
      title: "Comic Strip Generator",
      description: "The Comic Strip Generator is an AI-powered application that transforms written stories into visually engaging comic strips. By leveraging OpenAI's GPT-4 for script breakdown and DALL-E for image generation, this project automatically creates multi-panel comics from simple text input.",
      skills: [
        { title: "API Integration", desc: "Connect to OpenAI's GPT and DALL-E APIs" },
        { title: "Prompt Engineering", desc: "Craft effective prompts for AI image generation" },
        { title: "Image Processing", desc: "Handle and manipulate images with Python PIL" },
        { title: "Pipeline Development", desc: "Build multi-stage data processing workflows" }
      ],
      highlights: [
        "Transform any story into a multi-panel comic strip",
        "Automatically split narratives into visual scenes",
        "Generate high-quality illustrations with DALL-E 3",
        "Add text captions to each panel",
        "Customize panel count and visual style"
      ],
      codeSnippet: `# Panel generation example
def split_into_panels(script):
    prompt = f"Split the following story into 3-4 short comic panel descriptions. Make each panel visually descriptive:\\n\\n{script}"
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    text = response.choices[0].message.content
    panels = text.split('\\n')
    return [panel.strip("- ").strip() for panel in panels if panel.strip()]`,
      // Add images
      sampleImage1: comicStrip1,
      sampleImage2: comicStrip2
    },
    moodPlaylist: {
      title: "Mood Playlist Generator",
      description: "Create personalized Spotify playlists based on your current mood using AI. This application combines natural language processing with the Spotify API to analyze mood descriptions and generate the perfect playlist for any emotional state.",
      skills: [
        { title: "API Authentication", desc: "Implement OAuth flow with Spotify API" },
        { title: "Sentiment Analysis", desc: "Analyze mood descriptions with NLP" },
        { title: "Data Processing", desc: "Filter and organize music recommendations" },
        { title: "Web Integration", desc: "Create dynamic web interfaces with API data" }
      ],
      highlights: [
        "Generate playlists from natural language mood descriptions",
        "Customize music genres and energy levels",
        "Save playlists directly to your Spotify account",
        "Receive song recommendations with confidence scores",
        "Adjust recommendations based on feedback"
      ],
      codeSnippet: `# GPT-4 generates playlist theme based on mood input
def get_playlist_description(mood):
    prompt = f"Suggest a playlist theme and vibe description for someone who is feeling: '{mood}'. Give me a short 1-line theme name and 4-5 genres or artist-style keywords."
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message.content.strip()

description = get_playlist_description(mood)
track_uris = search_tracks(description)
playlist_url = create_playlist(f"Mood: {mood}", track_uris)`,
      // Add images
      sampleImage1: moodPlaylist1
    },
    flashcards: {
      title: "AI Flash Card Generator",
      description: "Turn your study notes into effective flashcards instantly with AI. This tool analyzes educational content and automatically generates question-answer pairs, making study preparation faster and more efficient.",
      skills: [
        { title: "Text Analysis", desc: "Extract key concepts from educational content" },
        { title: "UI Development", desc: "Create interactive study interfaces with Streamlit" },
        { title: "Educational Design", desc: "Apply learning principles to flashcard creation" },
        { title: "Data Persistence", desc: "Save and organize flashcard decks" }
      ],
      highlights: [
        "Generate comprehensive question sets from any text",
        "Support multiple subjects and difficulty levels",
        "Interactive study mode with spaced repetition",
        "Export to popular flashcard formats",
        "Track learning progress over time"
      ],
      codeSnippet: `# GPT-4 prompt for generating flashcards
prompt = f"""
You are a helpful study assistant. Turn the following class notes into a list of 
flashcards with clear Q&A format. Use simple language suitable for high school students.

Subject: {subject if subject else "General"}

Notes:
{notes_input}

Format:
Q: ...
A: ...
""" 

response = client.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": prompt}]
)`,
      // Add images
      sampleImage1: flashcards1,
      sampleImage2: flashcards2
    },
    historicalScene: {
      title: "Historical Scene Writer",
      description: "Travel through time with AI-generated historical narratives. This application creates immersive scenes from different time periods, complete with historically accurate details, perspectives, and writing styles.",
      skills: [
        { title: "Parameter Control", desc: "Fine-tune AI generation with specific controls" },
        { title: "UI/UX Design", desc: "Create intuitive interfaces for creative applications" },
        { title: "Historical Research", desc: "Integrate factual context into AI prompts" },
        { title: "Creative Writing", desc: "Guide AI to produce compelling narratives" }
      ],
      highlights: [
        "Select from dozens of historical periods and cultures",
        "Choose perspective, tone, and writing style",
        "Generate detailed descriptions of historical settings",
        "Create character-driven narratives with period-appropriate dialogue",
        "Export in multiple formats for educational use"
      ],
      codeSnippet: `# Build dynamic prompt based on user selections
prompt = f"Write a vivid, historically accurate short scene set during {period}."
if style != "No preference":
    prompt += f" Write it in the form of a {style.lower()}."
else:
    prompt += " Write it in the form of a first-person journal entry."
if tone != "No preference":
    prompt += f" Make the tone {tone.lower()}."
prompt += " It should reflect the events, language, and social atmosphere of that time."`,
      // Add images
      sampleImage1: historicalScene1,
      sampleImage2: historicalScene2
    },
    textSimplifier: {
      title: "Grade-Level Text Simplifier",
      description: "Make complex content accessible to readers of all levels. This tool automatically adjusts text complexity to match specific reading levels, perfect for educators, content creators, and accessibility advocates.",
      skills: [
        { title: "Readability Analysis", desc: "Implement algorithms to measure text complexity" },
        { title: "Content Adaptation", desc: "Transform text while preserving meaning" },
        { title: "Educational Standards", desc: "Apply grade-level guidelines to content" },
        { title: "Vocabulary Control", desc: "Manage word complexity for target audiences" }
      ],
      highlights: [
        "Adjust content for elementary through college reading levels",
        "Preserve core meaning while simplifying vocabulary and syntax",
        "Highlight changes between original and simplified text",
        "Provide readability metrics and statistics",
        "Process documents in multiple formats"
      ],
      codeSnippet: `# Prompt engineering for text simplification
prompt = f"""
Simplify the following text so it is easy to read and understand at a {level} reading level. 
Keep the key ideas, but use vocabulary and sentence structure suitable for that grade.

Text:
{input_text}
"""
response = client.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": prompt}]
)`,
      // Add images
      sampleImage1: textSimplifier1,
      sampleImage2: textSimplifier2
    },
    emailGenerator: {
      title: "LinkedIn Cold Email Generator",
      description: "Boost your networking success with personalized cold emails. This tool scrapes LinkedIn profiles and generates tailored outreach messages based on the recipient's background, experience, and shared connections.",
      skills: [
        { title: "Web Scraping", desc: "Extract profile data from LinkedIn pages" },
        { title: "Personalization AI", desc: "Generate custom messages based on profile analysis" },
        { title: "Template Design", desc: "Create effective email templates for different scenarios" },
        { title: "Automation", desc: "Build workflow systems for outreach campaigns" }
      ],
      highlights: [
        "Analyze LinkedIn profiles to identify key talking points",
        "Generate personalized introductions based on shared interests",
        "Customize email tone for different industries and roles",
        "Track open rates and response statistics",
        "A/B test different message approaches"
      ],
      codeSnippet: `// Generate personalized email based on LinkedIn profile
app.post('/generate-email', async (req, res) => {
    try {
        const { profile, template } = req.body;
        
        // Build comprehensive prompt with profile and template data
        const message = await anthropic.messages.create({
            model: "claude-3-5-haiku-20241022",
            max_tokens: 500,
            messages: [
                {
                    role: "user",
                    content: userPrompt
                }
            ],
            system: systemPrompt
        });
        
        // Extract subject line and email body
        const fullResponse = message.content[0].text.trim();
        // Processing logic to separate subject and body...
        
        res.json({ 
            email: email, 
            subject: subject 
        });
    } catch (error) {
        // Error handling...
    }
});`,
      // Add images
      sampleImage1: emailGenerator1
    }
  };

  // Subject content based on active tab
  const subjectContent = {
    genAI: {
      title: "Generative AI and LLMs",
      description: "Learn to harness the power of Large Language Models (LLMs) to build innovative applications. This course focuses on practical implementation of AI tools like OpenAI's GPT and DALLE to create useful, real-world projects that demonstrate the capabilities of generative AI.",
      topics: [
        "Fundamentals of Large Language Models",
        "Prompt Engineering Techniques",
        "API Integration with OpenAI and Other LLMs",
        "Building Multi-modal Applications",
        "Combining AI with Web Development",
        "Ethical Considerations in AI Development",
        "Project Planning and Implementation"
      ]
    },
    python: {
      title: "Python Programming",
      description: "From beginner to advanced levels, learn Python programming with personalized instruction. Whether you're just starting out or looking to master advanced concepts, I'll guide you through practical projects and real-world applications.",
      topics: [
        "Python Fundamentals",
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
        "Java Fundamentals",
        "Object-Oriented Programming",
        "Java Collections Framework",
        "Multithreading and Concurrency",
        "Advanced Java Topics (Reflection, Annotations)",
        "Design Patterns"
      ]
    },
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

      {/* Subjects Section */}
      <section ref={subjectsRef} className="subjects-section">
        <div className="subjects-container">
          <div className="subjects-header">
            <h2>Courses</h2>
          </div>
          
          <div className="subjects-tabs">
            <button 
              className={`subject-tab ${activeSubject === 'genAI' ? 'active' : ''}`}
              onClick={() => setActiveSubject('genAI')}
            >
              Generative AI and LLMs
            </button>
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
            
            {/* Project Examples Section for Generative AI tab */}
            {activeSubject === 'genAI' && (
              <div className="projects-showcase">
                <h4>Example Student Projects:</h4>
                <div className="project-examples">
                  <div 
                    className="project-card"
                    onClick={() => openProjectModal('comicStrip')}
                  >
                    <h5>Comic Strip Generator</h5>
                    <p>Create custom comic strips using OpenAI and DALL-E APIs</p>
                  </div>
                  <div 
                    className="project-card"
                    onClick={() => openProjectModal('moodPlaylist')}
                  >
                    <h5>Mood Playlist Generator</h5>
                    <p>Generate custom playlists based on mood using Spotify API and OpenAI</p>
                  </div>
                  <div 
                    className="project-card"
                    onClick={() => openProjectModal('flashcards')}
                  >
                    <h5>AI Flash Card Generator</h5>
                    <p>Convert study notes into intelligent flashcards with OpenAI and Streamlit</p>
                  </div>
                  <div 
                    className="project-card"
                    onClick={() => openProjectModal('historicalScene')}
                  >
                    <h5>Historical Scene Writer</h5>
                    <p>Generate immersive historical narratives with customizable settings</p>
                  </div>
                  <div 
                    className="project-card"
                    onClick={() => openProjectModal('textSimplifier')}
                  >
                    <h5>Grade-Level Text Simplifier</h5>
                    <p>Adapt complex content to different reading levels for educational purposes</p>
                  </div>
                  <div 
                    className="project-card"
                    onClick={() => openProjectModal('emailGenerator')}
                  >
                    <h5>Template-Based Linkedin Cold Email Generator</h5>
                    <p>Scrape a target's linkedin page and generate a custom cold email</p>
                  </div>
                </div>
              </div>
            )}
            
            {/* Project Modal */}
            {selectedProject && (
              <ProjectModal 
                project={projectDetails[selectedProject]} 
                onClose={closeProjectModal}
              />
            )}
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