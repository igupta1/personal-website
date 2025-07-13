import React, { useState } from 'react';
import { Brain, Zap, TrendingUp, Users, CheckCircle, Mail, Calendar, ArrowRight, Star, Building, ChevronRight } from 'lucide-react';
import headshotImage from '../assets/BV_GRAD-01.jpg';

function AiConsulting() {
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    company: '',
    message: ''
  });

  const handleInputChange = (e) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    try {
      const response = await fetch('https://formspree.io/f/xldnzykw', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData),
      });
      
      if (response.ok) {
        alert('Thank you for your inquiry! I\'ll get back to you within 24 hours.');
        // Reset form
        setFormData({
          name: '',
          email: '',
          company: '',
          message: ''
        });
      } else {
        alert('Sorry, there was an error sending your message. Please try again or email me directly at ishaan4g@gmail.com');
      }
    } catch (error) {
      console.error('Error submitting form:', error);
      alert('Sorry, there was an error sending your message. Please try again or email me directly at ishaan4g@gmail.com');
    }
  };

  const scrollToSection = (sectionId) => {
    const element = document.getElementById(sectionId);
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <div className="min-h-screen bg-primary-bg font-inter">
      {/* Services Section */}
      <section id="services" className="py-20 bg-primary-bg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-4xl md:text-5xl font-bold text-primary-text mb-6">
              AI Services That Drive Results
            </h2>
            <p className="text-xl text-gray-600 max-w-3xl mx-auto">
              Leveraging my experience building GenAI at Google, I deliver practical, secure, and high-ROI AI solutions that automate tasks, save costs, and drive real business results.
            </p>
          </div>
          
          {/* About Section */}
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
            <div className="grid lg:grid-cols-2 gap-16 items-center">
              <div>
                <p className="text-lg text-gray-600 mb-6 leading-relaxed">
                  I'm Ishaan, a Computer Science graduate from UCLA with experience building GenAI systems at Google used by millions of Gmail users.
                </p>
                <p className="text-lg text-gray-600 mb-8 leading-relaxed">
                  I've also contributed to projects at Amazon and Cisco, giving me a broad perspective on what it takes to build robust, secure, and scalable solutions.
                </p>
              </div>
              <div className="relative">
                <div className="bg-white rounded-2xl shadow-2xl p-8 flex items-center justify-center min-h-[400px]">
                  <div className="text-center">
                    <img 
                      src={headshotImage} 
                      alt="Ishaan Gupta - AI Consultant" 
                      className="w-64 h-64 mx-auto object-cover rounded-full shadow-lg"
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>
          
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
            <div className="group bg-white rounded-xl shadow-lg hover:shadow-2xl transition-all duration-300 p-8 border border-gray-100 hover:border-accent">
              <div className="w-16 h-16 bg-accent bg-opacity-10 rounded-lg flex items-center justify-center mb-6 group-hover:bg-opacity-20 transition-colors">
                <Brain className="w-8 h-8 text-accent" />
              </div>
              <h3 className="text-2xl font-bold text-primary-text mb-4">AI Readiness & Strategy</h3>
              <p className="text-gray-600 mb-6 leading-relaxed">
              An audit to identify your top AI opportunities. We'll build a clear implementation roadmap with a focus on immediate ROI and security.
              </p>
              <ul className="space-y-2">
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  AI Readiness Assessment
                </li>
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  Custom AI Roadmap
                </li>
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  ROI Analysis & Projections
                </li>
              </ul>
            </div>

            <div className="group bg-white rounded-xl shadow-lg hover:shadow-2xl transition-all duration-300 p-8 border border-gray-100 hover:border-accent">
              <div className="w-16 h-16 bg-accent bg-opacity-10 rounded-lg flex items-center justify-center mb-6 group-hover:bg-opacity-20 transition-colors">
                <Zap className="w-8 h-8 text-accent" />
              </div>
              <h3 className="text-2xl font-bold text-primary-text mb-4">Custom AI Workflow Automation</h3>
              <p className="text-gray-600 mb-6 leading-relaxed">
              I'll design and build secure, custom AI systems to automate your routine, day-to-day tasks like customer support, data entry, and content generation.
              </p>
              <ul className="space-y-2">
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  Workflow Optimization
                </li>
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  Intelligent Document Processing
                </li>
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  Customer Service Automation
                </li>
              </ul>
            </div>

            <div className="group bg-white rounded-xl shadow-lg hover:shadow-2xl transition-all duration-300 p-8 border border-gray-100 hover:border-accent">
              <div className="w-16 h-16 bg-accent bg-opacity-10 rounded-lg flex items-center justify-center mb-6 group-hover:bg-opacity-20 transition-colors">
                <TrendingUp className="w-8 h-8 text-accent" />
              </div>
              <h3 className="text-2xl font-bold text-primary-text mb-4">AI Tool Onboarding & Training</h3>
              <p className="text-gray-600 mb-6 leading-relaxed">
              Get your team onboarded with the best public AI tools for your needs. I provide hands-on training focused on secure, efficient, and effective usage.
              </p>
              <ul className="space-y-2">
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  Demand Forecasting
                </li>
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  Risk Assessment Models
                </li>
                <li className="flex items-center gap-2 text-sm text-gray-600">
                  <CheckCircle className="w-4 h-4 text-green-500" />
                  Customer Behavior Analysis
                </li>
              </ul>
            </div>
          </div>
        </div>
      </section>




      {/* Contact Section */}
      <section id="contact" className="py-20 bg-primary-bg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
          </div>
          
          <div className="grid lg:grid-cols-2 gap-16">
            <div>
              <h3 className="text-2xl font-bold text-primary-text mb-8">Get in Touch</h3>
              
              <div className="space-y-6">
                <div className="flex items-center gap-4">
                  <div className="w-12 h-12 bg-accent bg-opacity-10 rounded-lg flex items-center justify-center">
                    <Mail className="w-6 h-6 text-accent" />
                  </div>
                  <div>
                    <h4 className="font-semibold text-primary-text">Email</h4>
                    <p className="text-gray-600">ishaan4g@gmail.com</p>
                  </div>
                </div>
                
                <div className="flex items-center gap-4">
                  <div className="w-12 h-12 bg-accent bg-opacity-10 rounded-lg flex items-center justify-center">
                    <Calendar className="w-6 h-6 text-accent" />
                  </div>
                  <div>
                    <h4 className="font-semibold text-primary-text">Response Time</h4>
                    <p className="text-gray-600">Within 24 hours</p>
                  </div>
                </div>
                
                <div className="flex items-center gap-4">
                  <div className="w-12 h-12 bg-accent bg-opacity-10 rounded-lg flex items-center justify-center">
                    <Users className="w-6 h-6 text-accent" />
                  </div>
                  <div>
                    <h4 className="font-semibold text-primary-text">Free Consultation</h4>
                    <p className="text-gray-600">30-minute strategy call</p>
                  </div>
                </div>
              </div>
            </div>
            
            <div className="bg-white rounded-xl shadow-lg p-8">
              <p className="text-xl text-gray-600 mb-6 text-center">
                Schedule a free consultation today.
              </p>
              <form onSubmit={handleSubmit} action="https://formspree.io/f/xldnzykw" method="POST" className="space-y-6">
                <div>
                  <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-2">
                    Full Name *
                  </label>
                  <input
                    type="text"
                    id="name"
                    name="name"
                    value={formData.name}
                    onChange={handleInputChange}
                    required
                    className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-accent focus:border-transparent transition-colors"
                    placeholder="Your full name"
                  />
                </div>
                
                <div>
                  <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-2">
                    Email Address *
                  </label>
                  <input
                    type="email"
                    id="email"
                    name="email"
                    value={formData.email}
                    onChange={handleInputChange}
                    required
                    className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-accent focus:border-transparent transition-colors"
                    placeholder="youremail@company.com"
                  />
                </div>
                
                <div>
                  <label htmlFor="company" className="block text-sm font-medium text-gray-700 mb-2">
                    Company
                  </label>
                  <input
                    type="text"
                    id="company"
                    name="company"
                    value={formData.company}
                    onChange={handleInputChange}
                    className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-accent focus:border-transparent transition-colors"
                    placeholder="Your company name"
                  />
                </div>
                
                <div>
                  <label htmlFor="message" className="block text-sm font-medium text-gray-700 mb-2">
                    Project Details *
                  </label>
                  <textarea
                    id="message"
                    name="message"
                    value={formData.message}
                    onChange={handleInputChange}
                    required
                    rows={4}
                    className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-accent focus:border-transparent transition-colors resize-none"
                    placeholder="Tell me about your AI project goals and challenges..."
                  />
                </div>
                
                <button
                  type="submit"
                  className="w-full bg-accent hover:bg-blue-600 text-white font-semibold py-4 px-6 rounded-lg transition-colors duration-300 flex items-center justify-center gap-2"
                >
                  Send Message
                  <ArrowRight className="w-5 h-5" />
                </button>
              </form>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export default AiConsulting; 