import React, { useState, useEffect } from 'react';
import { Brain, Zap, TrendingUp, Users, CheckCircle, Mail, Calendar, ArrowRight, Star, Building, ChevronRight, BookOpen } from 'lucide-react';
import headshotImage from '../assets/BV_GRAD-01.jpg';

function AiConsulting() {
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    company: '',
    message: ''
  });

  const [activeTab, setActiveTab] = useState('finding-leads');

  // Handle hash fragment scrolling when component mounts
  useEffect(() => {
    const handleHashScroll = () => {
      const hash = window.location.hash;
      if (hash === '#blog') {
        setTimeout(() => {
          const element = document.getElementById('blog');
          if (element) {
            element.scrollIntoView({ behavior: 'smooth' });
          }
        }, 100); // Small delay to ensure DOM is ready
      }
    };

    // Check hash on mount
    handleHashScroll();

    // Also listen for hash changes
    window.addEventListener('hashchange', handleHashScroll);

    return () => {
      window.removeEventListener('hashchange', handleHashScroll);
    };
  }, []);

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




      {/* Blog Section */}
      <section id="blog" className="py-20 bg-secondary-bg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <div className="flex items-center justify-center gap-3 mb-6">
              <BookOpen className="w-8 h-8 text-accent" />
              <h2 className="text-4xl md:text-5xl font-bold text-primary-text">
                Strategic Adoption of AI Technologies
              </h2>
            </div>
            <p className="text-xl text-gray-600 max-w-5xl mx-auto">
              Blog: Use AI to Boost Leads, Conversions, and Customer Retention for SMBs
            </p>
          </div>
          
          <div className="bg-white rounded-2xl shadow-2xl overflow-hidden">
            <div className="p-8 lg:p-12">
              <div className="prose prose-lg max-w-none mb-8">
                                <p className="text-gray-700 leading-relaxed">
                   91% of small and medium businesses using AI say it directly boosts their revenue, and 86% report improved profit margins. Yet currently only about 28% of SMBs have adopted AI at all. In other words, seven out of ten businesses are leaving money on the table while <strong>AI-enabled competitors race ahead — closing deals faster, personalizing marketing at scale, and running leaner operations</strong>. Every quarter you wait, that gap widens. But how do you actually implement AI in everyday business workflows, beyond the consumer-centric web version of ChatGPT? Below, we break down some of the top AI-powered tools that can help you <strong>find more leads, close those leads, and deliver better service</strong> to reduce customer churn.
                </p>
              </div>
              
              {/* Tab Navigation */}
              <div className="mb-8">
                <div className="text-center mb-4">
                  <p className="text-sm text-gray-600 font-medium">
                    👆 Click through these 5 sections to explore AI implementation strategies:
                  </p>
                </div>
                <div className="bg-gray-50 rounded-lg p-2 border border-gray-200">
                  <nav className="flex space-x-2 overflow-x-auto">
                    {[
                      { id: 'finding-leads', label: 'Finding Leads' },
                      { id: 'closing-leads', label: 'Closing Leads' },
                      { id: 'reducing-churn', label: 'Reducing Churn' },
                      { id: 'other-considerations', label: 'Other Considerations (Data & Compliance)' },
                      { id: 'what-next', label: 'What Next?' }
                    ].map((tab) => (
                      <button
                        key={tab.id}
                        onClick={() => setActiveTab(tab.id)}
                        className={`whitespace-nowrap py-3 px-4 rounded-md font-medium text-sm transition-all duration-200 ${
                          activeTab === tab.id
                            ? 'bg-accent text-white shadow-md transform scale-105'
                            : 'bg-white text-gray-700 hover:bg-gray-100 hover:text-accent hover:shadow-sm border border-gray-200'
                        }`}
                      >
                        {tab.label}
                      </button>
                    ))}
                  </nav>
                </div>
              </div>
              
              {/* Tab Content */}
              <div className="prose prose-lg max-w-none">
                {activeTab === 'finding-leads' && (
                  <div>
                    <h3 className="text-2xl font-bold text-primary-text mb-6">Finding Leads</h3>
                    
                                        <h4 className="text-xl font-semibold text-primary-text mb-4">Social Media Marketing</h4>
                    <p className="text-gray-700 mb-6">
                       AI-powered analytics tools like <strong>Sprout Social</strong> and <strong>Hootsuite</strong> use AI to analyze engagement metrics, identify the best times to post, and even benchmark your performance against competitors. For example, AI can highlight which content gets the most traction and suggest optimal posting schedules. The result is data-driven social strategy – <strong>improved engagement and follower growth without the guesswork.</strong>
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Email Marketing & Personalization</h4>
                    <p className="text-gray-700 mb-6">
                       Modern email marketing suites like <strong>Instantly.ai</strong> or <strong>Apollo</strong> use AI to <strong>segment your audience, personalize content, and optimize send times for each subscriber</strong>. That means your customers get more relevant emails at the moments they're most likely to open them. This personalization pays off: marketers who use AI to tailor emails have seen a 41% increase in revenue and 13% higher click-through rates from email campaigns on average. In short, AI helps you send <strong>fewer blast emails and more targeted messages that convert.</strong>
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Local SEO (Google Business Profile)</h4>
                    <p className="text-gray-700 mb-6">
                       For businesses that rely on local customers, AI tools can <strong>improve your local search presence</strong>. Services like <strong>Moz Local</strong> use AI to automate your Google My Business updates – from posting updates and photos to responding to reviews – so your profile stays active and optimized. This matters because almost 87% of consumers now use Google to help decide on local businesses. An AI assistant can ensure your hours, offers, and responses to customer reviews are always up-to-date, <strong>boosting your local credibility and search rankings without manual effort</strong>.
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Content Creation (Blog posts, etc.)</h4>
                    <p className="text-gray-700 mb-6">
                       Tools like ChatGPT, Claude, Google Gemini or commercial platforms like Jasper AI are excellent for generating initial drafts of content. They can suggest topics, write outlines, or even produce long-form articles on a prompt. This can drastically <strong>speed up content marketing</strong>. Always remember to fact-check and edit AI-generated content to ensure accuracy and that it matches your brand voice – think of the AI as a first draft writer, not the final editor. Used well, these tools help you publish more content with <strong>less in-house writing time</strong>.
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Converting Website Visitors</h4>
                    <p className="text-gray-700 mb-6">
                       Getting traffic is one thing; converting visitors into leads or customers is another. AI conversion optimization tools like <strong>Instapage</strong> and <strong>Usermind</strong> can analyze visitor behavior on your site and automatically suggest or implement improvements. For example, AI can track which pages or product listings users linger on, what they click, and where they drop off. It might learn that visitors from ads tend to scroll halfway down your landing page and then leave – indicating your call-to-action isn't appearing early enough. These insights let you optimize page layouts and by <strong>tailoring the user journey to each visitor</strong>, businesses have seen <strong>significant lifts in conversion rates</strong>.
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">AI Agents for Lead Generation</h4>
                    <p className="text-gray-700 mb-6">
                       Beyond analytics, AI can directly help <strong>generate leads via automation</strong>. One clever approach is using AI-driven automation platforms like <strong>n8n</strong> or <strong>Zapier</strong> with AI integrations. For example, you can set up a workflow (no coding needed) that scrapes business leads from Google Maps and compiles them into a spreadsheet for your sales team. Instead of manually hunting for local businesses on Maps and copying details, <strong>an AI agent can do it 24/7</strong>, finding businesses in your niche and even enriching the data (using an AI like GPT to summarize each business or find emails). This kind of agent acts as a tireless virtual SDR (sales development rep) feeding your pipeline.
                    </p>
                  </div>
                )}
                
                {activeTab === 'closing-leads' && (
                  <div>
                    <h3 className="text-2xl font-bold text-primary-text mb-6">Closing Leads</h3>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Real-Time Sales Alerts (AI + Slack)</h4>
                    <p className="text-gray-700 mb-6">
                      <strong>Speed matters</strong> when nurturing hot prospects. AI can monitor your sales pipeline and customer interactions, then send instant alerts to your team when something needs attention. For instance, modern sales platforms can detect signals like a deal stalling or a customer mentioning a competitor, and then <strong>ping your team in Slack</strong> with an alert. These AI-driven alerts might say, "⚠️ Prospect XYZ hasn't replied in 7 days" or "🤖 Competitor mentioned on call with Client ABC." By surfacing risks and opportunities in real time (right where your team communicates), AI ensures you can act before a deal goes cold. Think of it as an AI sales assistant watching your CRM and sounding an alarm for anything you shouldn't miss.
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Competitor & Consumer Insights</h4>
                    <p className="text-gray-700 mb-6">
                      Staying ahead of the competition is crucial for closing deals. AI tools can digest vast amounts of market data and spit out actionable insights. For example, <strong>Semrush</strong> and <strong>SpyFu</strong> use AI to track your competitors' SEO keywords, ad campaigns, and even what's trending on their website, so you can adjust your strategy accordingly. On the social media side, platforms like Kontentino can "spy" on competitors' social performance – monitoring their posting frequency, engagement rates, and top-performing content. These insights help your sales pitch because you'll know exactly how to <strong>differentiate your offering.</strong> If AI finds that Competitor X's product gets poor reviews for support, your sales team can proactively tout your great support. Or if the market is shifting toward a feature you have and a rival lacks, you can highlight that to prospects. In short, AI-driven competitive analysis arms you with <strong>talking points and timing to outmaneuver others</strong> and win the deal.
                    </p>
                    
                                          <h4 className="text-xl font-semibold text-primary-text mb-4">AI Appointment Scheduling & Reminders</h4>
                      <p className="text-gray-700 mb-6">
                       Closing a lead often comes down to getting them on a call or demo. This is where an AI receptionist or scheduling assistant can shine. These tools (such as <strong>Calendly's AI</strong> features) automate the back-and-forth of booking meetings and sending reminders. For instance, an AI assistant can <strong>reach out to a prospect a day before a scheduled call</strong> with a friendly reminder or even handle rescheduling requests. Some AI scheduling bots can talk via email or SMS in natural language – so prospects feel like they're chatting with a helpful coordinator rather than clicking a generic link. By ensuring appointments are booked swiftly and that no one forgets the meeting, AI <strong>reduces no-shows</strong> and keeps your deal pipeline flowing. It's like having a personal receptionist who never sleeps, making sure your leads show up and are ready to talk business.
                      </p>
                  </div>
                )}
                
                {activeTab === 'reducing-churn' && (
                  <div>
                    <h3 className="text-2xl font-bold text-primary-text mb-6">Reducing Churn</h3>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">24/7 Customer Service with AI</h4>
                    <p className="text-gray-700 mb-6">
                      A big part of keeping customers happy (and renewing) is <strong>providing excellent service consistently</strong>. AI-powered chatbots are now making this easier for small businesses. With tools like <strong>Voiceflow</strong>, you can design a custom chatbot or voice assistant that handles common customer inquiries across your website, mobile app, or even phone system. Similarly, customer support suites such as <strong>Zendesk</strong> have AI add-ons (like <strong>Zendesk's</strong> Answer Bot) that automatically answer frequently asked questions and help users self-serve. The impact can be huge: <strong>Voiceflow's</strong> own AI support agent "Tico" resolves 97% of tier-1 support tickets autonomously – cutting support costs nearly in half while maintaining a 93% customer satisfaction rate. In practice, this means customers get instant answers at 2am about your return policy or troubleshooting steps, instead of waiting hours for an email reply. Your human support reps then focus only on the trickier issues. The result is <strong>faster service, happier customers, and a team freed from answering the same repetitive questions all day.</strong>
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Better Decision Making (AI + Your Data)</h4>
                    <p className="text-gray-700 mb-6">
                      Beyond customer-facing uses, AI can be turned inward to help you make smarter decisions. The key is <strong>making your business's data accessible to AI</strong>. This often involves organizing your documents, reports, and knowledge into a form AI can understand (for example, using a vector database to store embeddings of your text data). In simpler terms you feed your important files into an AI-readable format, and the AI can then pull from your actual data when answering questions or providing recommendations. This creates a kind of "AI brain" for your company. Imagine asking an AI assistant, "What was our best-selling product category last summer and what customer feedback did we get on it?" If your sales data and customer reviews are indexed, the AI could give you a data-backed answer in seconds. Companies doing this see more informed decisions because the <strong>AI surfaces insights humans might miss</strong>. Note: Good data is the fuel for AI – 74% of growing SMBs are increasing investments in data management to improve AI outcomes. So before deploying these advanced AI analytics, you may need to clean up data silos and digitize any paper records (OCR tools can help turn scanned docs into text). The payoff is an AI assistant that truly knows your business and can support your strategic planning with facts, not hunches.
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">AI for Admin and Back-Office Tasks</h4>
                    <p className="text-gray-700 mb-6">
                      AI can also streamline those time-consuming operational tasks that eat into your day. A great example is document processing. Suppose your business deals with dozens of invoices, receipts, or forms each week – rather than manually keying those into a spreadsheet or accounting software, an AI agent can handle it. Tools exist that use OCR (optical character recognition) to read PDFs or images of invoices and then extract key details (vendor, date, amount) to a Google Sheet or QuickBooks automatically. This kind of AI "bookkeeper's assistant" not only reduces human error but also <strong>frees your team from data entry drudgery</strong>. In fact, in a recent survey, SMBs reported using AI for tasks like generating invoices and compiling estimates, functions historically only automated at big companies. Beyond finance, similar AI agents can update CRM records, log call notes, or manage inventory levels based on patterns – basically any repetitive rules-driven process. The bottom line is efficiency: <strong>AI tools let a small team accomplish what used to require large staff, so you can scale without ballooning headcount (and focus your people on higher-value work).</strong>
                    </p>
                  </div>
                )}
                
                {activeTab === 'other-considerations' && (
                  <div>
                    <h3 className="text-2xl font-bold text-primary-text mb-6">Other Considerations (Data & Compliance)</h3>
                    <p className="text-gray-700 mb-6">
                      When implementing AI in your business, keep a couple of broader considerations in mind:
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Data Security & Compliance</h4>
                    <p className="text-gray-700 mb-6">
                      AI systems are hungry for data – that's what makes them smart – but you <strong>must handle customer and company data responsibly.</strong> If you work with sensitive information (personal data, health records, financial info), ensure that any AI tools or cloud providers you use are compliant with regulations like HIPAA, GDPR, or industry standards like SOC 2. <strong>Many major cloud AI platforms (Google, AWS, Azure) offer built-in compliance and encryption</strong>, which can be a safer route if compliance is non-negotiable. Also, be transparent with customers and employees about how AI is being used with their data. Trust and privacy go hand-in-hand: in fact, 81% of SMB leaders say they'd spend more on technology from vendors they trust to handle data securely. Treat your data (and your customers' data) with the same care you'd expect from a business you trust.
                    </p>
                    
                    <h4 className="text-xl font-semibold text-primary-text mb-4">Data Quality & Formats</h4>
                    <p className="text-gray-700 mb-6">
                      An old saying in AI is "garbage in, garbage out." The insights or automation you get from AI are only as good as the data you feed it. Take time to <strong>format and integrate your data so AI tools can access it easily.</strong> This might mean consolidating spreadsheets, cleaning up duplicate records, or digitizing paper documents. If you have critical info locked up in PDFs or images (say, scanned contracts or handwritten notes), <strong>use OCR technology to convert them into searchable text before trying AI on them.</strong> Businesses that invest in integrated, clean data see far better results – growing SMBs are twice as likely to have unified tech systems compared to those with declining revenue, avoiding the pitfalls of siloed, messy data. In practice, this could be as simple as using one CRM across all departments, or as advanced as building a central database that your various AI tools all connect to. The easier it is for AI to find relevant information, the more impactful its contributions will be.
                    </p>
                  </div>
                )}
                
                {activeTab === 'what-next' && (
                  <div>
                    <h3 className="text-2xl font-bold text-primary-text mb-6">What Next?</h3>
                    <p className="text-gray-700 mb-6">
                      By leveraging the right AI tools in the right parts of your business, even a small or medium business can punch above its weight. The technologies that once were luxuries for big enterprises – predictive analytics, intelligent automation, personalized customer experiences – are now accessible to SMBs and often surprisingly user-friendly. The key is to start with your biggest pain points (whether it's filling the lead funnel, shortening sales cycles, or improving customer retention) and then pilot an AI solution in that area. Remember, you don't need a technical background to get value from these tools; most come with friendly interfaces or support teams to help with setup.
                    </p>
                    <p className="text-gray-700 mb-6">
                      What you do need is an open mind and the willingness to invest a bit of time in the setup and training phase. Once your <strong>AI systems are running, they tend to keep learning and improving</strong>. Meanwhile, you'll be <strong>freeing up your human team</strong> to focus on what humans do best – building relationships, crafting strategy, and creatively solving problems. Those who adopt AI early will find themselves with a competitive edge in efficiency and insights. Those who wait too long risk falling behind as early adopters compound their advantages. In short, AI isn't a magic bullet, but it is a <strong>powerful assistive force.</strong> Harness it wisely, and you'll see the impact on your bottom line: more leads, more sales, happier customers, and more hours in the day. The future of business is augmented – and for SMBs ready to grow, the future is now.
                    </p>
                  </div>
                )}
              </div>
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