//ItMspDemo.js
import React from "react";
import { Link } from "react-router-dom";

function ItMspDemo() {
  return (
    <div className="about-page-combined">
      <div className="about-container" style={{ gridTemplateColumns: '1fr' }}>
        <div className="about-right-column">
          <section className="demo-section">
            <div className="demo-top-row">
              <div className="demo-section-header">
                <Link to="/gtm" className="back-link-inline">Back to GTM Systems</Link>
                <h2>IT Managed Service Provider Lead Generation</h2>
              </div>
            </div>
            <p>Coming soon.</p>
          </section>
        </div>
      </div>
    </div>
  );
}

export default ItMspDemo;
