import React, { useState, useEffect } from "react";
import { NavLink, useLocation } from "react-router-dom";

function Header() {
  const [scrolled, setScrolled] = useState(false);
  const location = useLocation();
  
  // Check if current route is an isolated page (tutoring or ai-consulting)
  const isIsolatedPage = location.pathname === '/tutoring' || location.pathname === '/ai-consulting';
  
  useEffect(() => {
    const handleScroll = () => {
      const isScrolled = window.scrollY > 10;
      if (isScrolled !== scrolled) {
        setScrolled(isScrolled);
      }
    };
    
    document.addEventListener('scroll', handleScroll);
    return () => {
      document.removeEventListener('scroll', handleScroll);
    };
  }, [scrolled]);

  return (
    <header className={`header ${scrolled ? 'scrolled' : ''}`}>
      <nav>
        {!isIsolatedPage && (
          <ul className="nav-links">
            <li>
              <NavLink
                to="/about"
                className={({ isActive }) => (isActive ? "active" : "")}
              >
                About
              </NavLink>
            </li>
            <li>
              <NavLink
                to="/portfolio"
                className={({ isActive }) => (isActive ? "active" : "")}
              >
                Portfolio
              </NavLink>
            </li>
          </ul>
        )}
      </nav>
    </header>
  );
}

export default Header;
