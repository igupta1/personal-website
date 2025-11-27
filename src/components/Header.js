import React, { useState, useEffect } from "react";
import { NavLink } from "react-router-dom";

function Header() {
  const [scrolled, setScrolled] = useState(false);
  
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
        <ul className="nav-links">
          <li>
            <NavLink
              to="/"
              className={({ isActive }) => (isActive ? "active" : "")}
            >
              Home
            </NavLink>
          </li>
          <li>
            <NavLink
              to="/ai-tools"
              className={({ isActive }) => (isActive ? "active" : "")}
            >
              AI Tools
            </NavLink>
          </li>
        </ul>
      </nav>
    </header>
  );
}

export default Header;
