import React from "react";
import { NavLink } from "react-router-dom";

function Header() {
  return (
    <header className="header">
      <nav>
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
          <li>
            <NavLink
              to="/tutoring"
              className={({ isActive }) => (isActive ? "active" : "")}
            >
              Tutoring
            </NavLink>
          </li>
        </ul>
      </nav>
    </header>
  );
}

export default Header;
