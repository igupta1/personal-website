//App.js
import React from "react";
import { BrowserRouter as Router, Routes, Route, Navigate } from "react-router-dom";
import Header from "./components/Header";
import Footer from "./components/Footer";
import About from "./pages/About";
import AITools from "./pages/AITools";
import ColdEmailDemo from "./pages/ColdEmailDemo";
import "./App.css";  // Use the existing App.css

function App() {
  return (
    <Router>
      <div className="app">
        <Header />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<About />} />
            <Route path="/ai-tools" element={<AITools />} />
            <Route path="/ai-tools/cold-email-deep-personalization" element={<ColdEmailDemo />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
        <Footer />
      </div>
    </Router>
  );
}

export default App;
