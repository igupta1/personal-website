//App.js
import React from "react";
import { BrowserRouter as Router, Routes, Route, Navigate } from "react-router-dom";
import Header from "./components/Header";
import Footer from "./components/Footer";
import About from "./pages/About";
import AITools from "./pages/AITools";
import LeadGenDemo from "./pages/LeadGenDemo";
import "./App.css";  // Use the existing App.css

function App() {
  return (
    <Router>
      <div className="app">
        <Header />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<About />} />
            <Route path="/gtm" element={<AITools />} />
<Route path="/gtm/marketing-agency" element={<LeadGenDemo />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
        <Footer />
      </div>
    </Router>
  );
}

export default App;
