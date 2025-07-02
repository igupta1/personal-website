/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
    "./public/index.html"
  ],
  theme: {
    extend: {
      colors: {
        'primary-bg': '#FFFFFF',
        'secondary-bg': '#F8F9FA',
        'dark-bg': '#0A192F',
        'charcoal': '#111827',
        'primary-text': '#1F2937',
        'accent': '#007BFF',
      },
      fontFamily: {
        'inter': ['Inter', 'sans-serif'],
      },
    },
  },
  plugins: [],
} 