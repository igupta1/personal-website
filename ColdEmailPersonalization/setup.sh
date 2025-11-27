#!/bin/bash

# Deep Multiline Icebreaker System - Quick Start Script

echo "================================================"
echo "🚀 Deep Multiline Icebreaker System Setup"
echo "================================================"
echo ""

# Check Python version
echo "1️⃣ Checking Python version..."
python3 --version

echo ""
echo "2️⃣ Creating virtual environment..."
python3 -m venv venv

echo ""
echo "3️⃣ Activating virtual environment..."
source venv/bin/activate

echo ""
echo "4️⃣ Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "5️⃣ Setting up environment file..."
if [ ! -f .env ]; then
    cp .env.template .env
    echo "⚠️  Please edit .env and add your OpenAI API key!"
    echo "   Open .env in a text editor and replace 'your_openai_api_key_here' with your actual key"
else
    echo "✅ .env already exists"
fi

echo ""
echo "================================================"
echo "✅ Setup Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo "1. Edit .env and add your OPENAI_API_KEY"
echo "2. Make sure 'apollo-contacts-export (21).csv' is in this directory"
echo "3. Run: python main.py"
echo ""

