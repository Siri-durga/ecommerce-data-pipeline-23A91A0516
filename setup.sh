#!/bin/bash

echo "Starting E-Commerce Data Pipeline setup..."

# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Create logs directory if not exists
mkdir -p logs

echo "Setup completed successfully."
echo "Activate virtual environment using: source venv/bin/activate"
