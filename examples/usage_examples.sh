#!/bin/bash

# Example usage scripts for MEGA 2.0 CLI

echo "MEGA 2.0 CLI Usage Examples"
echo "============================"
echo

# 1. Initial setup
echo "1. Setting up configuration:"
echo "mega2-cli config setup"
echo

# 2. List available projects and forms
echo "2. Listing available projects and forms:"
echo "mega2-cli list"
echo "mega2-cli list --project-id 1"
echo

# 3. Download data examples
echo "3. Downloading data:"
echo "# Using configured settings"
echo "mega2-cli download -o survey_data.csv"
echo
echo "# With explicit parameters"
echo "mega2-cli download --url https://odk.example.com \\"
echo "                   --email user@example.com \\"
echo "                   --project-id 1 \\"
echo "                   --form-id household_survey \\"
echo "                   -o household_data.xlsx \\"
echo "                   --format excel"
echo

# 4. Generate reports
echo "4. Generating PDF reports:"
echo "# Basic report"
echo "mega2-cli report survey_data.csv survey_report.pdf"
echo
echo "# Report with custom title"
echo "mega2-cli report household_data.xlsx monthly_report.pdf \\"
echo "                 --title \"Monthly Household Survey Report\""
echo

# 5. Configuration management
echo "5. Configuration management:"
echo "# Show current configuration"
echo "mega2-cli config show"
echo
echo "# Create sample configuration"
echo "mega2-cli config sample"
echo

# 6. Complete workflow example
echo "6. Complete workflow example:"
echo "# Step 1: Setup"
echo "mega2-cli config setup"
echo
echo "# Step 2: Download data"
echo "mega2-cli download --project-id 1 --form-id survey -o data.csv"
echo
echo "# Step 3: Generate report"
echo "mega2-cli report data.csv final_report.pdf --title \"Survey Analysis\""
echo

echo "For more information, use 'mega2-cli --help' or 'mega2-cli COMMAND --help'"