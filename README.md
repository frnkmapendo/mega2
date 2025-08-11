# MEGA 2.0 CLI - ODK Central Data Downloader and PDF Report Generator

A standalone Python application for downloading data from ODK Central servers and generating formatted PDF reports with charts, tables, and summaries.

## Features

- **ODK Central Integration**: Connect to ODK Central servers, authenticate, and download form submissions
- **PDF Report Generation**: Create formatted PDF reports with:
  - Summary statistics
  - Data visualizations (charts and graphs)
  - Data tables
  - Customizable templates
- **Command-Line Interface**: User-friendly CLI with multiple commands
- **Configuration Management**: Persistent configuration for ODK credentials and report settings
- **Multiple Output Formats**: Support for CSV, Excel, and JSON data exports
- **Progress Indicators**: Real-time feedback during data download and report generation

## Installation

### From PyPI (Recommended)

```bash
pip install mega2-cli
```

### From Source

```bash
git clone https://github.com/frnkmapendo/mega2.git
cd mega2
pip install .
```

### Development Installation

```bash
git clone https://github.com/frnkmapendo/mega2.git
cd mega2
pip install -e ".[dev]"
```

## Quick Start

### 1. Configure ODK Central Connection

```bash
mega2-cli config setup
```

This will prompt you for:
- ODK Central server URL
- Email and password
- Default project and form IDs (optional)
- Report preferences

### 2. Download Data

```bash
# Download data using configured settings
mega2-cli download -o my_data.csv

# Download with specific parameters
mega2-cli download --url https://odk-central.example.com \
                   --email user@example.com \
                   --password mypassword \
                   --project-id 1 \
                   --form-id survey_form \
                   -o survey_data.csv
```

### 3. Generate PDF Report

```bash
# Generate report from downloaded data
mega2-cli report my_data.csv report.pdf

# Generate report with custom title
mega2-cli report my_data.csv report.pdf --title "Monthly Survey Report"
```

## Commands

### `download` - Download Data from ODK Central

Download form submissions from ODK Central server.

```bash
mega2-cli download [OPTIONS]
```

**Options:**
- `--url`: ODK Central base URL
- `--email`: Email for authentication
- `--password`: Password for authentication
- `--project-id`: Project ID to download from
- `--form-id`: Form ID to download from
- `--output`, `-o`: Output file path
- `--format`: Output format (csv, excel, json)

**Examples:**

```bash
# Download using configured settings
mega2-cli download

# Download to specific file
mega2-cli download -o survey_data.xlsx --format excel

# Download with explicit parameters
mega2-cli download --url https://odk.example.com \
                   --project-id 1 \
                   --form-id my_form \
                   -o data.csv
```

### `report` - Generate PDF Report

Generate a formatted PDF report from data file.

```bash
mega2-cli report INPUT_FILE OUTPUT_FILE [OPTIONS]
```

**Arguments:**
- `INPUT_FILE`: Input data file (CSV, Excel, or JSON)
- `OUTPUT_FILE`: Output PDF file path

**Options:**
- `--title`: Custom report title

**Examples:**

```bash
# Basic report generation
mega2-cli report data.csv report.pdf

# Report with custom title
mega2-cli report data.csv monthly_report.pdf --title "Monthly Survey Analysis"
```

### `list` - List Projects and Forms

List available projects and forms on ODK Central server.

```bash
mega2-cli list [OPTIONS]
```

**Options:**
- `--url`: ODK Central base URL
- `--email`: Email for authentication
- `--password`: Password for authentication
- `--project-id`: Project ID to list forms for

**Examples:**

```bash
# List all projects
mega2-cli list

# List forms in specific project
mega2-cli list --project-id 1

# List with explicit credentials
mega2-cli list --url https://odk.example.com --email user@example.com
```

### `config` - Manage Configuration

Manage application configuration settings.

```bash
mega2-cli config ACTION
```

**Actions:**
- `setup`: Interactive configuration setup
- `show`: Display current configuration
- `sample`: Create sample configuration file

**Examples:**

```bash
# Interactive setup
mega2-cli config setup

# Show current configuration
mega2-cli config show

# Create sample configuration
mega2-cli config sample
```

## Configuration

### Configuration Directory

By default, configuration files are stored in `~/.mega2_cli/`. You can specify a custom directory using the `--config-dir` option.

### Configuration Files

- `config.yaml`: Main configuration file
- `config_sample.yaml`: Sample configuration (created by `config sample` command)

### Sample Configuration

```yaml
odk:
  base_url: https://your-odk-central-server.com
  email: your-email@example.com
  project_id: "1"
  form_id: your-form-id

report:
  title: ODK Central Data Report
  include_summary: true
  include_charts: true
  include_data_table: true
  max_table_rows: 20
  page_size: A4
  chart_configs:
    - type: bar
      x_col: status
      title: Submission Status Distribution
    - type: pie
      x_col: category
      title: Category Distribution
```

## Report Customization

### Chart Types

The PDF generator supports various chart types:

- `bar`: Bar charts for categorical data
- `pie`: Pie charts for proportional data
- `histogram`: Histograms for numeric distributions
- `scatter`: Scatter plots for correlations
- `line`: Line plots for trends

### Chart Configuration

You can customize charts in the configuration file:

```yaml
report:
  chart_configs:
    - type: bar
      x_col: region
      title: Submissions by Region
    - type: histogram
      x_col: age
      title: Age Distribution
    - type: scatter
      x_col: income
      y_col: satisfaction
      title: Income vs Satisfaction
```

## Error Handling

The application includes comprehensive error handling:

- **Authentication errors**: Clear messages for login failures
- **Network errors**: Timeout and connection error handling
- **Data errors**: Validation of data formats and content
- **File errors**: Permission and path validation

## Logging

Use the `--verbose` flag for detailed logging:

```bash
mega2-cli download --verbose
```

Logs include:
- Authentication status
- Download progress
- Error details
- Performance metrics

## Security Considerations

- **Password Storage**: Passwords are not stored in configuration files by default
- **Secure Authentication**: Uses ODK Central's session token authentication
- **Network Security**: All API calls use HTTPS
- **File Permissions**: Configuration files have restricted permissions

## Requirements

- Python 3.8 or higher
- Internet connection for ODK Central access
- Sufficient disk space for data downloads and reports

## Dependencies

Core dependencies:
- `pandas`: Data manipulation and analysis
- `requests`: HTTP client for ODK Central API
- `reportlab`: PDF generation
- `matplotlib`: Data visualization
- `seaborn`: Statistical data visualization
- `PyYAML`: Configuration file parsing
- `openpyxl`: Excel file support

## Troubleshooting

### Common Issues

**Authentication Failed**
- Verify ODK Central URL is correct
- Check email and password
- Ensure user has access to the project

**No Data Downloaded**
- Verify project and form IDs exist
- Check if form has submissions
- Ensure user has access to form data

**PDF Generation Failed**
- Check input data format is supported
- Verify output directory is writable
- Check for sufficient disk space

**Configuration Not Found**
- Run `mega2-cli config setup` to create configuration
- Verify configuration directory permissions
- Use `--config-dir` to specify custom location

### Getting Help

For additional help:
- Use `mega2-cli --help` for command overview
- Use `mega2-cli COMMAND --help` for command-specific help
- Check the GitHub repository for documentation and issues
- Enable verbose logging with `--verbose` for detailed error information

## Contributing

Contributions are welcome! Please see the project repository for:
- Bug reports and feature requests
- Development guidelines
- Code contribution process

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Changelog

### Version 1.0.0
- Initial release
- ODK Central integration
- PDF report generation
- CLI interface
- Configuration management
- Multiple output formats