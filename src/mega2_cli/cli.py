"""
Command-line interface for MEGA 2.0 CLI application.
Main entry point for the ODK Central data downloader and PDF report generator.
"""

import argparse
import sys
import logging
import os
from pathlib import Path
from typing import Optional
import pandas as pd

from .odk_client import ODKCentralAPI
from .pdf_generator import PDFReportGenerator, create_default_report_config
from .config import ConfigManager, ODKConfig, ReportConfig


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def progress_callback(message: str) -> None:
    """Simple progress callback function."""
    print(f"[INFO] {message}")


def cmd_download(args) -> int:
    """Handle download command."""
    setup_logging(args.verbose)
    
    config_manager = ConfigManager(args.config_dir)
    odk_config = config_manager.load_odk_config()
    
    # Override config with command line arguments
    if args.url:
        odk_config.base_url = args.url
    if args.email:
        odk_config.email = args.email
    if args.password:
        odk_config.password = args.password
    if args.project_id:
        odk_config.project_id = args.project_id
    if args.form_id:
        odk_config.form_id = args.form_id
    
    # Check if we have required configuration
    if not odk_config.base_url:
        print("Error: ODK Central base URL not configured. Use 'mega2-cli config setup' or provide --url")
        return 1
    
    if not odk_config.email:
        print("Error: Email not configured. Use 'mega2-cli config setup' or provide --email")
        return 1
    
    if not odk_config.password:
        print("Error: Password not provided. Use --password or configure interactively")
        return 1
    
    # Initialize ODK client
    progress_callback("Initializing ODK Central client...")
    client = ODKCentralAPI(odk_config.base_url, odk_config.project_id, odk_config.form_id)
    client.set_credentials(odk_config.email, odk_config.password)
    
    # Authenticate
    progress_callback("Authenticating with ODK Central...")
    if not client.authenticate():
        print("Error: Authentication failed. Check your credentials.")
        return 1
    
    progress_callback("Authentication successful!")
    
    # If no project/form specified, list available options
    if not args.project_id and not odk_config.project_id:
        progress_callback("Fetching available projects...")
        projects = client.fetch_projects()
        
        if not projects:
            print("Error: No projects found or failed to fetch projects.")
            return 1
        
        print("\nAvailable projects:")
        for project in projects:
            print(f"  ID: {project.get('id', 'N/A')} - Name: {project.get('name', 'N/A')}")
        
        print("\nPlease specify a project ID using --project-id or configure it with 'mega2-cli config setup'")
        return 1
    
    project_id = args.project_id or odk_config.project_id
    
    if not args.form_id and not odk_config.form_id:
        progress_callback(f"Fetching forms for project {project_id}...")
        forms = client.fetch_forms(project_id)
        
        if not forms:
            print(f"Error: No forms found for project {project_id} or failed to fetch forms.")
            return 1
        
        print(f"\nAvailable forms in project {project_id}:")
        for form in forms:
            print(f"  ID: {form.get('xmlFormId', 'N/A')} - Name: {form.get('name', 'N/A')}")
        
        print("\nPlease specify a form ID using --form-id or configure it with 'mega2-cli config setup'")
        return 1
    
    form_id = args.form_id or odk_config.form_id
    
    # Download submissions
    progress_callback(f"Downloading submissions from project {project_id}, form {form_id}...")
    
    if args.output:
        # Download directly to file
        file_format = args.format.lower()
        success = client.download_submissions_to_file(args.output, project_id, form_id, file_format)
        
        if success:
            progress_callback(f"Data downloaded successfully to {args.output}")
            return 0
        else:
            print("Error: Failed to download data.")
            return 1
    else:
        # Download to DataFrame and display info
        df = client.fetch_submissions(project_id, form_id)
        
        if df.empty or "Error" in df.columns:
            print("Error: No data retrieved or error occurred during download.")
            return 1
        
        print(f"\nDownload successful!")
        print(f"Records: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        print(f"Column names: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        
        # Save to default file if no output specified
        default_output = f"odk_data_{project_id}_{form_id}.csv"
        df.to_csv(default_output, index=False)
        progress_callback(f"Data saved to {default_output}")
        
        return 0


def cmd_report(args) -> int:
    """Handle report generation command."""
    setup_logging(args.verbose)
    
    # Check if input file exists
    if not os.path.exists(args.input):
        print(f"Error: Input file '{args.input}' not found.")
        return 1
    
    progress_callback(f"Loading data from {args.input}...")
    
    # Load data
    try:
        file_ext = Path(args.input).suffix.lower()
        if file_ext == '.csv':
            df = pd.read_csv(args.input)
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(args.input)
        elif file_ext == '.json':
            df = pd.read_json(args.input)
        else:
            print(f"Error: Unsupported file format '{file_ext}'. Supported formats: csv, xlsx, json")
            return 1
    except Exception as e:
        print(f"Error loading data: {e}")
        return 1
    
    progress_callback(f"Loaded {len(df)} records with {len(df.columns)} columns")
    
    # Load report configuration
    config_manager = ConfigManager(args.config_dir)
    report_config = config_manager.load_report_config()
    
    # Override with command line arguments
    if args.title:
        report_config.title = args.title
    
    # Convert to dict for PDF generator
    config_dict = {
        'include_summary': report_config.include_summary,
        'include_charts': report_config.include_charts,
        'include_data_table': report_config.include_data_table,
        'max_table_rows': report_config.max_table_rows,
        'chart_configs': report_config.chart_configs
    }
    
    # Create PDF generator
    progress_callback("Generating PDF report...")
    generator = PDFReportGenerator(title=report_config.title)
    
    # Generate report
    success = generator.generate_report(df, args.output, config_dict)
    
    if success:
        progress_callback(f"Report generated successfully: {args.output}")
        return 0
    else:
        print("Error: Failed to generate PDF report.")
        return 1


def cmd_config(args) -> int:
    """Handle configuration commands."""
    setup_logging(args.verbose)
    
    config_manager = ConfigManager(args.config_dir)
    
    if args.config_action == 'setup':
        # Interactive setup
        print("MEGA 2.0 CLI Configuration Setup")
        print("=" * 40)
        
        # ODK setup
        odk_config = config_manager.interactive_odk_setup()
        save_password = input("\nSave password to config file? (not recommended) (y/n) [n]: ").strip().lower().startswith('y')
        config_manager.save_odk_config(odk_config, save_password)
        
        # Report setup
        report_config = config_manager.interactive_report_setup()
        config_manager.save_report_config(report_config)
        
        print("\nConfiguration saved successfully!")
        return 0
    
    elif args.config_action == 'show':
        # Show current configuration
        config_manager.show_config_status()
        return 0
    
    elif args.config_action == 'sample':
        # Create sample configuration
        config_manager.create_sample_config()
        return 0
    
    else:
        print(f"Unknown config action: {args.config_action}")
        return 1


def cmd_list(args) -> int:
    """Handle list command to show projects and forms."""
    setup_logging(args.verbose)
    
    config_manager = ConfigManager(args.config_dir)
    odk_config = config_manager.load_odk_config()
    
    # Override config with command line arguments
    if args.url:
        odk_config.base_url = args.url
    if args.email:
        odk_config.email = args.email
    if args.password:
        odk_config.password = args.password
    
    if not odk_config.base_url or not odk_config.email or not odk_config.password:
        print("Error: ODK Central credentials not configured. Use 'mega2-cli config setup' or provide command line options.")
        return 1
    
    # Initialize ODK client
    client = ODKCentralAPI(odk_config.base_url)
    client.set_credentials(odk_config.email, odk_config.password)
    
    # Authenticate
    progress_callback("Authenticating with ODK Central...")
    if not client.authenticate():
        print("Error: Authentication failed. Check your credentials.")
        return 1
    
    # List projects
    progress_callback("Fetching projects...")
    projects = client.fetch_projects()
    
    if not projects:
        print("No projects found or failed to fetch projects.")
        return 1
    
    print("\nAvailable Projects:")
    print("-" * 50)
    for project in projects:
        print(f"ID: {project.get('id', 'N/A'):>3} | Name: {project.get('name', 'N/A')}")
    
    # If project ID specified, list forms
    if args.project_id:
        progress_callback(f"Fetching forms for project {args.project_id}...")
        forms = client.fetch_forms(args.project_id)
        
        if forms:
            print(f"\nForms in Project {args.project_id}:")
            print("-" * 50)
            for form in forms:
                print(f"ID: {form.get('xmlFormId', 'N/A'):>20} | Name: {form.get('name', 'N/A')}")
        else:
            print(f"No forms found in project {args.project_id}")
    
    return 0


def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="MEGA 2.0 CLI - ODK Central Data Downloader and PDF Report Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='mega2-cli 1.0.0'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--config-dir',
        help='Custom configuration directory (default: ~/.mega2_cli)'
    )
    
    # Create subparsers
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Download command
    download_parser = subparsers.add_parser('download', help='Download data from ODK Central')
    download_parser.add_argument('--url', help='ODK Central base URL')
    download_parser.add_argument('--email', help='Email for authentication')
    download_parser.add_argument('--password', help='Password for authentication')
    download_parser.add_argument('--project-id', help='Project ID')
    download_parser.add_argument('--form-id', help='Form ID')
    download_parser.add_argument('--output', '-o', help='Output file path')
    download_parser.add_argument('--format', choices=['csv', 'excel', 'json'], default='csv',
                               help='Output format (default: csv)')
    download_parser.set_defaults(func=cmd_download)
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Generate PDF report from data')
    report_parser.add_argument('input', help='Input data file (csv, xlsx, json)')
    report_parser.add_argument('output', help='Output PDF file path')
    report_parser.add_argument('--title', help='Report title')
    report_parser.set_defaults(func=cmd_report)
    
    # Config command
    config_parser = subparsers.add_parser('config', help='Manage configuration')
    config_parser.add_argument('config_action', choices=['setup', 'show', 'sample'],
                              help='Configuration action')
    config_parser.set_defaults(func=cmd_config)
    
    # List command
    list_parser = subparsers.add_parser('list', help='List projects and forms')
    list_parser.add_argument('--url', help='ODK Central base URL')
    list_parser.add_argument('--email', help='Email for authentication')
    list_parser.add_argument('--password', help='Password for authentication')
    list_parser.add_argument('--project-id', help='Project ID to list forms for')
    list_parser.set_defaults(func=cmd_list)
    
    return parser


def main() -> int:
    """Main entry point for the CLI application."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())