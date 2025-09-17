"""
Configuration management for MEGA 2.0 CLI application.
Handles ODK Central credentials and report settings.
"""

import os
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import getpass


@dataclass
class ODKConfig:
    """ODK Central server configuration."""
    base_url: str = ""
    email: str = ""
    password: str = ""
    project_id: str = ""
    form_id: str = ""
    
    def is_valid(self) -> bool:
        """Check if configuration has required fields."""
        return bool(self.base_url and self.email and self.password)


@dataclass 
class ReportConfig:
    """PDF report configuration."""
    title: str = "ODK Central Data Report"
    include_summary: bool = True
    include_charts: bool = True
    include_data_table: bool = True
    max_table_rows: int = 20
    page_size: str = "A4"
    chart_configs: list = None
    
    def __post_init__(self):
        if self.chart_configs is None:
            self.chart_configs = []


class ConfigManager:
    """Manage application configuration files."""
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_dir: Custom configuration directory (optional)
        """
        if config_dir:
            self.config_dir = Path(config_dir)
        else:
            # Use user's home directory
            self.config_dir = Path.home() / ".mega2_cli"
        
        self.config_dir.mkdir(exist_ok=True)
        self.config_file = self.config_dir / "config.yaml"
        self.credentials_file = self.config_dir / "credentials.json"
        
    def save_odk_config(self, config: ODKConfig, save_password: bool = False) -> bool:
        """
        Save ODK configuration to file.
        
        Args:
            config: ODK configuration object
            save_password: Whether to save password (not recommended)
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            config_data = asdict(config)
            
            if not save_password:
                # Remove password from saved config
                config_data.pop('password', None)
            
            with open(self.config_file, 'w') as f:
                yaml.dump({'odk': config_data}, f, default_flow_style=False)
            
            logging.info(f"ODK configuration saved to {self.config_file}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to save ODK configuration: {e}")
            return False
    
    def load_odk_config(self) -> ODKConfig:
        """
        Load ODK configuration from file.
        
        Returns:
            ODK configuration object
        """
        config = ODKConfig()
        
        if not self.config_file.exists():
            return config
            
        try:
            with open(self.config_file, 'r') as f:
                data = yaml.safe_load(f)
            
            if data and 'odk' in data:
                odk_data = data['odk']
                config.base_url = odk_data.get('base_url', '')
                config.email = odk_data.get('email', '')
                config.password = odk_data.get('password', '')
                config.project_id = odk_data.get('project_id', '')
                config.form_id = odk_data.get('form_id', '')
            
            return config
            
        except Exception as e:
            logging.error(f"Failed to load ODK configuration: {e}")
            return config
    
    def save_report_config(self, config: ReportConfig) -> bool:
        """
        Save report configuration to file.
        
        Args:
            config: Report configuration object
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # Load existing config if present
            config_data = {}
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    config_data = yaml.safe_load(f) or {}
            
            # Update with report config
            config_data['report'] = asdict(config)
            
            with open(self.config_file, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False)
            
            logging.info(f"Report configuration saved to {self.config_file}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to save report configuration: {e}")
            return False
    
    def load_report_config(self) -> ReportConfig:
        """
        Load report configuration from file.
        
        Returns:
            Report configuration object
        """
        config = ReportConfig()
        
        if not self.config_file.exists():
            return config
            
        try:
            with open(self.config_file, 'r') as f:
                data = yaml.safe_load(f)
            
            if data and 'report' in data:
                report_data = data['report']
                config.title = report_data.get('title', config.title)
                config.include_summary = report_data.get('include_summary', config.include_summary)
                config.include_charts = report_data.get('include_charts', config.include_charts)
                config.include_data_table = report_data.get('include_data_table', config.include_data_table)
                config.max_table_rows = report_data.get('max_table_rows', config.max_table_rows)
                config.page_size = report_data.get('page_size', config.page_size)
                config.chart_configs = report_data.get('chart_configs', config.chart_configs)
            
            return config
            
        except Exception as e:
            logging.error(f"Failed to load report configuration: {e}")
            return config
    
    def interactive_odk_setup(self) -> ODKConfig:
        """
        Interactive setup for ODK Central configuration.
        
        Returns:
            ODK configuration object
        """
        print("ODK Central Configuration Setup")
        print("=" * 35)
        
        config = ODKConfig()
        
        # Load existing config if available
        existing_config = self.load_odk_config()
        
        # Base URL
        default_url = existing_config.base_url
        url_prompt = f"ODK Central Base URL{f' [{default_url}]' if default_url else ''}: "
        config.base_url = input(url_prompt).strip() or default_url
        
        # Email
        default_email = existing_config.email
        email_prompt = f"Email{f' [{default_email}]' if default_email else ''}: "
        config.email = input(email_prompt).strip() or default_email
        
        # Password
        config.password = getpass.getpass("Password: ")
        
        # Project ID
        default_project = existing_config.project_id
        project_prompt = f"Project ID{f' [{default_project}]' if default_project else ''} (optional): "
        config.project_id = input(project_prompt).strip() or default_project
        
        # Form ID
        default_form = existing_config.form_id
        form_prompt = f"Form ID{f' [{default_form}]' if default_form else ''} (optional): "
        config.form_id = input(form_prompt).strip() or default_form
        
        return config
    
    def interactive_report_setup(self) -> ReportConfig:
        """
        Interactive setup for report configuration.
        
        Returns:
            Report configuration object
        """
        print("\nReport Configuration Setup")
        print("=" * 30)
        
        config = ReportConfig()
        
        # Load existing config if available
        existing_config = self.load_report_config()
        
        # Report title
        default_title = existing_config.title
        title_prompt = f"Report title [{default_title}]: "
        config.title = input(title_prompt).strip() or default_title
        
        # Include summary
        summary_prompt = f"Include summary statistics? (y/n) [{'y' if existing_config.include_summary else 'n'}]: "
        summary_input = input(summary_prompt).strip().lower()
        if summary_input:
            config.include_summary = summary_input.startswith('y')
        else:
            config.include_summary = existing_config.include_summary
        
        # Include charts
        charts_prompt = f"Include charts? (y/n) [{'y' if existing_config.include_charts else 'n'}]: "
        charts_input = input(charts_prompt).strip().lower()
        if charts_input:
            config.include_charts = charts_input.startswith('y')
        else:
            config.include_charts = existing_config.include_charts
        
        # Include data table
        table_prompt = f"Include data table? (y/n) [{'y' if existing_config.include_data_table else 'n'}]: "
        table_input = input(table_prompt).strip().lower()
        if table_input:
            config.include_data_table = table_input.startswith('y')
        else:
            config.include_data_table = existing_config.include_data_table
        
        # Max table rows
        rows_prompt = f"Maximum table rows [{existing_config.max_table_rows}]: "
        rows_input = input(rows_prompt).strip()
        if rows_input.isdigit():
            config.max_table_rows = int(rows_input)
        else:
            config.max_table_rows = existing_config.max_table_rows
        
        return config
    
    def create_sample_config(self) -> bool:
        """
        Create a sample configuration file.
        
        Returns:
            True if created successfully, False otherwise
        """
        try:
            sample_config = {
                'odk': {
                    'base_url': 'https://your-odk-central-server.com',
                    'email': 'your-email@example.com',
                    'project_id': '1',
                    'form_id': 'your-form-id'
                },
                'report': {
                    'title': 'ODK Central Data Report',
                    'include_summary': True,
                    'include_charts': True,
                    'include_data_table': True,
                    'max_table_rows': 20,
                    'page_size': 'A4',
                    'chart_configs': [
                        {
                            'type': 'bar',
                            'x_col': 'status',
                            'title': 'Submission Status Distribution'
                        },
                        {
                            'type': 'pie',
                            'x_col': 'category',
                            'title': 'Category Distribution'
                        }
                    ]
                }
            }
            
            sample_file = self.config_dir / "config_sample.yaml"
            with open(sample_file, 'w') as f:
                yaml.dump(sample_config, f, default_flow_style=False)
            
            print(f"Sample configuration created at: {sample_file}")
            print("Copy this to config.yaml and modify as needed.")
            return True
            
        except Exception as e:
            logging.error(f"Failed to create sample configuration: {e}")
            return False
    
    def show_config_status(self) -> None:
        """Display current configuration status."""
        print(f"Configuration Directory: {self.config_dir}")
        print(f"Configuration File: {self.config_file}")
        print()
        
        # ODK Config status
        odk_config = self.load_odk_config()
        print("ODK Central Configuration:")
        print(f"  Base URL: {odk_config.base_url or 'Not set'}")
        print(f"  Email: {odk_config.email or 'Not set'}")
        print(f"  Password: {'Set' if odk_config.password else 'Not set'}")
        print(f"  Project ID: {odk_config.project_id or 'Not set'}")
        print(f"  Form ID: {odk_config.form_id or 'Not set'}")
        print(f"  Valid: {'Yes' if odk_config.is_valid() else 'No'}")
        print()
        
        # Report Config status
        report_config = self.load_report_config()
        print("Report Configuration:")
        print(f"  Title: {report_config.title}")
        print(f"  Include Summary: {report_config.include_summary}")
        print(f"  Include Charts: {report_config.include_charts}")
        print(f"  Include Data Table: {report_config.include_data_table}")
        print(f"  Max Table Rows: {report_config.max_table_rows}")
        print(f"  Page Size: {report_config.page_size}")
        print(f"  Chart Configs: {len(report_config.chart_configs)} defined")
    
    def get_config_dir(self) -> Path:
        """Get the configuration directory path."""
        return self.config_dir