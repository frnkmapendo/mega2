"""
Basic tests for MEGA 2.0 CLI application.
"""

import unittest
import tempfile
import os
import pandas as pd
from pathlib import Path

# Import our modules
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from mega2_cli.config import ConfigManager, ODKConfig, ReportConfig
from mega2_cli.pdf_generator import PDFReportGenerator
from mega2_cli.odk_client import ODKCentralAPI


class TestConfigManager(unittest.TestCase):
    """Test configuration management functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_manager = ConfigManager(self.temp_dir)
    
    def test_odk_config_creation(self):
        """Test ODK configuration creation."""
        config = ODKConfig(
            base_url="https://test.example.com",
            email="test@example.com",
            password="testpass",
            project_id="1",
            form_id="test_form"
        )
        
        self.assertTrue(config.is_valid())
        self.assertEqual(config.base_url, "https://test.example.com")
        self.assertEqual(config.email, "test@example.com")
    
    def test_report_config_creation(self):
        """Test report configuration creation."""
        config = ReportConfig(
            title="Test Report",
            include_summary=True,
            include_charts=False,
            max_table_rows=10
        )
        
        self.assertEqual(config.title, "Test Report")
        self.assertTrue(config.include_summary)
        self.assertFalse(config.include_charts)
        self.assertEqual(config.max_table_rows, 10)
    
    def test_config_save_load(self):
        """Test saving and loading configuration."""
        # Create test ODK config
        odk_config = ODKConfig(
            base_url="https://test.example.com",
            email="test@example.com",
            project_id="1"
        )
        
        # Save config (without password)
        success = self.config_manager.save_odk_config(odk_config, save_password=False)
        self.assertTrue(success)
        
        # Load config
        loaded_config = self.config_manager.load_odk_config()
        self.assertEqual(loaded_config.base_url, "https://test.example.com")
        self.assertEqual(loaded_config.email, "test@example.com")
        self.assertEqual(loaded_config.project_id, "1")
        # Password should not be loaded since we didn't save it
        self.assertEqual(loaded_config.password, "")
    
    def test_sample_config_creation(self):
        """Test sample configuration creation."""
        success = self.config_manager.create_sample_config()
        self.assertTrue(success)
        
        sample_file = self.config_manager.config_dir / "config_sample.yaml"
        self.assertTrue(sample_file.exists())


class TestPDFGenerator(unittest.TestCase):
    """Test PDF report generation functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.generator = PDFReportGenerator("Test Report")
        
        # Create sample data
        self.sample_data = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
            'age': [25, 30, 35, 28],
            'city': ['New York', 'London', 'Tokyo', 'Paris'],
            'score': [85, 92, 78, 90],
            'status': ['active', 'active', 'inactive', 'active']
        })
    
    def test_summary_statistics_generation(self):
        """Test summary statistics generation."""
        summary = self.generator.generate_summary_statistics(self.sample_data)
        
        self.assertEqual(summary['total_submissions'], 4)
        self.assertEqual(summary['columns_count'], 5)
        self.assertGreater(len(summary['numeric_columns']), 0)
        self.assertGreater(len(summary['categorical_columns']), 0)
    
    def test_data_table_creation(self):
        """Test data table creation."""
        table = self.generator.create_data_table(self.sample_data, max_rows=3)
        self.assertIsNotNone(table)
    
    def test_pdf_generation(self):
        """Test PDF report generation."""
        output_file = os.path.join(self.temp_dir, "test_report.pdf")
        
        success = self.generator.generate_report(self.sample_data, output_file)
        self.assertTrue(success)
        self.assertTrue(os.path.exists(output_file))
        
        # Check file is not empty
        self.assertGreater(os.path.getsize(output_file), 0)


class TestODKClient(unittest.TestCase):
    """Test ODK Central API client functionality."""
    
    def test_client_initialization(self):
        """Test ODK client initialization."""
        client = ODKCentralAPI(
            base_url="https://test.example.com",
            project_id="1",
            form_id="test_form"
        )
        
        self.assertEqual(client.base_url, "https://test.example.com")
        self.assertEqual(client.project_id, "1")
        self.assertEqual(client.form_id, "test_form")
        self.assertIsNone(client.token)
    
    def test_credentials_management(self):
        """Test credentials setting and clearing."""
        client = ODKCentralAPI("https://test.example.com")
        
        # Set credentials
        client.set_credentials("test@example.com", "testpass")
        self.assertEqual(client.email, "test@example.com")
        self.assertEqual(client.password, "testpass")
        
        # Set token
        client.set_token("test_token")
        self.assertEqual(client.token, "test_token")
        
        # Clear credentials
        client.clear_credentials()
        self.assertIsNone(client.email)
        self.assertIsNone(client.password)
        self.assertIsNone(client.token)


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete workflow."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create sample CSV data
        self.sample_data = pd.DataFrame({
            'submission_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'respondent_name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'satisfaction': [4, 5, 3],
            'region': ['North', 'South', 'East']
        })
        
        self.csv_file = os.path.join(self.temp_dir, "test_data.csv")
        self.sample_data.to_csv(self.csv_file, index=False)
    
    def test_full_report_workflow(self):
        """Test the complete workflow from data to PDF report."""
        # Initialize configuration
        config_manager = ConfigManager(self.temp_dir)
        
        # Create report config
        report_config = ReportConfig(
            title="Integration Test Report",
            include_summary=True,
            include_charts=True,
            include_data_table=True,
            max_table_rows=5
        )
        
        # Save config
        success = config_manager.save_report_config(report_config)
        self.assertTrue(success)
        
        # Load config
        loaded_config = config_manager.load_report_config()
        self.assertEqual(loaded_config.title, "Integration Test Report")
        
        # Generate PDF report
        output_file = os.path.join(self.temp_dir, "integration_test_report.pdf")
        generator = PDFReportGenerator(loaded_config.title)
        
        # Load data
        data = pd.read_csv(self.csv_file)
        
        # Generate report
        config_dict = {
            'include_summary': loaded_config.include_summary,
            'include_charts': loaded_config.include_charts,
            'include_data_table': loaded_config.include_data_table,
            'max_table_rows': loaded_config.max_table_rows
        }
        
        success = generator.generate_report(data, output_file, config_dict)
        self.assertTrue(success)
        self.assertTrue(os.path.exists(output_file))
        self.assertGreater(os.path.getsize(output_file), 1000)  # Should be a reasonable size


if __name__ == '__main__':
    unittest.main()