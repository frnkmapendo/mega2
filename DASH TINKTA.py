import tkinter as tk
import ttkbootstrap as tb
from tkinter import ttk, messagebox, scrolledtext, filedialog
import requests
from io import StringIO, BytesIO
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import threading
from matplotlib.ticker import MaxNLocator
import json
from requests.auth import HTTPDigestAuth
import seaborn as sns
import datetime
from datetime import datetime, timezone, timedelta
import numpy as np
import matplotlib.figure as Figure
from matplotlib.patches import Rectangle
from ttkbootstrap.constants import *
import os
import sys
from ttkbootstrap.widgets import DateEntry
import sqlite3
import logging
import gc
import time
from functools import wraps
import hashlib
from pathlib import Path
from matplotlib.widgets import RectangleSelector
import matplotlib.transforms as transforms

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CURRENT_USER = os.getlogin()
VERSION = "2.0.1"  # Updated version number

def error_handler(func):
    """Decorator for comprehensive error handling"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            if hasattr(args[0], 'root') and args[0].root:
                try:
                    messagebox.showerror("Error", f"An error occurred in {func.__name__}: {str(e)}")
                except:
                    print(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

class ConfigManager:
    """Enhanced configuration manager"""
    
    def __init__(self):
        self.config_file = "dashboard_config.json"
        self.config = self.load_config()
    
    def load_config(self):
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load config: {e}")
        return self.get_default_config()
    
    def get_default_config(self):
        """Get default configuration"""
        return {
            "connection": {
                "url": "https://your-odk-central-url.com",
                "timeout": 30,
                "max_retries": 3
            },
            "ui": {
                "theme": "darkly",
                "window_width": 1600,
                "window_height": 1200,
                "auto_refresh": False,
                "auto_refresh_interval": 300
            },
            "data": {
                "cache_enabled": True,
                "max_cache_age_hours": 24,
                "display_limit": 1000
            }
        }
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save config: {e}")

class DataCache:
    """Enhanced data caching system"""
    
    def __init__(self):
        self.db_path = "dashboard_cache.db"
        self.init_database()
    
    def init_database(self):
        """Initialize cache database"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cache_data (
                    id INTEGER PRIMARY KEY,
                    cache_key TEXT UNIQUE,
                    timestamp TEXT,
                    form_id TEXT,
                    record_count INTEGER,
                    data_blob BLOB
                )
            ''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Cache database initialization failed: {e}")
    
    def cache_data(self, cache_key, data, form_id):
        """Cache data with metadata"""
        try:
            conn = sqlite3.connect(self.db_path)
            # Create a BytesIO object to store the pickled data
            data_blob_buffer = BytesIO()
            data.to_pickle(data_blob_buffer)  # Fix: Pass BytesIO object as path
            data_blob = data_blob_buffer.getvalue()  # Get the binary data
            
            conn.execute('''
                INSERT OR REPLACE INTO cache_data 
                (cache_key, timestamp, form_id, record_count, data_blob)
                VALUES (?, ?, ?, ?, ?)
            ''', (cache_key, datetime.now().isoformat(), form_id, len(data), data_blob))
            
            conn.commit()
            conn.close()
            logger.info(f"Cached {len(data)} records for key: {cache_key}")
        except Exception as e:
            logger.error(f"Caching failed: {e}")
    
    def get_cached_data(self, cache_key, max_age_hours=24):
        """Retrieve cached data if not expired"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute('''
                SELECT timestamp, record_count, data_blob FROM cache_data 
                WHERE cache_key = ?
            ''', (cache_key,))
            
            result = cursor.fetchone()
            if not result:
                conn.close()
                return None
            
            timestamp_str, record_count, data_blob = result
            cache_time = datetime.fromisoformat(timestamp_str)
            
            if datetime.now() - cache_time > timedelta(hours=max_age_hours):
                conn.close()
                return None
            
            conn.close()
            data = pd.read_pickle(BytesIO(data_blob))
            logger.info(f"Retrieved {len(data)} cached records for key: {cache_key}")
            return data
            
        except Exception as e:
            logger.error(f"Cache retrieval failed: {e}")
            return None

class ODKDataManager:
    """Enhanced ODK data management"""
    
    def __init__(self, config_manager, cache_manager):
        self.config = config_manager
        self.cache = cache_manager
        self.session = requests.Session()
    
    def safe_api_call(self, url, auth, timeout=30, retries=3):
        """Safe API call with retry logic"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, auth=auth, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"API call failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
    
    def generate_cache_key(self, url, project_id, form_id):
        """Generate cache key for data"""
        key_string = f"{url}_{project_id}_{form_id}_{datetime.now().strftime('%Y%m%d')}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def fetch_submissions_with_all_columns(self, base_url, project_id, form_id, auth, use_cache=True):
        """Fetch submissions ensuring all form columns are included"""
        cache_key = self.generate_cache_key(base_url, project_id, form_id)
        
        # Try cache first
        if use_cache:
            cached_data = self.cache.get_cached_data(cache_key)
            if cached_data is not None:
                return cached_data, "Cached Data"
        
        try:
            # Method 1: Try to get CSV export (includes all columns)
            csv_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions.csv"
            csv_response = self.safe_api_call(csv_url, auth, timeout=60)
            
            if csv_response.status_code == 200:
                csv_data = StringIO(csv_response.text)
                df = pd.read_csv(csv_data)
                if use_cache and not df.empty:
                    self.cache.cache_data(cache_key, df, form_id)
                return df, "CSV Export"
                
        except Exception as e:
            logger.warning(f"CSV export failed: {e}")
        
        try:
            # Method 2: Get individual submissions with full data
            submissions_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions"
            response = self.safe_api_call(submissions_url, auth, timeout=30)
            
            submissions = response.json()
            if not submissions:
                return pd.DataFrame(), "Empty Response"
            
            # Process all submissions
            full_submissions = []
            for submission in submissions:
                instance_id = submission.get('instanceId')
                if instance_id:
                    try:
                        submission_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions/{instance_id}"
                        sub_response = self.safe_api_call(submission_url, auth, timeout=30)
                        if sub_response.status_code == 200:
                            full_data = sub_response.json()
                            full_submissions.append(full_data)
                    except Exception as e:
                        logger.warning(f"Failed to fetch individual submission {instance_id}: {e}")
            
            if full_submissions:
                df = pd.json_normalize(full_submissions)
                if use_cache and not df.empty:
                    self.cache.cache_data(cache_key, df, form_id)
                return df, "Individual Submissions"
                
        except Exception as e:
            logger.warning(f"Individual submissions method failed: {e}")
        
        try:
            # Method 3: Standard submissions endpoint with enhanced normalization
            submissions_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions"
            response = self.safe_api_call(submissions_url, auth, timeout=30)
            
            data = response.json()
            if not data:
                return pd.DataFrame(), "No Data"
            
            df = pd.json_normalize(data, sep='_')
            if use_cache and not df.empty:
                self.cache.cache_data(cache_key, df, form_id)
            
            return df, "Standard API"
            
        except Exception as e:
            logger.error(f"All fetch methods failed: {e}")
            raise e

class Dashboard:
    def __init__(self, root):
        self.root = root
        self.root.title(f"ODK Data Dashboard v{VERSION}")
        self.root.geometry("1600x1200")
        
        # Initialize managers
        self.config_manager = ConfigManager()
        self.cache_manager = DataCache()
        self.odk_manager = ODKDataManager(self.config_manager, self.cache_manager)
        
        # Initialize all variables first
        self.initialize_variables()
        self.initialize_dataframes()

        # Setup UI components
        self.setup_ui()

        # Create status bar
        self.create_status_bar()
        
        # Setup auto-refresh
        self.auto_refresh_job = None
        self.setup_auto_refresh()
        
        logger.info("Dashboard initialized successfully")
        
    def initialize_variables(self):
        """Initialize all variables"""
        # Connection variables
        self.url_var = tk.StringVar(value="https://your-odk-central-url.com")
        self.project_id_var = tk.StringVar()
        self.form_id_var = tk.StringVar()
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()

        # Filter variables
        self.filter_column_var = tk.StringVar()
        self.filter_value_var = tk.StringVar()
        self.date_from_var = tk.StringVar()
        self.date_to_var = tk.StringVar()
        
        # Visualization variables
        self.selected_column_var = tk.StringVar()
        self.chart_type_var = tk.StringVar()
        
        # Statistics variables
        self.submission_count_var = tk.StringVar(value="0")
        self.filtered_count_var = tk.StringVar(value="0")
        
        # Data storage
        self.dataframe = None
        self.filtered_df = None
        self.last_update_time = None
        self.form_schema = None
        self.form_labels = {}
        self.field_mappings = {}
        self.choice_mappings = {}
        self.survey_sheet = None
        self.choices_sheet = None
        
        # UI components
        self.tree = None
        self.notebook = None
        self.filter_column_combo = None
        self.column_combo = None
        self.chart_type_combo = None
        
        # Status variables
        self.status_var = tk.StringVar(value="Ready")
        
        # Auto-refresh
        self.auto_refresh_var = tk.BooleanVar()

    def initialize_dataframes(self):
        """Initialize dataframes with empty DataFrames"""
        self.dataframe = pd.DataFrame()
        self.filtered_df = pd.DataFrame()
        self.form_labels = {}
        self.field_mappings = {}
        self.choice_mappings = {}
        self.last_update_time = None

    def setup_auto_refresh(self):
        """Setup automatic data refresh"""
        def auto_refresh_callback():
            if self.auto_refresh_var.get() and self.dataframe is not None:
                logger.info("Auto-refreshing data")
                self.refresh_data()
            
            # Schedule next refresh
            interval = self.config_manager.config["ui"]["auto_refresh_interval"] * 1000
            self.auto_refresh_job = self.root.after(interval, auto_refresh_callback)
        
        # Start auto-refresh timer
        interval = self.config_manager.config["ui"]["auto_refresh_interval"] * 1000
        self.auto_refresh_job = self.root.after(interval, auto_refresh_callback)

    def create_summary_stats(self):
        """Create summary statistics display"""
        try:
            if not hasattr(self, 'stats_frame'):
                return

            # Clear existing stats
            for widget in self.stats_frame.winfo_children():
                widget.destroy()

            # Safely calculate statistics
            total_records = 0
            total_columns = 0
            last_updated = "Not yet updated"

            if hasattr(self, 'filtered_df') and isinstance(self.filtered_df, pd.DataFrame):
                total_records = len(self.filtered_df)
                total_columns = len(self.filtered_df.columns)

            if hasattr(self, 'last_update_time') and self.last_update_time:
                last_updated = self.last_update_time.strftime('%Y-%m-%d %H:%M:%S')

            # Define stats
            stats = {
                'Total Records': f"{total_records:,}",
                'Total Columns': f"{total_columns:,}",
                'Last Updated': last_updated
            }

            # Create stat widgets
            for label, value in stats.items():
                stat_frame = ttk.Frame(self.stats_frame)
                stat_frame.pack(side="left", padx=20)
                
                ttk.Label(stat_frame, 
                        text=label,
                        style='info.TLabel').pack()
                
                ttk.Label(stat_frame, 
                        text=value,
                        style='info.Inverse.TLabel').pack()

        except Exception as e:
            logger.error(f"Error creating summary stats: {e}")

    def update_summary_stats(self):
        """Update the summary statistics"""
        # Clear existing stats
        for widget in self.stats_frame.winfo_children():
            widget.destroy()
        
        # Recreate stats
        self.create_summary_stats()

    def create_status_bar(self):
        """Create the status bar"""
        self.status_bar = ttk.Label(
            self.root,
            textvariable=self.status_var,
            relief="sunken",
            anchor="w",
            padding=(5, 2)
        )
        self.status_bar.pack(side="bottom", fill="x")   

    def on_frame_configure(self, event):
        """Update the scrollable region based on the size of the charts frame"""
        self.viz_canvas.configure(scrollregion=self.viz_canvas.bbox("all"))

    def add_chart_placeholder(self):
        """Add a placeholder message when no charts are available"""
        if hasattr(self, 'chart_placeholder') and self.chart_placeholder:
            return

        self.chart_placeholder = ttk.Frame(self.charts_frame, style='light.TFrame')
        self.chart_placeholder.grid(row=0, column=0, columnspan=2, padx=20, pady=20, sticky="nsew")
        
        # Create placeholder content with modern styling
        placeholder_frame = ttk.Frame(self.chart_placeholder, padding=20)
        placeholder_frame.pack(expand=True, fill="both", padx=30, pady=30)
        
        # Add an icon
        ttk.Label(placeholder_frame, text="📊", font=('Arial', 48), foreground="#17a2b8").pack(pady=(20, 10))
        
        # Add title and instruction with modern typography
        ttk.Label(
            placeholder_frame,
            text="Your Dashboard Visualizations",
            font=('Helvetica', 16, 'bold'),
            foreground="#212529"
        ).pack(pady=(10, 5))
        
        ttk.Label(
            placeholder_frame,
            text="Select a variable and chart type above, then click 'Add Chart'",
            wraplength=400,
            foreground="#6c757d"
        ).pack(pady=5)

    @error_handler        
    def setup_ui(self):
        """Set up the user interface"""
        # Create menu bar
        self.create_menu_bar()
        
        # User info frame
        self.create_user_info_frame()
        
        # Connection frame (now includes filter controls)
        self.create_connection_frame()
        
        # Data frame
        self.create_data_frame()
        
        # Setup individual components
        self.setup_treeview()
        self.setup_visualization_tab()
        
        # Bind keyboard shortcuts
        self.bind_keyboard_shortcuts()

    def create_menu_bar(self):
        """Create comprehensive menu bar"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Load Survey File", command=self.load_survey_file, accelerator="Ctrl+O")
        file_menu.add_command(label="Export Data", command=self.export_csv_with_labels, accelerator="Ctrl+E")
        file_menu.add_command(label="Export Data Dictionary", command=self.create_data_dictionary)
        file_menu.add_separator()
        file_menu.add_command(label="Settings", command=self.show_settings)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit, accelerator="Ctrl+Q")
        
        # Data menu
        data_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Data", menu=data_menu)
        data_menu.add_command(label="Fetch Data", command=self.download_data, accelerator="F5")
        data_menu.add_command(label="Refresh", command=self.refresh_data, accelerator="Ctrl+R")
        data_menu.add_command(label="Test Connection", command=self.test_connection)
        data_menu.add_separator()
        data_menu.add_checkbutton(label="Auto Refresh", variable=self.auto_refresh_var)
        data_menu.add_separator()
        data_menu.add_command(label="Clear Cache", command=self.clear_cache)
        data_menu.add_command(label="Data Validation", command=self.validate_data_integrity)
        
        # View menu
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="View", menu=view_menu)
        view_menu.add_command(label="Column Information", command=self.show_column_info)
        view_menu.add_command(label="Data Labels", command=self.show_labels_window, accelerator="Ctrl+L")
        view_menu.add_command(label="Statistics", command=self.show_statistics_window)
        
        # Tools menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="Clear All Charts", command=self.clear_all_charts)
        tools_menu.add_command(label="Memory Usage", command=self.show_memory_usage)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="Keyboard Shortcuts", command=self.show_shortcuts)
        help_menu.add_command(label="About", command=self.show_about)

    def bind_keyboard_shortcuts(self):
        """Bind keyboard shortcuts"""
        self.root.bind('<Control-o>', lambda e: self.load_survey_file())
        self.root.bind('<Control-e>', lambda e: self.export_csv_with_labels())
        self.root.bind('<Control-r>', lambda e: self.refresh_data())
        self.root.bind('<Control-l>', lambda e: self.show_labels_window())
        self.root.bind('<Control-q>', lambda e: self.root.quit())
        self.root.bind('<F5>', lambda e: self.download_data())

    def create_user_info_frame(self):
        """Create user info frame"""
        user_frame = tk.Frame(
            self.root,
            bg='#2b2b2b',
            relief='ridge',
            borderwidth=2
        )
        user_frame.pack(fill="x", padx=10, pady=5)

        inner_frame = tk.Frame(
            user_frame,
            bg='#2b2b2b',
            pady=8,
            padx=15
        )
        inner_frame.pack(fill="x")

        # Get current time in UTC
        current_time = datetime.now(timezone.utc)
        time_str = current_time.strftime('%Y-%m-%d')

        # Create the date label with fancy styling
        date_label = tk.Label(
            inner_frame,
            text=f"📅 Date: {time_str}",
            font=('Arial', 11, 'bold'),
            bg="#670808",
            fg='#00ff41',
            relief='groove',
            borderwidth=1,
            padx=10,
            pady=5
        )
        date_label.pack(side="right")

        # Welcome message
        welcome_label = tk.Label(
            inner_frame,
            text=f"👤 Welcome {CURRENT_USER} | Dashboard v{VERSION}",
            font=('calibri', 15, 'bold'),
            bg="#510505",
            fg="#18045F",  
            padx=10,
            pady=5
        )
        welcome_label.pack(side="left")

    def create_connection_frame(self):
        """Create connection frame"""
        connection_frame = ttk.LabelFrame(self.root, text="ODK Connection", padding=10)
        connection_frame.pack(fill="x", padx=10, pady=5)
        
        # Define styles
        style = ttk.Style()
        style.configure("TLabel", foreground="blue", background="#252323", font=("Segoe UI", 10))
        style.configure("TEntry", fieldbackground="#1e1e1e", foreground="lime", insertcolor="yellow")

        # Create left frame for connection inputs
        left_frame = ttk.Frame(connection_frame)
        left_frame.pack(side="left", fill="both", expand=True)
        
        # Create middle frame for filter controls
        middle_frame = ttk.Frame(connection_frame)
        middle_frame.pack(side="left", fill="y", padx=(10, 5))
        
        # Create right frame for statistics
        right_frame = ttk.Frame(connection_frame)
        right_frame.pack(side="right", fill="y", padx=(5, 0))
        
        # Connection form in left frame
        ttk.Label(left_frame, text="ODK URL:").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Entry(left_frame, textvariable=self.url_var, width=40).grid(row=0, column=1, pady=2, padx=5)
        
        ttk.Label(left_frame, text="Project ID:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(left_frame, textvariable=self.project_id_var, width=40).grid(row=1, column=1, pady=2, padx=5)
        
        ttk.Label(left_frame, text="Form ID:").grid(row=2, column=0, sticky="w", pady=2)
        ttk.Entry(left_frame, textvariable=self.form_id_var, width=40).grid(row=2, column=1, pady=2, padx=5)
        
        ttk.Label(left_frame, text="Username:").grid(row=0, column=2, sticky="w", pady=2, padx=(20, 0))
        ttk.Entry(left_frame, textvariable=self.username_var, width=20).grid(row=0, column=3, pady=2, padx=5)
        
        ttk.Label(left_frame, text="Password:").grid(row=1, column=2, sticky="w", pady=2, padx=(20, 0))
        ttk.Entry(left_frame, textvariable=self.password_var, show="*", width=20).grid(row=1, column=3, pady=2, padx=5)
        
        # Connection buttons
        btn_frame = ttk.Frame(left_frame)
        btn_frame.grid(row=3, column=0, columnspan=4, pady=10)
        
        ttk.Button(btn_frame, text="Test Connection", command=self.test_connection).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Fetch Data", command=self.download_data).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Refresh", command=self.refresh_data).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Export data", command=self.export_csv_with_labels).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Data Labels", command=self.show_labels_window).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Load Survey File", command=self.load_survey_file).pack(side="left", padx=5)

        # Progress Bar
        progress_frame = ttk.Frame(left_frame)
        progress_frame.grid(row=4, column=0, columnspan=4, pady=5)
        self.progress = ttk.Progressbar(progress_frame, orient=HORIZONTAL, length=400, mode='determinate')
        self.progress.pack(fill="x")
        
        # Filter controls in the middle frame
        filter_frame = ttk.LabelFrame(middle_frame, text="Filters", padding=(5, 5, 5, 5))
        filter_frame.pack(fill="both", expand=True)
        
        # Filter column controls
        filter_columns_frame = ttk.Frame(filter_frame)
        filter_columns_frame.pack(fill="x", padx=2, pady=(5, 2))
        
        ttk.Label(filter_columns_frame, text="Filter variable:").grid(row=0, column=0, sticky="w")
        self.filter_column_combo = ttk.Combobox(filter_columns_frame, textvariable=self.filter_column_var, width=10)
        self.filter_column_combo.grid(row=0, column=1, padx=(2, 5))
        
        ttk.Label(filter_columns_frame, text="Filter Value:").grid(row=0, column=2, sticky="w")
        ttk.Entry(filter_columns_frame, textvariable=self.filter_value_var, width=10).grid(row=0, column=3, padx=(2, 5))
        
        # Date filter controls - adjusted to reduce spacing
        date_frame = ttk.Frame(filter_frame)
        date_frame.pack(fill="x", padx=2, pady=(5, 2))
        
        # Date From with minimal padding between label and control
        date_from_label = ttk.Label(date_frame, text="Date From:")
        date_from_label.grid(row=0, column=0, sticky="w")
        
        date_from_calendar = DateEntry(
            date_frame,
            dateformat='%Y-%m-%d',
            width=10,  
            borderwidth=2,
            firstweekday=0,
            startdate=datetime.now().date() - timedelta(days=30),
            bootstyle="primary"
        )
        date_from_calendar.grid(row=0, column=1, padx=(2, 10), pady=2) 
        
        # Date To with minimal padding
        date_to_label = ttk.Label(date_frame, text="Date To:")
        date_to_label.grid(row=0, column=2, sticky="w")
        
        date_to_calendar = DateEntry(
            date_frame,
            dateformat='%Y-%m-%d',
            width=10,  
            borderwidth=2,
            firstweekday=0,
            startdate=datetime.now().date(),
            bootstyle="secondary"
        )
        date_to_calendar.grid(row=0, column=3, padx=(2, 0), pady=2)  # Reduced padding
        
        # Connect date widgets to variables
        def update_date_from(event):
            self.date_from_var.set(date_from_calendar.entry.get())
        
        def update_date_to(event):
            self.date_to_var.set(date_to_calendar.entry.get())
        
        date_from_calendar.bind('<<DateEntrySelected>>', update_date_from)
        date_to_calendar.bind('<<DateEntrySelected>>', update_date_to)
        
        # Filter buttons
        buttons_frame = ttk.Frame(filter_frame)
        buttons_frame.pack(fill="x", padx=2, pady=(5, 2))
        
        ttk.Button(buttons_frame, text="Apply Filters", 
                command=self.apply_filters, style='success.TButton').pack(side="left", padx=2)
        ttk.Button(buttons_frame, text="Reset Filters", 
                command=self.reset_filters, style='warning.TButton').pack(side="right", padx=2)
        
        # Data submission statistics in right frame
        stats_frame = ttk.LabelFrame(right_frame, text="Data Submission", padding=10)
        stats_frame.pack(fill="both", expand=True)

        # Total submissions counter
        total_counter_frame = ttk.Frame(stats_frame, style='primary.TFrame')
        total_counter_frame.pack(side="top", padx=10, pady=5, fill="x")

        ttk.Label(total_counter_frame, text="Data Submitted", 
                style='primary.Inverse.TLabel').pack(pady=2)
        total_count_label = ttk.Label(total_counter_frame, 
                                    textvariable=self.submission_count_var,
                                    style='primary.TLabel',
                                    font=('Helvetica', 16, 'bold'))
        total_count_label.pack()

        # Filtered submissions counter
        filtered_counter_frame = ttk.Frame(stats_frame, style='info.TFrame')
        filtered_counter_frame.pack(side="top", padx=10, pady=5, fill="x")

        ttk.Label(filtered_counter_frame, text="Filtered ", 
                style='info.Inverse.TLabel').pack(pady=2)
        filtered_count_label = ttk.Label(filtered_counter_frame, 
                                        textvariable=self.filtered_count_var,
                                        style='info.TLabel',
                                        font=('calibri', 16, 'bold'))
        filtered_count_label.pack()

#################################
        #########

    def create_data_frame(self):
        """Create data frame"""
        data_frame = ttk.LabelFrame(self.root, text="Data View", padding=10)
        data_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Notebook for data views
        self.notebook = ttk.Notebook(data_frame)
        self.notebook.pack(fill="both", expand=True)

    def setup_treeview(self):
        """Set up the treeview widget"""
        # Create treeview frame
        tree_frame = ttk.Frame(self.notebook)
        self.notebook.add(tree_frame, text="Data Table")
        
        # Create scrollbars
        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical")
        x_scroll = ttk.Scrollbar(tree_frame, orient="horizontal")
        
        # Create treeview
        self.tree = ttk.Treeview(
            tree_frame,
            yscrollcommand=y_scroll.set,
            xscrollcommand=x_scroll.set
        )
        
        # Configure scrollbars
        y_scroll.config(command=self.tree.yview)
        x_scroll.config(command=self.tree.xview)
        
        # Grid layout
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        
        # Configure grid weights
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
############
    def setup_visualization_tab(self):
        """Set up the visualization tab with a single flexible frame for controls/stats and charts (cards)"""
        # Create visualization frame as a notebook tab
        viz_frame = ttk.Frame(self.notebook)
        self.notebook.add(viz_frame, text="Visualizations")

        # Main vertical layout
        main_frame = ttk.Frame(viz_frame)
        main_frame.pack(fill="both", expand=True)

        # Top: Controls and Stats in a single flexible horizontal frame
        controls_stats_frame = ttk.Frame(main_frame)
        controls_stats_frame.pack(fill="x", padx=5, pady=5)

        # Controls (left side)
        controls_frame = ttk.LabelFrame(controls_stats_frame, text="Visualization Controls", padding=10)
        controls_frame.pack(side="left", fill="x", expand=True)
        self.setup_visualization_controls(controls_frame)

        # Stats (right side)
        stats_frame = ttk.LabelFrame(controls_stats_frame, text="Dashboard Statistics", padding=10)
        stats_frame.pack(side="left", fill="x")
        self.stats_frame = ttk.Frame(stats_frame)
        self.stats_frame.pack(fill="x")
        self.create_summary_stats()

        # Charts: single flexible frame below controls_stats_frame
        charts_canvas_frame = ttk.Frame(main_frame)
        charts_canvas_frame.pack(fill="both", expand=True, padx=5, pady=5)

        # Scrollable canvas for charts
        self.viz_canvas = tk.Canvas(charts_canvas_frame, bg='#2b2b2b')
        scrollbar_y = ttk.Scrollbar(charts_canvas_frame, orient="vertical", command=self.viz_canvas.yview)
        scrollbar_x = ttk.Scrollbar(charts_canvas_frame, orient="horizontal", command=self.viz_canvas.xview)
        self.viz_canvas.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
        self.viz_canvas.pack(side="left", fill="both", expand=True)
        scrollbar_y.pack(side="right", fill="y")
        scrollbar_x.pack(side="bottom", fill="x")

        # Chart cards frame (inside canvas)
        self.charts_frame = ttk.Frame(self.viz_canvas)
        self.canvas_window = self.viz_canvas.create_window((0, 0), window=self.charts_frame, anchor="nw", tags=("win",))
        self.charts_frame.grid_columnconfigure(0, weight=1)
        self.charts_frame.grid_columnconfigure(1, weight=1)
        self.charts_frame.grid_columnconfigure(2, weight=1)
        self.charts_frame.bind('<Configure>', self.on_frame_configure)
        self.viz_canvas.bind('<Configure>', self.on_canvas_configure)

        # Initialize chart tracking
        self.chart_grid = []
        self.current_row = 0
        self.current_col = 0

        # Add placeholder if no charts
        self.add_chart_placeholder()
        # Bind zoom/pan as before
        self.viz_canvas.bind("<MouseWheel>", self.on_mousewheel)
        self.viz_canvas.bind("<Button-2>", self.start_pan)
        self.viz_canvas.bind("<B2-Motion>", self.pan_canvas)
########

    def setup_visualization_controls(self, controls_frame):
        """Set up the visualization control buttons and dropdowns"""
        ttk.Label(controls_frame, text="Select variable:").pack(side="left", padx=5)
        self.column_combo = ttk.Combobox(controls_frame, textvariable=self.selected_column_var)
        self.column_combo.pack(side="left", padx=5)

        ttk.Label(controls_frame, text="Chart Type:").pack(side="left", padx=5)
        chart_types = ["Time Series", "Distribution", "Correlation", "Pie Chart", 
                    "Horizontal Bar", "Stacked Bar"]
        self.chart_type_combo = ttk.Combobox(controls_frame, 
                                            textvariable=self.chart_type_var,
                                            values=chart_types)
        self.chart_type_combo.pack(side="left", padx=5)

        ttk.Button(controls_frame, 
                text="Add Chart",
                command=self.add_visualization,
                style='success.TButton').pack(side="left", padx=5)

        ttk.Button(controls_frame, 
                text="Clear All Charts",
                command=self.clear_all_charts,
                style='danger.TButton').pack(side="left", padx=5) 

    def setup_charts_canvas(self, parent):
        """Set up the scrollable canvas for charts with modern styling"""
        canvas_frame = ttk.Frame(parent)
        canvas_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Use a more dashboard-like background color
        self.viz_canvas = tk.Canvas(canvas_frame, bg='#2b2b2b')
        scrollbar_y = ttk.Scrollbar(canvas_frame, orient="vertical", 
                                command=self.viz_canvas.yview)
        scrollbar_x = ttk.Scrollbar(canvas_frame, orient="horizontal", 
                                command=self.viz_canvas.xview)
        self.viz_canvas.configure(yscrollcommand=scrollbar_y.set,
                                xscrollcommand=scrollbar_x.set)
        
        scrollbar_y.pack(side="right", fill="y")
        scrollbar_x.pack(side="bottom", fill="x")
        self.viz_canvas.pack(side="left", fill="both", expand=True)
        
        self.charts_frame = ttk.Frame(self.viz_canvas)
        self.canvas_window = self.viz_canvas.create_window(
            (0, 0),
            window=self.charts_frame,
            anchor="nw",
            tags=("win",)
        )
        
        # Configure grid layout - change to 3 columns
        self.charts_frame.grid_columnconfigure(0, weight=1)
        self.charts_frame.grid_columnconfigure(1, weight=1)
        self.charts_frame.grid_columnconfigure(2, weight=1)
        
        self.charts_frame.bind('<Configure>', self.on_frame_configure)
        self.viz_canvas.bind('<Configure>', self.on_canvas_configure)
        
        # Initialize chart tracking
        self.chart_grid = []
        self.current_row = 0
        self.current_col = 0
        
        # Add placeholder
        self.add_chart_placeholder()
        
        # Add bindings for mouse zoom/pan
        self.viz_canvas.bind("<MouseWheel>", self.on_mousewheel)
        self.viz_canvas.bind("<Button-2>", self.start_pan)
        self.viz_canvas.bind("<B2-Motion>", self.pan_canvas)

    # Add these new methods to handle zooming and panning
    def on_mousewheel(self, event):
        """Handle mouse wheel events for zooming"""
        # Determine zoom direction (in/out)
        if event.delta > 0:
            scale_factor = 1.1  # Zoom in
        else:
            scale_factor = 0.9  # Zoom out
        
        # Get the chart under the cursor
        chart_widget = event.widget.winfo_containing(event.x_root, event.y_root)
        
        # Find the relevant chart in the grid
        for chart_container in self.chart_grid:
            if chart_widget and chart_widget.winfo_toplevel() == chart_container.winfo_toplevel():
                # Find the matplotlib canvas widget in the chart container
                for child in chart_container.winfo_children():
                    if isinstance(child, FigureCanvasTkAgg):
                        # Apply zoom to the figure
                        fig = child.figure
                        current_xlim = fig.axes[0].get_xlim()
                        current_ylim = fig.axes[0].get_ylim()
                        
                        # Get the cursor position as data coordinates
                        ax = fig.axes[0]
                        transform = ax.transData.inverted()
                        mouse_x, mouse_y = transform.transform((event.x, event.y))
                        
                        # Calculate new limits
                        new_xlim = [
                            mouse_x - (mouse_x - current_xlim[0]) / scale_factor,
                            mouse_x + (current_xlim[1] - mouse_x) / scale_factor
                        ]
                        new_ylim = [
                            mouse_y - (mouse_y - current_ylim[0]) / scale_factor,
                            mouse_y + (current_ylim[1] - mouse_y) / scale_factor
                        ]
                        
                        # Apply new limits
                        ax.set_xlim(new_xlim)
                        ax.set_ylim(new_ylim)
                        child.draw_idle()
                        break

    def start_pan(self, event):
        """Start panning the canvas"""
        self.viz_canvas.scan_mark(event.x, event.y)

    def pan_canvas(self, event):
        """Pan the canvas with mouse movement"""
        self.viz_canvas.scan_dragto(event.x, event.y, gain=1)

    # Modify the add_visualization method to add zoom controls
    def add_visualization(self):
        """Add a new chart with better spacing and responsiveness"""
        if self.filtered_df is None or self.filtered_df.empty:
            messagebox.showwarning("Warning", "No data available for visualization")
            return
        
        # Remove placeholder if it exists
        if hasattr(self, 'chart_placeholder') and self.chart_placeholder:
            self.chart_placeholder.destroy()
            self.chart_placeholder = None
        
        # Create chart container with better spacing
        chart_container = ttk.Frame(self.charts_frame, padding=10)
        
        # Improved grid layout with better spacing
        row = self.current_row
        col = self.current_col
        
        chart_container.grid(
            row=row, 
            column=col,
            padx=15,  # Increased horizontal padding
            pady=15,  # Increased vertical padding
            sticky="nsew",
            ipadx=5,  # Internal padding
            ipady=5
        )
        
        # Configure grid weights for responsiveness
        self.charts_frame.grid_rowconfigure(row, weight=1)
        self.charts_frame.grid_columnconfigure(col, weight=1)
        
        # Create inner frame with responsive sizing
        inner_frame = ttk.Frame(chart_container, style='light.TFrame')
        inner_frame.pack(fill="both", expand=True, padx=5, pady=5)

        # Add minimal header with title and controls
        header_frame = ttk.Frame(inner_frame)
        header_frame.pack(fill="x", padx=2, pady=(0, 2))
        
        # Get chart title from selected column
        column_label = self.get_column_label(column)
        title_text = f"{self.chart_type_var.get()}: {column_label}"
        if len(title_text) > 30:
            title_text = title_text[:27] + "..."
        
        ttk.Label(
            header_frame, 
            text=title_text,
            font=('Helvetica', 9, 'bold')
        ).pack(side="left")
        
        # Control buttons - more compact
        btn_frame = ttk.Frame(header_frame)
        btn_frame.pack(side="right")
        
        # Add smaller control buttons
        ttk.Button(btn_frame, text="⟲", width=2, style='secondary.TButton',
                command=lambda: self.reset_chart_zoom(inner_frame)).pack(side="right", padx=1)
        
        ttk.Button(btn_frame, text="×", width=2, style='danger.TButton',
                command=lambda: self.remove_chart(chart_container)).pack(side="right", padx=1)
        
        # Content area for the actual chart (smaller)
        chart_frame = ttk.Frame(inner_frame)
        chart_frame.pack(fill="both", expand=True)
        
        try:
            # Create the visualization in the chart_frame - pass modern_style parameter
            if chart_type == "Time Series":
                self.create_time_series_plot(chart_frame, modern_style=True)
            elif chart_type == "Distribution":
                self.create_distribution_plot(chart_frame, modern_style=True)
            elif chart_type == "Correlation":
                self.create_correlation_plot(chart_frame, modern_style=True)
            elif chart_type == "Pie Chart":
                self.create_pie_chart(chart_frame, column, modern_style=True)
            elif chart_type == "Horizontal Bar":
                self.create_horizontal_bar_chart(chart_frame, column, modern_style=True)
            elif chart_type == "Stacked Bar":
                self.create_stacked_bar_chart(chart_frame, column, modern_style=True)
        except Exception as e:
            logger.error(f"Failed to create {chart_type} chart: {e}")
            messagebox.showerror("Chart Error", f"Failed to create chart: {str(e)}")
            chart_container.destroy()
            return
        
        # Update grid position (for next chart)
        self.current_col = (self.current_col + 1) % 3  # Changed from 2 to 3 columns
        if self.current_col == 0:
            self.current_row += 1
        
        # Track the chart
        self.chart_grid.append(chart_container)
        
        # Update canvas scroll region
        self.viz_canvas.configure(scrollregion=self.viz_canvas.bbox("all"))
# Enhance the base chart creation to support better interactivity
    def create_base_chart(self, chart_type, width=10, height=7):
        """Create a base chart with interactive features"""
        fig = plt.Figure(figsize=(width, height), dpi=100, facecolor='white')
        fig.canvas.mpl_connect('scroll_event', self.on_figure_scroll)
        fig.canvas.mpl_connect('button_press_event', self.on_figure_press)
        fig.canvas.mpl_connect('motion_notify_event', self.on_figure_motion)
        fig.canvas.mpl_connect('button_release_event', self.on_figure_release)
        return fig

    # Event handlers for figure interaction
    def on_figure_scroll(self, event):
        """Handle scroll events on figures"""
        if event.inaxes:
            ax = event.inaxes
            xdata, ydata = event.xdata, event.ydata
            
            # Get current x and y limits
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            
            # Calculate zoom factor
            scale_factor = 1.1 if event.button == 'up' else 1/1.1
            
            # Set new limits
            ax.set_xlim([xdata - (xdata - xlim[0]) / scale_factor,
                        xdata + (xlim[1] - xdata) / scale_factor])
            ax.set_ylim([ydata - (ydata - ylim[0]) / scale_factor,
                        ydata + (ylim[1] - ydata) / scale_factor])
            
            # Redraw
            ax.figure.canvas.draw_idle()

    # Store pan information
    pan_start_x = None
    pan_start_y = None
    pan_axes = None

    def on_figure_press(self, event):
        """Handle mouse press events for panning"""
        if event.button == 2:  # Middle mouse button
            self.pan_start_x = event.xdata
            self.pan_start_y = event.ydata
            self.pan_axes = event.inaxes

    def on_figure_motion(self, event):
        """Handle mouse motion events for panning"""
        if hasattr(self, 'pan_start_x') and self.pan_start_x is not None:
            if event.inaxes == self.pan_axes and event.button == 2:
                dx = event.xdata - self.pan_start_x
                dy = event.ydata - self.pan_start_y
                
                # Get current axis limits
                xlim = self.pan_axes.get_xlim()
                ylim = self.pan_axes.get_ylim()
                
                # Set new limits
                self.pan_axes.set_xlim([xlim[0] - dx, xlim[1] - dx])
                self.pan_axes.set_ylim([ylim[0] - dy, ylim[1] - dy])
                
                # Redraw
                self.pan_axes.figure.canvas.draw_idle()

    def on_figure_release(self, event):
        """Handle mouse release events to end panning"""
        self.pan_start_x = None
        self.pan_start_y = None
        self.pan_axes = None
    # Add these methods to handle fit to screen and reset zoom
    def fit_chart_to_screen(self, chart_container):
        """Fit the chart to screen size"""
        # Find the matplotlib canvas in the chart container
        for child in chart_container.winfo_children():
            if isinstance(child, FigureCanvasTkAgg):
                # Reset the view limits to default
                fig = child.figure
                for ax in fig.axes:
                    ax.autoscale(True)
                    ax.relim()
                    ax.autoscale_view()
                child.draw_idle()
                break
    def create_responsive_figure(self, parent_width, parent_height):
        """Create figure with responsive sizing"""
        # Calculate figure size based on container
        fig_width = max(6, min(12, parent_width / 100))
        fig_height = max(4, min(8, parent_height / 100))
        
        fig = plt.Figure(figsize=(fig_width, fig_height), dpi=100, facecolor='white')
        return fig
    def reset_chart_zoom(self, chart_container):
        """Reset chart zoom to original view"""
        self.fit_chart_to_screen(chart_container)

    def remove_chart(self, chart_container):
        """Remove a specific chart from the grid"""
        chart_container.destroy()
        if chart_container in self.chart_grid:
            self.chart_grid.remove(chart_container)
        self.reorganize_grid()

    def reorganize_grid(self):
        """Enhanced grid reorganization with better spacing"""
        if not self.chart_grid:
            self.current_row = 0
            self.current_col = 0
            self.add_chart_placeholder()
            return
        
        # Reposition charts with improved spacing
        cols_per_row = 3
        for i, chart in enumerate(self.chart_grid):
            row = i // cols_per_row
            col = i % cols_per_row
            
            chart.grid(
                row=row, 
                column=col, 
                padx=15,  # Increased padding
                pady=15, 
                sticky="nsew",
                ipadx=5,
                ipady=5
            )
            
            # Configure grid weights
            self.charts_frame.grid_rowconfigure(row, weight=1)
            self.charts_frame.grid_columnconfigure(col, weight=1)
        
        # Update current position
        self.current_row = (len(self.chart_grid) - 1) // cols_per_row
        self.current_col = len(self.chart_grid) % cols_per_row
        
        # Update canvas scroll region
        self.root.after_idle(lambda: self.viz_canvas.configure(scrollregion=self.viz_canvas.bbox("all")))

    def clear_all_charts(self):
        """Remove all charts from the visualization area"""
        for chart in self.chart_grid:
            chart.destroy()
        self.chart_grid = []
        self.current_row = 0
        self.current_col = 0
        
        # Restore placeholder
        self.add_chart_placeholder()

    @error_handler
    def load_survey_file(self):
        """Load XLSForm survey file to extract labels and choices"""
        filename = filedialog.askopenfilename(
            title="Select XLSForm Survey File",
            filetypes=[
                ("Excel files", "*.xlsx *.xls"),
                ("All files", "*.*")
            ]
        )
        
        if filename:
            self.load_xlsform_data(filename)

    def load_xlsform_data(self, filename):
        """Load survey and choices data from XLSForm file"""
        try:
            # Read Excel file with multiple sheets
            excel_file = pd.ExcelFile(filename)
            
            # Load survey sheet
            if 'survey' in excel_file.sheet_names:
                self.survey_sheet = pd.read_excel(filename, sheet_name='survey')
                logger.info(f"Loaded survey sheet with {len(self.survey_sheet)} rows")
            else:
                messagebox.showwarning("Warning", "No 'survey' sheet found in the file")
                return
            
            # Load choices sheet
            if 'choices' in excel_file.sheet_names:
                self.choices_sheet = pd.read_excel(filename, sheet_name='choices')
                logger.info(f"Loaded choices sheet with {len(self.choices_sheet)} rows")
            
            # Process the loaded data
            self.process_xlsform_data()
            
            messagebox.showinfo("Success", f"Successfully loaded XLSForm data from {filename}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Could not load XLSForm file: {str(e)}")

    def process_xlsform_data(self):
        """Process XLSForm survey and choices data to create mappings"""
        try:
            if self.survey_sheet is not None:
                # Process survey sheet to extract field labels
                for _, row in self.survey_sheet.iterrows():
                    field_name = str(row.get('name', '')).strip()
                    field_label = str(row.get('label', '')).strip()
                    field_type = str(row.get('type', '')).strip()
                    
                    if field_name and field_name != 'nan':
                        # Store the label
                        if field_label and field_label != 'nan':
                            self.form_labels[field_name] = field_label
                        
                        # Store field metadata
                        self.field_mappings[field_name] = {
                            'type': field_type,
                            'label': field_label if field_label != 'nan' else field_name
                        }
                        
                        # Handle select_one and select_multiple types
                        if 'select_one' in field_type or 'select_multiple' in field_type:
                            type_parts = field_type.split()
                            if len(type_parts) > 1:
                                choice_list_name = type_parts[1]
                                self.field_mappings[field_name]['choice_list'] = choice_list_name
                
                logger.info(f"Processed {len(self.form_labels)} field labels from survey sheet")
            
            if self.choices_sheet is not None:
                # Process choices sheet to create choice mappings
                for _, row in self.choices_sheet.iterrows():
                    list_name = str(row.get('list_name', '')).strip()
                    choice_name = str(row.get('name', '')).strip()
                    choice_label = str(row.get('label', '')).strip()
                    
                    if list_name and list_name != 'nan' and choice_name and choice_name != 'nan':
                        if list_name not in self.choice_mappings:
                            self.choice_mappings[list_name] = {}
                        
                        self.choice_mappings[list_name][choice_name] = choice_label if choice_label != 'nan' else choice_name
                
                logger.info(f"Processed {len(self.choice_mappings)} choice lists from choices sheet")
                
                # Map choices to field mappings
                for field_name, field_info in self.field_mappings.items():
                    if 'choice_list' in field_info:
                        choice_list = field_info['choice_list']
                        if choice_list in self.choice_mappings:
                            field_info['choices'] = self.choice_mappings[choice_list]
            
            # Update UI if data is already loaded
            if self.dataframe is not None and not self.dataframe.empty:
                self.update_ui_after_download("Survey file loaded")
                
        except Exception as e:
            logger.error(f"Error processing XLSForm data: {e}")
            messagebox.showerror("Error", f"Error processing XLSForm data: {str(e)}")

    def get_form_schema(self, auth):
        """Fetch form schema to understand all available fields and their labels"""
        try:
            base_url = self.url_var.get().rstrip('/')
            project_id = self.project_id_var.get()
            form_id = self.form_id_var.get()
            
            # Get form definition/schema
            form_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}"
            response = self.odk_manager.safe_api_call(form_url, auth, timeout=30)
            
            form_data = response.json()
            self.form_schema = form_data
            
            # Try to get the XLSForm source file
            try:
                xlsx_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/xlsx"
                xlsx_response = self.odk_manager.safe_api_call(xlsx_url, auth, timeout=30)
                
                if xlsx_response.status_code == 200:
                    # Save to temporary file and process
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
                        temp_file.write(xlsx_response.content)
                        temp_filename = temp_file.name
                    
                    self.load_xlsform_data(temp_filename)
                    
                    # Clean up temp file
                    os.unlink(temp_filename)
                    
                    logger.info("Successfully downloaded and processed XLSForm from ODK Central")
                    
            except Exception as e:
                logger.warning(f"Could not fetch XLSForm from ODK Central: {e}")
                # Add better handling for missing XLSForm (improved)
                if "404" in str(e):
                    logger.info(f"XLSForm not available for form {form_id}. This is normal if the form was not uploaded as XLSForm.")
                
            return True
            
        except Exception as e:
            logger.error(f"Could not fetch form schema: {e}")
            return False

    def get_column_label(self, column_name):
        """Get human-readable label for a column with enhanced XLSForm support"""
        # Direct match from XLSForm
        if column_name in self.form_labels:
            return self.form_labels[column_name]
        
        # Try without common prefixes
        clean_name = column_name
        prefixes_to_remove = ['data_', 'xml_', 'meta_', '__']
        for prefix in prefixes_to_remove:
            if clean_name.startswith(prefix):
                clean_name = clean_name[len(prefix):]
                break
        
        if clean_name in self.form_labels:
            return self.form_labels[clean_name]
        
        # Try partial matching
        for field_name, label in self.form_labels.items():
            if field_name in column_name or column_name in field_name:
                return label
        
        # Create readable version of column name
        readable = column_name.replace('_', ' ').replace('-', ' ').title()
        
        # Handle common ODK system fields
        system_fields = {
            'instanceID': 'Instance ID',
            'submissionDate': 'Submission Date',
            'reviewState': 'Review State',
            'deviceID': 'Device ID',
            'submitterID': 'Submitter ID',
            'submitterName': 'Submitter Name',
            'attachmentsPresent': 'Has Attachments',
            'attachmentsExpected': 'Expected Attachments',
            'status': 'Status',
            'version': 'Version',
            'createdAt': 'Created At',
            'updatedAt': 'Updated At'
        }
        
        return system_fields.get(column_name, readable)

    def get_choice_label(self, field_name, value):
        """Get human-readable label for a choice value"""
        if value is None or pd.isna(value):
            return str(value)
        
        # Clean field name
        clean_field = field_name.replace('data_', '').replace('xml_', '').replace('meta_', '')
        
        # Check if field has choices defined
        if clean_field in self.field_mappings:
            field_info = self.field_mappings[clean_field]
            
            # Direct choices mapping
            if 'choices' in field_info:
                choice_dict = field_info['choices']
                if str(value) in choice_dict:
                    return f"{choice_dict[str(value)]} ({value})"
            
            # Choice list mapping
            if 'choice_list' in field_info:
                choice_list = field_info['choice_list']
                if choice_list in self.choice_mappings:
                    choice_dict = self.choice_mappings[choice_list]
                    if str(value) in choice_dict:
                        return f"{choice_dict[str(value)]} ({value})"
        
        return str(value)

    @error_handler
    def show_labels_window(self):
        """Show window with field labels and mappings"""
        if not self.form_labels and not self.field_mappings:
            messagebox.showinfo("Info", "No form labels available. Try downloading data first or loading a survey file.")
            return
        
        labels_window = tb.Toplevel(self.root)
        labels_window.title("Form Field Labels & Mappings")
        labels_window.geometry("900x700")
        
        # Create notebook for different views
        labels_notebook = ttk.Notebook(labels_window)
        labels_notebook.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Labels tab
        labels_frame = ttk.Frame(labels_notebook)
        labels_notebook.add(labels_frame, text="Field Labels")
        
        labels_text = scrolledtext.ScrolledText(labels_frame, wrap=tk.WORD)
        labels_text.pack(fill="both", expand=True, padx=5, pady=5)
        
        labels_content = "Form Field Labels & Choices\n" + "="*60 + "\n\n"
        
        if self.dataframe is not None and not self.dataframe.empty:
            labels_content += f"Available columns in data: {len(self.dataframe.columns)}\n"
            labels_content += f"Form labels found: {len(self.form_labels)}\n"
            labels_content += f"Choice lists available: {len(self.choice_mappings)}\n\n"
            
            labels_content += "Column Name → Label → Choices Mapping:\n" + "-"*50 + "\n"
            
            for col in self.dataframe.columns:
                label = self.get_column_label(col)
                clean_col = col.replace('data_', '').replace('xml_', '').replace('meta_', '')
                
                labels_content += f"✓ Column: {col}\n"
                if label != col.replace('_', ' ').title():
                    labels_content += f"  → Label: {label}\n"
                
                # Show choices if available
                if clean_col in self.field_mappings:
                    field_info = self.field_mappings[clean_col]
                    if 'choices' in field_info:
                        labels_content += f"  → Choices: {field_info['choices']}\n"
                    elif 'choice_list' in field_info:
                        choice_list = field_info['choice_list']
                        if choice_list in self.choice_mappings:
                            choices = self.choice_mappings[choice_list]
                            labels_content += f"  → Choice List '{choice_list}': {choices}\n"
                
                labels_content += "\n"
        else:
            labels_content += "Survey Data (from loaded XLSForm):\n" + "-"*30 + "\n"
            for field, label in self.form_labels.items():
                labels_content += f"{field}: {label}\n"
                if field in self.field_mappings and 'choices' in self.field_mappings[field]:
                    choices = self.field_mappings[field]['choices']
                    labels_content += f"  Choices: {choices}\n"
                labels_content += "\n"
        
        labels_text.insert(tk.END, labels_content)
        labels_text.config(state=tk.DISABLED)
        
        # Choice Lists tab
        if self.choice_mappings:
            choices_frame = ttk.Frame(labels_notebook)
            labels_notebook.add(choices_frame, text="Choice Lists")
            
            choices_text = scrolledtext.ScrolledText(choices_frame, wrap=tk.WORD)
            choices_text.pack(fill="both", expand=True, padx=5, pady=5)
            
            choices_content = "All Choice Lists\n" + "="*30 + "\n\n"
            
            for list_name, choices in self.choice_mappings.items():
                choices_content += f"Choice List: {list_name}\n" + "-" * (len(list_name) + 13) + "\n"
                for value, label in choices.items():
                    choices_content += f"  {value} → {label}\n"
                choices_content += "\n"
            
            choices_text.insert(tk.END, choices_content)
            choices_text.config(state=tk.DISABLED)

    @error_handler
    def download_data(self):
        if not self.validate_inputs():
            return      
        self.progress.start(5)
        self.status_var.set("Downloading data...")
        threading.Thread(target=self._download_thread, daemon=True).start()

    def refresh_data(self):
        if self.dataframe is None:
            messagebox.showwarning("Warning", "No data to refresh. Please download data first.")
            return
        
        self.download_data()

    def _download_thread(self):
        try:
            auth = (self.username_var.get(), self.password_var.get())
            
            # First, try to get form schema
            self.get_form_schema(auth)
            
            # Fetch submissions with all columns
            self.dataframe, method_used = self.odk_manager.fetch_submissions_with_all_columns(
                self.url_var.get().rstrip('/'),
                self.project_id_var.get(),
                self.form_id_var.get(),
                auth,
                use_cache=self.config_manager.config["data"]["cache_enabled"]
            )
            
            if self.dataframe.empty:
                self.root.after(0, lambda: self.status_var.set("No data available"))
                self.root.after(0, lambda: self.progress.stop())
                return
            
            self.filtered_df = self.dataframe.copy()
            
            # Update last update time
            self.last_update_time = datetime.now(timezone.utc)
            
            # Update UI
            self.root.after(0, lambda: self.update_ui_after_download(method_used))
            # Update summary stats
            self.root.after(0, self.update_summary_stats)
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            self.root.after(0, lambda: messagebox.showerror("Error", f"Download failed: {error_msg}"))
            self.root.after(0, lambda: self.status_var.set("Download failed"))
        finally:
            self.root.after(0, lambda: self.progress.stop())

    def update_ui_after_download(self, method_used):
        # Update filter column choices with labels
        if self.filter_column_combo:
            filter_choices = []
            for col in self.dataframe.columns:
                label = self.get_column_label(col)
                if label != col.replace('_', ' ').title():
                    # Show label with original column name
                    display_text = f"{label} ({col})"
                else:
                    display_text = col
                filter_choices.append(display_text)
            
            self.filter_column_combo.configure(values=filter_choices)
        
        # Update visualization column choices with labels
        if self.column_combo:
            column_choices = []
            for col in self.dataframe.columns:
                label = self.get_column_label(col)
                if label != col.replace('_', ' ').title():
                    # Show label with original column name
                    display_text = f"{label} ({col})"
                else:
                    display_text = col
                column_choices.append(display_text)
            
            self.column_combo.configure(values=column_choices)

        # Update table view and statistics
        self.update_table_view(self.dataframe)
        self.update_statistics()
        
        # Update status with method used and column count
        column_count = len(self.dataframe.columns)
        row_count = len(self.dataframe)
        status_msg = f"Downloaded {row_count} records with {column_count} columns using {method_used}"
        if self.last_update_time:
            status_msg += f" at {self.last_update_time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        self.status_var.set(status_msg)

    def validate_inputs(self):
        required_fields = {
            "ODK URL": self.url_var.get(),
            "Project ID": self.project_id_var.get(),
            "Form ID": self.form_id_var.get(),
            "Username": self.username_var.get(),
            "Password": self.password_var.get()
        }
        
        missing_fields = [field for field, value in required_fields.items() if not value.strip()]
        
        if missing_fields:
            messagebox.showerror("Error", f"Please fill in the following fields:\n- {'\n- '.join(missing_fields)}")
            return False
            
        return True

    def extract_column_name_from_display(self, display_text):
        """Extract original column name from display text that includes labels"""
        if '(' in display_text and ')' in display_text:
            # Extract column name from "Label (column_name)" format
            return display_text.split('(')[-1].split(')')[0]
        return display_text

    @error_handler
    def apply_filters(self):
        if self.dataframe is None:
            messagebox.showwarning("Warning", "No data to filter. Download data first.")
            return
            
        try:
            self.filtered_df = self.dataframe.copy()
            
            # Apply column filter
            if self.filter_column_var.get() and self.filter_value_var.get():
                display_column = self.filter_column_var.get()
                column = self.extract_column_name_from_display(display_column)
                value = self.filter_value_var.get()
                
                if column not in self.dataframe.columns:
                    messagebox.showerror("Error", f"Column '{column}' not found in data")
                    return
                
                # Handle different data types for filtering
                if self.filtered_df[column].dtype in ['object', 'string']:
                    # String filtering with case-insensitive contains
                    self.filtered_df = self.filtered_df[
                        self.filtered_df[column].astype(str).str.contains(value, case=False, na=False)
                    ]
                else:
                    # Numeric filtering - try exact match first, then range
                    try:
                        numeric_value = float(value)
                        self.filtered_df = self.filtered_df[self.filtered_df[column] == numeric_value]
                    except ValueError:
                        # If not numeric, fall back to string contains
                        self.filtered_df = self.filtered_df[
                            self.filtered_df[column].astype(str).str.contains(value, case=False, na=False)
                        ]
            
            # Apply date filters
            if self.date_from_var.get() or self.date_to_var.get():
                try:
                    # Look for date columns
                    date_columns = [col for col in self.filtered_df.columns 
                                  if any(date_term in col.lower() for date_term in 
                                        ['date', 'time', 'created', 'updated', 'submission'])]
                    
                    if date_columns:
                        date_col = date_columns[0]  # Use first date column found
                        
                        # Convert column to datetime
                        date_series = pd.to_datetime(self.filtered_df[date_col], errors='coerce')
                        
                        if self.date_from_var.get():
                            date_from = pd.to_datetime(self.date_from_var.get())
                            self.filtered_df = self.filtered_df[date_series >= date_from]
                        
                        if self.date_to_var.get():
                            date_to = pd.to_datetime(self.date_to_var.get())
                            # Add one day to include the end date
                            date_to = date_to + pd.Timedelta(days=1)
                            self.filtered_df = self.filtered_df[date_series < date_to]
                    else:
                        messagebox.showwarning("Warning", "No date columns found for date filtering")
                        
                except ValueError as e:
                    messagebox.showerror("Error", f"Invalid date format. Use YYYY-MM-DD format: {str(e)}")
                    return
            
            self.update_table_view(self.filtered_df)
            self.update_statistics()
            self.update_summary_stats()
            
            # Update status
            filter_count = len(self.filtered_df)
            total_count = len(self.dataframe)
            self.status_var.set(f"Applied filters: showing {filter_count} of {total_count} records")
            
        except Exception as e:
            logger.error(f"Filter application failed: {e}")
            messagebox.showerror("Error", f"Filter application failed: {str(e)}")

    def reset_filters(self):
        self.filter_column_var.set('')
        self.filter_value_var.set('')
        self.date_from_var.set('')
        self.date_to_var.set('')
        if self.dataframe is not None:
            self.filtered_df = self.dataframe.copy()
            self.update_table_view(self.filtered_df)
            self.update_statistics()
            self.update_summary_stats()
            self.status_var.set("Filters reset - showing all data")

    def update_statistics(self):
        total_count = len(self.dataframe) if self.dataframe is not None else 0
        filtered_count = len(self.filtered_df) if self.filtered_df is not None else 0
        
        self.submission_count_var.set(f"{total_count:,}")
        self.filtered_count_var.set(f"{filtered_count:,}")

    def update_table_view(self, df):
        """Update the table view with the given dataframe"""
        if not self.tree:
            return
            
        # Clear existing items
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if df is None or df.empty:
            self.status_var.set("No data to display")
            return
        
        # Set up columns
        self.tree["columns"] = list(df.columns)
        self.tree["show"] = "headings"
        
        # Configure columns with labels
        for col in df.columns:
            # Get human-readable label
            display_label = self.get_column_label(col)
            
            # Show both original name and label in heading if they differ
            if display_label != col.replace('_', ' ').title():
                heading_text = f"{display_label}\n({col})"
            else:
                heading_text = display_label
            
            self.tree.heading(col, text=heading_text, command=lambda c=col: self.sort_treeview(c))
            
            # Calculate column width based on content
            if len(df) > 0:
                max_content_width = df[col].astype(str).str.len().max()
                header_width = len(str(col))
                max_width = max(max_content_width, header_width) * 10
            else:
                max_width = len(str(col)) * 10
                
            # Set reasonable bounds for column width
            width = min(max(max_width, 80), 400)
            self.tree.column(col, width=width, minwidth=50)
        
        # Add data rows (limit to first 1000 for performance)
        display_limit = self.config_manager.config["data"]["display_limit"]
        rows_to_display = min(len(df), display_limit)
        
        for idx in range(rows_to_display):
            row = df.iloc[idx]
            values = []
            for val in row:
                if pd.isna(val):
                    values.append("")
                elif isinstance(val, (int, float)) and not pd.isna(val):
                    values.append(str(val))
                else:
                    # Truncate long text values for display
                    str_val = str(val)
                    if len(str_val) > 100:
                        str_val = str_val[:97] + "..."
                    values.append(str_val)
            
            self.tree.insert("", "end", values=values)
        
        # Update status
        if len(df) > display_limit:
            self.status_var.set(f"Displaying first {display_limit} of {len(df)} records")
        else:
            self.status_var.set(f"Displaying all {len(df)} records")

    def sort_treeview(self, col):
        """Sort treeview data by column"""
        if self.filtered_df is None:
            return
            
        try:
            # Toggle sort order
            if hasattr(self, '_last_sort_col') and self._last_sort_col == col:
                self._sort_ascending = not getattr(self, '_sort_ascending', True)
            else:
                self._sort_ascending = True
            
            self._last_sort_col = col
            
            # Sort the dataframe
            self.filtered_df = self.filtered_df.sort_values(by=[col], ascending=self._sort_ascending)
            
            # Update the display
            self.update_table_view(self.filtered_df)
            
            # Update status to show sort info
            sort_order = "ascending" if self._sort_ascending else "descending"
            current_status = self.status_var.get()
            self.status_var.set(f"{current_status} (sorted by {col} {sort_order})")
            
        except Exception as e:
            logger.error(f"Could not sort by column {col}: {e}")
            messagebox.showerror("Error", f"Could not sort by column {col}: {str(e)}")

    # Chart creation methods
    @error_handler
    def create_pie_chart(self, parent, column, modern_style=False):
        # Get parent dimensions for responsive sizing
        parent.update_idletasks()
        parent_width = parent.winfo_width() or 400
        parent_height = parent.winfo_height() or 300
        
        # Create responsive figure
        fig_width = max(4, min(8, parent_width / 80))
        fig_height = max(3, min(6, parent_height / 80))
        
        fig = plt.Figure(figsize=(fig_width, fig_height), dpi=100, facecolor='white')
        ax = fig.add_subplot(111)
        ax.set_facecolor('white')
        

        # Get human-readable label for the column
        column_label = self.get_column_label(column)
        
        # Chart data processing...
        value_counts = self.filtered_df[column].value_counts()
        
        # Modern styling enhancements
        if modern_style:
            # Use a more modern color palette
            colors = plt.cm.tab10(np.linspace(0, 1, len(value_counts)))
            # Adjust text sizes and fonts
            plt.rcParams['font.family'] = 'Arial'
            title_fontsize = 14
            label_fontsize = 10
        else:
            # Your existing styling
            colors = plt.cm.Pastel1(np.linspace(0, 1, len(value_counts)))
            title_fontsize = 12
            label_fontsize = 9
        
        # Create the pie chart
        patches, texts, autotexts = ax.pie(
            value_counts, 
            labels=value_counts.index if not modern_style else None,  # For modern style, use legend instead
            autopct='%1.1f%%', 
            startangle=90,
            colors=colors,
            wedgeprops={'edgecolor': 'white', 'linewidth': 1} if modern_style else {}
        )
        
        if modern_style:
            plt.setp(autotexts, size=11, weight="bold", color='white')
            # Use legend instead of labels directly on pie for cleaner look
            ax.legend(
                patches, 
                value_counts.index, 
                title="Categories",
                loc="center left", 
                bbox_to_anchor=(1.0, 0.5),
                fontsize=label_fontsize
            )
        else:
            plt.setp(autotexts, size=9, weight="bold", color='black')
            plt.setp(texts, size=9, color='black')
        
        # Chart title
        ax.set_title(
            f'Distribution of {column_label}', 
            color='#303030' if modern_style else 'black', 
            pad=20, 
            fontsize=title_fontsize,
            fontweight='bold' if modern_style else 'normal'
        )
        
        # Better layout with more padding
        fig.tight_layout(pad=2.0)  # Increased padding
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        #######
    @error_handler
    def create_time_series_plot(self, parent, modern_style=False):
        parent.update_idletasks()
        parent_width = parent.winfo_width() or 400
        parent_height = parent.winfo_height() or 300
        
        # Create responsive figure
        fig_width = max(4, min(8, parent_width / 80))
        fig_height = max(3, min(6, parent_height / 80))
        
        fig = plt.Figure(figsize=(fig_width, fig_height), dpi=100, facecolor='white')
        ax = fig.add_subplot(111)
        ax.set_facecolor('white')
        
        try:
            date_columns = [col for col in self.filtered_df.columns 
                        if any(date_term in col.lower() for date_term in 
                                ['date', 'time', 'created', 'updated', 'submission'])]
            
            if not date_columns:
                raise ValueError("No date columns found")
            
            date_col = date_columns[0]
            date_series = pd.to_datetime(self.filtered_df[date_col], errors='coerce')
            valid_dates = date_series.dropna()
            
            if valid_dates.empty:
                raise ValueError("No valid dates found")
            
            submissions_by_date = valid_dates.value_counts().sort_index()
            ax.plot(submissions_by_date.index, submissions_by_date.values, 
                    marker='o', linestyle='-', linewidth=2, color='#2196F3')
            
            ax.set_title(f'Submissions Over Time ({date_col})', color='black', pad=20, fontsize=12)
            ax.set_xlabel('Date', color='black')
            ax.set_ylabel('Number of Submissions', color='black')
            ax.grid(True, alpha=0.3)
            
            # Style the axes
            ax.tick_params(colors='black')
            for spine in ax.spines.values():
                spine.set_color('black')
            
            # Rotate x-axis labels
            plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
            
            fig.tight_layout()
            
        except Exception as e:
            messagebox.showerror("Error", f"Could not create time series plot: {str(e)}")
            return
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)
        # Add a fit button to the chart container
        fit_btn = ttk.Button(parent, text="Fit", 
                        command=lambda: self.fit_chart_to_screen(parent))
        fit_btn.pack(side="top", anchor="e")
        
        fig.tight_layout(pad=2.0)  # Increased padding
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

    @error_handler
    def create_distribution_plot(self, parent, modern_style=False):
        numeric_cols = self.filtered_df.select_dtypes(include=['number']).columns
        
        if len(numeric_cols) == 0:
            messagebox.showinfo("Info", "No numeric columns available for distribution plot")
            return
        
        fig = plt.Figure(figsize=(5, 4), dpi=100, facecolor='white')
        
        n_cols = min(2, len(numeric_cols))
        n_rows = (len(numeric_cols) + 1) // 2
        
        for idx, col in enumerate(numeric_cols, 1):
            ax = fig.add_subplot(n_rows, n_cols, idx)
            ax.set_facecolor('white')
            
            column_label = self.get_column_label(col)
            sns.histplot(data=self.filtered_df, x=col, ax=ax, color='#2196F3')
            
            ax.set_title(f'Distribution of {column_label}', color='black', fontsize=10)
            ax.set_xlabel(column_label, color='black')
            ax.set_ylabel('Count', color='black')
            
            # Style the axes
            ax.tick_params(colors='black')
            for spine in ax.spines.values():
                spine.set_color('black')
        
        fig.tight_layout(pad=2.0)  # Increased padding
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        # Add a fit button to the chart container
        fit_btn = ttk.Button(parent, text="Fit", 
                        command=lambda: self.fit_chart_to_screen(parent))
        fit_btn.pack(side="top", anchor="e")
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

    @error_handler
    def create_correlation_plot(self, parent):
        numeric_df = self.filtered_df.select_dtypes(include=['number'])
        
        if len(numeric_df.columns) < 2:
            messagebox.showinfo("Info", "Not enough numeric columns for correlation analysis")
            return
        
        fig = plt.Figure(figsize=(10, 8), dpi=100, facecolor='white')
        ax = fig.add_subplot(111)
        ax.set_facecolor('white')
        
        label_mapping = {col: self.get_column_label(col) for col in numeric_df.columns}
        display_df = numeric_df.rename(columns=label_mapping)
        
        corr_matrix = display_df.corr()
        
        sns.heatmap(corr_matrix, annot=True, cmap='RdYlBu', ax=ax,
                    annot_kws={'size': 8, 'color': 'black'},
                    fmt='.2f')
        
        ax.set_title('Correlation Matrix', color='black', pad=20, fontsize=12)
        
        ax.tick_params(colors='black')
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right', color='black')
        plt.setp(ax.get_yticklabels(), rotation=0, color='black')
        
        fig.tight_layout(pad=2.0)  # Increased padding
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

        # Add a fit button to the chart container
        fit_btn = ttk.Button(parent, text="Fit", 
                        command=lambda: self.fit_chart_to_screen(parent))
        fit_btn.pack(side="top", anchor="e")
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

    @error_handler
    def create_horizontal_bar_chart(self, parent, column, modern_style=False):
        # Create a smaller figure with appropriate size
        fig = plt.Figure(figsize=(5, 4), dpi=100, facecolor='white')
        ax = fig.add_subplot(111)
        ax.set_facecolor('white')
        
        
        column_label = self.get_column_label(column)
        value_counts = self.filtered_df[column].value_counts()
        
        mapped_counts = pd.Series(dtype=int)
        for value, count in value_counts.items():
            choice_label = self.get_choice_label(column, value)
            if " (" in choice_label and choice_label.endswith(")"):
                display_label = choice_label.split(" (")[0]
            else:
                display_label = choice_label
            mapped_counts[display_label] = mapped_counts.get(display_label, 0) + count
        
        value_counts = mapped_counts[:15] if len(mapped_counts) > 15 else mapped_counts
        
        colors = plt.cm.Pastel1(np.linspace(0, 1, len(value_counts)))
        bars = ax.barh(range(len(value_counts)), value_counts.values, color=colors)
        
        # Add value labels on the bars
        for i, v in enumerate(value_counts.values):
            ax.text(v + max(value_counts.values) * 0.01, i, str(v), 
                    va='center', color='black')
        
        ax.set_title(f'Distribution of {column_label}', color='black', pad=20, fontsize=12)
        ax.set_xlabel('Count', color='black')
        ax.set_ylabel(column_label, color='black')
        
        # Style the axes
        ax.tick_params(colors='black')
        for spine in ax.spines.values():
            spine.set_color('black')
        
        labels = [label[:30] + '...' if len(label) > 30 else label for label in value_counts.index]
        ax.set_yticks(range(len(value_counts)))
        ax.set_yticklabels(labels, color='black')
        
        fig.tight_layout()
        

        ######addd if possible
        # Add a fit button to the chart container
        fit_btn = ttk.Button(parent, text="Fit", 
                        command=lambda: self.fit_chart_to_screen(parent))
        fit_btn.pack(side="top", anchor="e")
        
        fig.tight_layout(pad=2.0)  # Increased padding
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

    @error_handler
    def create_stacked_bar_chart(self, parent, column, modern_style=False):
        if len(self.filtered_df.columns) < 2:
            messagebox.showwarning("Warning", "Need at least two columns for a stacked bar chart")
            return
        
        # Create a selection window for the second column
        selection_window = tb.Toplevel(parent)
        selection_window.title("Select Second Column")
        selection_window.geometry("400x150")
        
        ttk.Label(selection_window, text="Select second column for cross-analysis:").pack(pady=5)
        second_column_var = tk.StringVar()
        
        # Create choices with labels
        column_choices = []
        column_mapping = {}
        for col in self.filtered_df.columns:
            if col != column:
                label = self.get_column_label(col)
                display_text = f"{label} ({col})" if label != col.replace('_', ' ').title() else col
                column_choices.append(display_text)
                column_mapping[display_text] = col
        
        second_column_combo = ttk.Combobox(selection_window, 
                                        textvariable=second_column_var,
                                        values=column_choices,
                                        width=50)
        second_column_combo.pack(pady=5)
        
        def create_stacked_chart():
            display_second_column = second_column_var.get()
            if not display_second_column:
                messagebox.showwarning("Warning", "Please select a second column")
                return
            
            second_column = column_mapping.get(display_second_column, display_second_column)
            
            fig = plt.Figure(figsize=(12, 8), dpi=100, facecolor='white')
            ax = fig.add_subplot(111)
            ax.set_facecolor('white')
            
            column_label = self.get_column_label(column)
            second_column_label = self.get_column_label(second_column)
            
            # Create cross-tabulation
            df_for_chart = self.filtered_df.copy()
            for col in [column, second_column]:
                df_for_chart[col] = df_for_chart[col].apply(
                    lambda x: self.get_choice_label(col, x).split(" (")[0] 
                    if " (" in self.get_choice_label(col, x) and self.get_choice_label(col, x).endswith(")") 
                    else self.get_choice_label(col, x)
                )
            
            ct = pd.crosstab(df_for_chart[column], df_for_chart[second_column])
            
            # Create stacked bar chart
            ct.plot(kind='bar', stacked=True, ax=ax, colormap='Set3')
            
            ax.set_title(f'{column_label} vs {second_column_label}', color='black', pad=20)
            ax.set_xlabel(column_label, color='black')
            ax.set_ylabel('Count', color='black')
            
            # Style the axes
            ax.tick_params(colors='black')
            for spine in ax.spines.values():
                spine.set_color('black')
            
            # Rotate and align x-axis labels
            labels = [label[:20] + '...' if len(label) > 20 else label for label in ct.index]
            ax.set_xticklabels(labels, rotation=45, ha='right', color='black')
            
            # Style the legend
            legend = ax.legend(title=second_column_label, bbox_to_anchor=(1.05, 1))
            legend.get_title().set_color('black')
            plt.setp(legend.get_texts(), color='black')
            
            fig.tight_layout()
            
            canvas = FigureCanvasTkAgg(fig, master=parent)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)
            
            selection_window.destroy()
        
        ttk.Button(selection_window, text="Create Chart", 
                command=create_stacked_chart, 
                style='success.TButton').pack(pady=10)
        # Add a fit button to the chart container
        fit_btn = ttk.Button(parent, text="Fit", 
                        command=lambda: self.fit_chart_to_screen(parent))
        fit_btn.pack(side="top", anchor="e")
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)
#######
    def on_canvas_configure(self, event):
        """Enhanced canvas configuration with responsive chart resizing"""
        # Update canvas scroll region
        self.viz_canvas.configure(scrollregion=self.viz_canvas.bbox("all"))
        
        # Calculate responsive dimensions
        canvas_width = event.width
        canvas_height = event.height
        
        # Resize the charts frame window
        self.viz_canvas.itemconfig(self.canvas_window, width=canvas_width)
        
        # Update chart sizes responsively
        if hasattr(self, 'chart_grid') and self.chart_grid:
            # Calculate optimal chart dimensions based on grid
            cols_per_row = 3
            chart_width = (canvas_width - (cols_per_row + 1) * 30) / cols_per_row  # Account for padding
            chart_height = chart_width * 0.75  # Maintain aspect ratio
            
            for chart_container in self.chart_grid:
                self.resize_chart_container(chart_container, chart_width, chart_height) 

    def resize_chart_container(self, container, width, height):
        """Resize individual chart containers"""
        try:
            # Find matplotlib canvas in container
            for widget in container.winfo_children():
                if hasattr(widget, 'winfo_children'):
                    for child in widget.winfo_children():
                        if isinstance(child, FigureCanvasTkAgg):
                            # Resize the figure
                            fig = child.figure
                            fig.set_size_inches(width/fig.dpi, height/fig.dpi)
                            child.draw_idle()
                            break
        except Exception as e:
            logger.error(f"Error resizing chart container: {e}")                

    # Export and utility methods
    @error_handler
    def export_csv_with_labels(self):
        """Export data with both original columns and label headers"""
        if self.filtered_df is None or self.filtered_df.empty:
            messagebox.showwarning("Warning", "No data to export")
            return
        
        try:
            # Ask user for export preference
            export_window = tb.Toplevel(self.root)
            export_window.title("Export Options")
            export_window.geometry("450x250")
            
            export_type = tk.StringVar(value="original")
            
            ttk.Label(export_window, text="Choose export format:").pack(pady=10)
            
            ttk.Radiobutton(export_window, text="Original column names", 
                          variable=export_type, value="original").pack(pady=5)
            ttk.Radiobutton(export_window, text="Human-readable labels as headers", 
                          variable=export_type, value="labels").pack(pady=5)
            ttk.Radiobutton(export_window, text="Both (labels with original names)", 
                          variable=export_type, value="both").pack(pady=5)
            ttk.Radiobutton(export_window, text="Labels with choice mappings", 
                          variable=export_type, value="choices").pack(pady=5)
            
            def do_export():
                filename = filedialog.asksaveasfilename(
                    defaultextension=".csv",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                    title="Save data as CSV"
                )
                
                if filename:
                    export_df = self.filtered_df.copy()
                    
                    if export_type.get() == "labels":
                        # Use labels as column headers
                        new_columns = {}
                        for col in export_df.columns:
                            label = self.get_column_label(col)
                            new_columns[col] = label
                        export_df = export_df.rename(columns=new_columns)
                    
                    elif export_type.get() == "both":
                        # Use "Label (original_name)" format
                        new_columns = {}
                        for col in export_df.columns:
                            label = self.get_column_label(col)
                            if label != col.replace('_', ' ').title():
                                new_columns[col] = f"{label} ({col})"
                            else:
                                new_columns[col] = col
                        export_df = export_df.rename(columns=new_columns)
                    
                    elif export_type.get() == "choices":
                        # Replace choice values with labels and rename columns
                        new_columns = {}
                        for col in export_df.columns:
                            # Rename column
                            label = self.get_column_label(col)
                            if label != col.replace('_', ' ').title():
                                new_columns[col] = f"{label} ({col})"
                            else:
                                new_columns[col] = col
                            
                            # Map choice values to labels
                            export_df[col] = export_df[col].apply(
                                lambda x: self.get_choice_label(col, x) if pd.notna(x) else x
                            )
                        
                        export_df = export_df.rename(columns=new_columns)
                    
                    export_df.to_csv(filename, index=False)
                    messagebox.showinfo("Success", f"Data exported to {filename}")
                
                export_window.destroy()
            
            ttk.Button(export_window, text="Export", 
                      command=do_export, style='success.TButton').pack(pady=10)
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            messagebox.showerror("Error", f"Export failed: {str(e)}")

    @error_handler
    def create_data_dictionary(self):
        """Create a data dictionary with field descriptions"""
        if not self.form_labels and not self.field_mappings:
            messagebox.showinfo("Info", "No form metadata available for data dictionary")
            return
        
        try:
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                title="Save data dictionary as CSV"
            )
            
            if filename:
                # Create data dictionary
                dictionary_data = []
                
                if self.dataframe is not None:
                    for col in self.dataframe.columns:
                        label = self.get_column_label(col)
                        dtype = str(self.dataframe[col].dtype)
                        non_null_count = self.dataframe[col].notna().sum()
                        total_count = len(self.dataframe)
                        
                        # Get field metadata
                        clean_col = col.replace('data_', '').replace('xml_', '').replace('meta_', '')
                        field_info = self.field_mappings.get(clean_col, {})
                        
                        row = {
                            'Column_Name': col,
                            'Label': label,
                            'Data_Type': dtype,
                            'Non_Null_Count': non_null_count,
                            'Total_Count': total_count,
                            'Completeness_Percent': round((non_null_count / total_count) * 100, 2),
                            'Field_Type': field_info.get('type', ''),
                            'Required': field_info.get('required', ''),
                            'Constraint': field_info.get('constraint', ''),
                        }
                        
                        # Add choices if available
                        choices_str = ''
                        if 'choices' in field_info:
                            choices_str = '; '.join([f"{k}={v}" for k, v in field_info['choices'].items()])
                        elif 'choice_list' in field_info:
                            choice_list = field_info['choice_list']
                            if choice_list in self.choice_mappings:
                                choices_str = '; '.join([f"{k}={v}" for k, v in self.choice_mappings[choice_list].items()])
                        
                        row['Choices'] = choices_str
                        dictionary_data.append(row)
                
                # Save to CSV
                dictionary_df = pd.DataFrame(dictionary_data)
                dictionary_df.to_csv(filename, index=False)
                
                messagebox.showinfo("Success", f"Data dictionary saved to {filename}")
                
        except Exception as e:
            logger.error(f"Data dictionary export failed: {e}")
            messagebox.showerror("Error", f"Data dictionary export failed: {str(e)}")

    # Dialog and utility methods
    @error_handler
    def test_connection(self):
        """Test ODK Central connection"""
        if not self.validate_inputs():
            return
        
        url = self.url_var.get()
        username = self.username_var.get()
        password = self.password_var.get()
        
        def test_in_thread():
            try:
                response = self.odk_manager.safe_api_call(
                    f"{url.rstrip('/')}/v1/projects",
                    (username, password),
                    timeout=10
                )
                
                if response and response.status_code == 200:
                    self.root.after(0, lambda: messagebox.showinfo("Connection Test", "✅ Connection successful!"))
                else:
                    self.root.after(0, lambda: messagebox.showerror("Connection Test", f"❌ Connection failed: {response.status_code if response else 'No response'}"))
                    
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Connection Test", f"❌ Connection failed: {str(e)}"))
        
        threading.Thread(target=test_in_thread, daemon=True).start()

    def clear_cache(self):
        """Clear application cache"""
        try:
            if os.path.exists(self.cache_manager.db_path):
                os.remove(self.cache_manager.db_path)
                self.cache_manager.init_database()
                messagebox.showinfo("Cache", "Cache cleared successfully")
                logger.info("Cache cleared")
            else:
                messagebox.showinfo("Cache", "No cache to clear")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            messagebox.showerror("Cache", f"Could not clear cache: {e}")

    def validate_data_integrity(self):
        """Show data validation report"""
        if self.dataframe is None or self.dataframe.empty:
            messagebox.showwarning("Validation", "No data available for validation")
            return
        
        # Basic validation
        total_rows = len(self.dataframe)
        total_columns = len(self.dataframe.columns)
        null_percentages = self.dataframe.isnull().sum() / len(self.dataframe) * 100
        duplicate_rows = self.dataframe.duplicated().sum()
        
        validation_text = f"""Data Validation Report
{'=' * 30}

Total Rows: {total_rows:,}
Total Columns: {total_columns}
Duplicate Rows: {duplicate_rows}

Columns with High Missing Data (>20%):
"""
        
        high_missing = null_percentages[null_percentages > 20]
        if not high_missing.empty:
            for col, percentage in high_missing.items():
                validation_text += f"  {col}: {percentage:.1f}% missing\n"
        else:
            validation_text += "  None\n"
        
        validation_text += f"\nData Quality: {'Good' if duplicate_rows < total_rows * 0.1 else 'Issues Detected'}"
        
        messagebox.showinfo("Data Validation", validation_text)

    def show_column_info(self):
        """Show information about available columns"""
        if self.dataframe is None:
            messagebox.showwarning("Info", "No data available")
            return
            
        info_window = tb.Toplevel(self.root)
        info_window.title("Column Information")
        info_window.geometry("600x400")
        
        # Create text widget with scrollbar
        text_frame = ttk.Frame(info_window)
        text_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        text_widget = scrolledtext.ScrolledText(text_frame, wrap=tk.WORD)
        text_widget.pack(fill="both", expand=True)
        
        # Display column information with labels
        info_text = f"Total Columns: {len(self.dataframe.columns)}\n"
        info_text += f"Total Rows: {len(self.dataframe)}\n"
        info_text += f"Form Labels Available: {len(self.form_labels)}\n\n"
        info_text += "Available Columns with Labels:\n" + "="*50 + "\n\n"
        
        for i, col in enumerate(self.dataframe.columns, 1):
            dtype = str(self.dataframe[col].dtype)
            non_null_count = self.dataframe[col].notna().sum()
            sample_values = self.dataframe[col].dropna().unique()[:3]
            
            # Get human-readable label
            label = self.get_column_label(col)
            
            info_text += f"{i}. {col}\n"
            if label != col.replace('_', ' ').title():
                info_text += f"   Label: {label}\n"
            info_text += f"   Type: {dtype}\n"
            info_text += f"   Non-null values: {non_null_count}/{len(self.dataframe)}\n"
            info_text += f"   Sample values: {list(sample_values)}\n"
            
            # Add choice mappings if available
            clean_col = col.replace('data_', '').replace('xml_', '').replace('meta_', '')
            if clean_col in self.field_mappings and 'choices' in self.field_mappings[clean_col]:
                choices = self.field_mappings[clean_col]['choices']
                info_text += f"   Choices: {dict(list(choices.items())[:3])}{'...' if len(choices) > 3 else ''}\n"
            
            info_text += "\n"
        
        text_widget.insert(tk.END, info_text)
        text_widget.config(state=tk.DISABLED)

    def show_statistics_window(self):
        """Show detailed statistics window"""
        if self.dataframe is None:
            messagebox.showwarning("Statistics", "No data available")
            return
        
        stats_window = tb.Toplevel(self.root)
        stats_window.title("Data Statistics")
        stats_window.geometry("700x500")
        
        stats_text = scrolledtext.ScrolledText(stats_window, wrap=tk.WORD)
        stats_text.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Generate comprehensive statistics
        stats_content = f"Data Statistics Report\n{'=' * 40}\n\n"
        stats_content += f"Dataset Overview:\n{'-' * 20}\n"
        stats_content += f"Total Records: {len(self.dataframe):,}\n"
        stats_content += f"Total Columns: {len(self.dataframe.columns)}\n"
        stats_content += f"Memory Usage: {self.dataframe.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB\n\n"
        
        # Data quality metrics
        stats_content += f"Data Quality:\n{'-' * 15}\n"
        null_count = self.dataframe.isnull().sum().sum()
        total_cells = self.dataframe.size
        completeness = (total_cells - null_count) / total_cells * 100
        stats_content += f"Completeness: {completeness:.2f}%\n"
        stats_content += f"Missing Values: {null_count:,}\n"
        stats_content += f"Duplicate Rows: {self.dataframe.duplicated().sum():,}\n\n"
        
        # Column types
        stats_content += f"Column Types:\n{'-' * 15}\n"
        type_counts = self.dataframe.dtypes.value_counts()
        for dtype, count in type_counts.items():
            stats_content += f"{dtype}: {count} columns\n"
        
        stats_text.insert(tk.END, stats_content)
        stats_text.config(state=tk.DISABLED)

    def show_memory_usage(self):
        """Show memory usage information"""
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            
            # DataFrame memory usage
            df_memory = 0
            if self.dataframe is not None:
                df_memory = self.dataframe.memory_usage(deep=True).sum() / 1024 / 1024
            
            filtered_memory = 0
            if self.filtered_df is not None:
                filtered_memory = self.filtered_df.memory_usage(deep=True).sum() / 1024 / 1024
            
            memory_text = f"""Memory Usage Information
{'=' * 30}

Process Memory:
  RSS: {memory_info.rss / 1024 / 1024:.2f} MB
  VMS: {memory_info.vms / 1024 / 1024:.2f} MB

DataFrame Memory:
  Original Data: {df_memory:.2f} MB
  Filtered Data: {filtered_memory:.2f} MB
  Total: {df_memory + filtered_memory:.2f} MB

System Memory:
  Available: {psutil.virtual_memory().available / 1024 / 1024 / 1024:.2f} GB
  Used: {psutil.virtual_memory().percent:.1f}%
"""
            
            messagebox.showinfo("Memory Usage", memory_text)
            
        except ImportError:
            messagebox.showinfo("Memory Usage", "Install 'psutil' package to view detailed memory information.")
        except Exception as e:
            messagebox.showerror("Error", f"Could not retrieve memory information: {e}")

    def show_settings(self):
        """Show application settings"""
        settings_window = tb.Toplevel(self.root)
        settings_window.title("Settings")
        settings_window.geometry("500x400")
        
        notebook = ttk.Notebook(settings_window)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Connection settings
        conn_frame = ttk.Frame(notebook)
        notebook.add(conn_frame, text="Connection")
        
        ttk.Label(conn_frame, text="Connection Timeout (seconds):").pack(pady=5)
        timeout_var = tk.IntVar(value=self.config_manager.config["connection"]["timeout"])
        ttk.Entry(conn_frame, textvariable=timeout_var).pack(pady=5)
        
        # UI settings
        ui_frame = ttk.Frame(notebook)
        notebook.add(ui_frame, text="Interface")
        
        auto_refresh_var = tk.BooleanVar(value=self.auto_refresh_var.get())
        ttk.Checkbutton(ui_frame, text="Enable Auto Refresh", variable=auto_refresh_var).pack(pady=5)
        
        ttk.Label(ui_frame, text="Auto Refresh Interval (seconds):").pack(pady=5)
        interval_var = tk.IntVar(value=self.config_manager.config["ui"]["auto_refresh_interval"])
        ttk.Entry(ui_frame, textvariable=interval_var).pack(pady=5)
        
        # Data settings
        data_frame = ttk.Frame(notebook)
        notebook.add(data_frame, text="Data")
        
        cache_var = tk.BooleanVar(value=self.config_manager.config["data"]["cache_enabled"])
        ttk.Checkbutton(data_frame, text="Enable Data Caching", variable=cache_var).pack(pady=5)
        
        ttk.Label(data_frame, text="Display Limit:").pack(pady=5)
        limit_var = tk.IntVar(value=self.config_manager.config["data"]["display_limit"])
        ttk.Entry(data_frame, textvariable=limit_var).pack(pady=5)
        
        # Save button
        def save_settings():
            self.config_manager.config["connection"]["timeout"] = timeout_var.get()
            self.config_manager.config["ui"]["auto_refresh_interval"] = interval_var.get()
            self.config_manager.config["data"]["cache_enabled"] = cache_var.get()
            self.config_manager.config["data"]["display_limit"] = limit_var.get()
            self.auto_refresh_var.set(auto_refresh_var.get())
            
            self.config_manager.save_config()
            messagebox.showinfo("Settings", "Settings saved successfully!")
            settings_window.destroy()
        
        ttk.Button(settings_window, text="Save Settings", 
                  command=save_settings, style='success.TButton').pack(pady=10)

    def show_shortcuts(self):
        """Show keyboard shortcuts"""
        shortcuts_text = """Keyboard Shortcuts
==================

Ctrl+O    - Load Survey File
Ctrl+E    - Export Data
Ctrl+R    - Refresh Data
Ctrl+L    - Show Field Labels
Ctrl+Q    - Quit Application
F5        - Fetch Data

Data Table:
- Double-click column header to sort
- Right-click for context menu

Charts:
- Use mouse wheel to scroll
- Click 'X' to remove individual charts
"""
        messagebox.showinfo("Keyboard Shortcuts", shortcuts_text)

    def show_about(self):
        """Show about dialog"""
        about_text = f"""ODK Data Dashboard v{VERSION}

Enhanced dashboard for ODK Central data visualization and analysis.

Features:
• Multiple chart types and visualizations
• Advanced data filtering and search
• Data export with multiple formats
• Form schema integration with XLSForm support
• Data caching for improved performance
• Auto-refresh capabilities
• Comprehensive data validation
• Memory usage monitoring

Current User: {CURRENT_USER}
Python Version: {sys.version.split()[0]}
"""
        messagebox.showinfo("About ODK Dashboard", about_text)

    def memory_cleanup(self):
        """Clean up memory resources"""
        try:
            # Close all matplotlib figures
            plt.close('all')
            
            # Clear chart cache if it exists
            if hasattr(self, 'chart_cache'):
                self.chart_cache.clear()
            
            # Force garbage collection
            gc.collect()
            
            logger.info("Memory cleanup completed")
        except Exception as e:
            logger.error(f"Error during memory cleanup: {e}")

def main():
    """Enhanced main function with comprehensive error handling"""
    try:
        logger.info(f"Starting ODK Dashboard v{VERSION}")
        
        root = tb.Window(
            title=f"ODK Central Dashboard v{VERSION}",
            themename="darkly",
            size=(1600, 1200)
        )
        
        # Configure enhanced styles
        style = ttk.Style()
        style.configure('primary.TFrame', background='#007bff')
        style.configure('info.TFrame', background='#17a2b8')
        style.configure('success.TFrame', background='#28a745')
        style.configure('warning.TFrame', background='#ffc107')
        style.configure('danger.TFrame', background='#dc3545')
        
        style.configure('primary.TLabel', background='#007bff', foreground='white')
        style.configure('info.TLabel', background='#17a2b8', foreground='white')
        style.configure('success.TLabel', background='#28a745', foreground='white')
        style.configure('warning.TLabel', background='#ffc107', foreground='black')
        style.configure('danger.TLabel', background='#dc3545', foreground='white')
        
        style.configure('primary.Inverse.TLabel', 
                       background='#007bff', foreground='white', 
                       font=('Helvetica', 10, 'bold'))
        style.configure('info.Inverse.TLabel', 
                       background="#17a2b8", foreground='white', 
                       font=('Helvetica', 10, 'bold'))
        
        app = Dashboard(root)
        
        # Center window
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        x = (screen_width - 1600) // 2
        y = (screen_height - 1200) // 2
        root.geometry(f"1600x1200+{x}+{y}")
        
        # Set background
        root.configure(bg="#450505")
        
        # Set up close handler
        def on_closing():
            logger.info("Application closing...")
            app.memory_cleanup()
            if app.auto_refresh_job:
                root.after_cancel(app.auto_refresh_job)
            root.destroy()
        
        root.protocol("WM_DELETE_WINDOW", on_closing)
        
        logger.info("Dashboard started successfully")
        root.mainloop()
        
    except Exception as e:
        error_msg = f"Application startup failed: {str(e)}"
        logger.critical(error_msg)
        try:
            tb.dialogs.Messagebox.show_error("Application Error", error_msg)
        except:
            print(error_msg)
        raise
    # Add these modern dashboard styles
    style.configure('light.TFrame', background='white')
    style.configure('secondary.TButton', background='#6c757d', foreground='white')

if __name__ == "__main__":
    main()