import tkinter as tk
import ttkbootstrap as tb
from tkinter import ttk, messagebox, scrolledtext
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
from datetime import datetime, timezone
import numpy as np
import matplotlib.figure as Figure
from matplotlib.patches import Rectangle
from ttkbootstrap.constants import *
import os
import sys
from ttkbootstrap.widgets import DateEntry
from datetime import datetime, timedelta
import logging

# Try to import optional dependencies
try:
    from scipy import stats
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    logging.warning("scipy not available - some statistical features will be disabled")

try:
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB_DATES = True
except ImportError:
    HAS_MATPLOTLIB_DATES = False

# Configure logging for better error tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dashboard.log'),
        logging.StreamHandler()
    ]
)

class DateUtils:
    """Enhanced date handling and validation utilities"""
    
    # Common date formats to try when parsing
    DATE_FORMATS = [
        '%Y-%m-%d',           # ISO format: 2023-12-31
        '%Y-%m-%d %H:%M:%S',  # ISO datetime: 2023-12-31 23:59:59
        '%Y-%m-%dT%H:%M:%S',  # ISO T format: 2023-12-31T23:59:59
        '%Y-%m-%dT%H:%M:%SZ', # ISO Z format: 2023-12-31T23:59:59Z
        '%d/%m/%Y',           # DD/MM/YYYY: 31/12/2023
        '%m/%d/%Y',           # MM/DD/YYYY: 12/31/2023
        '%d-%m-%Y',           # DD-MM-YYYY: 31-12-2023
        '%m-%d-%Y',           # MM-DD-YYYY: 12-31-2023
        '%d.%m.%Y',           # DD.MM.YYYY: 31.12.2023
        '%Y%m%d',             # YYYYMMDD: 20231231
        '%d %b %Y',           # DD Mon YYYY: 31 Dec 2023
        '%d %B %Y',           # DD Month YYYY: 31 December 2023
        '%b %d, %Y',          # Mon DD, YYYY: Dec 31, 2023
        '%B %d, %Y',          # Month DD, YYYY: December 31, 2023
    ]
    
    @staticmethod
    def parse_date(date_string, default_format=None):
        """
        Parse date string with multiple format attempts
        
        Args:
            date_string: String to parse
            default_format: Preferred format to try first
            
        Returns:
            datetime object or None if parsing fails
        """
        if not date_string or pd.isna(date_string):
            return None
            
        date_string = str(date_string).strip()
        
        # Try default format first if provided
        if default_format:
            try:
                return datetime.strptime(date_string, default_format)
            except ValueError:
                pass
        
        # Try common formats
        for fmt in DateUtils.DATE_FORMATS:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        
        # Try pandas parsing as last resort
        try:
            return pd.to_datetime(date_string, infer_datetime_format=True)
        except:
            return None
    
    @staticmethod
    def validate_date_range(start_date, end_date):
        """
        Validate that date range is logical
        
        Returns:
            tuple: (is_valid, error_message)
        """
        if start_date is None or end_date is None:
            return True, ""  # Allow None values
        
        if start_date > end_date:
            return False, "Start date must be before end date"
        
        # Check for reasonable date ranges (not too far in past/future)
        now = datetime.now()
        year_1900 = datetime(1900, 1, 1)
        year_2100 = datetime(2100, 1, 1)
        
        if start_date < year_1900:
            return False, "Start date is too far in the past (before 1900)"
        
        if end_date > year_2100:
            return False, "End date is too far in the future (after 2100)"
        
        return True, ""
    
    @staticmethod
    def format_date_for_display(date_obj, format_type='short'):
        """
        Format date for user-friendly display
        
        Args:
            date_obj: datetime object
            format_type: 'short', 'long', or 'iso'
        """
        if date_obj is None:
            return "N/A"
        
        try:
            if format_type == 'short':
                return date_obj.strftime('%Y-%m-%d')
            elif format_type == 'long':
                return date_obj.strftime('%B %d, %Y')
            elif format_type == 'iso':
                return date_obj.isoformat()
            else:
                return str(date_obj)
        except:
            return str(date_obj)
    
    @staticmethod
    def detect_date_columns(df):
        """
        Detect potential date columns in DataFrame
        
        Returns:
            list: Column names that likely contain dates
        """
        date_columns = []
        
        # Keywords that suggest date columns
        date_keywords = ['date', 'time', 'created', 'updated', 'submission', 
                        'start', 'end', 'timestamp', 'when', 'day']
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Check if column name contains date keywords
            if any(keyword in col_lower for keyword in date_keywords):
                date_columns.append(col)
                continue
            
            # Check if column values look like dates
            if len(df) > 0:
                sample_values = df[col].dropna().head(10)
                date_like_count = 0
                
                for value in sample_values:
                    if DateUtils.parse_date(value) is not None:
                        date_like_count += 1
                
                # If more than 50% of sample values are date-like
                if date_like_count > len(sample_values) * 0.5:
                    date_columns.append(col)
        
        return date_columns
    
    @staticmethod
    def convert_column_to_datetime(df, column_name):
        """
        Convert a DataFrame column to datetime with error handling
        
        Returns:
            tuple: (success, error_message, converted_series)
        """
        try:
            if column_name not in df.columns:
                return False, f"Column '{column_name}' not found", None
            
            original_series = df[column_name].copy()
            converted_series = pd.Series(index=original_series.index, dtype='datetime64[ns]')
            
            failed_conversions = []
            
            for idx, value in original_series.items():
                parsed_date = DateUtils.parse_date(value)
                if parsed_date is not None:
                    converted_series.iloc[idx] = parsed_date
                else:
                    failed_conversions.append(str(value))
            
            success_rate = (len(original_series) - len(failed_conversions)) / len(original_series)
            
            if success_rate < 0.5:
                return False, f"Could not parse {len(failed_conversions)} of {len(original_series)} values", None
            
            error_msg = ""
            if failed_conversions:
                error_msg = f"Warning: {len(failed_conversions)} values could not be parsed"
            
            return True, error_msg, converted_series
            
        except Exception as e:
            return False, str(e), None

CURRENT_USER = os.getlogin()

class ErrorHandler:
    """Enhanced error handling with user-friendly messages and logging"""
    
    @staticmethod
    def handle_error(error, context="Operation", show_user=True, log_level=logging.ERROR):
        """
        Centralized error handling with logging and user feedback
        
        Args:
            error: The exception or error message
            context: Context where the error occurred
            show_user: Whether to show messagebox to user
            log_level: Logging level for the error
        """
        error_msg = str(error)
        
        # Log the error with context
        logging.log(log_level, f"{context}: {error_msg}")
        
        # Create user-friendly error message
        user_msg = ErrorHandler._get_user_friendly_message(error, context)
        
        if show_user:
            messagebox.showerror("Error", user_msg)
        
        return user_msg
    
    @staticmethod
    def _get_user_friendly_message(error, context):
        """Convert technical errors to user-friendly messages"""
        error_str = str(error).lower()
        
        # Connection errors
        if "connection" in error_str or "timeout" in error_str:
            return f"Connection problem during {context}. Please check your internet connection and try again."
        
        # Authentication errors
        if "auth" in error_str or "credential" in error_str or "login" in error_str:
            return f"Authentication failed during {context}. Please check your username and password."
        
        # Data processing errors
        if "pandas" in error_str or "dataframe" in error_str:
            return f"Data processing error during {context}. The data format may be incompatible."
        
        # File errors
        if "file" in error_str or "directory" in error_str or "permission" in error_str:
            return f"File access error during {context}. Please check file permissions and try again."
        
        # Memory errors
        if "memory" in error_str or "size" in error_str:
            return f"Memory error during {context}. The dataset may be too large. Try reducing the data size."
        
        # Default message
        return f"An error occurred during {context}: {str(error)[:100]}{'...' if len(str(error)) > 100 else ''}"
    
    @staticmethod
    def show_warning(message, title="Warning"):
        """Show warning message to user"""
        logging.warning(f"{title}: {message}")
        messagebox.showwarning(title, message)
    
    @staticmethod
    def show_info(message, title="Information"):
        """Show info message to user"""
        logging.info(f"{title}: {message}")
        messagebox.showinfo(title, message)
    
    @staticmethod
    def show_success(message, title="Success"):
        """Show success message to user"""
        logging.info(f"{title}: {message}")
        messagebox.showinfo(title, message)
class Dashboard:
    def __init__(self, root):
        self.root = root
        self.root.title("ODK Data Dashboard")
        
        # Responsive window management
        self.setup_responsive_window()
                
        # Initialize all variables first
        self.initialize_variables()
        self.initialize_dataframes()

        # Setup UI components
        self.setup_ui()

        # Create status bar
        self.create_status_bar()
        
        # Setup window event handlers
        self.setup_window_handlers()
        
    def setup_responsive_window(self):
        """Setup responsive window with proper sizing and state management"""
        # Get screen dimensions
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        # Calculate responsive window size (80% of screen, but with minimums)
        min_width, min_height = 1000, 700
        max_width, max_height = 1800, 1200
        
        window_width = max(min_width, min(int(screen_width * 0.8), max_width))
        window_height = max(min_height, min(int(screen_height * 0.8), max_height))
        
        # Center window on screen
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
        self.root.minsize(min_width, min_height)
        
        # Allow window to be resizable
        self.root.resizable(True, True)
        
        # Save window state
        self.window_state = {
            'width': window_width,
            'height': window_height,
            'x': x,
            'y': y,
            'maximized': False
        }
        
        logging.info(f"Window initialized: {window_width}x{window_height} on {screen_width}x{screen_height} screen")
    
    def setup_window_handlers(self):
        """Setup window event handlers for responsive behavior"""
        # Handle window resize
        self.root.bind('<Configure>', self.on_window_configure)
        
        # Handle window state changes
        self.root.bind('<KeyPress-F11>', self.toggle_fullscreen)
        self.root.bind('<Control-plus>', self.increase_font_size)
        self.root.bind('<Control-minus>', self.decrease_font_size)
        self.root.bind('<Control-0>', self.reset_font_size)
        
        # Handle window close
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Initialize font scaling
        self.font_scale = 1.0
    
    def on_window_configure(self, event):
        """Handle window resize events"""
        if event.widget == self.root:
            # Update window state
            self.window_state['width'] = self.root.winfo_width()
            self.window_state['height'] = self.root.winfo_height()
            
            # Adjust UI elements based on window size
            self.adjust_ui_for_size()
    
    def adjust_ui_for_size(self):
        """Adjust UI elements based on current window size"""
        width = self.window_state['width']
        height = self.window_state['height']
        
        # Adjust chart frame size
        if hasattr(self, 'chart_frame'):
            if width < 1200:
                # Smaller window: stack elements vertically
                chart_height = max(300, height // 3)
            else:
                # Larger window: more space for charts
                chart_height = max(400, height // 2)
                
        # Adjust table display
        if hasattr(self, 'tree'):
            if width < 1000:
                # Hide some columns on smaller screens
                self.tree.column('#0', width=0, stretch=False)
            else:
                # Show all columns on larger screens
                self.tree.column('#0', width=50, stretch=False)
    
    def toggle_fullscreen(self, event=None):
        """Toggle fullscreen mode"""
        current_state = self.root.attributes('-fullscreen')
        self.root.attributes('-fullscreen', not current_state)
        self.window_state['maximized'] = not current_state
    
    def increase_font_size(self, event=None):
        """Increase font size for better accessibility"""
        self.font_scale = min(1.5, self.font_scale + 0.1)
        self.apply_font_scaling()
    
    def decrease_font_size(self, event=None):
        """Decrease font size"""
        self.font_scale = max(0.7, self.font_scale - 0.1)
        self.apply_font_scaling()
    
    def reset_font_size(self, event=None):
        """Reset font size to default"""
        self.font_scale = 1.0
        self.apply_font_scaling()
    
    def apply_font_scaling(self):
        """Apply current font scaling to UI elements"""
        try:
            # Update default font
            default_font = ('Helvetica', int(10 * self.font_scale))
            self.root.option_add('*Font', default_font)
            
            # Force UI update
            self.root.update_idletasks()
            
            logging.info(f"Font scale applied: {self.font_scale:.1f}x")
        except Exception as e:
            ErrorHandler.handle_error(e, "applying font scaling", show_user=False)
    
    def on_closing(self):
        """Handle application closing with cleanup"""
        try:
            # Save window state for next session
            self.save_window_state()
            
            # Cleanup resources
            if hasattr(self, 'dataframe') and self.dataframe is not None:
                del self.dataframe
            if hasattr(self, 'filtered_df') and self.filtered_df is not None:
                del self.filtered_df
                
            logging.info("Application closing - cleanup completed")
            
        except Exception as e:
            ErrorHandler.handle_error(e, "application cleanup", show_user=False)
        finally:
            self.root.destroy()
    
    def save_window_state(self):
        """Save current window state for next session"""
        try:
            import json
            state_file = 'window_state.json'
            
            with open(state_file, 'w') as f:
                json.dump(self.window_state, f)
                
        except Exception as e:
            logging.warning(f"Could not save window state: {e}")
    
    def load_window_state(self):
        """Load saved window state"""
        try:
            import json
            state_file = 'window_state.json'
            
            if os.path.exists(state_file):
                with open(state_file, 'r') as f:
                    saved_state = json.load(f)
                    
                # Apply saved state if reasonable
                if (saved_state.get('width', 0) > 800 and 
                    saved_state.get('height', 0) > 600):
                    self.window_state.update(saved_state)
                    
                    geometry = f"{saved_state['width']}x{saved_state['height']}"
                    if 'x' in saved_state and 'y' in saved_state:
                        geometry += f"+{saved_state['x']}+{saved_state['y']}"
                    
                    self.root.geometry(geometry)
                    
        except Exception as e:
            logging.warning(f"Could not load window state: {e}")
        
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
        self.form_schema = None  # Store form schema for reference
        self.form_labels = {}    # Store field labels from form definition
        self.field_mappings = {} # Store field name to label mappings
        self.choice_mappings = {} # Store choice values to labels
        self.survey_sheet = None  # Store survey sheet data
        self.choices_sheet = None # Store choices sheet data
        
        # UI components
        self.tree = None
        self.notebook = None
        self.filter_column_combo = None
        self.column_combo = None
        self.chart_type_combo = None
        
        # Status variables
        self.status_var = tk.StringVar(value="Ready")

    def _optimize_dataframe_memory(self, df):
        """Optimize DataFrame memory usage by converting data types"""
        if df.empty:
            return df
            
        original_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
        
        for col in df.columns:
            col_type = df[col].dtype
            
            # Convert object columns with few unique values to category
            if col_type == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:  # Less than 50% unique values
                    df[col] = df[col].astype('category')
            
            # Optimize numeric columns
            elif col_type.kind in 'if':  # integer or float
                col_min = df[col].min()
                col_max = df[col].max()
                
                if col_type.kind == 'i':  # integer
                    if col_min >= 0:  # unsigned integers
                        if col_max < 256:
                            df[col] = df[col].astype('uint8')
                        elif col_max < 65536:
                            df[col] = df[col].astype('uint16')
                        elif col_max < 4294967296:
                            df[col] = df[col].astype('uint32')
                    else:  # signed integers
                        if col_min >= -128 and col_max < 127:
                            df[col] = df[col].astype('int8')
                        elif col_min >= -32768 and col_max < 32767:
                            df[col] = df[col].astype('int16')
                        elif col_min >= -2147483648 and col_max < 2147483647:
                            df[col] = df[col].astype('int32')
                
                elif col_type.kind == 'f':  # float
                    if (df[col] == df[col].astype('float32')).all():
                        df[col] = df[col].astype('float32')
        
        new_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
        memory_saved = original_memory - new_memory
        
        if memory_saved > 1:  # Only log if significant savings
            logging.info(f"Memory optimization: {original_memory:.1f}MB -> {new_memory:.1f}MB "
                        f"(saved {memory_saved:.1f}MB)")
        
        return df

    def initialize_dataframes(self):
        """Initialize dataframes with empty DataFrames"""
        self.dataframe = pd.DataFrame()
        self.filtered_df = pd.DataFrame()
        self.form_labels = {}
        self.field_mappings = {}
        self.choice_mappings = {}
        self.last_update_time = None

    def create_summary_stats(self):
        """Create summary statistics display"""
        try:
            # Create the stats frame if it doesn't exist
            if not hasattr(self, 'stats_frame'):
                return

            # Clear existing stats
            for widget in self.stats_frame.winfo_children():
                widget.destroy()

            stats_style = {
                'font': ('Helvetica', 10, 'bold'),
                'fg': '#00ff41',
                'background': '#2b2b2b'
            }

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
            ErrorHandler.handle_error(e, "creating summary statistics", show_user=False)
            # Create minimal stats if there's an error
            if hasattr(self, 'stats_frame'):
                ttk.Label(self.stats_frame, 
                        text="Statistics temporarily unavailable",
                        style='warning.TLabel').pack()

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
        
    def setup_ui(self):
        """Set up the user interface"""
        # User info frame
# User info frame
        user_frame = tk.Frame(
            self.root,
            bg='#2b2b2b',  # Dark background
            relief='ridge',
            borderwidth=2
        )
        user_frame.pack(fill="x", padx=10, pady=5)

        # Add some padding inside the frame
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
            fg='#00ff41',  # Bright green text
            relief='groove',
            borderwidth=1,
            padx=10,
            pady=5
        )
        date_label.pack(side="right")

        # Optional: Add a welcome message on the left
        welcome_label = tk.Label(
            inner_frame,
            text=f"👤 Welcome {CURRENT_USER}",
            font=('calibri', 15, 'bold'),
            bg="#510505",
            fg="#18045F",  
            padx=10,
            pady=5
        )
        welcome_label.pack(side="left")

        # Create main frames
        connection_frame = ttk.LabelFrame(self.root, text="ODK Connection", padding=10)
        connection_frame.pack(fill="x", padx=10, pady=5)
        
        filter_frame = ttk.LabelFrame(self.root, text="Filters", padding=10)
        filter_frame.pack(fill="x", padx=10, pady=5)
        
        data_frame = ttk.LabelFrame(self.root, text="Data View", padding=10)
        data_frame.pack(fill="both", expand=True, padx=10, pady=5)
        # Define styles
        style = ttk.Style()
        style.configure("TLabel", foreground="blue", background="#252323", font=("Segoe UI", 10))
        style.configure("TEntry", fieldbackground="#1e1e1e", foreground="lime", insertcolor="yellow")

        # Connection form
        ttk.Label(connection_frame, text="ODK URL:").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Entry(connection_frame, textvariable=self.url_var, width=40).grid(row=0, column=1, pady=2, padx=5)
        
        ttk.Label(connection_frame, text="Project ID:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(connection_frame, textvariable=self.project_id_var, width=40).grid(row=1, column=1, pady=2, padx=5)
        
        ttk.Label(connection_frame, text="Form ID:").grid(row=2, column=0, sticky="w", pady=2)
        ttk.Entry(connection_frame, textvariable=self.form_id_var, width=40).grid(row=2, column=1, pady=2, padx=5)
        
        ttk.Label(connection_frame, text="Username:").grid(row=0, column=2, sticky="w", pady=2, padx=(20, 0))
        ttk.Entry(connection_frame, textvariable=self.username_var, width=20).grid(row=0, column=3, pady=2, padx=5)
        
        ttk.Label(connection_frame, text="Password:").grid(row=1, column=2, sticky="w", pady=2, padx=(20, 0))
        ttk.Entry(connection_frame, textvariable=self.password_var, show="*", width=20).grid(row=1, column=3, pady=2, padx=5)
        
        # Connection buttons
        btn_frame = ttk.Frame(connection_frame)
        btn_frame.grid(row=3, column=0, columnspan=4, pady=10)
        
        ttk.Button(btn_frame, text="Fetch Data", command=self.download_data).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Refresh", command=self.refresh_data).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Export data", command=self.export_csv).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Data Labels", command=self.show_labels_window).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Load Survey File", command=self.load_survey_file).pack(side="left", padx=5)

        #progress Bar
        progress_frame=ttk.Frame(connection_frame)
        progress_frame.grid(row=4,column=0,columnspan=4,pady=5)
        self.progress = ttk.Progressbar(progress_frame, orient=HORIZONTAL, length=400, mode='determinate')
        self.progress.pack(fill="x")

        # Filter section
        filter_controls = ttk.Frame(filter_frame)
        filter_controls.pack(fill="x", padx=5, pady=5)
                
        ttk.Label(filter_controls, text="Date From:").grid(row=1, column=0, sticky="w", pady=2)

        # Date From calendar dropdown
        date_from_calendar = DateEntry(
            filter_controls,
            dateformat='%Y-%m-%d',
            width=12,
            borderwidth=2,
            firstweekday=0,
            startdate=datetime.now().date(),
            bootstyle="primary"
        )
        date_from_calendar.grid(row=1, column=1, pady=2, padx=5)

        ttk.Label(filter_controls, text="Date To:").grid(row=1, column=2, sticky="w", pady=2)

        # Date To calendar dropdown
        date_to_calendar = DateEntry(
            filter_controls,
            dateformat='%Y-%m-%d',
            width=12,
            borderwidth=2,
            firstweekday=0,
            startdate=datetime.now().date(),
            bootstyle="secondary"
        )
        date_to_calendar.grid(row=1, column=3, pady=2, padx=5)

        # Set default dates (optional)
        date_from_calendar.set_date(datetime.now().date() - timedelta(days=30))  # Default: 30 days ago
        date_to_calendar.set_date(datetime.now().date())  # Default: today


        filter_buttons = ttk.Frame(filter_frame)
        filter_buttons.pack(fill="x", padx=5, pady=5)
        
        ttk.Button(filter_buttons, text="Apply Filters", 
                command=self.apply_filters, style='success.TButton').pack(side="left", padx=5)
        ttk.Button(filter_buttons, text="Resets", 
                command=self.reset_filters, style='warning.TButton').pack(side="right", padx=5)
        #########################################################################
        # Statistics section
        stats_frame = ttk.LabelFrame(filter_frame, text="Data Submission", padding=10)
        stats_frame.pack(fill="x", padx=5, pady=5)

        # Total submissions counter
        total_counter_frame = ttk.Frame(stats_frame, style='primary.TFrame')
        total_counter_frame.pack(side="left", padx=10, pady=5)

        ttk.Label(total_counter_frame, text="Data Submitted", 
                style='primary.Inverse.TLabel').pack(pady=2)
        total_count_label = ttk.Label(total_counter_frame, 
                                    textvariable=self.submission_count_var,
                                    style='primary.TLabel',
                                    font=('Helvetica', 16, 'bold'))
        total_count_label.pack()

        # Filtered submissions counter
        filtered_counter_frame = ttk.Frame(stats_frame, style='info.TFrame')
        filtered_counter_frame.pack(side="left", padx=10, pady=5)

        ttk.Label(filtered_counter_frame, text="Filtered ", 
                style='info.Inverse.TLabel').pack(pady=2)
        filtered_count_label = ttk.Label(filtered_counter_frame, 
                                        textvariable=self.filtered_count_var,
                                        style='info.TLabel',
                                        font=('calibri', 16, 'bold'))
        filtered_count_label.pack()
        #########################################################################
        # Notebook for data views - MOVED INSIDE data_frame
        self.notebook = ttk.Notebook(data_frame)
        self.notebook.pack(fill="both", expand=True)
        
        # Setup treeview (Data Table tab)
        self.setup_treeview()

        # NEW: Visualization tab with controls
        self.setup_visualization_tab()

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

    def setup_visualization_tab(self):
        """Set up the visualization tab with controls and chart display area"""
        # Create visualization frame as a notebook tab
        viz_frame = ttk.Frame(self.notebook)
        self.notebook.add(viz_frame, text="Visualizations")
        
        # Main vertical layout
        main_layout = ttk.Frame(viz_frame)
        main_layout.pack(fill="both", expand=True)
        
        # Top row - Control panel and summary stats
        top_row = ttk.Frame(main_layout)
        top_row.pack(fill="x", padx=5, pady=5)
        
        # Controls
        controls_frame = ttk.LabelFrame(top_row, text="Visualization Controls", padding=10)
        controls_frame.pack(fill="x", side="left", expand=True)

        # Add controls
        self.setup_visualization_controls(controls_frame)

        # Summary stats frame
        self.stats_frame = ttk.Frame(top_row)
        self.stats_frame.pack(fill="x", side="right", padx=5)
        
        # Initialize summary stats
        self.create_summary_stats()

        # Create scrollable canvas for charts
        self.setup_charts_canvas(main_layout)
        
        # Create grid frame for charts
        self.charts_frame = ttk.Frame(self.viz_canvas)
        self.canvas_window = self.viz_canvas.create_window(
            (0, 0),
            window=self.charts_frame,
            anchor="nw",
            tags=("win",)
        )
        
        # Configure grid layout
        self.charts_frame.grid_columnconfigure(0, weight=1)
        self.charts_frame.grid_columnconfigure(1, weight=1)
        
        self.charts_frame.bind('<Configure>', self.on_frame_configure)
        self.viz_canvas.bind('<Configure>', self.on_canvas_configure)
        
        # Initialize chart tracking
        self.chart_grid = []
        self.current_row = 0
        self.current_col = 0

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
        """Set up the scrollable canvas for charts"""
        canvas_frame = ttk.Frame(parent)
        canvas_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.viz_canvas = tk.Canvas(canvas_frame, bg='#2b2b2b')
        scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", 
                                command=self.viz_canvas.yview)
        self.viz_canvas.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side="right", fill="y")
        self.viz_canvas.pack(side="left", fill="both", expand=True)
        
        self.charts_frame = ttk.Frame(self.viz_canvas)
        self.canvas_window = self.viz_canvas.create_window(
            (0, 0),
            window=self.charts_frame,
            anchor="nw",
            tags=("win",)
        )
        
        # Configure grid layout
        self.charts_frame.grid_columnconfigure(0, weight=1)
        self.charts_frame.grid_columnconfigure(1, weight=1)
        
        self.charts_frame.bind('<Configure>', self.on_frame_configure)
        self.viz_canvas.bind('<Configure>', self.on_canvas_configure)
        
        # Initialize chart tracking
        self.chart_grid = []
        self.current_row = 0
        self.current_col = 0
        
        # Add placeholder
        self.add_chart_placeholder()

    def add_chart_placeholder(self):
        """Add placeholder message when no charts are present"""
        self.chart_placeholder = ttk.Label(
            self.charts_frame,
            text="Select a column and chart type, then click 'Add Chart' to create visualizations",
            font=('Helvetica', 12),
            anchor='center'
        )
        self.chart_placeholder.grid(row=0, column=0, columnspan=2, pady=20)               

    def create_summary_stats(self):
        """Create summary statistics display"""
        stats_style = {'font': ('Helvetica', 10, 'bold'), 'fg': '#00ff41'}

        # Example stats - replace with your actual stats
        stats = {
            'Total Records': f"{len(self.filtered_df) if hasattr(self, 'filtered_df') else 0:,}",
            'Columns': f"{len(self.filtered_df.columns) if hasattr(self, 'filtered_df') else 0}",
            'Last Updated': datetime.now().strftime('%H:%M:%S')
        }
        
        for i, (label, value) in enumerate(stats.items()):
            stat_frame = ttk.Frame(self.stats_frame)
            stat_frame.pack(side="left", padx=20)
            
            ttk.Label(stat_frame, text=label, **stats_style).pack()
            ttk.Label(stat_frame, text=value, **stats_style).pack()

    def add_visualization(self):
        """Add a new chart to the visualization grid"""
        if self.filtered_df is None or self.filtered_df.empty:
            messagebox.showwarning("Warning", "No data available for visualization")
            return
        
        chart_type = self.chart_type_var.get()
        display_column = self.selected_column_var.get()
        column = self.extract_column_name_from_display(display_column) if display_column else ""
        
        if not column and chart_type not in ["Correlation", "Time Series"]:
            messagebox.showwarning("Warning", "Please select a column to visualize")
            return
        
        # Remove placeholder if it exists
        if hasattr(self, 'chart_placeholder') and self.chart_placeholder:
            self.chart_placeholder.destroy()
            self.chart_placeholder = None
        
        # Create frame for the new chart
        chart_container = ttk.LabelFrame(self.charts_frame, 
                                    text=f"{chart_type}: {display_column}", 
                                    padding=10)
        
        # Add to grid
        chart_container.grid(row=self.current_row, column=self.current_col,
                            padx=5, pady=5, sticky="nsew")
        
        # Add close button
        close_btn = ttk.Button(chart_container, text="×", width=3,
                            command=lambda: self.remove_chart(chart_container))
        close_btn.pack(side="right", padx=2, pady=2)
        
        try:
            # Create the visualization
            if chart_type == "Time Series":
                self.create_time_series_plot(chart_container)
            elif chart_type == "Distribution":
                self.create_distribution_plot(chart_container)
            elif chart_type == "Correlation":
                self.create_correlation_plot(chart_container)
            elif chart_type == "Pie Chart":
                self.create_pie_chart(chart_container, column)
            elif chart_type == "Horizontal Bar":
                self.create_horizontal_bar_chart(chart_container, column)
            elif chart_type == "Stacked Bar":
                self.create_stacked_bar_chart(chart_container, column)
            
            # Update grid position
            self.current_col = (self.current_col + 1) % 2
            if self.current_col == 0:
                self.current_row += 1
            
            # Track the chart
            self.chart_grid.append(chart_container)
            
            # Update canvas scroll region
            self.viz_canvas.configure(scrollregion=self.viz_canvas.bbox("all"))
            
        except Exception as e:
            messagebox.showerror("Error", f"Visualization failed: {str(e)}")
            chart_container.destroy()

    def remove_chart(self, chart_container):
        """Remove a specific chart from the grid"""
        chart_container.destroy()
        self.chart_grid.remove(chart_container)
        self.reorganize_grid()

    def reorganize_grid(self):
        """Reorganize remaining charts in the grid"""
        if not self.chart_grid:
            self.current_row = 0
            self.current_col = 0
            return
        
        # Reposition all remaining charts
        for i, chart in enumerate(self.chart_grid):
            row = i // 2
            col = i % 2
            chart.grid(row=row, column=col, padx=5, pady=5, sticky="nsew")
        
        self.current_row = (len(self.chart_grid) - 1) // 2
        self.current_col = (len(self.chart_grid) - 1) % 2
        
        # Update canvas scroll region
        self.viz_canvas.configure(scrollregion=self.viz_canvas.bbox("all"))

    def clear_all_charts(self):
        """Remove all charts from the visualization area"""
        for chart in self.chart_grid:
            chart.destroy()
        self.chart_grid = []
        self.current_row = 0
        self.current_col = 0
        
        # Restore placeholder
        self.chart_placeholder = ttk.Label(self.charts_frame, 
                                        text="Select a column and chart type, then click 'Add Chart' to create visualizations",
                                        font=('Helvetica', 12),
                                        anchor='center')
        self.chart_placeholder.grid(row=0, column=0, columnspan=2, pady=20)

    def load_survey_file(self):
        """Load XLSForm survey file to extract labels and choices"""
        from tkinter import filedialog
        
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
                print(f"Loaded survey sheet with {len(self.survey_sheet)} rows")
            else:
                messagebox.showwarning("Warning", "No 'survey' sheet found in the file")
                return
            
            # Load choices sheet
            if 'choices' in excel_file.sheet_names:
                self.choices_sheet = pd.read_excel(filename, sheet_name='choices')
                print(f"Loaded choices sheet with {len(self.choices_sheet)} rows")
            
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
                    choice_list_name = str(row.get('choice_filter', ''))
                    
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
                            # Extract choice list name from type (e.g., "select_one gender_list")
                            type_parts = field_type.split()
                            if len(type_parts) > 1:
                                choice_list_name = type_parts[1]
                                self.field_mappings[field_name]['choice_list'] = choice_list_name
                
                print(f"Processed {len(self.form_labels)} field labels from survey sheet")
            
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
                
                print(f"Processed {len(self.choice_mappings)} choice lists from choices sheet")
                
                # Map choices to field mappings
                for field_name, field_info in self.field_mappings.items():
                    if 'choice_list' in field_info:
                        choice_list = field_info['choice_list']
                        if choice_list in self.choice_mappings:
                            field_info['choices'] = self.choice_mappings[choice_list]
            
            # Update UI if data is already loaded
            if self.dataframe is not None:
                self.update_ui_after_download("Survey file loaded")
                
        except Exception as e:
            print(f"Error processing XLSForm data: {str(e)}")
            messagebox.showerror("Error", f"Error processing XLSForm data: {str(e)}")

    def get_form_schema(self, auth):
        """Fetch form schema to understand all available fields and their labels"""
        try:
            base_url = self.url_var.get().rstrip('/')
            project_id = self.project_id_var.get()
            form_id = self.form_id_var.get()
            
            # Get form definition/schema
            form_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}"
            response = requests.get(form_url, auth=auth, timeout=30)
            response.raise_for_status()
            
            form_data = response.json()
            self.form_schema = form_data
            
            # Try to get the XLSForm source file
            try:
                xlsx_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/xlsx"
                xlsx_response = requests.get(xlsx_url, auth=auth, timeout=30)
                
                if xlsx_response.status_code == 200:
                    # Save to temporary file and process
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
                        temp_file.write(xlsx_response.content)
                        temp_filename = temp_file.name
                    
                    self.load_xlsform_data(temp_filename)
                    
                    # Clean up temp file
                    import os
                    os.unlink(temp_filename)
                    
                    print("Successfully downloaded and processed XLSForm from ODK Central")
                    
            except Exception as e:
                print(f"Could not fetch XLSForm from ODK Central: {str(e)}")
            
            # Try to get the XForm XML definition for detailed field information
            try:
                xml_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/xml"
                xml_response = requests.get(xml_url, auth=auth, timeout=30)
                xml_response.raise_for_status()
                
                # Parse form XML to extract field labels
                self.parse_form_xml(xml_response.text)
                
            except Exception as e:
                print(f"Could not fetch form XML: {str(e)}")
            
            # Try to get form fields from the fields endpoint
            try:
                fields_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/fields"
                fields_response = requests.get(fields_url, auth=auth, timeout=30)
                if fields_response.status_code == 200:
                    fields_data = fields_response.json()
                    self.parse_form_fields(fields_data)
            except Exception as e:
                print(f"Could not fetch form fields: {str(e)}")
            
            return True
            
        except Exception as e:
            print(f"Could not fetch form schema: {str(e)}")
            return False

    def parse_form_xml(self, xml_content):
        """Parse XForm XML to extract field labels and metadata"""
        try:
            import xml.etree.ElementTree as ET
            
            # Parse the XML
            root = ET.fromstring(xml_content)
            
            # Define namespaces commonly used in XForms
            namespaces = {
                'xf': 'http://www.w3.org/2002/xforms',
                'h': 'http://www.w3.org/1999/xhtml',
                'jr': 'http://openrosa.org/javarosa',
                'odk': 'http://www.opendatakit.org/xforms'
            }
            
            # Extract labels from the form
            labels = {}
            
            # Look for bind elements that define field names
            binds = root.findall('.//xf:bind', namespaces)
            for bind in binds:
                nodeset = bind.get('nodeset', '')
                if nodeset:
                    # Clean the nodeset to get field name
                    field_name = nodeset.split('/')[-1] if '/' in nodeset else nodeset
                    field_name = field_name.replace('/data/', '').replace('/', '_')
                    
                    # Store field metadata
                    field_info = {
                        'nodeset': nodeset,
                        'type': bind.get('type', ''),
                        'required': bind.get('required', '') == 'true()',
                        'readonly': bind.get('readonly', '') == 'true()',
                        'constraint': bind.get('constraint', '')
                    }
                    
                    if field_name not in self.field_mappings:
                        self.field_mappings[field_name] = field_info
                    else:
                        self.field_mappings[field_name].update(field_info)
            
            # Look for input/select elements with labels
            inputs = root.findall('.//xf:input', namespaces)
            selects = root.findall('.//xf:select1', namespaces) + root.findall('.//xf:select', namespaces)
            
            for element in inputs + selects:
                ref = element.get('ref', '')
                if ref:
                    field_name = ref.split('/')[-1] if '/' in ref else ref
                    field_name = field_name.replace('/data/', '').replace('/', '_')
                    
                    # Find label for this input
                    label_elem = element.find('xf:label', namespaces)
                    if label_elem is not None and label_elem.text:
                        labels[field_name] = label_elem.text.strip()
                    
                    # For select elements, also get choices
                    if element.tag.endswith('select1') or element.tag.endswith('select'):
                        choices = {}
                        items = element.findall('.//xf:item', namespaces)
                        for item in items:
                            value_elem = item.find('xf:value', namespaces)
                            label_elem = item.find('xf:label', namespaces)
                            if value_elem is not None and label_elem is not None:
                                if value_elem.text and label_elem.text:
                                    choices[value_elem.text.strip()] = label_elem.text.strip()
                        
                        if choices:
                            if field_name not in self.field_mappings:
                                self.field_mappings[field_name] = {}
                            self.field_mappings[field_name]['choices'] = choices
            
            # Also look for group labels
            groups = root.findall('.//xf:group', namespaces)
            for group in groups:
                label_elem = group.find('xf:label', namespaces)
                if label_elem is not None and label_elem.text:
                    ref = group.get('ref', '')
                    if ref:
                        group_name = ref.split('/')[-1] if '/' in ref else ref
                        labels[group_name] = f"[Group] {label_elem.text.strip()}"
            
            # Store labels (but don't override XLSForm labels if they exist)
            for field, label in labels.items():
                if field not in self.form_labels:
                    self.form_labels[field] = label
            
            print(f"Extracted {len(labels)} field labels from form XML")
            
        except ImportError:
            print("XML parsing not available - install xml package")
        except Exception as e:
            print(f"Error parsing form XML: {str(e)}")

    def parse_form_fields(self, fields_data):
        """Parse form fields data from the fields endpoint"""
        try:
            if isinstance(fields_data, list):
                for field in fields_data:
                    if isinstance(field, dict):
                        name = field.get('name', '')
                        path = field.get('path', '')
                        field_type = field.get('type', '')
                        
                        if name:
                            # Use path as a more descriptive name if available
                            display_name = path if path else name
                            display_name = display_name.replace('/data/', '').replace('/', '_')
                            
                            if display_name not in self.field_mappings:
                                self.field_mappings[display_name] = {}
                            
                            self.field_mappings[display_name].update({
                                'api_name': name,
                                'path': path,
                                'type': field_type
                            })
                            
                            # If no label exists from XLSForm, create one from the field name
                            if display_name not in self.form_labels:
                                # Create a readable label from field name
                                readable_name = display_name.replace('_', ' ').title()
                                self.form_labels[display_name] = readable_name
            
            print(f"Processed {len(fields_data)} fields from API")
            
        except Exception as e:
            print(f"Error parsing form fields: {str(e)}")

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
        
        if self.dataframe is not None:
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
        
        # Field mappings tab
        if self.field_mappings:
            mappings_frame = ttk.Frame(labels_notebook)
            labels_notebook.add(mappings_frame, text="Field Details")
            
            mappings_text = scrolledtext.ScrolledText(mappings_frame, wrap=tk.WORD)
            mappings_text.pack(fill="both", expand=True, padx=5, pady=5)
            
            mappings_content = "Field Details & Metadata\n" + "="*50 + "\n\n"
            
            for field_name, details in self.field_mappings.items():
                mappings_content += f"Field: {field_name}\n"
                for key, value in details.items():
                    if key == 'choices' and isinstance(value, dict):
                        mappings_content += f"  {key}:\n"
                        for choice_val, choice_label in value.items():
                            mappings_content += f"    {choice_val} → {choice_label}\n"
                    else:
                        mappings_content += f"  {key}: {value}\n"
                mappings_content += "\n"
            
            mappings_text.insert(tk.END, mappings_content)
            mappings_text.config(state=tk.DISABLED)

    def fetch_submissions_with_all_columns(self, auth):
        """Fetch submissions ensuring all form columns are included with memory optimization"""
        base_url = self.url_var.get().rstrip('/')
        project_id = self.project_id_var.get()
        form_id = self.form_id_var.get()
        
        # Memory management settings
        MAX_ROWS_WITHOUT_WARNING = 5000
        MAX_ROWS_ABSOLUTE = 50000
        
        try:
            # Method 1: Try to get CSV export (includes all columns)
            csv_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions.csv"
            csv_response = requests.get(csv_url, auth=auth, timeout=60, stream=True)
            
            if csv_response.status_code == 200:
                # Parse CSV data with memory optimization
                csv_data = StringIO(csv_response.text)
                
                # First, check the size by reading just the first few lines
                sample_lines = []
                line_count = 0
                csv_data.seek(0)
                for line in csv_data:
                    sample_lines.append(line)
                    line_count += 1
                    if line_count > 5:  # Read first 6 lines to estimate size
                        break
                
                # Estimate total rows (minus header)
                csv_data.seek(0)
                total_lines = sum(1 for _ in csv_data) - 1  # Subtract header
                
                csv_data.seek(0)
                
                # Memory warning for large datasets
                if total_lines > MAX_ROWS_WITHOUT_WARNING:
                    if total_lines > MAX_ROWS_ABSOLUTE:
                        # For very large datasets, sample the data
                        df = pd.read_csv(csv_data, nrows=MAX_ROWS_ABSOLUTE)
                        ErrorHandler.show_warning(
                            f"Dataset has {total_lines} rows. Loaded first {MAX_ROWS_ABSOLUTE} rows to prevent memory issues. "
                            f"Consider filtering data on the server or downloading in smaller batches.",
                            "Large Dataset")
                    else:
                        # Load all data but warn user
                        df = pd.read_csv(csv_data)
                        ErrorHandler.show_info(
                            f"Loading {total_lines} rows. This may take a moment.", 
                            "Loading Large Dataset")
                else:
                    # Normal size dataset
                    df = pd.read_csv(csv_data)
                
                # Optimize memory usage
                df = self._optimize_dataframe_memory(df)
                
                return df, f"CSV Export ({len(df)} rows)"
                
        except MemoryError:
            ErrorHandler.handle_error("Insufficient memory to load full dataset", 
                                    "memory management")
            # Try to load a smaller sample
            try:
                csv_data.seek(0)
                df = pd.read_csv(csv_data, nrows=1000)
                df = self._optimize_dataframe_memory(df)
                ErrorHandler.show_warning(
                    "Loaded sample of 1000 rows due to memory constraints", 
                    "Memory Limited")
                return df, "CSV Export (Sample)"
            except Exception:
                return pd.DataFrame(), "Memory Error"
        except Exception as e:
            ErrorHandler.handle_error(e, "CSV export", show_user=False)
        
        try:
            # Method 2: Get individual submissions with full data (memory-conscious)
            submissions_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions"
            response = requests.get(submissions_url, auth=auth, timeout=30)
            response.raise_for_status()
            
            submissions = response.json()
            
            if not submissions:
                return pd.DataFrame(), "Empty Response"
            
            # Limit submissions for memory management
            max_submissions = min(len(submissions), 1000)  # Process max 1000 submissions
            if len(submissions) > max_submissions:
                ErrorHandler.show_info(
                    f"Processing first {max_submissions} of {len(submissions)} submissions", 
                    "Memory Optimization")
            
            # Process each submission to get full data
            full_submissions = []
            
            for submission in submissions[:max_submissions]:
                instance_id = submission.get('instanceId')
                if instance_id:
                    # Get full submission data
                    submission_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions/{instance_id}"
                    sub_response = requests.get(submission_url, auth=auth, timeout=30)
                    
                    if sub_response.status_code == 200:
                        full_data = sub_response.json()
                        full_submissions.append(full_data)
            
            if full_submissions:
                # Normalize the full submission data
                df = pd.json_normalize(full_submissions)
                return df, "Individual Submissions"
                
        except Exception as e:
            print(f"Individual submissions method failed: {str(e)}")
        
        try:
            # Method 3: Standard submissions endpoint with enhanced normalization
            submissions_url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}/submissions"
            response = requests.get(submissions_url, auth=auth, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                return pd.DataFrame(), "No Data"
            
            # Enhanced normalization to capture nested data
            df = pd.json_normalize(data, sep='_')
            
            # If there's XML data, try to parse it
            if 'xml' in df.columns:
                self.parse_xml_submissions(df)
            
            return df, "Standard API"
            
        except Exception as e:
            print(f"Standard API method failed: {str(e)}")
            raise e

    def parse_xml_submissions(self, df):
        """Parse XML submission data to extract form fields"""
        try:
            import xml.etree.ElementTree as ET
            
            xml_data = []
            
            for idx, row in df.iterrows():
                if pd.notna(row.get('xml')):
                    try:
                        root = ET.fromstring(row['xml'])
                        
                        # Extract all elements from XML
                        xml_fields = {}
                        for elem in root.iter():
                            if elem.text and elem.tag:
                                # Clean tag name
                                tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                                xml_fields[f"xml_{tag}"] = elem.text
                        
                        xml_data.append(xml_fields)
                    except Exception as e:
                        print(f"Error parsing XML for row {idx}: {str(e)}")
                        xml_data.append({})
                else:
                    xml_data.append({})
            
            # Convert to DataFrame and merge with original
            if xml_data:
                xml_df = pd.DataFrame(xml_data)
                # Merge with original dataframe
                for col in xml_df.columns:
                    df[col] = xml_df[col]
                    
        except ImportError:
            print("XML parsing not available - install xml package")
        except Exception as e:
            print(f"XML parsing failed: {str(e)}")

    def export_csv(self):
        """Legacy method - redirect to enhanced export"""
        self.export_csv_with_labels()
        
    def generate_visualization(self):
        if self.filtered_df is None or self.filtered_df.empty:
            ErrorHandler.show_warning("No data available for visualization. Please download data first.")
            return
        
        chart_type = self.chart_type_var.get()
        display_column = self.selected_column_var.get()
        
        # Extract actual column name from display text
        column = self.extract_column_name_from_display(display_column) if display_column else ""
        
        if not column and chart_type not in ["Correlation", "Time Series"]:
            ErrorHandler.show_warning("Please select a column to visualize for this chart type.")
            return
        
        if column and column not in self.filtered_df.columns:
            ErrorHandler.show_warning(f"Column '{column}' not found in current data. Please refresh the data.")
            return
        
        # Check data size for performance warning
        data_size = len(self.filtered_df)
        if data_size > 10000:
            result = messagebox.askyesno("Large Dataset", 
                f"You're visualizing {data_size} records. This may take a moment. Continue?")
            if not result:
                return
        
        # Clear existing chart
        for widget in self.chart_frame.winfo_children():
            widget.destroy()
        
        # Show loading indicator
        loading_label = ttk.Label(self.chart_frame, 
                                text="Generating visualization...",
                                font=('Helvetica', 12))
        loading_label.pack(expand=True)
        self.root.update()
        
        try:
            # Create figure and place it in the chart frame
            if chart_type == "Time Series":
                self.create_time_series_plot(self.chart_frame)
            elif chart_type == "Distribution":
                self.create_distribution_plot(self.chart_frame)
            elif chart_type == "Correlation":
                self.create_correlation_plot(self.chart_frame)
            elif chart_type == "Pie Chart":
                self.create_pie_chart(self.chart_frame, column)
            elif chart_type == "Horizontal Bar":
                self.create_horizontal_bar_chart(self.chart_frame, column)
            elif chart_type == "Stacked Bar":
                self.create_stacked_bar_chart(self.chart_frame, column)
                
            # Remove loading indicator
            loading_label.destroy()
            
        except MemoryError as e:
            loading_label.destroy()
            ErrorHandler.handle_error(e, "creating visualization (insufficient memory)")
            self._show_visualization_fallback("Memory error: Dataset too large for visualization")
        except Exception as e:
            loading_label.destroy()
            ErrorHandler.handle_error(e, "creating visualization")
            self._show_visualization_fallback("Visualization failed. Please try a different chart type or smaller dataset.")
    
    def _show_visualization_fallback(self, message):
        """Show fallback message when visualization fails"""
        self.chart_placeholder = ttk.Label(self.chart_frame, 
                                        text=message,
                                        font=('Helvetica', 11),
                                        anchor='center',
                                        foreground='red')
        self.chart_placeholder.pack(expand=True)

    def create_pie_chart(self, parent, column):
        # Create a figure with dynamic size
        fig = plt.Figure(figsize=(10, 7), dpi=100, facecolor='white')
        ax = fig.add_subplot(111)
        ax.set_facecolor('white')
        
        # Get human-readable label for the column
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
        
        value_counts = mapped_counts
        
        if len(value_counts) > 10:
            other_count = value_counts[10:].sum()
            value_counts = value_counts[:10]
            value_counts['Others'] = other_count
        
        colors = plt.cm.Pastel1(np.linspace(0, 1, len(value_counts)))
        patches, texts, autotexts = ax.pie(value_counts, labels=value_counts.index, 
                                        autopct='%1.1f%%', startangle=90,
                                        colors=colors)
        
        plt.setp(autotexts, size=9, weight="bold", color='black')
        plt.setp(texts, size=9, color='black')
        
        ax.set_title(f'Distribution of {column_label}', color='black', pad=20, fontsize=12)
        
        if len(value_counts) > 5:
            legend = ax.legend(patches, value_counts.index, 
                            title="Categories",
                            loc="center left", 
                            bbox_to_anchor=(1.05, 0.5))
            legend.get_title().set_color('black')
            plt.setp(legend.get_texts(), color='black')
        
        fig.tight_layout()
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

    def create_time_series_plot(self, parent):
        fig = plt.Figure(figsize=(12, 6), dpi=100, facecolor='white')
        ax = fig.add_subplot(111)
        ax.set_facecolor('white')
        
        try:
            # Use enhanced date detection
            date_columns = DateUtils.detect_date_columns(self.filtered_df)
            
            if not date_columns:
                raise ValueError("No date columns found for time series analysis")
            
            # Try each date column until we find one that works
            successful_conversion = False
            date_col = None
            converted_dates = None
            
            for col in date_columns:
                success, error_msg, dates = DateUtils.convert_column_to_datetime(
                    self.filtered_df, col)
                
                if success and dates is not None:
                    date_col = col
                    converted_dates = dates
                    successful_conversion = True
                    
                    if error_msg:  # Warning about some failed conversions
                        logging.warning(f"Date conversion warning for {col}: {error_msg}")
                    break
            
            if not successful_conversion:
                raise ValueError("Could not parse dates in any detected date column")
            
            # Filter out invalid dates
            valid_dates = converted_dates.dropna()
            
            if valid_dates.empty:
                raise ValueError("No valid dates found after parsing")
            
            # Check for reasonable date range
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            
            is_valid, range_error = DateUtils.validate_date_range(min_date, max_date)
            if not is_valid:
                ErrorHandler.show_warning(f"Date range issue: {range_error}")
            
            # Create time series aggregation
            # Group by date (day) and count submissions
            date_only = valid_dates.dt.date
            submissions_by_date = date_only.value_counts().sort_index()
            
            # Convert index back to datetime for plotting
            submissions_by_date.index = pd.to_datetime(submissions_by_date.index)
            
            # Plot the time series
            ax.plot(submissions_by_date.index, submissions_by_date.values, 
                   marker='o', linestyle='-', linewidth=2, color='#2196F3',
                   markersize=4, alpha=0.8)
            
            # Add trend line if enough data points and scipy is available
            if len(submissions_by_date) > 5 and HAS_SCIPY:
                try:
                    from scipy import stats
                    x_numeric = np.arange(len(submissions_by_date))
                    slope, intercept, r_value, p_value, std_err = stats.linregress(
                        x_numeric, submissions_by_date.values)
                    
                    trend_line = slope * x_numeric + intercept
                    ax.plot(submissions_by_date.index, trend_line, 
                           '--', color='red', alpha=0.7, linewidth=1,
                           label=f'Trend (R²={r_value**2:.3f})')
                    ax.legend()
                except Exception as e:
                    logging.warning(f"Could not add trend line: {e}")
            
            # Enhanced formatting
            column_label = self.get_column_label(date_col)
            ax.set_title(f'Submissions Over Time\n({column_label})', 
                        color='black', pad=20, fontsize=12, fontweight='bold')
            ax.set_xlabel('Date', color='black', fontsize=10)
            ax.set_ylabel('Number of Submissions', color='black', fontsize=10)
            ax.grid(True, alpha=0.3, linestyle=':')
            
            # Style the axes
            ax.tick_params(colors='black', labelsize=9)
            for spine in ax.spines.values():
                spine.set_color('black')
                spine.set_linewidth(0.8)
            
            # Smart date formatting on x-axis
            if HAS_MATPLOTLIB_DATES:
                try:
                    import matplotlib.dates as mdates
                    date_range = (max_date - min_date).days
                    
                    if date_range <= 7:  # Week or less
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                        ax.xaxis.set_major_locator(mdates.DayLocator())
                    elif date_range <= 90:  # 3 months or less
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                        ax.xaxis.set_major_locator(mdates.WeekdayLocator())
                    elif date_range <= 365:  # Year or less
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
                        ax.xaxis.set_major_locator(mdates.MonthLocator())
                    else:  # More than a year
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
                        ax.xaxis.set_major_locator(mdates.YearLocator())
                except Exception as e:
                    logging.warning(f"Could not apply date formatting: {e}")
                    # Fall back to default formatting
            
            # Rotate x-axis labels for better readability
            plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
            
            # Add statistics text
            stats_text = f'Total: {len(valid_dates)} submissions\n'
            stats_text += f'Period: {DateUtils.format_date_for_display(min_date)} to {DateUtils.format_date_for_display(max_date)}\n'
            stats_text += f'Daily avg: {len(valid_dates)/max(1, date_range):.1f}'
            
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                   verticalalignment='top', bbox=dict(boxstyle='round', 
                   facecolor='wheat', alpha=0.8), fontsize=8)
            
            fig.tight_layout()
            
        except Exception as e:
            ErrorHandler.handle_error(e, "creating time series plot")
            # Create a simple error message plot
            ax.text(0.5, 0.5, f'Time series plot unavailable\n{str(e)[:50]}...', 
                   transform=ax.transAxes, ha='center', va='center',
                   bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.8))
            ax.set_title('Time Series Plot Error', color='red')
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

    def create_distribution_plot(self, parent):
        numeric_cols = self.filtered_df.select_dtypes(include=['number']).columns
        
        if len(numeric_cols) == 0:
            messagebox.showinfo("Info", "No numeric columns available for distribution plot")
            return
        
        fig = plt.Figure(figsize=(12, 8), dpi=100, facecolor='white')
        
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
        
        fig.tight_layout()
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

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
        
        fig.tight_layout()
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

    def create_horizontal_bar_chart(self, parent, column):
        fig = plt.Figure(figsize=(12, 8), dpi=100, facecolor='white')
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
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

    def create_stacked_bar_chart(self, parent, column):
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
            
            fig = plt.Figure(figsize=(12, 8), dpi=100, facecolor='#2b2b2b')
            ax = fig.add_subplot(111)
            ax.set_facecolor('#2b2b2b')
            
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
            
            ax.set_title(f'{column_label} vs {second_column_label}', color='white', pad=20)
            ax.set_xlabel(column_label, color='white')
            ax.set_ylabel('Count', color='white')
            
            # Style the axes
            ax.tick_params(colors='white')
            ax.spines['bottom'].set_color('white')
            ax.spines['top'].set_color('white')
            ax.spines['left'].set_color('white')
            ax.spines['right'].set_color('white')
            
            # Rotate and align x-axis labels
            labels = [label[:20] + '...' if len(label) > 20 else label for label in ct.index]
            ax.set_xticklabels(labels, rotation=45, ha='right', color='white')
            
            # Style the legend
            legend = ax.legend(title=second_column_label, bbox_to_anchor=(1.05, 1))
            legend.get_title().set_color('white')
            plt.setp(legend.get_texts(), color='white')
            
            fig.tight_layout()
            
            canvas = FigureCanvasTkAgg(fig, master=parent)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)
            
            selection_window.destroy()
        
        ttk.Button(selection_window, text="Create Chart", 
                command=create_stacked_chart, 
                style='success.TButton').pack(pady=10)
        
        def create_chart():
            display_second_column = second_column_var.get()
            if not display_second_column:
                messagebox.showwarning("Warning", "Please select a second column")
                return
            
            second_column = column_mapping.get(display_second_column, display_second_column)
            
            # Get labels for both columns
            column_label = self.get_column_label(column)
            second_column_label = self.get_column_label(second_column)
            
            # Create the stacked bar chart
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Create cross-tabulation of the two columns with enhanced choice mapping
            df_for_chart = self.filtered_df.copy()
            
            # Map choice values for both columns
            for col in [column, second_column]:
                df_for_chart[col] = df_for_chart[col].apply(
                    lambda x: self.get_choice_label(col, x).split(" (")[0] 
                    if " (" in self.get_choice_label(col, x) and self.get_choice_label(col, x).endswith(")") 
                    else self.get_choice_label(col, x)
                )
            
            ct = pd.crosstab(df_for_chart[column], df_for_chart[second_column])
            
            # Create stacked bar chart
            ct.plot(kind='bar', stacked=True, ax=ax, colormap='Set3')
            
            ax.set_title(f'{column_label} vs {second_column_label}')
            ax.set_xlabel(column_label)
            ax.set_ylabel('Count')
            
            # Wrap long x-axis labels
            labels = [label[:20] + '...' if len(label) > 20 else label for label in ct.index]
            ax.set_xticklabels(labels, rotation=45, ha='right')
            
            plt.legend(title=second_column_label, bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            
            canvas = FigureCanvasTkAgg(fig, master=parent)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)
            
            selection_window.destroy()
        
        ttk.Button(selection_window, text="Create Chart", 
                  command=create_chart, style='success.TButton').pack(pady=10)

    def export_csv_with_labels(self):
        """Export data with both original columns and label headers"""
        if self.filtered_df is None or self.filtered_df.empty:
            messagebox.showwarning("Warning", "No data to export")
            return
        
        try:
            from tkinter import filedialog
            
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
            messagebox.showerror("Error", f"Export failed: {str(e)}")

    def create_data_dictionary(self):
        """Create a data dictionary with field descriptions"""
        if not self.form_labels and not self.field_mappings:
            messagebox.showinfo("Info", "No form metadata available for data dictionary")
            return
        
        try:
            from tkinter import filedialog
            
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
            messagebox.showerror("Error", f"Data dictionary export failed: {str(e)}")

    def create_time_series_plot(self, parent):
        fig, ax = plt.subplots(figsize=(10, 6))
        
        try:
            # Check if submissionDate column exists
            date_columns = [col for col in self.filtered_df.columns 
                          if any(date_term in col.lower() for date_term in 
                                ['date', 'time', 'created', 'updated', 'submission'])]
            
            if not date_columns:
                messagebox.showwarning("Warning", "No date columns found for time series")
                return
            
            # Use the first available date column
            date_col = date_columns[0]
            
            # Convert to datetime and handle different formats
            date_series = pd.to_datetime(self.filtered_df[date_col], errors='coerce')
            
            # Remove any invalid dates
            valid_dates = date_series.dropna()
            
            if valid_dates.empty:
                messagebox.showwarning("Warning", "No valid dates found in the selected column")
                return
            
            submissions_by_date = valid_dates.value_counts().sort_index()
            submissions_by_date.plot(kind='line', ax=ax, marker='o')
            
            ax.set_title(f'Submissions Over Time (using {date_col})')
            ax.set_xlabel('Date')
            ax.set_ylabel('Number of Submissions')
            ax.grid(True, alpha=0.3)
            
            # Format x-axis dates
            plt.xticks(rotation=45)
            plt.tight_layout()
            
        except Exception as e:
            messagebox.showerror("Error", f"Could not create time series plot: {str(e)}")
            return
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    def create_distribution_plot(self, parent):
        numeric_cols = self.filtered_df.select_dtypes(include=['number']).columns
        
        if len(numeric_cols) == 0:
            messagebox.showinfo("Info", "No numeric columns available for distribution plot")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 8))
        axes = axes.ravel()
        
        for idx, col in enumerate(numeric_cols[:4]):
            column_label = self.get_column_label(col)
            sns.histplot(data=self.filtered_df, x=col, ax=axes[idx])
            axes[idx].set_title(f'Distribution of {column_label}')
            axes[idx].set_xlabel(column_label)
        
        # Hide unused subplots
        for idx in range(len(numeric_cols[:4]), 4):
            axes[idx].set_visible(False)
        
        plt.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    def create_correlation_plot(self, parent):
        numeric_df = self.filtered_df.select_dtypes(include=['number'])
        
        if len(numeric_df.columns) < 2:
            messagebox.showinfo("Info", "Not enough numeric columns for correlation analysis")
            return
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Create column label mapping for correlation matrix
        label_mapping = {}
        for col in numeric_df.columns:
            label_mapping[col] = self.get_column_label(col)
        
        # Rename columns for display
        display_df = numeric_df.rename(columns=label_mapping)
        
        sns.heatmap(display_df.corr(), annot=True, cmap='coolwarm', ax=ax)
        ax.set_title('Correlation Matrix')
        
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

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
            
            # Validate authentication before proceeding
            if not auth[0] or not auth[1]:
                self.root.after(0, lambda: ErrorHandler.show_warning(
                    "Please enter both username and password", "Authentication Required"))
                return
            
            # First, try to get form schema
            self.get_form_schema(auth)
            
            # Fetch submissions with all columns
            self.dataframe, method_used = self.fetch_submissions_with_all_columns(auth)
            
            if self.dataframe is None or self.dataframe.empty:
                self.root.after(0, lambda: ErrorHandler.show_warning(
                    "No data found. Please check your form ID and permissions.", 
                    "No Data Available"))
                self.root.after(0, lambda: self.status_var.set("No data available"))
                return
            
            # Check data quality
            if len(self.dataframe.columns) == 0:
                self.root.after(0, lambda: ErrorHandler.show_warning(
                    "Data has no columns. The form may be empty or inaccessible.", 
                    "Data Quality Issue"))
                return
                
            self.filtered_df = self.dataframe.copy()
            
            # Update last update time
            self.last_update_time = datetime.now(timezone.utc)
            
            # Update UI
            self.root.after(0, lambda: self.update_ui_after_download(method_used))
            # Update summary stats
            self.root.after(0, self.update_summary_stats)
            
            # Show success message
            row_count = len(self.dataframe)
            col_count = len(self.dataframe.columns)
            self.root.after(0, lambda: ErrorHandler.show_success(
                f"Successfully downloaded {row_count} records with {col_count} columns"))
            
        except requests.exceptions.ConnectionError as e:
            self.root.after(0, lambda: ErrorHandler.handle_error(
                e, "connecting to ODK Central server"))
            self.root.after(0, lambda: self.status_var.set("Connection failed"))
        except requests.exceptions.Timeout as e:
            self.root.after(0, lambda: ErrorHandler.handle_error(
                e, "downloading data (server timeout)"))
            self.root.after(0, lambda: self.status_var.set("Download timeout"))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                self.root.after(0, lambda: ErrorHandler.handle_error(
                    "Invalid credentials", "authentication"))
            elif e.response.status_code == 403:
                self.root.after(0, lambda: ErrorHandler.handle_error(
                    "Access denied. Check your permissions", "authorization"))
            elif e.response.status_code == 404:
                self.root.after(0, lambda: ErrorHandler.handle_error(
                    "Form not found. Check your project and form IDs", "data access"))
            else:
                self.root.after(0, lambda: ErrorHandler.handle_error(
                    e, f"HTTP error ({e.response.status_code})"))
            self.root.after(0, lambda: self.status_var.set("Download failed"))
        except Exception as e:
            self.root.after(0, lambda: ErrorHandler.handle_error(e, "downloading data"))
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
        status_msg = f"Downloaded {row_count} records with {column_count} columns using {method_used} at {self.last_update_time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        self.status_var.set(status_msg)
        
        # Show column information in a popup for first download
        if hasattr(self, '_first_download') and not self._first_download:
            self.show_column_info()
            self._first_download = True

    def show_column_info(self):
        """Show information about available columns"""
        if self.dataframe is None:
            return
            
        info_window = tb.Toplevel(self.root)
        info_window.title("Available Columns")
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
                self.update_summary_stats()  # Add this line
                
                # Update status
                filter_count = len(self.filtered_df)
                total_count = len(self.dataframe)
                self.status_var.set(f"Applied filters: showing {filter_count} of {total_count} records")
            
        except Exception as e:
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
            self.update_summary_stats()  # Add this line
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
        display_limit = 1000
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
            messagebox.showerror("Error", f"Could not sort by column {col}: {str(e)}")


def main():
    try:
        root = tb.Window(
            title="ODK Central Dashboard - Enhanced",
            themename="darkly",
            size=(1200, 800)
        )
        
        # Configure styles
        style = ttk.Style()
        style.configure('primary.TFrame', background='#007bff')
        style.configure('info.TFrame', background='#17a2b8')
        style.configure('primary.TLabel', background='#007bff', foreground='white')
        style.configure('info.TLabel', background='#17a2b8', foreground='white')
        style.configure('primary.Inverse.TLabel', 
                       background='#007bff', foreground='white', 
                       font=('Helvetica', 10))
        style.configure('info.Inverse.TLabel', 
                       background="#0e4d57", foreground='white', 
                       font=('Helvetica', 10))
        
        app = Dashboard(root)
        app._first_download = False  # Track first download
        
        # Center window
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        x = (screen_width - 1200) // 2
        y = (screen_height - 800) // 2
        root.geometry(f"1200x800+{x}+{y}")
        
        root.configure(bg="#450505")
        root.mainloop()
        
    except Exception as e:
        tb.dialogs.Messagebox.show_error(
            "Application Error",
            f"An error occurred while starting the application:\n{str(e)}"
        )
        raise


if __name__ == "__main__":
    main()