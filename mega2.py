import pandas as pd
import requests
import threading
import webbrowser
import logging
from shiny import debounce
import asyncio
import seaborn as sns
from palmerpenguins import load_penguins
import time
from datetime import datetime
from shiny import App, ui, render, reactive, Session
from shinywidgets import output_widget, render_widget
from ipyleaflet import Map, Marker, MarkerCluster, Popup, basemaps, CircleMarker, Icon, AwesomeIcon, TileLayer

# ===== Configure Logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log_audit_event(action, user, details=None):
    logging.info(f"[AUDIT] {action} by {user} at {datetime.utcnow().isoformat()} | Details: {details}")
    # Add timestamp to log
    with open("audit_log.txt", "a") as f:
        f.write(f"{datetime.utcnow().isoformat()} - {action} by {user} | {details}\n")

# ===== ODK Central API Integration =====
class ODKCentralAPI:
    def __init__(self, base_url, project_id=None, form_id=None):
        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self.form_id = form_id
        self.token = None
        self.email = None
        self.password = None
        # Add caching to improve performance
        self._projects_cache = {}
        self._forms_cache = {}
        self._submissions_cache = {}
        self._cache_expiry = {}
        self._cache_lifetime = 300  # Cache lifetime in seconds
        
    def set_credentials(self, email, password):
        self.email = email
        self.password = password

    def set_token(self, token):
        self.token = token

    def clear_credentials(self):
        self.email = None
        self.password = None
        self.token = None
        
        # Clear caches when logging out
        self._projects_cache = {}
        self._forms_cache = {}
        self._submissions_cache = {}
        self._cache_expiry = {}

    def authenticate(self):
        if not self.email or not self.password:
            return False
        try:
            auth_url = f"{self.base_url}/v1/sessions"
            response = requests.post(auth_url, json={"email": self.email, "password": self.password}, timeout=10)
            response.raise_for_status()
            self.token = response.json().get("token")
            logging.info("Authentication successful for user: %s", self.email)
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"ODK Authentication failed: {e}")
            self.token = None
            return False

    def fetch_projects(self):
        # Use cached projects if available and not expired
        cache_key = 'projects'
        if cache_key in self._projects_cache and time.time() < self._cache_expiry.get(cache_key, 0):
            logging.info("Using cached projects data")
            return self._projects_cache[cache_key]
      
        if not self.token and not self.authenticate():
            logging.warning("No token available, cannot fetch projects.")
            return []
        try:
            headers = {"Authorization": f"Bearer {self.token}"}
            url = f"{self.base_url}/v1/projects"
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            projects = response.json()
            # Cache the projects data
            self._projects_cache[cache_key] = projects
            self._cache_expiry[cache_key] = time.time() + self._cache_lifetime
            return response.json()
        except Exception as e:
            logging.error(f"Failed to fetch projects: {e}")
            return []

    def fetch_forms(self, project_id):
        # Use cached forms if available and not expired
        cache_key = f'forms_{project_id}'
        if cache_key in self._forms_cache and time.time() < self._cache_expiry.get(cache_key, 0):
            logging.info(f"Using cached forms data for project {project_id}")
            return self._forms_cache[cache_key]
        
        if not self.token and not self.authenticate():
            logging.warning("No token available, cannot fetch forms.")
            return []
        try:
            headers = {"Authorization": f"Bearer {self.token}"}
            url = f"{self.base_url}/v1/projects/{project_id}/forms"
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            forms = response.json()
            # Cache the forms data
            self._forms_cache[cache_key] = forms
            self._cache_expiry[cache_key] = time.time() + self._cache_lifetime
            
            return response.json()
        except Exception as e:
            logging.error(f"Failed to fetch forms: {e}")
            return []

    def fetch_submissions(self, project_id=None, form_id=None, force_refresh=True,):
        # Use cached submissions if available, not expired, and not forced to refresh
        cache_key = f'submissions_{project_id}_{form_id}'
        if not force_refresh and cache_key in self._submissions_cache and time.time() < self._cache_expiry.get(cache_key, 0):
            logging.info(f"Using cached submissions data for project {project_id}, form {form_id}")
            return self._submissions_cache[cache_key]
        if not self.token and not self.authenticate():
            logging.warning("No token available, cannot fetch submissions.")
            return pd.DataFrame()
        project_id = project_id or self.project_id
        form_id = form_id or self.form_id
        if not project_id or not form_id:
            logging.warning("Missing project or form ID for submissions fetch.")
            return pd.DataFrame()
        try:
            headers = {"Authorization": f"Bearer {self.token}"}
            url = f"{self.base_url}/v1/projects/{project_id}/forms/{form_id}/submissions.csv"
            # Use streaming for better performance with large datasets
            with requests.get(url, headers=headers, timeout=60, stream=True) as response:
                response.raise_for_status()
                from io import StringIO
            
               # Read in chunks to avoid memory issues
                csv_data = StringIO()
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                    if chunk:
                        csv_data.write(chunk)
                
                csv_data.seek(0)
                df = pd.read_csv(csv_data)
                
                # Cache the results
                self._submissions_cache[cache_key] = df
                self._cache_expiry[cache_key] = time.time() + self._cache_lifetime
                
            return df
        except requests.exceptions.Timeout:
            logging.error(f"Request timed out when fetching submissions for project {project_id}, form {form_id}")
            return pd.DataFrame({"Error": ["Request timed out. The server took too long to respond."]})
        except requests.exceptions.ConnectionError:
            logging.error(f"Connection error when fetching submissions for project {project_id}, form {form_id}")
            return pd.DataFrame({"Error": ["Connection error. Could not connect to the server."]})
        except Exception as e:
            logging.error(f"Failed to fetch submissions: {e}")
            return pd.DataFrame({"Error": [f"Failed to fetch submissions: {str(e)}"]})

# ===== Enhanced UI Definition with Separate Donut Chart Cards =====
app_ui = ui.page_bootstrap(
    #title="MEGA 2.0 Dashboard",
    theme="flatly",
    ui.tags.head(
       # IMPORTANT: Added jQuery and Bootstrap libraries explicitly
        ui.tags.script(src="https://code.jquery.com/jquery-3.6.0.min.js"),
        ui.tags.link(rel="stylesheet", href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"),
        ui.tags.script(src="https://cdn.jsdelivr.net/npm/@popperjs/core@2.11.6/dist/umd/popper.min.js"),
        ui.tags.script(src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"),
        ui.tags.link(rel="stylesheet", href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"),
        ui.tags.link(rel="stylesheet", href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css"),
        ui.tags.script("""
        function saveToken(token) {
            sessionStorage.setItem('odk_token', token);
        }
        function clearToken() {
            sessionStorage.removeItem('odk_token');
        }
        document.addEventListener('DOMContentLoaded', function() {
            // FIXED: Clear any existing token to force login screen
            clearToken();
            
            setTimeout(function() {
              // FIXED: Don't auto-restore token on initial page load
              /* 
              var token = sessionStorage.getItem('odk_token');
              if (token) {
                  Shiny.setInputValue('restoreTokenFromJS', {token: token}, {priority: 'event'});
              }
              */
              
              // Initialize Bootstrap dropdowns
              var dropdownElementList = [].slice.call(document.querySelectorAll('.dropdown-toggle'))
              dropdownElementList.map(function (dropdownToggleEl) {
                return new bootstrap.Dropdown(dropdownToggleEl)
              });
            }, 500);
        });
        Shiny.addCustomMessageHandler('saveToken', function(msg) {
            saveToken(msg.token);
        });
        Shiny.addCustomMessageHandler('clearToken', function(msg) {
            clearToken();
        });
        
        // Handle search input
        $(document).on('input', '#column-search-input', function() {
            var searchText = $(this).val().toLowerCase();
            $('.column-option').each(function() {
                var columnText = $(this).text().toLowerCase();
                if (columnText.includes(searchText)) {
                    $(this).show();
                } else {
                    $(this).hide();
                }
            });
        });
        
        // Add this to your JavaScript section to ensure proper loading indicator behavior
        document.addEventListener('DOMContentLoaded', function() {
            // Hide loading indicator on initial load
            const loadingIndicator = document.getElementById('loading-indicator');
            const loadingText = document.getElementById('loading-text');
            if (loadingIndicator) loadingIndicator.style.display = 'none';
            if (loadingText) loadingText.style.display = 'none';
            
            // Define custom message handlers
            Shiny.addCustomMessageHandler('showLoading', function(message) {
                console.log('Show loading:', message);
                const loadingIndicator = document.getElementById('loading-indicator');
                const loadingText = document.getElementById('loading-text');
                if (loadingIndicator) loadingIndicator.style.display = 'block';
                if (loadingText) {
                    loadingText.style.display = 'block';
                    loadingText.textContent = message || 'Loading data, please wait...';
                }
            });
            
            Shiny.addCustomMessageHandler('hideLoading', function(message) {
                console.log('Hide loading');
                const loadingIndicator = document.getElementById('loading-indicator');
                const loadingText = document.getElementById('loading-text');
                if (loadingIndicator) loadingIndicator.style.display = 'none';
                if (loadingText) loadingText.style.display = 'none';
            });
        });
        
        // Handle Select All link
        $(document).on('click', '.select-all-link', function(e) {
            e.preventDefault();
            e.stopPropagation();
            $('.column-option:visible .column-checkbox').prop('checked', true).trigger('change');
        });
        
        // Handle None link
        $(document).on('click', '.select-none-link', function(e) {
            e.preventDefault();
            e.stopPropagation();
            $('.column-option:visible .column-checkbox').prop('checked', false).trigger('change');
        });
        
        // Update button text with selected count
        function updateSelectedCount() {
            var totalCount = $('.column-checkbox').length;
            var selectedCount = $('.column-checkbox:checked').length;
            $('#dropdown-counter').text(selectedCount + ' of ' + totalCount);
            Shiny.setInputValue('selected_column_count', selectedCount);
        }
        
        // Trigger count update when any checkbox changes
        $(document).on('change', '.column-checkbox', function() {
            updateSelectedCount();
        });
        
        // Update count on initial load
        $(document).ready(function() {
            setTimeout(updateSelectedCount, 500);
        });
        """),
        ui.tags.style("""
            /* Enhanced Bootstrap Integration with Separate Donut Chart Cards */
            :root {
                --bs-primary: #058aff;
                --bs-primary-rgb: 5, 138, 255;
                --bs-secondary: #6c757d;
                --bs-success: #198754;
                --bs-danger: #dc3545;
                --bs-warning: #ffc107;
                --bs-info: #0dcaf0;
                --bs-light: #f8f9fa;
                --bs-dark: #212529;
                --teal-primary: #4ecdc4;
                --teal-secondary: #44b9b1;
                --teal-tertiary: #3ba6a0;
                --orange-primary: #ff9800;
                --orange-secondary: #ff6f00;
                --purple-primary: #9c27b0;
                --purple-secondary: #7b1fa2;
            }
            
            body {
                background: linear-gradient(135deg, #ECF0F5 0%, #e8f2ff 100%) !important;
                min-height: 100vh;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }
            
            /* Enhanced Login Container */
            .login-container-custom {
                max-width: 450px;
                margin: 60px auto;
                background: white;
                border-radius: 20px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.1), 0 15px 12px rgba(0,0,0,0.05);
                overflow: hidden;
                border: 1px solid rgba(255,255,255,0.2);
            }
            
            .login-header {
                background: linear-gradient(135deg, var(--bs-primary) 0%, #006fd6 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }
            
            .login-body {
                padding: 40px 30px 30px;
            }
            
            /* Enhanced Login Button with Animation */
            #login {
                background: linear-gradient(135deg, var(--bs-primary) 0%, #006fd6 100%) !important;
                color: white !important;
                font-size: 18px !important;
                font-weight: 600 !important;
                border-radius: 12px !important;
                width: 100% !important;
                padding: 14px 20px !important;
                border: none !important;
                margin-top: 20px;
                box-shadow: 0 8px 20px rgba(5, 138, 255, 0.3);
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                position: relative;
                overflow: hidden;
            }
            
            #login:hover {
                transform: translateY(-2px);
                box-shadow: 0 12px 30px rgba(5, 138, 255, 0.4);
                background: linear-gradient(135deg, #006fd6 0%, #0056b3 100%) !important;
            }
            
            #login:active {
                transform: translateY(0);
                box-shadow: 0 4px 15px rgba(5, 138, 255, 0.3);
            }
            
            /* Ripple effect for login button */
            #login::before {
                content: '';
                position: absolute;
                top: 50%;
                left: 50%;
                width: 0;
                height: 0;
                border-radius: 50%;
                background: rgba(255, 255, 255, 0.3);
                transition: width 0.6s, height 0.6s, top 0.6s, left 0.6s;
                transform: translate(-50%, -50%);
                z-index: 0;
            }
            
            #login:active::before {
                width: 300px;
                height: 300px;
            }
            
            #login span {
                position: relative;
                z-index: 1;
            }
            
            /* Enhanced Form Controls */
            input[type="text"], input[type="password"], select, .selectize-input {
                border: 2px solid #e3f2fd !important;
                border-radius: 10px !important;
                padding: 12px 16px !important;
                font-size: 16px !important;
                background: #fafbfc !important;
                transition: all 0.3s ease;
                width: 100%;
            }
            
            input[type="text"]:focus, input[type="password"]:focus, select:focus, .selectize-input.focus {
                border-color: var(--bs-primary) !important;
                box-shadow: 0 0 0 0.2rem rgba(5, 138, 255, 0.15) !important;
                background: white !important;
                outline: none !important;
            }
            
            /* Enhanced Labels */
            label {
                font-weight: 600;
                color: #495057;
                margin-bottom: 8px;
                display: block;
            }
            
            /* Enhanced Action Buttons */
            .btn, button {
                border-radius: 10px !important;
                font-weight: 500;
                padding: 10px 20px;
                transition: all 0.3s ease;
                border: none;
            }
            
            /* Primary button styling */
            load_data, logout {
                background: linear-gradient(135deg, var(--bs-primary) 0%, #006fd6 100%);
                color: white;
                box-shadow: 0 4px 15px rgba(5, 138, 255, 0.2);
            }
            
            load_data:hover, logout:hover {
                transform: translateY(-1px);
                box-shadow: 0 6px 20px rgba(5, 138, 255, 0.3);
                background: linear-gradient(135deg, #006fd6 0%, #0056b3 100%);
            }
            
            /* Download button styling */
            #download_data {
                background: linear-gradient(135deg, var(--bs-success) 0%, #146c43 100%);
                color: white;
                box-shadow: 0 4px 15px rgba(25, 135, 84, 0.2);
            }
            
            #download_data:hover {
                transform: translateY(-1px);
                box-shadow: 0 6px 20px rgba(25, 135, 84, 0.3);
                background: linear-gradient(135deg, #146c43 0%, #0f5132 100%);
            }
            
            /* Enhanced Cards */
            .card {
                border: none !important;
                border-radius: 16px !important;
                box-shadow: 0 8px 25px rgba(0,0,0,0.08) !important;
                margin-bottom: 24px;
                overflow: hidden;
                background: white;
                transition: transform 0.3s ease, box-shadow 0.3s ease;
            }
            
            .card:hover {
                transform: translateY(-5px);
                box-shadow: 0 12px 35px rgba(0,0,0,0.12) !important;
            }
            
            .card-header {
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%) !important;
                border-bottom: 2px solid #dee2e6 !important;
                font-weight: 600 !important;
                color: #495057 !important;
                padding: 20px !important;
            }
            
            .card-body {
                padding: 24px !important;
                height: auto !important; /* Changed from fixed height */
                min-height: 350px !important; /* Minimum height */
                overflow: auto !important; /* Add scrollbar if content exceeds height */
            }
            
            /* Data table card body can be taller */
            .data-table-card .card-body {
                height: auto !important;
                max-height: 600px !important;
            }
            
            /* Map card body needs more height */
            .map-card .card-body {
                min-height: 500px !important;
                padding: 0 !important; /* Remove padding for map to fill space */
            }
            
            /* Specialized Donut Chart Card Headers */
            .card-header-sample {
                background: linear-gradient(135deg, var(--teal-primary) 0%, var(--teal-secondary) 100%) !important;
                color: white !important;
                border-bottom: 2px solid var(--teal-tertiary) !important;
            }
            
            .card-header-sex {
                background: linear-gradient(135deg, var(--orange-primary) 0%, var(--orange-secondary) 100%) !important;
                color: white !important;
                border-bottom: 2px solid #e65100 !important;
            }
            
            .card-header-age {
                background: linear-gradient(135deg, var(--purple-primary) 0%, var(--purple-secondary) 100%) !important;
                color: white !important;
                border-bottom: 2px solid #6a1b9a !important;
            }
            
            .card-header-map {
                background: linear-gradient(135deg, #03A9F4 0%, #0288D1 100%) !important;
                color: white !important;
                border-bottom: 2px solid #0277BD !important;
            }
            
            /* Logo container with text overlay */
            .logo-container {
                position: relative;
                display: inline-block;
                margin: 0 auto;
            }
            
            .logo-image {
                height: 100px;
                max-width: 100%;
            }
            
            .logo-text {
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                color: #fff;
                font-weight: 700;
                font-size: 24px;
                text-align: center;
                text-shadow: 1px 1px 3px rgba(0,0,0,0.7);
                background: rgba(5, 138, 255, 0.7);
                padding: 8px 16px;
                border-radius: 10px;
                width: 80%;
            }
            
            /* Enhanced Title Banner */
            .title-banner {
                display: flex;
                justify-content: center;
                align-items: center;
                background: white;
                padding: 24px;
                margin: 24px auto;
                max-width: 900px;
                border-radius: 20px;
                box-shadow: 0 8px 30px rgba(0,0,0,0.08);
                border: 1px solid rgba(255,255,255,0.2);
            }
            
            .logout-container {
                position: absolute;
                top: 20px;
                right: 30px;
                z-index: 100;
            }
            
            /* Enhanced Tables */
            table.dataTable {
                border-collapse: separate !important;
                border-spacing: 0;
                border: 2px solid var(--primary-color) !important;
                border-radius: 12px !important;
                overflow: hidden;
                font-size: 0.9rem;
                box-shadow: 0 4px 15px rgba(0,0,0,0.05);
                font-size: 14px;
                width: 100% !important;
            }
            
            table.dataTable thead th {
                background: linear-gradient(135deg, var(--primary-color) 0%, #0056b3 100%) !important;
                color: white;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                border: none;
                padding: 16px;
            }
            
            table.dataTable tbody tr:hover {
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%) !important;
                transition: all 0.2s ease;
            }
            
            table.dataTable thead th {
              padding: 12px !important;
              color: white !important;
              border-bottom: none !important;
            }
            
            table.dataTable tbody td {
              padding: 10px !important;
              border-top: 1px solid #e9ecef !important;
            }
            
            table.dataTable tbody tr:hover {
              background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%) !important;
            }  
            
            /* Enhanced Controls Layout */
            .controls-container {
                background: white;
                padding: 24px;
                border-radius: 16px;
                margin-bottom: 24px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.05);
            }
            
            .horizontal-controls {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 24px;
            }
            
            /* Alert Styles */
            .alert {
                border-radius: 12px;
                border: none;
                font-weight: 500;
                display: flex;
                align-items: center;
            }
            
            .alert-info {
                background: rgba(13, 202, 240, 0.1);
                color: #0c63e4;
                border-left: 4px solid #0dcaf0;
            }
            
            .alert-warning {
                background: rgba(255, 193, 7, 0.1);
                color: #997404;
                border-left: 4px solid #ffc107;
            }
            
            .alert-success {
                background: rgba(25, 135, 84, 0.1);
                color: #146c43;
                border-left: 4px solid var(--bs-success);
            }
            
            .error-message {
                color: var(--bs-danger);
                background: rgba(220, 53, 69, 0.1);
                padding: 12px;
                border-radius: 8px;
                margin-top: 15px;
                text-align: center;
                font-weight: 500;
                border-left: 4px solid var(--bs-danger);
            }
            
            /* Download Section Enhancement */
            .download-section {
                background: white;
                padding: 24px;
                border-radius: 16px;
                margin-bottom: 24px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.05);
                text-align: center;
            }
            
            .download-info {
                color: #6c757d;
                font-size: 14px;
                margin-top: 10px;
            }
            
            /* Donut Chart Specific Styling */
            .donut-chart-container {
                text-align: center;
                padding: 20px;
                height: 350px !important; /* Fixed height for donut charts */
                overflow: visible !important; /* Don't clip overflow */
            }
            
            .chart-stats {
                display: flex;
                justify-content: space-around;
                margin-top: 15px;
                padding-top: 15px;
                border-top: 1px solid #e9ecef;
            }
            
            .stat-item {
                text-align: center;
            }
            
            .stat-value {
                font-size: 1.5rem;
                font-weight: bold;
                color: #495057;
            }
            
            .stat-label {
                font-size: 0.875rem;
                color: #6c757d;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            
            /* Loading States */
            .loading-spinner {
                display: inline-block;
                width: 20px;
                height: 20px;
                border: 3px solid rgba(255,255,255,.3);
                border-radius: 50%;
                border-top-color: #fff;
                animation: spin 1s ease-in-out infinite;
            }
            
            @keyframes spin {
                to { transform: rotate(360deg); }
            }
            
            /* Responsive Design */
            @media (max-width: 768px) {
                .title-banner {
                    flex-direction: column;
                    text-align: center;
                }
                
                .logo-container {
                    margin-right: 0;
                    margin-bottom: 16px;
                }
                
                .horizontal-controls {
                    grid-template-columns: 1fr;
                }
                
                .logout-container {
                    position: relative;
                    top: auto;
                    right: auto;
                    margin-bottom: 20px;
                    text-align: center;
                }
                
                .logo-text {
                    font-size: 18px;
                }
                
                .chart-stats {
                    flex-direction: column;
                    gap: 10px;
                }
            }
            
            @keyframes fadeInUp {
              from {
                opacity: 0;
                transform: translateY(30px);
              }
              to {
                opacity: 1;
                transform: translateY(0);
              }
            }
            
            .fade-in {
              animation: fadeInUp 0.6s ease-out;
            }  
            /* Smooth Transitions */
            * {
                transition: all 0.2s ease;
            }

            /* Version info display */
            .version-info {
                position: fixed;
                bottom: 10px;
                left: 10px;
                font-size: 12px;
                color: #6c757d;
                background: rgba(255,255,255,0.7);
                padding: 5px 10px;
                border-radius: 10px;
                z-index: 1000;
            }
            
            /* FIXED DROPDOWN STYLES */
            #submission-field-dropdown-toggle {
                background-color: #666;
                color: white;
                border: 1px solid #555;
                border-radius: 0.25rem;
            }
            
            #submission-field-dropdown-toggle:hover, 
            #submission-field-dropdown-toggle:focus {
                background-color: #555;
                color: white;
            }
            
            .column-checkbox-list {
                max-height: 300px;
                overflow-y: auto;
            }
            
            .column-option {
                margin-bottom: 0.25rem;
            }
            
            .dropdown-search, .select-options {
                background-color: #f8f9fa;
            }
            
            .dropdown-menu {
                max-width: 100%;
                box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            }
            
            /* ipyleaflet map styling */
            .leaflet-container {
                height: 600px !important;
                width: 100% !important;
                border-radius: 0 0 16px 16px !important;
            }
            
            .leaflet-popup-content {
                font-size: 14px;
                line-height: 1.6;
                padding: 5px;
            }
            
            .leaflet-popup-content h4 {
                margin-top: 0;
                margin-bottom: 8px;
                font-size: 16px;
                font-weight: bold;
                color: #0288D1;
            }
            
            .marker-popup-content {
                min-width: 150px;
            }
            
            .marker-popup-content div {
                margin-bottom: 5px;
            }
            
            .marker-popup-content strong {
                color: #333;
            }
            
            /* GPS Info Box */
            .gps-info-box {
                position: absolute;
                bottom: 15px;
                right: 15px;
                background: rgba(255,255,255,0.9);
                border-radius: 8px;
                padding: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                z-index: 1000;
                max-width: 250px;
                font-size: 12px;
            }
            
            .gps-info-title {
                font-weight: bold;
                margin-bottom: 5px;
                color: #0288D1;
                border-bottom: 1px solid #e9ecef;
                padding-bottom: 3px;
            }
        """)
    ),
    
    # Enhanced Header with Dashboard text inside AAPH Logo
    ui.div(
        {"class": "title-banner animate__animated animate__fadeInDown"},
        ui.div(
            {"class": "logo-container"},
            ui.img(
                src="https://aaph.or.tz/sites/default/files/AAPHlogo.png",
                alt="AAPH Logo",
                class_="logo-image"
            ),
            ui.div(
                {"class": "logo-text"},
                "MEGA 2.0 Dashboard"
            )
        )
    ),
    
    ui.output_ui("main_ui"),
    
    # Version info with updated timestamp and username
    ui.div(
        {"class": "version-info"},
        f"Version 2.1.0 | Last update: 2025-06-23 11:40:14 | User: frnkmapendo"
    )
)

def server(input, output, session: Session):
    # Initialize reactive values
    logged_in_value = reactive.Value(False)
    odk_email_value = reactive.Value("")
    odk_data_value = reactive.Value(pd.DataFrame())
    login_message_value = reactive.Value("")
    data_message_value = reactive.Value("")
    project_choices_value = reactive.Value({})
    form_choices_value = reactive.Value({})
    selected_project_id_value = reactive.Value(None)
    selected_form_id_value = reactive.Value(None)
    odk_token_value = reactive.Value(None)
    is_loading_data = reactive.Value(False)
    gps_columns_value = reactive.Value([])  # Track GPS column names for the current form
    paired_coordinates_value = reactive.Value({})  # Track paired lat/lon columns

    odk_api = ODKCentralAPI(
        base_url="https://central.aaph.or.tz"
    )
    
    # Show/hide loading indicator
    def show_loading(message="Loading data, please wait..."):
        session.send_custom_message("showLoading", message)
        
    def hide_loading():
        session.send_custom_message("hideLoading", {})
    
    # NEW: Force clear session on app start
    @session.on_connect
    def _():
        # Clear any stored token to ensure login screen shows
        logged_in_value.set(False)
        session.send_custom_message("clearToken", {})
        odk_api.clear_credentials()
        logging.info("Cleared session token on app start")
        hide_loading()

    def map_sample_labels(df):
        if "sample" in df.columns:
            mapping = {
                1: "Public school",
                2: "Out of school",
                "1": "Public school",
                "2": "Out of school"
            }
            df["sample"] = df["sample"].map(mapping).fillna(df["sample"])
        return df

    def map_a04_labels(df):
        if "A04" in df.columns:
            mapping = {
                1: "Male",
                2: "Female",
                "1": "Male",
                "2": "Female"
            }
            df["A04"] = df["A04"].map(mapping).fillna(df["A04"])
        return df

    def categorize_age(df):
        if "A03" in df.columns:
            # Convert to numeric, coerce errors to NaN
            df["A03"] = pd.to_numeric(df["A03"], errors='coerce')
            
            # Create age group categories
            conditions = [
                (df["A03"] >= 10) & (df["A03"] <= 14),
                (df["A03"] >= 15) & (df["A03"] <= 19),
                (df["A03"] >= 20) & (df["A03"] <= 24)
            ]
            choices = ["10-14", "15-19", "20-24"]
            
            # Create a new column with the age groups
            df["age_group"] = pd.NA
            df.loc[conditions[0], "age_group"] = choices[0]
            df.loc[conditions[1], "age_group"] = choices[1]
            df.loc[conditions[2], "age_group"] = choices[2]
        return df
      
        # OPTIMIZED to use cached data when possible
        async def load_data_from_api(project_id, form_id, force_refresh=False):
            """Centralized function to load data from API with improved performance"""
            if not project_id or not form_id:
                hide_loading()  # Make sure to hide loading on early return
                return pd.DataFrame(), "Missing project or form ID"
                
            is_loading_data.set(True)
            show_loading("Fetching data from ODK Central...")
            try:
                data = odk_api.fetch_submissions(project_id, form_id, force_refresh)
                if isinstance(data, pd.DataFrame) and not data.empty:
                    show_loading("Processing data...")
                    await asyncio.sleep(0.1)  # Give UI time to update
                    
                    data = map_sample_labels(data)
                    data = map_a04_labels(data)
                    data = categorize_age(data)
                    message = f"Loaded {len(data)} submissions."
                    return data, message
                else:
                    return pd.DataFrame({"Error": ["No data returned from API"]}), "No data returned from API"
            except Exception as e:
                logging.error(f"Error loading data: {str(e)}")
                return pd.DataFrame({"Error": [f"Error: {str(e)}"]}), f"Error: {str(e)}"
            finally:
                is_loading_data.set(False)
                hide_loading()  # Always hide loading indicator when function completes

    def identify_gps_columns(df):
      if df is None or not isinstance(df, pd.DataFrame):
        logging.error("identify_gps_columns received invalid input")
        return []
        """Find GPS location columns in the dataset"""
        gps_columns = []
        paired_coordinates = {}
        
        # Look for the specific pattern mentioned by the user
        base_col = "patietn_health-gps_location"
        lat_col = "patietn_health-gps_location_Latitude"
        lon_col = "patietn_health-gps_location_Longitude"
        
        # Check if the specific columns exist
        if lat_col in df.columns and lon_col in df.columns:
            logging.info(f"Found paired GPS coordinates: {lat_col} and {lon_col}")
            gps_columns.extend([lat_col, lon_col])
            paired_coordinates[lat_col] = lon_col
            
            # Also add the base column if it exists
            if base_col in df.columns:
                gps_columns.append(base_col)
                
            paired_coordinates_value.set(paired_coordinates)
            return gps_columns
        
        # If specific columns aren't found, continue with general detection logic
        # Check for common GPS column names
        if "gps_location" in df.columns:
            gps_columns.append("gps_location")
        
        # Look for other columns with GPS suffixes or patterns
        for col in df.columns:
            # Skip if already identified
            if col in gps_columns:
                continue
                
            col_lower = col.lower()
            
            # Look for paired latitude/longitude columns
            if "latitude" in col_lower or "lat" in col_lower:
                # Try to find matching longitude column
                base_name = col.rsplit('_', 1)[0] if '_' in col else col.replace('latitude', '').replace('lat', '')
                potential_lon_cols = [c for c in df.columns if ('longitude' in c.lower() or 'lon' in c.lower()) 
                                     and (base_name in c or c.startswith(base_name))]
                
                if potential_lon_cols:
                    lon_col = potential_lon_cols[0]
                    logging.info(f"Found paired coordinates: {col} and {lon_col}")
                    gps_columns.extend([col, lon_col])
                    paired_coordinates[col] = lon_col
                    continue
            
            # General GPS column detection
            if ("gps" in col_lower or "location" in col_lower or "coordinates" in col_lower or 
                "geo" in col_lower or "position" in col_lower):
                if col not in gps_columns:
                    gps_columns.append(col)
            
            # Also check for columns with "_gps" suffix which is common in ODK
            if col_lower.endswith("_gps"):
                if col not in gps_columns:
                    gps_columns.append(col)
        
        # Look for specific patterns in sample values to confirm GPS data
        for col in df.columns:
            if col in gps_columns:
                continue
                
            # Check a sample of values to see if they match GPS format
            try:
                sample = df[col].dropna().astype(str).iloc[:10] if len(df) > 10 else df[col].dropna().astype(str)
                for val in sample:
                    # Look for strings that contain space-separated numbers (lat/lon format)
                    if ' ' in val:
                        parts = val.split(' ')
                        if len(parts) >= 2:
                            # Try to convert first two parts to float - if successful, likely GPS
                            try:
                                lat = float(parts[0])
                                lon = float(parts[1])
                                # Check if values are in reasonable range for lat/lon
                                if -90 <= lat <= 90 and -180 <= lon <= 180:
                                    if col not in gps_columns:
                                        gps_columns.append(col)
                                        break
                            except (ValueError, TypeError):
                                pass
            except Exception as e:
                logging.debug(f"Error checking column {col} for GPS data: {e}")
        
        paired_coordinates_value.set(paired_coordinates)
        return gps_columns

    def load_data_from_api(project_id, form_id):
        """Centralized function to load data from API to avoid duplication"""
        if not project_id or not form_id:
            return pd.DataFrame({"Error": ["No data returned from API"]}), "No data returned from API" 
            
            
        is_loading_data.set(True)
        try:
            data = odk_api.fetch_submissions(project_id, form_id)
            if isinstance(data, pd.DataFrame) and not data.empty:
                data = map_sample_labels(data)
                data = map_a04_labels(data)
                data = categorize_age(data)
                
                # Identify GPS columns in the data
                gps_cols = identify_gps_columns(data)
                gps_columns_value.set(gps_cols)
                logging.info(f"Identified GPS columns: {gps_cols}")
                
                message = f"Loaded {len(data)} submissions."
                if gps_cols:
                    message += f" Found GPS data in columns: {', '.join(gps_cols)}"
                return data, message
            else:
                return pd.DataFrame({"Error": ["No data returned from API"]}), "No data returned from API"
        except Exception as e:
            logging.error(f"Error loading data: {str(e)}")
            return pd.DataFrame({"Error": [f"Error: {str(e)}"]}), f"Error: {str(e)}"
        finally:
            is_loading_data.set(False)

      @reactive.Effect
      @reactive.event(input.restoreTokenFromJS)
      async def restore_token_from_js():
              # Skip if no credentials data
              if not input.restoreTokenFromJS():
                  return
                  
              logging.info("Attempting to restore token from session storage")
              creds = input.restoreTokenFromJS()
              if not creds or not isinstance(creds, dict):
                  logging.warning("No credentials found in session storage")
                  logged_in_value.set(False)  # Ensure logged_in is False
                  session.send_custom_message("clearToken", {})
                  return
                  
              token = creds.get("token", "")
              if not token:
                  logging.warning("Empty token in session storage")
                  logged_in_value.set(False)  # Ensure logged_in is False
                  session.send_custom_message("clearToken", {})
                  return
                  
              show_loading("Restoring session...")
              try:
                  odk_api.set_token(token)
                  projects = odk_api.fetch_projects()
                  
                  if not projects:
                      logged_in_value.set(False)
                      odk_token_value.set(None)
                      login_message_value.set("Invalid or expired session token.")
                      session.send_custom_message("clearToken", {})
                      logging.warning("Token validation failed - no projects returned")
                      hide_loading()
                      return
                  
              # Token is valid, complete login process
              logged_in_value.set(True)
              odk_token_value.set(token)
              odk_email_value.set("Session Restored")
              login_message_value.set("")
              
              # Setup initial data
              project_choices = {str(p['id']): p['name'] for p in projects}
              project_choices_value.set(project_choices)
              
              project_id = list(project_choices.keys())[0] if project_choices else None
              selected_project_id_value.set(project_id)
              
              forms = odk_api.fetch_forms(project_id) if project_id else []
              form_choices = {f['xmlFormId']: f['name'] for f in forms}
              form_choices_value.set(form_choices)
              
              form_id = list(form_choices.keys())[0] if form_choices else None
              selected_form_id_value.set(form_id)
              
              odk_api.project_id = project_id
              odk_api.form_id = form_id
              
              # Load data using the centralized function
              data, message = load_data_from_api(project_id, form_id)
              odk_data_value.set(data)
              data_message_value.set(message)
              
              log_audit_event("Token restore/login", "Session Restored")
              
              logging.info(f"Successfully restored session with token")
          except Exception as e:
              logged_in_value.set(False)
              odk_token_value.set(None)
              logging.error(f"Token restore failed: {e}")
              login_message_value.set("Session restoration failed.")
              session.send_custom_message("clearToken", {})
  
      @output
      @render.ui
      def main_ui():
          data = odk_data_value.get()
          column_selector = None
          row_selector = None
          sample_filter = None
          school_filter = None
  
          if not logged_in_value.get():
              return ui.div(
                  {"class": "container-fluid"},
                  ui.div(
                      {"class": "row justify-content-center"},
                      ui.div(
                          {"class": "col-md-6 col-lg-4"},
                          ui.div(
                              {"class": "login-container-custom animate__animated animate__fadeInUp"},
                              # Enhanced Login Header
                              ui.div(
                                  {"class": "login-header"},
                                  ui.tags.i({"class": "fas fa-shield-alt fa-2x mb-3"}),
                                  ui.h3("Secure Login", {"class": "mb-0 fw-bold"})
                              ),
                              # Enhanced Login Body
                              ui.div(
                                  {"class": "login-body"},
                                  ui.div(
                                      {"class": "mb-3"},
                                      ui.tags.label(
                                          [
                                              ui.tags.i({"class": "fas fa-user me-2"}),
                                              "Username"
                                          ],
                                          {"for": "odk_email"}
                                      ),
                                      ui.input_text("odk_email", None, placeholder="Enter your username")
                                  ),
                                  ui.div(
                                      {"class": "mb-4"},
                                      ui.tags.label(
                                          [
                                              ui.tags.i({"class": "fas fa-lock me-2"}),
                                              "Password"
                                          ],
                                          {"for": "odk_pass"}
                                      ),
                                      ui.input_password("odk_pass", None, placeholder="Enter your password")
                                  ),
                                  ui.input_action_button(
                                      "login", 
                                      ui.tags.span(
                                          ui.tags.i({"class": "fas fa-sign-in-alt me-2"}),
                                          "Sign In"
                                      )
                                  ),
                                  ui.div(login_message_value.get(), {"class": "error-message"}) if login_message_value.get() else ""
                              )
                          )
                      )
                  )
              )
          else:
              project_choices = project_choices_value.get()
              form_choices = form_choices_value.get()
              selected_project_id = selected_project_id_value.get()
              selected_form_id = selected_form_id_value.get()
              
              # Build selectors
              project_selector = None
              form_selector = None
              if project_choices:
                  project_selector = ui.input_select(
                      "selected_project",
                      "Select Project",
                      choices=project_choices,
                      selected=selected_project_id
                  )
              if form_choices:
                  form_selector = ui.input_select(
                      "selected_form",
                      "Select Form",
                      choices=form_choices,
                      selected=selected_form_id
                  )
              
              if not data.empty:
                  data = map_sample_labels(data)
                  data = map_a04_labels(data)
                  data = categorize_age(data)
                  if "sample" in data.columns:
                      values = sorted(data["sample"].dropna().astype(str).unique().tolist())
                      sample_filter = ui.input_select(
                          "sample_filter",
                          "Filter by Sample",
                          ["All"] + values,
                          selected="All"
                      )
                  if "school" in data.columns:
                      values = sorted(data["school"].dropna().astype(str).unique().tolist())
                      school_filter = ui.input_select(
                          "school_filter",
                          "Filter by School",
                          ["All"] + values,
                          selected="All"
                      )
                  columns = list(data.columns)
                  total_columns = len(columns)
                  
                  # FIXED DROPDOWN COLUMN SELECTOR WITH SEARCH AND SELECT ALL/NONE FUNCTIONALITY
                  column_selector = ui.div(
                      {"class": "form-group", "id": "submission-field-dropdown"},
                      ui.tags.label("Select Variables", {"class": "form-label"}),
                      
                      # Bootstrap 5 Dropdown
                      ui.tags.div(
                          {"class": "dropdown"},
                          ui.tags.button(
                              [
                                  ui.tags.span(f"0 of {total_columns}", id="dropdown-counter"),
                                  ui.tags.span({"class": "ms-1"}, ui.tags.i({"class": "fas fa-chevron-down"}))
                              ],
                              **{
                                  "id": "submission-field-dropdown-toggle",
                                  "class": "btn dropdown-toggle d-flex justify-content-between align-items-center w-100",
                                  "type": "button",
                                  "data-bs-toggle": "dropdown", 
                                  "aria-expanded": "false"
                              }
                          ),
                          ui.tags.div(
                              {"class": "dropdown-menu w-100 p-0", "id": "column-dropdown-list"},
                              # Search input at top with functionality
                              ui.tags.div(
                                  {"class": "dropdown-search px-2 py-2 border-bottom"},
                                  ui.tags.input(
                                      {
                                          "type": "text", 
                                          "class": "form-control form-control-sm", 
                                          "placeholder": "Search columns...", 
                                          "id": "column-search-input",
                                          "oninput": """
                                              const searchTerm = this.value.toLowerCase();
                                              document.querySelectorAll('.column-option').forEach(function(option) {
                                                  const label = option.querySelector('.form-check-label').textContent.toLowerCase();
                                                  if (label.includes(searchTerm)) {
                                                      option.style.display = '';
                                                  } else {
                                                      option.style.display = 'none';
                                                  }
                                              });
                                          """
                                      }
                                  )
                              ),
                              # Select All / None links with onclick handlers
                              ui.tags.div(
                                  {"class": "select-options px-2 py-2 border-bottom"},
                                  ui.tags.a(
                                      "Select All", 
                                      {
                                          "href": "#", 
                                          "class": "select-all-link me-2",
                                          "onclick": """
                                              document.querySelectorAll('.column-checkbox').forEach(function(checkbox) {
                                                  checkbox.checked = true;
                                              });
                                              updateDropdownCounter();
                                              return false;
                                          """
                                      }
                                  ),
                                  ui.tags.a(
                                      "None", 
                                      {
                                          "href": "#", 
                                          "class": "select-none-link ms-2",
                                          "onclick": """
                                              document.querySelectorAll('.column-checkbox').forEach(function(checkbox) {
                                                  checkbox.checked = false;
                                              });
                                              updateDropdownCounter();
                                              return false;
                                          """
                                      }
                                  )
                              ),
                              # Scrollable area with checkboxes
                              ui.tags.div(
                                  {"class": "column-checkbox-list p-2", "style": "max-height: 300px; overflow-y: auto;"},
                                  *[
                                      ui.tags.div(
                                          {"class": "form-check column-option"},
                                          ui.tags.input({
                                              "type": "checkbox", 
                                              "id": f"col_{i}", 
                                              "class": "form-check-input column-checkbox",
                                              "checked": "checked" if col in columns[:min(len(columns), 6)] else None,
                                              "onchange": "updateDropdownCounter();"
                                          }),
                                          ui.tags.label(col, {"class": "form-check-label ms-2", "for": f"col_{i}"})
                                      )
                                      for i, col in enumerate(columns)
                                  ]
                              )
                          )
                      ),
                      
                      # Add JavaScript for dropdown functionality
                      ui.tags.script("""
                      function updateDropdownCounter() {
                          const totalCheckboxes = document.querySelectorAll('.column-checkbox').length;
                          const checkedCheckboxes = document.querySelectorAll('.column-checkbox:checked').length;
                          const counterElement = document.getElementById('dropdown-counter');
                          
                          if (counterElement) {
                              counterElement.textContent = `${checkedCheckboxes} of ${totalCheckboxes}`;
                          }
                      }
                  
                      // Initialize counter on page load
                      document.addEventListener('DOMContentLoaded', function() {
                          updateDropdownCounter();
                          
                          // Focus search input when dropdown is shown
                          const dropdownToggle = document.getElementById('submission-field-dropdown-toggle');
                          if (dropdownToggle) {
                              dropdownToggle.addEventListener('click', function() {
                                  setTimeout(function() {
                                      const searchInput = document.getElementById('column-search-input');
                                      if (searchInput) {
                                          searchInput.focus();
                                      }
                                  }, 100);
                              });
                          }
                          
                          // Clear search when dropdown is hidden
                          const dropdown = document.getElementById('column-dropdown-list');
                          if (dropdown) {
                              dropdown.addEventListener('hidden.bs.dropdown', function() {
                                  const searchInput = document.getElementById('column-search-input');
                                  if (searchInput) {
                                      searchInput.value = '';
                                      // Reset visibility of all options
                                      document.querySelectorAll('.column-option').forEach(function(option) {
                                          option.style.display = '';
                                      });
                                  }
                              });
                          }
                      });
                      """)
                  )
                  
                  row_selector = ui.input_select(
                      "n_rows",
                      "Row page",
                      choices=[1, 5, 10, 20, 50, 100],
                      selected=5
                  )
              
              # Loading indicator
              loading_indicator = ui.div(
                  {"class": "alert alert-warning mb-3", "id": "loading-indicator"},
                  ui.tags.i({"class": "fas fa-sync fa-spin me-2"}),
                  "Loading data, please wait..."
              ) if is_loading_data.get() else ""
              
              # Download button is available for all users
              download_button = ui.download_button(
                  "download_data", 
                  ui.tags.span(
                      ui.tags.i({"class": "fas fa-download me-2"}),
                      "Download Data"
                  )
              )
              
              # Added force refresh button
              refresh_button = ui.input_action_button(
                  "force_refresh", 
                  ui.tags.span(
                      ui.tags.i({"class": "fas fa-sync-alt me-2"}),
                      "Force Refresh"
                  )
              )
  
              return ui.div(
                  {"class": "container-fluid"},
                  # Logout button
                  ui.div(
                      {"class": "logout-container"},
                      ui.input_action_button(
                          "logout", 
                          ui.tags.span(
                              ui.tags.i({"class": "fas fa-sign-out-alt me-2"}),
                              "Logout"
                          )
                      ),
                  ),
                  
                  # User info with Bootstrap alert - Removed role display
                  ui.div(
                      {"class": "alert alert-info d-flex align-items-center mt-3"},
                      ui.tags.i({"class": "fas fa-user-fa-check-circle me-3"}),
                      ui.tags.span(f"Logged in as {odk_email_value.get()}")
                  ),
                  
                  # Loading indicator
                  loading_indicator,
                  
                  # REARRANGED: Combined Controls section with project/form selection and column selection
                  ui.div(
                      {"class": "controls-container"},
                      ui.div(
                          {"class": "row mb-3"},
                          # Left side - Project & Form Selection
                          ui.div(
                              {"class": "col-md-6"},
                              ui.h4("Project & Form Selection", {"class": "mb-3 text-primary"}),
                              ui.div(
                                  {"class": "mb-3"},
                                  project_selector if project_selector else ""
                              ),
                              ui.div(
                                  {"class": "mb-3"},
                                  form_selector if form_selector else ""
                              ),
                              ui.input_action_button(
                                  "load_data", 
                                  ui.tags.span(
                                      ui.tags.i({"class": "fas fa-sync-alt me-2"}),
                                      "Refresh Data"
                                  )
                              ),
                              ui.output_text("data_message", {"class": "mt-2 text-muted"}),
                          ),
                          # Right side - Data Filters & Display Options
                          ui.div(
                              {"class": "col-md-6"},
                              ui.h4("Select Variable", {"class": "mb-3 text-primary"}),
                              # REARRANGED: Row with column selector and table display rows
                              ui.div(
                                  {"class": "row mb-3"},
                                  # Column selector (left) - FIXED: Removed duplicate output
                                  ui.div(
                                      {"class": "col-md-9"},
                                      ui.tags.span({"id": "submission-filters"}),
                                      column_selector if column_selector else ""
                                  ),
                                  # Table Display Rows (right)
                                  ui.div(
                                      {"class": "col-md-3"},
                                      row_selector if row_selector else ""
                                  )
                              ),
                              # Sample and School filters
                              ui.div(
                                  {"class": "row"},
                                  ui.div(
                                      {"class": "col-md-6"},
                                      ui.div(sample_filter if sample_filter else "")
                                  ),
                                  ui.div(
                                      {"class": "col-md-6"},
                                      ui.div(school_filter if school_filter else "")
                                  )
                              )
                          )
                      )
                  ),
                  
                  # Enhanced Download section
                  ui.div(
                      {"class": "download-section"},
                      ui.h5("Export Data", {"class": "mb-3 text-primary"}),
                      download_button,
                      ui.div(
                          "Download data as CSV.", 
                          {"class": "download-info"}
                      )
                  ),
                  
                  # Age Group and Schools Charts side by side
                  ui.div(
                      {"class": "row"},
                      # Age Group Bar Chart (Left side)
                      ui.div(
                          {"class": "col-lg-6 col-md-12 mb-4"},
                          ui.div(
                              {"class": "card h-100"},
                              ui.div(
                                  {"class": "card-header card-header-age d-flex align-items-center"},
                                  ui.tags.i({"class": "fas fa-user-clock me-2"}),
                                  ui.h5("Adolescent Age Group Distribution", {"class": "mb-0"})
                              ),
                              ui.div(
                                  {"class": "card-body"},
                                  output_widget("age_group_chart")
                              )
                          )
                      ),
                      # Schools Bar Chart (Right side)
                      ui.div(
                          {"class": "col-lg-6 col-md-12 mb-4"},
                          ui.div(
                              {"class": "card h-100"},
                              ui.div(
                                  {"class": "card-header d-flex align-items-center"},
                                  ui.tags.i({"class": "fas fa-school me-2"}),
                                  ui.h5("Schools Interviewed", {"class": "mb-0"})
                              ),
                              ui.div(
                                  {"class": "card-body"},
                                  output_widget("school_count_chart")
                              )
                          )
                      ),
                  ),
                  
                  # SEPARATE DONUT CHART CARDS - SIDE BY SIDE
                  ui.div(
                      {"class": "row"},
                      # Sample Distribution Donut Chart
                      ui.div(
                          {"class": "col-lg-6 col-md-12 mb-4"},
                          ui.div(
                              {"class": "card h-100"},
                              ui.div(
                                  {"class": "card-header card-header-sample d-flex align-items-center"},
                                  ui.tags.i({"class": "fas fa-chart-pie me-2"}),
                                  ui.h5("Sample Distribution", {"class": "mb-0"})
                              ),
                              ui.div(
                                  {"class": "card-body donut-chart-container"},
                                  output_widget("sd02_donut_chart"),
                              )
                          ),
                      ),
                      # Sex Distribution Donut Chart  
                      ui.div(
                          {"class": "col-lg-6 col-md-12 mb-4"},
                          ui.div(
                              {"class": "card h-100"},
                              ui.div(
                                  {"class": "card-header card-header-sex d-flex align-items-center"},
                                  ui.tags.i({"class": "fas fa-venus-mars me-2"}),
                                  ui.h5("Sex Distribution", {"class": "mb-0"})
                              ),
                              ui.div(
                                  {"class": "card-body donut-chart-container"},
                                  output_widget("a04_donut_chart"),
                              )
                          ),
                      )
                  ),
                  
                  # NEW: Map Visualization Card with ipyleaflet
                  ui.div(
                      {"class": "row"},
                      ui.div(
                          {"class": "col-12 mb-4"},
                          ui.div(
                              {"class": "card h-100 map-card"},
                              ui.div(
                                  {"class": "card-header card-header-map d-flex align-items-center"},
                                  ui.tags.i({"class": "fas fa-map-marker-alt me-2"}),
                                  ui.h5("Geographical Distribution", {"class": "mb-0"})
                              ),
                              ui.div(
                                  {"class": "card-body"},
                                  output_widget("location_map"),
                                  ui.output_ui("gps_info_box")  # New: Add GPS info display
                              )
                          )
                      )
                  ),
                  
                  # Main data table (MOVED TO BOTTOM)
                  ui.div(
                      {"class": "card mb-4 data-table-card"},
                      ui.div(
                          {"class": "card-header d-flex align-items-center"},
                          ui.tags.i({"class": "fas fa-table me-2"}),
                          ui.h5("All Submissions", {"class": "mb-0"})
                      ),
                      ui.div(
                          {"class": "card-body"},
                          ui.output_data_frame("submission_table")
                      )
                  )
              )
      
      # Output for GPS information box overlay on map
      @output
      @render.ui
      def gps_info_box():
          gps_columns = gps_columns_value.get()
          paired_coords = paired_coordinates_value.get()
          
          if not gps_columns:
              return ui.div()
          
          # Create more detailed info about how GPS data is stored
          column_info = []
          for col in gps_columns:
              if col in paired_coords:
                  column_info.append(f"{col} (paired with {paired_coords[col]})")
              elif col in paired_coords.values():
                  # Skip longitude columns as they're mentioned with their latitude pair
                  continue
              elif col == "patietn_health-gps_location":
                  column_info.append(f"{col} (base column)")
              else:
                  column_info.append(col)
              
          return ui.div(
              {"class": "gps-info-box"},
              ui.div(
                  {"class": "gps-info-title"},
                  "GPS Data Available"
              ),
              ui.div(
                  f"Found GPS coordinates in {len(column_info)} format(s):",
                  ui.tags.ul(
                      *[ui.tags.li(col_info) for col_info in column_info]
                  )
              )
          )
      
      # Download function
      @output
      @render.download(filename="Botnar_Adolescent_2.csv")
      def download_data():
          df = filtered_df()
          selected = selected_columns()
          from io import StringIO
          buffer = StringIO()
          if df is not None and not df.empty and selected:
              cols_to_download = [col for col in selected if col in df.columns] if selected else list(df.columns)
              df[cols_to_download].to_csv(buffer, index=False)
              log_audit_event("Download Data", odk_email_value.get(), f"Cols: {cols_to_download}")
          else:
              buffer.write("No data loaded\n")
          yield buffer.getvalue().encode("utf-8")
  
      @reactive.Effect
      @reactive.event(input.login)
      def handle_login():
          email = input.odk_email()
          password = input.odk_pass()
          if not email or not password:
              login_message_value.set("Please enter both ODK Central email and password")
              return
            
          show_loading("Authenticating...")
          is_loading_data.set(True)
          try:
              # Show progress bar during login
              with ui.Progress(min=1, max=10) as p:
                  p.set(message="Authenticating...", detail="Connecting to server...")
                  
                  # Set credentials
                  odk_api.set_credentials(email, password)
                  
                  # Progress step 1-3: Authentication
                  for i in range(1, 4):
                      p.set(i, message="Authenticating", detail="Verifying credentials...")
                      await asyncio.sleep(0.2)
                      
                  # Authenticate
                  if not odk_api.authenticate():
                      login_message_value.set("ODK Central authentication failed")
                      return
                      
                  # Progress step 4-6: Loading projects
                  for i in range(4, 7):
                      p.set(i, message="Loading", detail="Retrieving projects...")
                      await asyncio.sleep(0.2)
              
          try:
              odk_api.set_credentials(email, password)
              if odk_api.authenticate():
                  logged_in_value.set(True)
                  odk_email_value.set(email)
                  login_message_value.set("")
                  session.send_custom_message("saveToken", {"token": odk_api.token})
                  
                  # Get projects
                  projects = odk_api.fetch_projects()
                  project_choices = {str(p['id']): p['name'] for p in projects}
                  project_choices_value.set(project_choices)
                  
                  project_id = list(project_choices.keys())[0] if project_choices else None
                  selected_project_id_value.set(project_id)
                  
                  # Progress step 7-8: Loading forms
                  p.set(7, message="Loading", detail="Retrieving forms...")
                  await asyncio.sleep(0.2)
                  
                  # Get forms
                  forms = odk_api.fetch_forms(project_id) if project_id else []
                  form_choices = {f['xmlFormId']: f['name'] for f in forms}
                  form_choices_value.set(form_choices)
                  
                  form_id = list(form_choices.keys())[0] if form_choices else None
                  selected_form_id_value.set(form_id)
                  
                  odk_api.project_id = project_id
                  odk_api.form_id = form_id
                  
                  # Progress step 8-10: Loading data
                  for i in range(8, 11):
                      p.set(i, message="Loading", detail="Retrieving submissions...")
                      await asyncio.sleep(0.2)
                  
                  # Load data with optimized function
                  data, message = await load_data_from_api(project_id, form_id)
                  odk_data_value.set(data)
                  data_message_value.set(message)
                  
                  # Complete login
                  logged_in_value.set(True)
                  odk_email_value.set(email)
                  login_message_value.set("")
                  session.send_custom_message("saveToken", {"token": odk_api.token})
                  
                  log_audit_event("Login", email)
                  
          except Exception as e:
              login_message_value.set(f"Login error: {str(e)}")
              logging.error(f"Login error: {str(e)}")
          finally:
              is_loading_data.set(False)
              hide_loading()
  
  
      @reactive.Effect
      @reactive.event(input.logout)
      def handle_logout():
          log_audit_event("Logout", odk_email_value.get())
          logged_in_value.set(False)
          odk_email_value.set("")
          odk_data_value.set(pd.DataFrame())
          data_message_value.set("")
          login_message_value.set("")
          project_choices_value.set({})
          form_choices_value.set({})
          selected_project_id_value.set(None)
          selected_form_id_value.set(None)
          odk_token_value.set(None)
          gps_columns_value.set([])
          paired_coordinates_value.set({})
          odk_api.clear_credentials()
          session.send_custom_message("clearToken", {})
  
      @reactive.Effect
      @debounce(1000)
      @reactive.event(input.load_data)
      def load_odk_data():
          project_id = selected_project_id_value.get()
          form_id = selected_form_id_value.get()
          
          if not project_id or not form_id:
              data_message_value.set("Missing project or form ID.")
              return
              
          show_loading("Refreshing data...")
          is_loading_data.set(True)
          try:
              # Show progress bar during data loading
              with ui.Progress(min=1, max=5) as p:
                  p.set(1, message="Preparing", detail="Preparing to fetch data...")
                  await asyncio.sleep(0.2)
                  
                  p.set(2, message="Connecting", detail="Connecting to server...")
                  await asyncio.sleep(0.2)
                  
                  p.set(3, message="Downloading", detail="Downloading submissions...")
                  # Use the centralized function to load data with caching
                  data, message = await load_data_from_api(project_id, form_id, force_refresh=False)
                  
                  p.set(4, message="Processing", detail="Processing data...")
                  await asyncio.sleep(0.2)
                  
                  p.set(5, message="Finishing", detail="Completing data load...")
                  odk_data_value.set(data)
                  data_message_value.set(message)
                  
                  log_audit_event("Data Refresh", odk_email_value.get(), f"Project: {project_id}, Form: {form_id}")
                  
          except Exception as e:
              data_message_value.set(f"Error loading data: {str(e)}")
              logging.error(f"Error loading data: {str(e)}")
          finally:
              is_loading_data.set(False)
              hide_loading()
              
      # Force refresh - bypass cache
      @reactive.Effect
      @reactive.event(input.force_refresh)
      async def force_reload_odk_data():
          project_id = selected_project_id_value.get()
          form_id = selected_form_id_value.get()
          
          if not project_id or not form_id:
              data_message_value.set("Missing project or form ID.")
              return
          
          show_loading("Forcing data refresh (bypassing cache)...")
          is_loading_data.set(True)
          try:
              # Show progress bar during data loading
              with ui.Progress(min=1, max=5) as p:
                  p.set(1, message="Preparing", detail="Preparing to fetch data...")
                  await asyncio.sleep(0.2)
                  
                  p.set(2, message="Connecting", detail="Connecting to server...")
                  await asyncio.sleep(0.2)
                  
                  p.set(3, message="Downloading", detail="Downloading submissions...")
                  # Use the centralized function to load data with force_refresh=True to bypass cache
                  data, message = await load_data_from_api(project_id, form_id, force_refresh=True)
                  
                  p.set(4, message="Processing", detail="Processing data...")
                  await asyncio.sleep(0.2)
                  
                  p.set(5, message="Finishing", detail="Completing data load...")
                  odk_data_value.set(data)
                  data_message_value.set(message + " (Force refreshed)")
                  
                  log_audit_event("Force Data Refresh", odk_email_value.get(), f"Project: {project_id}, Form: {form_id}")
                  
          except Exception as e:
              data_message_value.set(f"Error loading data: {str(e)}")
              logging.error(f"Error loading data: {str(e)}")
          finally:
              is_loading_data.set(False)
              hide_loading()
  
      @reactive.Effect
      @reactive.event(input.selected_project)
      def project_selection_effect():
          project_id = input.selected_project()
          if not project_id:
              return
            
          show_loading("Loading forms for selected project...")    
          is_loading_data.set(True)
          try:
              selected_project_id_value.set(project_id)
              forms = odk_api.fetch_forms(project_id)
              form_choices = {f['xmlFormId']: f['name'] for f in forms}
              form_choices_value.set(form_choices)
              
              form_id = list(form_choices.keys())[0] if form_choices else None
              if not form_id:
                  data_message_value.set("No forms available in selected project.")
                  odk_data_value.set(pd.DataFrame())
                  return
                  
              selected_form_id_value.set(form_id)
              odk_api.project_id = project_id
              odk_api.form_id = form_id
              
              # Reset the GPS columns for the new form
              gps_columns_value.set([])
              paired_coordinates_value.set({})
              
              # Load data using the centralized function
              data, message = await load_data_from_api(project_id, form_id)
              odk_data_value.set(data)
              data_message_value.set(message)
              
              log_audit_event("Project Selection", odk_email_value.get(), f"Project: {project_id}, Form: {form_id}")
          except Exception as e:
              logging.error(f"Error in project selection: {str(e)}")
              data_message_value.set(f"Error loading project data: {str(e)}")
          finally:
              is_loading_data.set(False)
              hide_loading()
  
      @reactive.Effect
      @reactive.event(input.selected_form)
      def form_selection_effect():
          form_id = input.selected_form()
          project_id = selected_project_id_value.get()
          if not form_id or not project_id:
              return
            
          show_loading("Loading submissions for selected form...")    
          is_loading_data.set(True)
          try:
              selected_form_id_value.set(form_id)
              odk_api.form_id = form_id
              
              # Reset the GPS columns for the new form
              gps_columns_value.set([])
              paired_coordinates_value.set({})
              
              # Load data using the centralized function
              data, message = load_data_from_api(project_id, form_id)
              odk_data_value.set(data)
              data_message_value.set(message)
              
              log_audit_event("Form Selection", odk_email_value.get(), f"Project: {project_id}, Form: {form_id}")
          except Exception as e:
              logging.error(f"Error in form selection: {str(e)}")
              data_message_value.set(f"Error loading form data: {str(e)}")
          finally:
              is_loading_data.set(False)
              hide_loading()
  
      @output
      @render.text
      def data_message():
          return data_message_value.get()
  
      @reactive.Calc(memoize=True)
      def filtered_df():
          df = odk_data_value.get()
          if df is None or df.empty:
              return pd.DataFrame()
              
          # Apply filters
          for col in ["sample", "school"]:
              filter_input = getattr(input, f"{col}_filter")() if hasattr(input, f"{col}_filter") else None
              if filter_input and filter_input != "All" and col in df.columns:
                  df = df[df[col].astype(str) == filter_input]
                  
          return df
  
      @reactive.Calc
      def selected_columns():
          """Get selected columns from individual checkboxes"""
          df = odk_data_value.get()
          if df is None or df.empty:
              return []
              
          try:
              # Get all column checkbox values
              columns = list(df.columns)
              selected = []
              
              # Collect the checked columns
              for i, col in enumerate(columns):
                  checkbox_id = f"col_{i}"
                  if hasattr(input, checkbox_id) and getattr(input, checkbox_id)():
                      selected.append(col)
                      
              if not selected:
                  # Default to first 6 columns if nothing is selected
                  return list(df.columns)[:min(len(df.columns),6)]
                  
              return selected
            
            except Exception as e:
                logging.error(f"Error in selected_columns: {str(e)}")
                return list(df.columns)[:min(len(df.columns),6)]
  
      # New effect to update the button text
      @reactive.Effect
      def update_column_button_text():
          selected = selected_columns()
          if selected:
              count = len(selected)
              session.send_custom_message("updateColumnButtonText", {"count": count})
  
      @reactive.Calc
      def n_rows():
          try:
              value = input.n_rows()
              return int(value) if value is not None else 5
          except Exception:
              return 5
  
      @output
      @render.data_frame
      def submission_table():
          df = filtered_df()
          cols = selected_columns()
          rows = n_rows()
          
          if df is None or df.empty:
              return pd.DataFrame({"Message": ["No data loaded"]})
              
          show_cols = [c for c in cols if c in df.columns]
          if show_cols:
              return df[show_cols].head(rows)
          return df.head(rows)  # Fallback to all columns
  
      # Keep the table function for backward compatibility
      @output
      @render.data_frame
      def school_count_table():
          df = filtered_df()
          if df is not None and not df.empty and "school" in df.columns:
              school_counts = df["school"].value_counts().reset_index()
              school_counts.columns = ["School", "Count"]
              return school_counts
          else:
              return pd.DataFrame({"Message": ["No data loaded or no school column"]})
      
      # Updated horizontal bar chart for schools to fit in card
      @output
      @render_widget
      def school_count_chart():
          import plotly.express as px
          import plotly.graph_objects as go
          
          df = filtered_df()
          if df is not None and not df.empty and "school" in df.columns:
              # Get school counts and sort by count (descending)
              school_counts = df["school"].value_counts().reset_index()
              school_counts.columns = ["School", "Count"]
              school_counts = school_counts.sort_values("Count", ascending=True)  # For horizontal bars, we use ascending=True
              
              # Limit the number of schools to display if there are too many
              max_schools = 15
              if len(school_counts) > max_schools:
                  school_counts = school_counts.tail(max_schools)  # Show top schools by count
              
              # Calculate bar height based on number of schools
              bar_height = min(20, 350 / max(len(school_counts), 1))  # Adjust bar height to fit
              
              # Create colorful horizontal bar chart
              fig = go.Figure()
              fig.add_trace(go.Bar(
                  y=school_counts["School"],
                  x=school_counts["Count"],
                  orientation='h',
                  marker=dict(
                      color=school_counts["Count"],
                      colorscale='Viridis',  # You can try other colorscales like 'Turbo', 'Plasma', 'Bluered'
                      showscale=False
                  ),
                  text=school_counts["Count"],
                  textposition='auto',
                  hovertemplate='<b>%{y}</b><br>Count: %{x}<extra></extra>'
              ))
              
              # Adjust layout to fit in card
              fig.update_layout(
                  showlegend=False,
                  xaxis_title="Number of Submissions",
                  yaxis_title=None,
                  margin=dict(l=10, r=10, t=10, b=10),  # Reduced margins
                  height=375,  # Fixed height to match card
                  xaxis=dict(
                      gridcolor='rgba(00,35,102,0.1)',
                      zeroline=False
                  ),
                  yaxis=dict(
                      automargin=True
                  ),
                  plot_bgcolor='rgba(0,0,0,0)',
                  paper_bgcolor='rgba(0,0,0,0)',
                  font=dict(size=10)  # Smaller font size
              )
              
              # If there are many schools, make the y-axis text smaller
              if len(school_counts) > 10:
                  fig.update_layout(
                      yaxis=dict(
                          tickfont=dict(size=9),
                          automargin=True
                      )
                  )
              
              return fig
          else:
              # Create empty figure with message
              fig = go.Figure()
              fig.add_annotation(
                  text="No school data available",
                  showarrow=False,
                  font=dict(size=14)
              )
              fig.update_layout(
                  height=375,
                  xaxis=dict(showticklabels=False),
                  yaxis=dict(showticklabels=False)
              )
              return fig
      
      # Updated bar chart for age groups with percentages to fit in card
      @output
      @render_widget
      def age_group_chart():
          import plotly.express as px
          import plotly.graph_objects as go
          
          df = filtered_df()
          if df is not None and not df.empty and "age_group" in df.columns:
              # Get age group counts
              age_counts = df["age_group"].value_counts().reset_index()
              age_counts.columns = ["Age Group", "Count"]
              
              # Calculate percentages
              total = age_counts["Count"].sum()
              age_counts["Percentage"] = (age_counts["Count"] / total * 100).round(1)
              
              # Ensure the order of age groups is correct
              order = ["10-14", "15-19", "20-24"]
              age_counts["Age Group"] = pd.Categorical(age_counts["Age Group"], categories=order, ordered=True)
              age_counts = age_counts.sort_values("Age Group")
              
              # Create bar chart for age groups with percentages
              fig = go.Figure()
              fig.add_trace(go.Bar(
                  x=age_counts["Age Group"],
                  y=age_counts["Count"],
                  marker=dict(
                      color=['rgba(156, 39, 176, 0.8)', 'rgba(123, 31, 162, 0.8)', 'rgba(106, 27, 154, 0.8)'],
                      line=dict(color='rgba(156, 39, 176, 1.0)', width=2)
                  ),
                  text=[f"{count}<br>({pct}%)" for count, pct in zip(age_counts["Count"], age_counts["Percentage"])],
                  textposition='auto',
                  hovertemplate='<b>%{x}</b><br>Count: %{y}<br>Percentage: %{text}<extra></extra>'
              ))
              
              # Adjust layout to fit in card
              fig.update_layout(
                  title=None,
                  xaxis_title="Age Group (years)",
                  yaxis_title="Number of Adolescents",
                  margin=dict(l=10, r=10, t=30, b=10),  # Reduced margins
                  height=375,  # Fixed height to match card
                  xaxis=dict(
                      type='category',
                      categoryorder='array',
                      categoryarray=order,
                      tickangle=0,
                      gridcolor='rgba(0,0,0,0.1)'
                  ),
                  yaxis=dict(
                      gridcolor='rgba(0,0,0,0.1)'
                  ),
                  plot_bgcolor='rgba(0,0,0,0)',
                  paper_bgcolor='rgba(0,0,0,0)',
                  font=dict(size=12)
              )
              
              # Add percentage annotations above each bar
              for i, row in age_counts.iterrows():
                  fig.add_annotation(
                      x=row["Age Group"],
                      y=row["Count"],
                      text=f"{row['Percentage']}%",
                      showarrow=False,
                      yshift=15,
                      font=dict(size=12, color='rgba(106, 27, 154, 0.8)', family="Arial Black")
                  )
              
              return fig
          else:
              # Create empty figure with message
              fig = go.Figure()
              fig.add_annotation(
                  text="No age data available",
                  showarrow=False,
                  font=dict(size=14)
              )
              fig.update_layout(
                  height=375,
                  xaxis=dict(showticklabels=False),
                  yaxis=dict(showticklabels=False)
              )
              return fig
  
      # Enhanced donut charts with statistics - Fixed for Sample Distribution
      @output
      @render_widget
      def sd02_donut_chart():
          import plotly.express as px
          import plotly.graph_objects as go
          
          df = filtered_df()
          if df is not None and not df.empty and "sample" in df.columns:
              value_counts = df["sample"].value_counts().reset_index()
              value_counts.columns = ["Sample", "Count"]
              
              # Create donut chart with teal color scheme
              fig = go.Figure(data=[go.Pie(
                  labels=value_counts["Sample"], 
                  values=value_counts["Count"],
                  hole=0.5,  # This creates the donut hole
                  marker_colors=['#4ecdc4', '#44b9b1', '#3ba6a0', '#2d8f89'],  # Teal gradient
                  textinfo='label+percent',
                  textfont=dict(size=12, color='white'),
                  hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
              )])
              
              fig.update_layout(
                  showlegend=True,
                  legend=dict(
                      orientation="h",
                      yanchor="bottom",
                      y=-0.15,
                      xanchor="center",
                      x=0.5
                  ),
                  margin=dict(l=20, r=20, t=20, b=50),
                  height=350,
                  font=dict(size=12)
              )
              
              return fig
          else:
              # Create empty figure with message
              fig = go.Figure()
              fig.add_annotation(
                  text="No sample distribution data available",
                  showarrow=False,
                  font=dict(size=14)
              )
              fig.update_layout(
                  height=350,
                  xaxis=dict(showticklabels=False),
                  yaxis=dict(showticklabels=False)
              )
              return fig
  
      # Enhanced donut charts with statistics - Fixed for Sex Distribution
      @output
      @render_widget
      def a04_donut_chart():
          import plotly.express as px
          import plotly.graph_objects as go
          
          df = filtered_df()
          if df is not None and not df.empty and "A04" in df.columns:
              value_counts = df["A04"].value_counts().reset_index()
              value_counts.columns = ["Sex", "Count"]
              
              # Create donut chart with orange color scheme
              fig = go.Figure(data=[go.Pie(
                  labels=value_counts["Sex"], 
                  values=value_counts["Count"],
                  hole=0.5,  # This creates the donut hole
                  marker_colors=['#ff9800', '#ff6f00', '#e65100', '#bf360c'],  # Orange gradient
                  textinfo='label+percent',
                  textfont=dict(size=12, color='white'),
                  hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
              )])
              
              fig.update_layout(
                  showlegend=True,
                  legend=dict(
                      orientation="h",
                      yanchor="top",
                      y=-0.15,
                      xanchor="center",
                      x=0.5
                  ),
                  margin=dict(l=20, r=20, t=20, b=50),
                  height=350,
                  font=dict(size=12)
              )
              
              return fig
          else:
              # Create empty figure with message
              fig = go.Figure()
              fig.add_annotation(
                  text="No sex distribution data available",
                  showarrow=False,
                  font=dict(size=14)
              )
              fig.update_layout(
                  height=350,
                  xaxis=dict(showticklabels=False),
                  yaxis=dict(showticklabels=False)
              )
              return fig
  
      # Updated Map visualization function using ipyleaflet with GPS columns from form
      @output
      @render_widget
      def location_map():
          from ipyleaflet import Map, Marker, MarkerCluster, Popup, basemaps, CircleMarker, Icon, AwesomeIcon
          
          df = filtered_df()
          gps_columns = gps_columns_value.get()
          paired_coords = paired_coordinates_value.get()
          
          # Default center coordinates (Tanzania)
          center_lat = -6.8
          center_lon = 39.2
          zoom_level = 6
          
          # Initialize the map
          m = Map(
              center=(center_lat, center_lon),
              zoom=zoom_level,
              basemap=basemaps.OpenStreetMap.Mapnik,
              scroll_wheel_zoom=True
          )
          
          # Function to extract latitude and longitude from GPS string
          def extract_lat_lon(gps_string):
              if not isinstance(gps_string, str):
                  return None, None
                  
              try:
                  parts = gps_string.split()
                  if len(parts) >= 2:
                      lat = float(parts[0])
                      lon = float(parts[1])
                      # Check if values are in reasonable range for lat/lon
                      if -90 <= lat <= 90 and -180 <= lon <= 180:
                          return lat, lon
              except (ValueError, TypeError):
                  pass
              return None, None
              
              # Check for paired coordinate columns like the ones specified
              has_patietn_health_coords = False
              lat_col = "patietn_health-gps_location_Latitude"
              lon_col = "patietn_health-gps_location_Longitude"
          
          if lat_col in df.columns and lon_col in df.columns:
              has_patietn_health_coords = True
              
              # Create a copy of the DataFrame for mapping
              map_df = df.copy()
              
              # Convert to numeric and handle potential non-numeric values
              map_df[lat_col] = pd.to_numeric(map_df[lat_col], errors='coerce')
              map_df[lon_col] = pd.to_numeric(map_df[lon_col], errors='coerce')
              
              # Drop rows with missing coordinates
              map_df = map_df.dropna(subset=[lat_col, lon_col])
              
              if len(map_df) > 0:
                  # Calculate the center of the map from the data
                  center_lat = map_df[lat_col].mean()
                  center_lon = map_df[lon_col].mean()
                  m.center = (center_lat, center_lon)
                  m.zoom = 10
                  
                  # Create markers for each location
                  markers = []
                  
                  for idx, row in map_df.iterrows():
                      # Create popup content with available information
                      popup_content = "<div class='marker-popup-content'>"
                      
                      if "school" in row:
                          popup_content += f"<h4>{row['school']}</h4>"
                      
                      if "sample" in row:
                          popup_content += f"<div><strong>Sample:</strong> {row['sample']}</div>"
                      
                      if "A04" in row:  # Sex information
                          popup_content += f"<div><strong>Sex:</strong> {row['A04']}</div>"
                      
                      if "age_group" in row:
                          popup_content += f"<div><strong>Age Group:</strong> {row['age_group']}</div>"
                          
                      # Add GPS coordinates to popup
                      popup_content += f"<div><strong>Coordinates:</strong> {row[lat_col]}, {row[lon_col]}</div>"
                          
                      popup_content += "</div>"
                      
                      # Create marker with popup
                      marker_color = 'blue'
                      if "sample" in row:
                          if row["sample"] == "Public school":
                              marker_color = 'green'
                          elif row["sample"] == "Out of school":
                              marker_color = 'orange'
                              
                      # Use CircleMarker for better visibility
                      marker = CircleMarker(
                          location=(row[lat_col], row[lon_col]),
                          radius=8,
                          color=marker_color,
                          fill_color=marker_color,
                          fill_opacity=0.7,
                          popup=Popup(
                              html=popup_content,
                              max_width=300,
                              close_button=True
                          )
                      )
                      markers.append(marker)
                  
                  # If we have many markers, use a marker cluster for better performance
                  if len(markers) > 100:
                      marker_cluster = MarkerCluster(markers=markers)
                      m.add(marker_cluster)
                  else:
                      # Add all markers to the map
                      for marker in markers:
                          m.add(marker)
                  
                  # Add scale control
                  from ipyleaflet import ScaleControl
                  m.add(ScaleControl(position="bottomleft"))
                  
                  # Add fullscreen control
                  from ipyleaflet import FullScreenControl
                  m.add(FullScreenControl())
                  
                  # Add layer control to switch between different basemaps
                  from ipyleaflet import LayersControl
                  m.add(LayersControl(position="topright"))
                  
                  # Add a different basemap as an option
                  from ipyleaflet import TileLayer
                  satellite = TileLayer(
                      url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                      attribution="Esri World Imagery",
                      name="Satellite"
                  )
                  m.add(satellite)
                  return m
          
          # If we didn't find or couldn't use the specific patient health GPS columns, try other approaches
          if not has_patietn_health_coords and gps_columns:
              # We have GPS data available, try to plot it
              map_df = df.copy()
              coordinates = []
              
              # Check for other paired coordinates
              for lat_col, lon_col in paired_coords.items():
                  if lat_col in df.columns and lon_col in df.columns:
                      map_df["latitude"] = pd.to_numeric(map_df[lat_col], errors='coerce')
                      map_df["longitude"] = pd.to_numeric(map_df[lon_col], errors='coerce')
                      
                      # Drop rows with missing coordinates
                      valid_coords = map_df.dropna(subset=["latitude", "longitude"])
                      
                      for idx, row in valid_coords.iterrows():
                          coordinates.append((idx, row["latitude"], row["longitude"]))
                      
                      if coordinates:
                          break
              
              # If no paired coordinates, try columns with combined GPS format
              if not coordinates:
                  for col in gps_columns:
                      if col in paired_coords or col in paired_coords.values():
                          continue
                          
                      map_df["latitude"] = None
                      map_df["longitude"] = None
                      
                      for idx, row in map_df.iterrows():
                          lat, lon = extract_lat_lon(row[col])
                          if lat is not None and lon is not None:
                              map_df.at[idx, "latitude"] = lat
                              map_df.at[idx, "longitude"] = lon
                              coordinates.append((idx, lat, lon))
                      
                      if coordinates:
                          break
              
              # If we have coordinates, update the map
              if coordinates:
                  # Calculate the center of the map from the data
                  lats = [lat for _, lat, _ in coordinates]
                  lons = [lon for _, _, lon in coordinates]
                  center_lat = sum(lats) / len(lats)
                  center_lon = sum(lons) / len(lons)
                  m.center = (center_lat, center_lon)
                  m.zoom = 10
                  
                  # Create markers for each location
                  markers = []
                  
                  for idx, lat, lon in coordinates:
                      row = map_df.iloc[idx]
                      
                      # Create popup content with available information
                      popup_content = "<div class='marker-popup-content'>"
                      
                      if "school" in row:
                          popup_content += f"<h4>{row['school']}</h4>"
                      
                      if "sample" in row:
                          popup_content += f"<div><strong>Sample:</strong> {row['sample']}</div>"
                      
                      if "A04" in row:  # Sex information
                          popup_content += f"<div><strong>Sex:</strong> {row['A04']}</div>"
                      
                      if "age_group" in row:
                          popup_content += f"<div><strong>Age Group:</strong> {row['age_group']}</div>"
                          
                      # Add GPS coordinates to popup
                      popup_content += f"<div><strong>Coordinates:</strong> {lat}, {lon}</div>"
                          
                      popup_content += "</div>"
                      
                      # Create marker with popup
                      marker_color = 'blue'
                      if "sample" in row:
                          if row["sample"] == "Public school":
                              marker_color = 'green'
                          elif row["sample"] == "Out of school":
                              marker_color = 'orange'
                              
                      # Use CircleMarker for better visibility
                      marker = CircleMarker(
                          location=(lat, lon),
                          radius=8,
                          color=marker_color,
                          fill_color=marker_color,
                          fill_opacity=0.7,
                          popup=Popup(
                              html=popup_content,
                              max_width=300,
                              close_button=True
                          )
                      )
                      markers.append(marker)
                  
                  # If we have many markers, use a marker cluster for better performance
                  if len(markers) > 100:
                      marker_cluster = MarkerCluster(markers=markers)
                      m.add(marker_cluster)
                  else:
                      # Add all markers to the map
                      for marker in markers:
                          m.add(marker)
                  
                  # Add scale control
                  from ipyleaflet import ScaleControl
                  m.add(ScaleControl(position="bottomleft"))
                  
                  # Add fullscreen control
                  from ipyleaflet import FullScreenControl
                  m.add(FullScreenControl())
                  
                  # Add layer control to switch between different basemaps
                  from ipyleaflet import LayersControl
                  m.add(LayersControl(position="topright"))
                  
                  # Add a different basemap as an option
                  from ipyleaflet import TileLayer
                  satellite = TileLayer(
                      url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                      attribution="Esri World Imagery",
                      name="Satellite"
                  )
                  m.add(satellite)
                  return m
                  
          # If we couldn't find valid coordinates, show an appropriate message
          from ipyleaflet import Popup
          popup = Popup(
              html="<div style='text-align: center; padding: 10px;'><h4>No valid GPS coordinates found</h4></div>",
              close_button=True
          )
          m.add(popup)
          return m
  
      # Additional statistics for sample distribution
      @output
      @render.ui
      def sample_stats():
          df = filtered_df()
          if df is not None and not df.empty and "sample" in df.columns:
              value_counts = df["sample"].value_counts()
              total = len(df)
              most_common = value_counts.index[0] if len(value_counts) > 0 else "N/A"
              
              return ui.div(
                  {"class": "chart-stats"},
                  ui.div(
                      {"class": "stat-item"},
                      ui.div(str(total), {"class": "stat-value"}),
                      ui.div("Total", {"class": "stat-label"})
                  ),
                  ui.div(
                      {"class": "stat-item"},
                      ui.div(str(len(value_counts)), {"class": "stat-value"}),
                      ui.div("Categories", {"class": "stat-label"})
                  ),
                  ui.div(
                      {"class": "stat-item"},
                      ui.div(most_common, {"class": "stat-value"}),
                      ui.div("Most Common", {"class": "stat-label"})
                  )
              )
          return ui.div()
  
      # Additional statistics for sex distribution
      @output
      @render.ui
      def sex_stats():
          df = filtered_df()
          if df is not None and not df.empty and "A04" in df.columns:
              value_counts = df["A04"].value_counts()
              total = len(df)
              most_common = value_counts.index[0] if len(value_counts) > 0 else "N/A"
              male_count = value_counts.get("Male", 0)
              female_count = value_counts.get("Female", 0)
              
              return ui.div(
                  {"class": "chart-stats"},
                  ui.div(
                      {"class": "stat-item"},
                      ui.div(str(total), {"class": "stat-value"}),
                      ui.div("Total", {"class": "stat-label"})
                  ),
                  ui.div(
                      {"class": "stat-item"},
                      ui.div(str(male_count), {"class": "stat-value"}),
                      ui.div("Male", {"class": "stat-label"})
                  ),
                  ui.div(
                      {"class": "stat-item"},
                      ui.div(str(female_count), {"class": "stat-value"}),
                      ui.div("Female", {"class": "stat-label"})
                  )
              )
          return ui.div()
  
      @output
      @render.text
      def selected_columns_count():
          df = odk_data_value.get()
          if df is None or df.empty:
              return ""
          selected = selected_columns()
          n_total = len(df.columns)
          n_sel = len(selected) if selected else 0
          return f"{n_sel} of {n_total} columns selected"

def open_browser():
    url = "http://127.0.0.1:8000"
    print(f"Attempting to open browser at {url} ...")
    try:
        webbrowser.open(url)
    except Exception as e:
        print(f"Could not open browser automatically: {e}")
        print(f"Please open your browser and go to {url}")

def start_app():
    app = App(app_ui, server)
    print("Starting Shiny app on http://127.0.0.1:8000 ...")
    threading.Timer(2.0, open_browser).start()
    app.run(host="127.0.0.1", port=8000)

if __name__ == "__main__":
    start_app()
