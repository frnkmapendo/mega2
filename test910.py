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
