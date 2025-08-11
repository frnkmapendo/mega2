"""
ODK Central API client for CLI application.
Extracted and refactored from the original web application.
"""

import requests
import pandas as pd
import time
import logging
from typing import Optional, List, Dict, Any


class ODKCentralAPI:
    """ODK Central API client for downloading form submissions."""
    
    def __init__(self, base_url: str, project_id: Optional[str] = None, form_id: Optional[str] = None):
        """
        Initialize ODK Central API client.
        
        Args:
            base_url: Base URL of ODK Central server
            project_id: Default project ID (optional)
            form_id: Default form ID (optional)
        """
        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self.form_id = form_id
        self.token = None
        self.email = None
        self.password = None
        
        # Caching for improved performance
        self._projects_cache = {}
        self._forms_cache = {}
        self._submissions_cache = {}
        self._cache_expiry = {}
        self._cache_lifetime = 300  # Cache lifetime in seconds
        
    def set_credentials(self, email: str, password: str) -> None:
        """Set authentication credentials."""
        self.email = email
        self.password = password

    def set_token(self, token: str) -> None:
        """Set authentication token directly."""
        self.token = token

    def clear_credentials(self) -> None:
        """Clear authentication credentials and cache."""
        self.email = None
        self.password = None
        self.token = None
        
        # Clear caches
        self._projects_cache = {}
        self._forms_cache = {}
        self._submissions_cache = {}
        self._cache_expiry = {}

    def authenticate(self) -> bool:
        """
        Authenticate with ODK Central server.
        
        Returns:
            True if authentication successful, False otherwise
        """
        if not self.email or not self.password:
            logging.error("Email and password required for authentication")
            return False
            
        try:
            auth_url = f"{self.base_url}/v1/sessions"
            response = requests.post(
                auth_url,
                json={"email": self.email, "password": self.password},
                timeout=10
            )
            response.raise_for_status()
            self.token = response.json().get("token")
            logging.info("Authentication successful for user: %s", self.email)
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"ODK Authentication failed: {e}")
            self.token = None
            return False

    def fetch_projects(self) -> List[Dict[str, Any]]:
        """
        Fetch list of projects from ODK Central.
        
        Returns:
            List of project dictionaries
        """
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
            
            return projects
        except Exception as e:
            logging.error(f"Failed to fetch projects: {e}")
            return []

    def fetch_forms(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Fetch list of forms for a specific project.
        
        Args:
            project_id: Project ID to fetch forms for
            
        Returns:
            List of form dictionaries
        """
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
            
            return forms
        except Exception as e:
            logging.error(f"Failed to fetch forms: {e}")
            return []

    def fetch_submissions(self, project_id: Optional[str] = None, form_id: Optional[str] = None, 
                         force_refresh: bool = True) -> pd.DataFrame:
        """
        Fetch form submissions as a pandas DataFrame.
        
        Args:
            project_id: Project ID (uses default if not provided)
            form_id: Form ID (uses default if not provided)
            force_refresh: Whether to force refresh cached data
            
        Returns:
            DataFrame containing submission data
        """
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

    def download_submissions_to_file(self, output_file: str, project_id: Optional[str] = None, 
                                   form_id: Optional[str] = None, file_format: str = "csv") -> bool:
        """
        Download submissions directly to a file.
        
        Args:
            output_file: Path to output file
            project_id: Project ID (uses default if not provided)
            form_id: Form ID (uses default if not provided)
            file_format: Output format ('csv', 'excel', 'json')
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            df = self.fetch_submissions(project_id, form_id)
            
            if df.empty or "Error" in df.columns:
                logging.error("No data to download or error occurred")
                return False
            
            if file_format.lower() == "csv":
                df.to_csv(output_file, index=False)
            elif file_format.lower() in ["excel", "xlsx"]:
                df.to_excel(output_file, index=False)
            elif file_format.lower() == "json":
                df.to_json(output_file, orient="records", indent=2)
            else:
                logging.error(f"Unsupported file format: {file_format}")
                return False
                
            logging.info(f"Submissions downloaded to {output_file}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to download submissions to file: {e}")
            return False