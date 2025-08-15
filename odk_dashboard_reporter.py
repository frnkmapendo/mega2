import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import os
import sys
import tempfile
import zipfile
import io
import atexit
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, quote, unquote
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import requests
import json
import logging
import re
from PIL import Image as PILImage, ImageTk, ImageOps

# Handle imports with fallbacks
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import seaborn as sns
    import numpy as np
    
    # Set style
    plt.style.use('default')
    plt.rcParams['figure.facecolor'] = 'white'
    plt.rcParams['axes.facecolor'] = 'white'
    plt.rcParams['font.size'] = 12
    
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.utils import ImageReader
    import numpy as np
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    class tqdm:
        def __init__(self, desc="Progress", unit="B", unit_scale=True):
            self.desc = desc
        def update(self, n): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass

try:
    import folium
    from folium.plugins import MarkerCluster
    HAS_FOLIUM = True
except ImportError:
    HAS_FOLIUM = False

# Updated constants with current values
CURRENT_USER = os.getlogin()
CURRENT_DATETIME = "2025-08-15 07:33:14"  # Using the provided date/time

# Global list to track temporary files for cleanup
_temp_files_to_cleanup = []

def cleanup_temp_files():
    """Clean up temporary files on exit."""
    global _temp_files_to_cleanup
    for temp_path in _temp_files_to_cleanup:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
                # Try to remove parent directory if it's empty
                parent_dir = os.path.dirname(temp_path)
                try:
                    os.rmdir(parent_dir)
                except (OSError, FileNotFoundError):
                    pass  # Directory not empty or already removed
        except Exception as e:
            logging.warning(f"Could not clean up temp file {temp_path}: {e}")
    _temp_files_to_cleanup.clear()

# Register cleanup function
atexit.register(cleanup_temp_files)

# ============================================================================
# Enhanced Image Processing Utilities for High Quality (Fixed)
# ============================================================================

class HighQualityImageProcessor:
    """Handle high-quality image processing for PDF reports with proper file management."""
    
    SUPPORTED_FORMATS = {
        '.png': 'PNG',
        '.jpg': 'JPEG',
        '.jpeg': 'JPEG',
        '.gif': 'GIF',
        '.bmp': 'BMP',
        '.tiff': 'TIFF',
        '.tif': 'TIFF'
    }
    
    PREFERRED_FORMATS = ['.png', '.jpg', '.jpeg']
    
    @staticmethod
    def validate_image(image_path: str) -> bool:
        """Validate if image file is valid and supported with preference for PNG/JPG."""
        if not os.path.exists(image_path):
            return False
        
        try:
            file_ext = Path(image_path).suffix.lower()
            if file_ext not in HighQualityImageProcessor.SUPPORTED_FORMATS:
                return False
            
            with PILImage.open(image_path) as img:
                # Verify image integrity
                img.verify()
                return True
        except Exception:
            return False
    
    @staticmethod
    def get_image_info(image_path: str) -> Dict[str, Any]:
        """Get detailed image information."""
        try:
            with PILImage.open(image_path) as img:
                return {
                    'format': img.format,
                    'mode': img.mode,
                    'size': img.size,
                    'width': img.width,
                    'height': img.height,
                    'has_transparency': img.mode in ('RGBA', 'LA') or 'transparency' in img.info,
                    'dpi': img.info.get('dpi', (72, 72)),
                    'file_size': os.path.getsize(image_path),
                    'is_preferred_format': Path(image_path).suffix.lower() in HighQualityImageProcessor.PREFERRED_FORMATS
                }
        except Exception as e:
            logging.error(f"Error getting image info: {e}")
            return {}
    
    @staticmethod
    def optimize_image_for_pdf(image_path: str, max_width: int = 600, max_height: int = 200, 
                              target_dpi: int = 300, quality: int = 95) -> Optional[str]:
        """
        Optimize image for PDF with high quality preservation and proper file management.
        
        Args:
            image_path: Path to source image
            max_width: Maximum width in pixels
            max_height: Maximum height in pixels
            target_dpi: Target DPI for PDF
            quality: JPEG quality (for JPEG output)
        
        Returns:
            Path to optimized image or None if failed
        """
        global _temp_files_to_cleanup
        
        try:
            # Create a more persistent temp directory
            temp_dir = tempfile.mkdtemp(prefix='odk_hq_img_', suffix='_persist')
            
            # Get image info
            img_info = HighQualityImageProcessor.get_image_info(image_path)
            original_format = img_info.get('format', 'UNKNOWN')
            
            logging.info(f"Processing image: {Path(image_path).name}")
            logging.info(f"Original format: {original_format}, Size: {img_info.get('size')}, DPI: {img_info.get('dpi')}")
            
            with PILImage.open(image_path) as img:
                # Handle transparency and mode conversion
                if img.mode == 'P':
                    img = img.convert('RGBA')
                
                # For images with transparency, preserve it or convert to white background
                if img.mode in ('RGBA', 'LA'):
                    # Create white background
                    background = PILImage.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'RGBA':
                        background.paste(img, mask=img.split()[-1])  # Use alpha channel as mask
                    else:
                        background.paste(img)
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Calculate optimal size maintaining aspect ratio
                width, height = img.size
                ratio = min(max_width / width, max_height / height)
                
                # Only resize if image is larger than max dimensions or DPI is very high
                current_dpi = img_info.get('dpi', (72, 72))[0]
                needs_resize = ratio < 1 or current_dpi > target_dpi * 1.5
                
                if needs_resize:
                    new_width = int(width * ratio)
                    new_height = int(height * ratio)
                    
                    # Use high-quality resampling
                    img = img.resize((new_width, new_height), PILImage.Resampling.LANCZOS)
                    logging.info(f"Resized to: {new_width}x{new_height}")
                else:
                    logging.info("No resizing needed - image already optimal size")
                
                # Determine output format and path
                source_ext = Path(image_path).suffix.lower()
                if source_ext in ['.jpg', '.jpeg']:
                    output_format = 'JPEG'
                    temp_path = os.path.join(temp_dir, 'header_image.jpg')
                else:
                    # Default to PNG for best quality
                    output_format = 'PNG'
                    temp_path = os.path.join(temp_dir, 'header_image.png')
                
                # Save with high quality settings
                save_kwargs = {'format': output_format, 'dpi': (target_dpi, target_dpi)}
                
                if output_format == 'JPEG':
                    save_kwargs.update({
                        'quality': quality,
                        'optimize': True,
                        'progressive': True
                    })
                elif output_format == 'PNG':
                    save_kwargs.update({
                        'optimize': True,
                        'compress_level': 6  # Good compression without quality loss
                    })
                
                img.save(temp_path, **save_kwargs)
                
                # Add to cleanup list (will be cleaned up after PDF generation)
                _temp_files_to_cleanup.append(temp_path)
                
                # Verify the saved image
                final_info = HighQualityImageProcessor.get_image_info(temp_path)
                logging.info(f"Optimized image: Format: {output_format}, Size: {final_info.get('size')}, "
                           f"DPI: {final_info.get('dpi')}, File size: {final_info.get('file_size')} bytes")
                
                return temp_path
                
        except Exception as e:
            logging.error(f"Error optimizing image: {e}")
            return None
    
    @staticmethod
    def get_image_dimensions_inches(image_path: str, target_dpi: int = 300) -> tuple:
        """Get image dimensions in inches based on DPI."""
        try:
            with PILImage.open(image_path) as img:
                width_px, height_px = img.size
                dpi = img.info.get('dpi', (target_dpi, target_dpi))[0]
                
                width_inches = width_px / dpi
                height_inches = height_px / dpi
                
                return (width_inches, height_inches)
        except Exception:
            return (0, 0)
    
    @staticmethod
    def create_preview_image(image_path: str, max_size: tuple = (300, 150)) -> Optional[PILImage.Image]:
        """Create a preview image for GUI display."""
        try:
            with PILImage.open(image_path) as img:
                # Handle transparency for preview
                if img.mode in ('RGBA', 'LA', 'P'):
                    # Create white background for preview
                    background = PILImage.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'RGBA':
                        background.paste(img, mask=img.split()[-1])
                    elif img.mode == 'P' and 'transparency' in img.info:
                        img = img.convert('RGBA')
                        background.paste(img, mask=img.split()[-1])
                    else:
                        background.paste(img)
                    img = background
                
                # Calculate preview size
                img.thumbnail(max_size, PILImage.Resampling.LANCZOS)
                return img.copy()
                
        except Exception as e:
            logging.error(f"Error creating preview: {e}")
            return None

# ============================================================================
# Map Handling for Geographic Data
# ============================================================================

class MapHandler:
    """Handle map generation and display using OpenStreetMap via folium."""
    
    def __init__(self, debug=False):
        self.default_location = [-6.8235, 39.2695]  # Default location (e.g., Dar es Salaam)
        self.default_zoom = 7
        self.debug = debug
    
    def create_map_from_geopoints(self, data, lat_column=None, lon_column=None, 
                                  label_column=None, cluster=True):
        """
        Create a map visualization from geopoint data in the dataframe with improved detection.
        
        Args:
            data: DataFrame containing geopoint data
            lat_column: Column name for latitude
            lon_column: Column name for longitude
            label_column: Column name for marker labels
            cluster: Whether to cluster nearby markers
            
        Returns:
            HTML string of the map or None if no valid data
        """
        if not HAS_FOLIUM:
            return None
        
        # Debug output if enabled
        if self.debug:
            logging.info(f"Map creation with data shape: {data.shape}")
            if lat_column and lon_column:
                logging.info(f"Using specified columns: {lat_column} (lat) and {lon_column} (lon)")
            
        # Try to auto-detect geopoint columns if not specified
        if lat_column is None or lon_column is None:
            lat_column, lon_column = self._detect_geopoint_columns(data)
            if self.debug and (lat_column is not None and lon_column is not None):
                logging.info(f"Auto-detected columns: {lat_column} (lat) and {lon_column} (lon)")
            
        if lat_column is None or lon_column is None:
            if self.debug:
                logging.warning("Could not detect latitude/longitude columns")
            return None
            
        # Handle ODK-style geopoint columns (stored as "lat lon alt acc" in a single column)
        if lat_column == lon_column:  # This happens when we detect an ODK geopoint column
            # Extract coordinates from the geopoint string
            geopoint_col = lat_column
            if self.debug:
                logging.info(f"Processing ODK geopoint column: {geopoint_col}")
                sample = data[geopoint_col].dropna().iloc[0] if not data[geopoint_col].dropna().empty else None
                if sample:
                    logging.info(f"Sample geopoint data: {sample}")
            
            # Create temporary lat/lon columns
            try:
                # Make a working copy to avoid SettingWithCopyWarning
                working_data = data.copy()
                
                # Try different parsing approaches based on common ODK formats
                
                # 1. Standard ODK format: "lat lon alt acc" (space-separated)
                try:
                    # Extract from geopoint strings like "lat lon alt acc"
                    # Handle both string and list formats
                    if isinstance(working_data[geopoint_col].iloc[0], str):
                        working_data['temp_lat'] = working_data[geopoint_col].astype(str).str.split().str[0]
                        working_data['temp_lon'] = working_data[geopoint_col].astype(str).str.split().str[1]
                    elif isinstance(working_data[geopoint_col].iloc[0], list):
                        working_data['temp_lat'] = working_data[geopoint_col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
                        working_data['temp_lon'] = working_data[geopoint_col].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
                    
                    # Convert to float with coercion
                    working_data['temp_lat'] = pd.to_numeric(working_data['temp_lat'], errors='coerce')
                    working_data['temp_lon'] = pd.to_numeric(working_data['temp_lon'], errors='coerce')
                    
                    if self.debug:
                        valid_coords = working_data['temp_lat'].notna().sum()
                        logging.info(f"Parsed {valid_coords} valid coordinates from ODK geopoint")
                    
                    if working_data['temp_lat'].notna().sum() > 0:
                        lat_column = 'temp_lat'
                        lon_column = 'temp_lon'
                        data = working_data  # Use the working copy with parsed columns
                except Exception as e:
                    if self.debug:
                        logging.warning(f"Standard ODK format parsing failed: {e}")
                
                # 2. If that fails, try comma-separated format "lat,lon,alt,acc"
                if 'temp_lat' not in working_data.columns or working_data['temp_lat'].notna().sum() == 0:
                    try:
                        working_data['temp_lat'] = working_data[geopoint_col].astype(str).str.split(',').str[0]
                        working_data['temp_lon'] = working_data[geopoint_col].astype(str).str.split(',').str[1]
                        
                        # Convert to float with coercion
                        working_data['temp_lat'] = pd.to_numeric(working_data['temp_lat'], errors='coerce')
                        working_data['temp_lon'] = pd.to_numeric(working_data['temp_lon'], errors='coerce')
                        
                        if working_data['temp_lat'].notna().sum() > 0:
                            lat_column = 'temp_lat'
                            lon_column = 'temp_lon'
                            data = working_data  # Use the working copy with parsed columns
                    except Exception as e:
                        if self.debug:
                            logging.warning(f"Comma-separated format parsing failed: {e}")
            except Exception as e:
                if self.debug:
                    logging.error(f"Error extracting coordinates from geopoint column: {e}")
                return None
            
        # Filter rows with valid coordinates
        valid_data = data.dropna(subset=[lat_column, lon_column])
        
        # Check if we have any valid points
        if valid_data.empty:
            if self.debug:
                logging.warning("No valid coordinates found after filtering")
            return None
            
        # Try to convert coordinates to float if they're not already
        try:
            valid_data[lat_column] = pd.to_numeric(valid_data[lat_column], errors='coerce')
            valid_data[lon_column] = pd.to_numeric(valid_data[lon_column], errors='coerce')
            
            # Filter again after conversion
            valid_data = valid_data.dropna(subset=[lat_column, lon_column])
            
            if valid_data.empty:
                if self.debug:
                    logging.warning("No valid numeric coordinates after conversion")
                return None
                
            if self.debug:
                logging.info(f"Final valid coordinates count: {len(valid_data)}")
                sample_coords = valid_data[[lat_column, lon_column]].head(3).values.tolist()
                logging.info(f"Sample coordinates (lat, lon): {sample_coords}")
        except Exception as e:
            if self.debug:
                logging.error(f"Error converting coordinates to float: {e}")
            return None
            
        # Create base map at the center of points
        center_lat = valid_data[lat_column].mean()
        center_lon = valid_data[lon_column].mean()
        
        if self.debug:
            logging.info(f"Map center at: {center_lat}, {center_lon}")
        
        m = folium.Map(location=[center_lat, center_lon], 
                       zoom_start=self.default_zoom,
                       tiles='OpenStreetMap')
        
        # Add markers (with or without clustering)
        if cluster:
            marker_cluster = MarkerCluster().add_to(m)
            
        for idx, row in valid_data.iterrows():
            try:
                lat = float(row[lat_column])
                lon = float(row[lon_column])
                
                # Skip invalid coordinates
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    if self.debug:
                        logging.warning(f"Invalid coordinates: {lat}, {lon}")
                    continue
                    
                # Create marker label
                if label_column and label_column in row:
                    label = f"{row[label_column]}"
                else:
                    label = f"Point {idx+1}"
                    
                # Additional info popup
                popup_text = f"<b>{label}</b><br>Lat: {lat:.6f}<br>Lon: {lon:.6f}"
                
                # Create the marker
                marker = folium.Marker(
                    location=[lat, lon],
                    popup=folium.Popup(popup_text, max_width=300),
                    tooltip=label
                )
                
                # Add to cluster or directly to map
                if cluster:
                    marker.add_to(marker_cluster)
                else:
                    marker.add_to(m)
            except Exception as e:
                if self.debug:
                    logging.warning(f"Error adding marker at index {idx}: {e}")
                continue
        
        # Save to HTML string
        try:
            map_html = io.BytesIO()
            m.save(map_html, close_file=False)
            map_html.seek(0)
            return map_html.read().decode()
        except Exception as e:
            if self.debug:
                logging.error(f"Error saving map to HTML: {e}")
            return None
    
    def _detect_geopoint_columns(self, data):
        """Auto-detect latitude and longitude columns in DataFrame with enhanced detection."""
        lat_columns = []
        lon_columns = []
        
        # Common patterns for latitude/longitude columns - expanded for better detection
        lat_patterns = ['lat', 'latitude', '_lat', 'y', 'northing']
        lon_patterns = ['lon', 'lng', 'long', 'longitude', '_lon', 'x', 'easting']
        
        # Check for columns with exact matches first (highest confidence)
        for col in data.columns:
            col_lower = col.lower()
            # Exact matches for latitude
            if col_lower in ('lat', 'latitude'):
                lat_columns.insert(0, col)  # Give highest priority
            # Exact matches for longitude
            elif col_lower in ('lon', 'lng', 'long', 'longitude'):
                lon_columns.insert(0, col)  # Give highest priority
        
        # If exact matches found, return them immediately
        if lat_columns and lon_columns:
            if self.debug:
                logging.info(f"Found exact match columns: {lat_columns[0]} and {lon_columns[0]}")
            return lat_columns[0], lon_columns[0]
        
        # Look for ODK-style geopoint columns
        for col in data.columns:
            if 'geopoint' in col.lower() or 'coordinates' in col.lower():
                # Check if this column might contain coordinates
                try:
                    sample = data[col].dropna().iloc[0] if not data[col].dropna().empty else None
                    if sample:
                        # For string format "lat lon alt acc"
                        if isinstance(sample, str) and len(sample.split()) >= 2:
                            if self.debug:
                                logging.info(f"Detected ODK geopoint column (string): {col}")
                            return col, col
                        # For list/array format [lat, lon, alt, acc]
                        elif isinstance(sample, (list, tuple)) and len(sample) >= 2:
                            if self.debug:
                                logging.info(f"Detected ODK geopoint column (list): {col}")
                            return col, col
                except Exception as e:
                    if self.debug:
                        logging.debug(f"Error checking potential geopoint column {col}: {e}")
        
        # Look for partial matches if no exact matches were found
        for col in data.columns:
            col_lower = col.lower()
            # Check for latitude patterns
            if any(pattern in col_lower for pattern in lat_patterns):
                lat_columns.append(col)
            # Check for longitude patterns
            if any(pattern in col_lower for pattern in lon_patterns):
                lon_columns.append(col)
        
        # If we have potential latitude/longitude columns
        if lat_columns and lon_columns:
            # Try to find pairs with matching prefixes
            for lat_col in lat_columns:
                for lon_col in lon_columns:
                    # Extract common prefix
                    lat_parts = lat_col.lower().split('lat')
                    if len(lat_parts) > 0 and lat_parts[0]:
                        prefix = lat_parts[0]
                        if lon_col.lower().startswith(prefix):
                            if self.debug:
                                logging.info(f"Found matching prefix pair: {lat_col} and {lon_col}")
                            return lat_col, lon_col
            
            # If no matching prefixes, return the first pair
            if self.debug:
                logging.info(f"Using first available pair: {lat_columns[0]} and {lon_columns[0]}")
            return lat_columns[0], lon_columns[0]
        
        # If still no matches, look for columns with numeric data in appropriate ranges
        if self.debug:
            logging.info("Trying to identify coordinate columns by value ranges")
        
        potential_lat = []
        potential_lon = []
        
        for col in data.columns:
            try:
                # Try to convert to numeric
                numeric_data = pd.to_numeric(data[col], errors='coerce')
                valid_count = numeric_data.notna().sum()
                
                # Only consider columns with sufficient valid numeric data
                if valid_count > 5:  # At least 5 valid values
                    min_val, max_val = numeric_data.min(), numeric_data.max()
                    
                    # Check for latitude range (-90 to 90)
                    if -90 <= min_val <= max_val <= 90:
                        potential_lat.append((col, valid_count))
                    
                    # Check for longitude range (-180 to 180)
                    if -180 <= min_val <= max_val <= 180:
                        potential_lon.append((col, valid_count))
            except Exception:
                continue
        
        # Sort by valid count (descending) and take the best matches
        if potential_lat and potential_lon:
            potential_lat.sort(key=lambda x: x[1], reverse=True)
            potential_lon.sort(key=lambda x: x[1], reverse=True)
            
            lat_col = potential_lat[0][0]
            lon_col = potential_lon[0][0]
            
            # Make sure we don't use the same column for both
            if lat_col == lon_col and len(potential_lon) > 1:
                lon_col = potential_lon[1][0]
            elif lat_col == lon_col and len(potential_lat) > 1:
                lat_col = potential_lat[1][0]
            
            if lat_col != lon_col:
                if self.debug:
                    logging.info(f"Identified by value range: {lat_col} (lat) and {lon_col} (lon)")
                return lat_col, lon_col
        
        if self.debug:
            logging.warning("Could not detect latitude/longitude columns")
        return None, None
    
    def save_map_to_temp_file(self, html_content):
        """Save map HTML to a temporary file."""
        if not html_content:
            return None
            
        # Create a temp file
        fd, path = tempfile.mkstemp(suffix='.html', prefix='odk_map_')
        with os.fdopen(fd, 'w') as temp:
            temp.write(html_content)
        
        # Add to cleanup list
        global _temp_files_to_cleanup
        _temp_files_to_cleanup.append(path)
        
        return path
    
    def convert_map_to_image(self, html_content, width=800, height=500, dpi=150):
        """Convert map HTML to an image for PDF reports."""
        if not html_content:
            return None
            
        try:
            # Save to temp file
            temp_html_path = self.save_map_to_temp_file(html_content)
            if not temp_html_path:
                return None
            
            # Path for the image output
            temp_img_fd, temp_img_path = tempfile.mkstemp(suffix='.png', prefix='odk_map_img_')
            os.close(temp_img_fd)
            
            # Add to cleanup list
            global _temp_files_to_cleanup
            _temp_files_to_cleanup.append(temp_img_path)
            
            # Create a basic map image with PIL
            from PIL import Image, ImageDraw, ImageFont
            
            img = Image.new('RGB', (width, height), color=(245, 245, 245))
            draw = ImageDraw.Draw(img)
            
            # Draw a map-like placeholder
            draw.rectangle([20, 20, width-20, height-20], fill=(225, 225, 225), outline=(200, 200, 200))
            
            # Add grid lines to simulate a map
            for x in range(40, width-20, 40):
                draw.line([(x, 20), (x, height-20)], fill=(210, 210, 210), width=1)
            
            for y in range(40, height-20, 40):
                draw.line([(20, y), (width-20, y)], fill=(210, 210, 210), width=1)
            
            # Add text
            try:
                font = ImageFont.truetype("arial.ttf", 24)
            except:
                font = ImageFont.load_default()
                
            draw.text((width//2-150, height//2-20), 
                     "Map will be displayed here", 
                     fill=(100, 100, 100), font=font)
            
            draw.text((width//2-140, height//2+20), 
                     "Interactive in HTML report", 
                     fill=(100, 100, 100), font=font)
                     
            # Save the image
            img.save(temp_img_path, format='PNG', dpi=(dpi, dpi))
            
            return temp_img_path
            
        except Exception as e:
            logging.error(f"Error converting map to image: {e}")
            return None

# ============================================================================
# ODK Central Client (keeping the same implementation)
# ============================================================================

class ODKCentralClient:
    """Client for interacting with ODK Central API."""
    
    def __init__(self, base_url: str, username: str, password: str, 
                 project_id: Optional[int] = None):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.project_id = project_id
        self.session = requests.Session()
        self.token = None
        
    def authenticate(self) -> bool:
        try:
            auth_url = urljoin(self.base_url, '/v1/sessions')
            response = self.session.post(
                auth_url,
                json={'email': self.username, 'password': self.password},
                timeout=30
            )
            response.raise_for_status()
            
            self.token = response.json().get('token')
            self.session.headers.update({
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            })
            
            return True
        except Exception as e:
            logging.error(f"Authentication failed: {e}")
            return False
    
    def get_projects(self) -> List[Dict[str, Any]]:
        try:
            url = urljoin(self.base_url, '/v1/projects')
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Failed to get projects: {e}")
            return []
    
    def get_forms(self, project_id: Optional[int] = None) -> List[Dict[str, Any]]:
        pid = project_id or self.project_id
        if not pid:
            return []
            
        try:
            url = urljoin(self.base_url, f'/v1/projects/{pid}/forms')
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Failed to get forms: {e}")
            return []
    
    def get_submissions(self, form_id: str, project_id: Optional[int] = None) -> pd.DataFrame:
        pid = project_id or self.project_id
        if not pid:
            return pd.DataFrame()
            
        try:
            # URL encode the form_id properly
            encoded_form_id = quote(form_id, safe='')
            csv_url = urljoin(self.base_url, 
                             f'/v1/projects/{pid}/forms/{encoded_form_id}/submissions.csv.zip')
            
            logging.info(f"Downloading from: {csv_url}")
            
            csv_response = self.session.get(csv_url, stream=True, timeout=120)
            csv_response.raise_for_status()
            
            content = b''
            for chunk in csv_response.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
            
            if not content:
                logging.error("No content received from server")
                return pd.DataFrame()
            
            temp_dir = tempfile.mkdtemp(prefix='odk_reporter_')
            temp_zip_path = os.path.join(temp_dir, 'submissions.zip')
            
            try:
                with open(temp_zip_path, 'wb') as temp_file:
                    temp_file.write(content)
                
                with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                    csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        logging.error("No CSV files found in downloaded zip")
                        return pd.DataFrame()
                    
                    main_csv = csv_files[0]
                    with zip_ref.open(main_csv) as csv_file:
                        df = pd.read_csv(io.TextIOWrapper(csv_file, encoding='utf-8'))
                
                logging.info(f"Successfully loaded {len(df)} submissions")
                return df
                
            finally:
                try:
                    if os.path.exists(temp_zip_path):
                        os.remove(temp_zip_path)
                    os.rmdir(temp_dir)
                except Exception as e:
                    logging.warning(f"Could not clean up temp files: {e}")
            
        except Exception as e:
            logging.error(f"Error processing submissions: {e}")
            return pd.DataFrame()

# ============================================================================
# Dashboard Analytics Engine (keeping the same implementation)
# ============================================================================

class DashboardAnalytics:
    """Analytics engine for dashboard metrics."""
    
    def __init__(self, data: pd.DataFrame, form_info: Dict = None):
        self.data = data.copy() if not data.empty else pd.DataFrame()
        self.form_info = form_info or {}
        self.date_column = None
        self._prepare_data()
    
    def _prepare_data(self):
        """Prepare and clean data for analytics."""
        if self.data.empty:
            return
        
        try:
            # Find submission date column - be more flexible
            date_columns = []
            for col in self.data.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['submit', 'date', 'time', 'created', 'start', 'end']):
                    date_columns.append(col)
            
            if date_columns:
                # Try each date column until we find one that works
                for date_col in date_columns:
                    try:
                        self.data['submission_date'] = pd.to_datetime(self.data[date_col], errors='coerce')
                        
                        # Check if we got valid dates
                        valid_dates = self.data['submission_date'].notna().sum()
                        if valid_dates > 0:
                            self.date_column = date_col
                            self.data = self.data.dropna(subset=['submission_date'])
                            self.data['date_only'] = self.data['submission_date'].dt.date
                            logging.info(f"Using date column: {date_col} with {valid_dates} valid dates")
                            break
                    except Exception as e:
                        logging.warning(f"Could not parse date column {date_col}: {e}")
                        continue
            
            if self.date_column is None:
                logging.warning("No valid date column found. Some analytics will be limited.")
                
        except Exception as e:
            logging.error(f"Error preparing data: {e}")
    
    def get_daily_submissions(self) -> pd.DataFrame:
        """Get daily submission counts."""
        if self.date_column is None or self.data.empty:
            return pd.DataFrame()
        
        try:
            daily_counts = self.data.groupby('date_only').size().reset_index(name='submissions')
            daily_counts['date'] = pd.to_datetime(daily_counts['date_only'])
            return daily_counts.sort_values('date')
        except Exception as e:
            logging.error(f"Error calculating daily submissions: {e}")
            return pd.DataFrame()
    
    def get_weekly_trend(self) -> Dict[str, Any]:
        """Get weekly trend analysis."""
        if self.date_column is None or self.data.empty:
            return {}
        
        try:
            self.data['weekday'] = self.data['submission_date'].dt.day_name()
            weekly_counts = self.data.groupby('weekday').size()
            
            weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            weekly_counts = weekly_counts.reindex(weekday_order, fill_value=0)
            
            return {
                'weekday_counts': weekly_counts,
                'peak_day': weekly_counts.idxmax() if len(weekly_counts) > 0 and weekly_counts.max() > 0 else 'N/A',
                'peak_count': weekly_counts.max() if len(weekly_counts) > 0 else 0,
                'avg_daily': weekly_counts.mean() if len(weekly_counts) > 0 else 0
            }
        except Exception as e:
            logging.error(f"Error calculating weekly trend: {e}")
            return {}
    
    def get_completion_stats(self) -> Dict[str, Any]:
        """Get completion rate statistics."""
        if self.data.empty:
            return {'completion_rate': 0, 'total_fields': 0, 'avg_completed': 0, 'total_submissions': 0}
        
        try:
            total_fields = len(self.data.columns)
            total_cells = len(self.data) * total_fields
            filled_cells = total_cells - self.data.isnull().sum().sum()
            completion_rate = (filled_cells / total_cells) * 100 if total_cells > 0 else 0
            
            return {
                'completion_rate': completion_rate,
                'total_fields': total_fields,
                'total_submissions': len(self.data),
                'avg_completed_fields': filled_cells / len(self.data) if len(self.data) > 0 else 0
            }
        except Exception as e:
            logging.error(f"Error calculating completion stats: {e}")
            return {'completion_rate': 0, 'total_fields': 0, 'avg_completed': 0, 'total_submissions': 0}
    
def get_recent_activity(self, days: int = 7) -> Dict[str, Any]:
    """Get recent activity statistics."""
    if self.date_column is None or self.data.empty:
        return {'recent_submissions': 0, 'daily_average': 0, 'days_analyzed': days}
    
    try:
        # Convert to a standard datetime format without timezone for comparison
        recent_date = datetime.now().replace(tzinfo=None) - timedelta(days=days)
        
        # Handle timezone-aware and naive datetime objects
        self.data['submission_date_notz'] = self.data['submission_date'].dt.tz_localize(None) if hasattr(self.data['submission_date'].iloc[0], 'tz_localize') else self.data['submission_date']
        
        # Filter using the timezone-naive versions
        recent_data = self.data[self.data['submission_date_notz'] >= recent_date]
        
        return {
            'recent_submissions': len(recent_data),
            'daily_average': len(recent_data) / days if days > 0 else 0,
            'days_analyzed': days
        }
    except Exception as e:
        logging.error(f"Error calculating recent activity: {e}")
        return {'recent_submissions': 0, 'daily_average': 0, 'days_analyzed': days}

# ============================================================================
# Enhanced Dashboard PDF Reporter with Fixed Image Handling and Map Support
# ============================================================================

class FixedHighQualityDashboardPDFReporter:
    """Generate modern dashboard-style PDF reports with fixed high-quality header image support."""
    
    def __init__(self, analytics: DashboardAnalytics, header_image_path: Optional[str] = None):
        if not HAS_REPORTLAB:
            raise ImportError("reportlab is required for PDF generation")
        
        self.analytics = analytics
        self.header_image_path = header_image_path
        self.optimized_image_path = None  # Store optimized image path
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup modern dashboard styles."""
        try:
            # Dashboard title
            self.styles.add(ParagraphStyle(
                name='DashboardTitle',
                parent=self.styles['Title'],
                fontSize=28,
                spaceAfter=20,
                alignment=TA_CENTER,
                textColor=colors.HexColor('#2E86AB'),
                fontName='Helvetica-Bold'
            ))
            
            # Metric header
            self.styles.add(ParagraphStyle(
                name='MetricHeader',
                parent=self.styles['Heading2'],
                fontSize=18,
                spaceAfter=10,
                textColor=colors.HexColor('#A23B72'),
                fontName='Helvetica-Bold'
            ))
            
            # Section header
            self.styles.add(ParagraphStyle(
                name='SectionHeader',
                parent=self.styles['Heading3'],
                fontSize=16,
                spaceAfter=10,
                spaceBefore=20,
                textColor=colors.HexColor('#C73E1D'),
                fontName='Helvetica-Bold'
            ))
            
            # Check if Italic style already exists before adding it
            if 'Italic' not in self.styles:
                self.styles.add(ParagraphStyle(
                    name='Italic',
                    parent=self.styles['Normal'],
                    fontName='Helvetica-Oblique',
                    textColor=colors.darkgrey
                ))
                
        except Exception as e:
            logging.error(f"Error setting up styles: {e}")
    
    def generate_dashboard_report(self, output_path: str, title: str = "ODK Central Dashboard Report") -> bool:
        """Generate comprehensive dashboard report with header image."""
        try:
            logging.info(f"PDF generation to: {output_path}")
##########################################################################
            if hasattr(self.analytics, 'custom_charts') and self.analytics.custom_charts:
                story.extend(self._create_custom_charts())

            # Build the PDF
            doc.build(story)
##################################################################################
            # Pre-process header image if provided
            if self.header_image_path and HighQualityImageProcessor.validate_image(self.header_image_path):
                logging.info("Pre-processing header image...")
                self.optimized_image_path = HighQualityImageProcessor.optimize_image_for_pdf(
                    self.header_image_path, 
                    max_width=600,
                    max_height=200,
                    target_dpi=300,
                    quality=95
                )
            
            doc = SimpleDocTemplate(
                output_path,
                pagesize=A4,
                rightMargin=50,
                leftMargin=50,
                topMargin=50,
                bottomMargin=50
            )
            
            story = []
            
            # High-quality header image (using pre-processed image)
            if self.optimized_image_path:
                story.extend(self._create_fixed_header_image())
            
            # Title and header
            story.extend(self._create_dashboard_header(title))
            
            # Trend Analysis (positioned early)
            story.extend(self._create_trend_analysis())
            
            # Key metrics overview
            story.extend(self._create_metrics_overview())
            
            # Daily submissions table
            story.extend(self._create_submissions_table())
            
            # Visualizations
            if HAS_MATPLOTLIB:
                story.extend(self._create_dashboard_charts())
            
            # Geographic visualization (if geopoint data available)
            story.extend(self._create_map_visualization())
            
            # Build the PDF
            doc.build(story)
            
            logging.info("PDF generation completed successfully")
            
            # Clean up temp files after successful PDF generation
            cleanup_temp_files()
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to generate dashboard report: {e}")
            import traceback
            logging.error(f"Full error: {traceback.format_exc()}")
            # Clean up temp files on error too
            cleanup_temp_files()
            return False
    
    def generate_html_report(self, output_path: str, title: str = "ODK Central Dashboard Report") -> bool:
        """Generate an HTML report with interactive maps."""
        try:
            logging.info(f"HTML report generation to: {output_path}")
            
            # Create map handler
            map_handler = MapHandler()
            
            # Start building HTML
            html_parts = []
            html_parts.append("""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{title}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
                    .container {{ max-width: 1200px; margin: 0 auto; }}
                    h1 {{ color: #2E86AB; text-align: center; }}
                    h2 {{ color: #A23B72; margin-top: 30px; }}
                    h3 {{ color: #C73E1D; margin-top: 25px; }}
                    .section {{ margin: 30px 0; padding: 20px; background-color: #f8f9fa; border-radius: 5px; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                    th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #2E86AB; color: white; }}
                    tr:hover {{ background-color: #f5f5f5; }}
                    .chart-container {{ height: 400px; margin: 30px 0; }}
                    .map-container {{ height: 500px; margin: 30px 0; }}
                    .footer {{ text-align: center; margin-top: 50px; font-size: 0.8em; color: #666; }}
                </style>
            </head>
            <body>
                <div class="container">
            """.format(title=title))
            
            # Add header
            html_parts.append(f"<h1>{title}</h1>")
            
            if self.header_image_path and os.path.exists(self.header_image_path):
                # Copy header image to output directory
                header_img_name = os.path.basename(self.header_image_path)
                output_dir = os.path.dirname(output_path)
                header_img_path = os.path.join(output_dir, header_img_name)
                try:
                    import shutil
                    shutil.copy2(self.header_image_path, header_img_path)
                    html_parts.append(f'<div style="text-align: center;"><img src="{header_img_name}" style="max-width: 80%; max-height: 200px;"></div>')
                except Exception as e:
                    logging.error(f"Failed to copy header image: {e}")
            
            # Add form info
            if self.analytics.form_info:
                form_name = self.analytics.form_info.get('name', 'Unknown Form')
                form_id = self.analytics.form_info.get('xmlFormId', 'N/A')
                form_name = unquote(str(form_name))
                form_id = unquote(str(form_id))
                html_parts.append(f"<p><strong>Form:</strong> {form_name} (ID: {form_id})</p>")
            
            # Basic info
            html_parts.append(f"<p>Created on {CURRENT_DATETIME} UTC</p>")
            html_parts.append(f"<p>Created by: {CURRENT_USER}</p>")
            
            # Metrics overview
            html_parts.append('<div class="section">')
            html_parts.append('<h2>📊 Key Report Overview</h2>')
            
            completion_stats = self.analytics.get_completion_stats()
            
            # Make sure get_recent_activity exists or use a fallback
            recent_activity = {'recent_submissions': 0, 'daily_average': 0, 'days_analyzed': 7}
            if hasattr(self.analytics, 'get_recent_activity'):
                try:
                    recent_activity = self.analytics.get_recent_activity(7)
                except Exception as e:
                    logging.error(f"Error getting recent activity: {e}")
            
            html_parts.append('<table>')
            html_parts.append('<tr><th>Metric</th><th>Value</th><th>Description</th></tr>')
            html_parts.append(f'<tr><td>Total Submissions</td><td>{completion_stats.get("total_submissions", 0):,}</td><td>All time submissions</td></tr>')
            html_parts.append(f'<tr><td>Completion Rate</td><td>{completion_stats.get("completion_rate", 0):.1f}%</td><td>Average field completion</td></tr>')
            html_parts.append(f'<tr><td>Total Fields</td><td>{completion_stats.get("total_fields", 0):,}</td><td>Number of form fields</td></tr>')
            html_parts.append(f'<tr><td>Recent Activity (7 days)</td><td>{recent_activity.get("recent_submissions", 0):,}</td><td>Submissions in last week</td></tr>')
            html_parts.append(f'<tr><td>Daily Average</td><td>{recent_activity.get("daily_average", 0):.1f}</td><td>Average submissions per day</td></tr>')
            html_parts.append('</table>')
            html_parts.append('</div>')
            
            # Map visualization section
            if HAS_FOLIUM:
                html_parts.append('<div class="section">')
                html_parts.append('<h2>🗺️ Geographic Distribution</h2>')
                
                # Debug output the columns
                logging.info(f"Available columns for geo detection in HTML report: {', '.join(self.analytics.data.columns)}")
                
                # Look for specific columns with geopoint data
                potential_lat_cols = []
                potential_lon_cols = []
                
                for col in self.analytics.data.columns:
                    col_lower = col.lower()
                    if any(term in col_lower for term in ['latitude', 'lat', '_lat']):
                        potential_lat_cols.append(col)
                        logging.info(f"Potential latitude column detected for HTML report: {col}")
                    if any(term in col_lower for term in ['longitude', 'long', 'lon', 'lng', '_lon']):
                        potential_lon_cols.append(col)
                        logging.info(f"Potential longitude column detected for HTML report: {col}")
                
                # First try automatic detection
                map_html = map_handler.create_map_from_geopoints(self.analytics.data)
                
                # If that fails, try with explicit columns
                if not map_html and potential_lat_cols and potential_lon_cols:
                    map_html = map_handler.create_map_from_geopoints(
                        self.analytics.data, 
                        lat_column=potential_lat_cols[0],
                        lon_column=potential_lon_cols[0]
                    )
                
                if map_html:
                    html_parts.append('<p>The map below shows the geographical distribution of data collection points:</p>')
                    html_parts.append('<div class="map-container" id="map">')
                    # Insert the actual map HTML directly
                    html_parts.append(map_html)
                    html_parts.append('</div>')
                else:
                    html_parts.append('<p>No geographic data available in this dataset.</p>')
                    
                html_parts.append('</div>')
            
            # Close HTML document
            html_parts.append("""
                    <div class="footer">
                        <p>Generated by ODK Central Dashboard Reporter</p>
                    </div>
                </div>
            </body>
            </html>
            """)
            
            # Write HTML to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(''.join(html_parts))
                
            logging.info("HTML report generation completed successfully")
            return True
            
        except Exception as e:
            logging.error(f"Failed to generate HTML report: {e}")
            import traceback
            logging.error(f"Full error: {traceback.format_exc()}")
            return False
    
    def _create_fixed_header_image(self) -> List:
        """Create header image section with proper file handling."""
        story = []
        
        try:
            if not self.optimized_image_path or not os.path.exists(self.optimized_image_path):
                logging.warning("Optimized image path not available")
                return story
            
            # Get dimensions in inches
            width_inches, height_inches = HighQualityImageProcessor.get_image_dimensions_inches(
                self.optimized_image_path, target_dpi=300
            )
            
            # Ensure reasonable size limits for PDF
            max_width_inches = 7.0  # Maximum width for A4 page
            max_height_inches = 2.5  # Maximum height for header
            
            if width_inches > max_width_inches:
                ratio = max_width_inches / width_inches
                width_inches = max_width_inches
                height_inches = height_inches * ratio
            
            if height_inches > max_height_inches:
                ratio = max_height_inches / height_inches
                height_inches = max_height_inches
                width_inches = width_inches * ratio
            
            # Create ReportLab Image with proper error handling
            try:
                # Use direct file path instead of ImageReader
                header_image = Image(self.optimized_image_path, 
                                   width=width_inches*inch, 
                                   height=height_inches*inch)
                header_image.hAlign = 'CENTER'
                
                story.append(header_image)
                story.append(Spacer(1, 20))
                
                logging.info(f"Added header image: {width_inches:.2f}x{height_inches:.2f} inches")
                
            except Exception as img_error:
                logging.error(f"Error creating ReportLab Image: {img_error}")
                
                # Fallback: copy image to a more stable location
                try:
                    import shutil
                    fallback_path = os.path.join(os.path.dirname(self.optimized_image_path), 'fallback_header.png')
                    shutil.copy2(self.optimized_image_path, fallback_path)
                    
                    header_image = Image(fallback_path, 
                                       width=width_inches*inch, 
                                       height=height_inches*inch)
                    header_image.hAlign = 'CENTER'
                    
                    story.append(header_image)
                    story.append(Spacer(1, 20))
                    
                    logging.info(f"Added fallback header image: {width_inches:.2f}x{height_inches:.2f} inches")
                    
                except Exception as fallback_error:
                    logging.error(f"Fallback image creation also failed: {fallback_error}")
                    
                    # Final fallback: skip image
                    story.append(Paragraph("Header Image (Could not load)", self.styles['Normal']))
                    story.append(Spacer(1, 20))
            
        except Exception as e:
            logging.error(f"Error creating header image: {e}")

        return story
    ###########################################################
    def _create_custom_charts(self) -> List:
        """Create custom charts section based on user selections."""
        story = []
        
        try:
            # Check if custom charts were defined
            if not hasattr(self.analytics, 'custom_charts') or not self.analytics.custom_charts:
                return story
            
            story.append(Paragraph("📊 Custom Visualizations", self.styles['SectionHeader']))
            story.append(Spacer(1, 10))
            
            # Create a chart for each custom visualization
            for i, chart_info in enumerate(self.analytics.custom_charts):
                variable = chart_info['variable']
                chart_type = chart_info['chart_type']
                
                story.append(Paragraph(f"{chart_type} for {variable}", self.styles['MetricHeader']))
                story.append(Spacer(1, 10))
                
                # Create the chart
                fig = plt.Figure(figsize=(8, 5), dpi=150)
                ax = fig.add_subplot(111)
                
                # Generate chart based on type
                if chart_type == "Horizontal Bar Chart":
                    self._generate_horizontal_bar_chart(ax, variable)
                elif chart_type == "Vertical Bar Chart":
                    self._generate_vertical_bar_chart(ax, variable)
                elif chart_type == "Pie Chart":
                    self._generate_pie_chart(ax, variable)
                elif chart_type == "Line Chart":
                    self._generate_line_chart(ax, variable)
                elif chart_type == "Area Chart":
                    self._generate_area_chart(ax, variable)
                elif chart_type == "Count Plot":
                    self._generate_count_plot(ax, variable)
                
                # Save chart to buffer
                img_buffer = io.BytesIO()
                fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
                img_buffer.seek(0)
                plt.close(fig)
                
                # Add chart to report
                chart_img = Image(img_buffer, width=7*inch, height=4*inch)
                chart_img.hAlign = 'CENTER'
                story.append(chart_img)
                
                # Add description
                data_type = "categorical" if self.analytics.data[variable].dtype.kind not in 'ifc' else "numeric"
                if data_type == "categorical":
                    top_values = self.analytics.data[variable].value_counts().head(3)
                    top_vals_str = ", ".join([f"{k} ({v} occurrences)" for k, v in top_values.items()])
                    description = f"""
                    This visualization shows the distribution of <b>{variable}</b> values.
                    This is a <b>{data_type}</b> variable with {self.analytics.data[variable].nunique()} unique values.
                    The most common values are: {top_vals_str}.
                    """
                else:
                    avg = self.analytics.data[variable].mean()
                    std = self.analytics.data[variable].std()
                    min_val = self.analytics.data[variable].min()
                    max_val = self.analytics.data[variable].max()
                    description = f"""
                    This visualization shows the distribution of <b>{variable}</b> values.
                    This is a <b>{data_type}</b> variable with mean {avg:.2f} and standard deviation {std:.2f}.
                    Range: minimum {min_val:.2f} to maximum {max_val:.2f}.
                    """
                
                story.append(Paragraph(description, self.styles['Normal']))
                story.append(Spacer(1, 20))
                
            
        except Exception as e:
            logging.error(f"Error creating custom charts: {e}")
            story.append(Paragraph(f"Error creating custom charts: {str(e)}", self.styles['Normal']))
        
        return story

    # Add methods for chart generation
    def _generate_horizontal_bar_chart(self, ax, variable):
        """Generate horizontal bar chart for the given variable."""
        data = self.analytics.data
        
        if data[variable].dtype.kind in 'ifc':  # integer, float, complex
            # Numeric data: create histogram
            counts, bins = np.histogram(data[variable].dropna(), bins=min(10, len(data[variable].unique())))
            bin_labels = [f"{bins[i]:.1f} - {bins[i+1]:.1f}" for i in range(len(bins)-1)]
            y_pos = np.arange(len(bin_labels))
            ax.barh(y_pos, counts, align='center', color='skyblue')
            ax.set_yticks(y_pos)
            ax.set_yticklabels(bin_labels)
        else:
            # Categorical data: value counts
            value_counts = data[variable].value_counts().sort_values()
            # Limit to top 15 categories if too many
            if len(value_counts) > 15:
                value_counts = value_counts.tail(15)
                ax.set_title(f"Top 15 values for {variable}")
            value_counts.plot.barh(ax=ax, color='skyblue')
        
        ax.set_xlabel("Count")
        ax.set_ylabel(variable)
        ax.set_title(f"Horizontal Bar Chart for {variable}")
    ###########################################################
    def _create_dashboard_header(self, title: str) -> List:
        """Create dashboard header with title and summary."""
        story = []
        
        try:
            # Clean the title - remove URL encoding
            clean_title = unquote(title)
            story.append(Paragraph(clean_title, self.styles['DashboardTitle']))
            story.append(Spacer(1, 20))
            
            # Form info if available
            if self.analytics.form_info:
                form_name = self.analytics.form_info.get('name', 'Unknown Form')
                form_id = self.analytics.form_info.get('xmlFormId', 'N/A')
                # Clean form names
                form_name = unquote(str(form_name))
                form_id = unquote(str(form_id))
                story.append(Paragraph(f"Form: {form_name} (ID: {form_id})", self.styles['MetricHeader']))
            
            # Use the updated current date and time
            story.append(Paragraph(f"Created on {CURRENT_DATETIME} UTC", self.styles['Normal']))
            story.append(Paragraph(f"Created by: {CURRENT_USER}", self.styles['Normal']))
            story.append(Spacer(1, 30))
            
        except Exception as e:
            logging.error(f"Error creating header: {e}")
            story.append(Paragraph("Dashboard Report", self.styles['Title']))
            story.append(Spacer(1, 30))
        
        return story
    
    def _create_trend_analysis(self) -> List:
        """Create trend analysis section - positioned early in the report."""
        story = []
        
        try:
            story.append(Paragraph("📊 Trend Analysis", self.styles['SectionHeader']))
            
            weekly_data = self.analytics.get_weekly_trend()
            completion_stats = self.analytics.get_completion_stats()
            
            if weekly_data and weekly_data.get('weekday_counts') is not None and not weekly_data['weekday_counts'].empty:
                story.append(Paragraph("Weekly Patterns:", self.styles['MetricHeader']))
                peak_day = weekly_data.get('peak_day', 'N/A')
                peak_count = weekly_data.get('peak_count', 0)
                avg_daily = weekly_data.get('avg_daily', 0)
                
                weekly_summary = f"""
                • Most active day: <b>{peak_day}</b> ({peak_count} submissions)<br/>
                • Average daily submissions: <b>{avg_daily:.1f}</b><br/>
                • Weekly submission pattern shows activity distribution across weekdays
                """
                story.append(Paragraph(weekly_summary, self.styles['Normal']))
                story.append(Spacer(1, 15))
            
            # Data quality section
            story.append(Paragraph("Data Quality:", self.styles['MetricHeader']))
            total_subs = completion_stats.get('total_submissions', 0)
            completion_rate = completion_stats.get('completion_rate', 0)
            avg_completed = completion_stats.get('avg_completed_fields', 0)
            
            data_quality = f"""
            • Total submissions: <b>{total_subs:,}</b><br/>
            • Data completion rate: <b>{completion_rate:.1f}%</b><br/>
            • Average fields completed per submission: <b>{avg_completed:.1f}</b><br/>
            • Data collection shows {"excellent" if completion_rate > 90 else "good" if completion_rate > 70 else "moderate"} completion rates
            """
            story.append(Paragraph(data_quality, self.styles['Normal']))
            
        except Exception as e:
            logging.error(f"Error creating trend analysis: {e}")
            story.append(Paragraph("Error creating trend analysis", self.styles['Normal']))
        
        story.append(Spacer(1, 30))
        return story
    
    def _create_metrics_overview(self) -> List:
        """Create key metrics overview section."""
        story = []
        
        try:
            story.append(Paragraph("📊 Key Report Overview", self.styles['SectionHeader']))
            
            completion_stats = self.analytics.get_completion_stats()
            
            # Make sure get_recent_activity exists or use a fallback
            recent_activity = {'recent_submissions': 0, 'daily_average': 0, 'days_analyzed': 7}
            if hasattr(self.analytics, 'get_recent_activity'):
                try:
                    recent_activity = self.analytics.get_recent_activity(7)
                except Exception as e:
                    logging.error(f"Error getting recent activity for metrics overview: {e}")
            
            metrics_data = [
                ['Metric', 'Value', 'Description'],
                ['Total Submissions', f"{completion_stats.get('total_submissions', 0):,}", 'All time submissions'],
                ['Completion Rate', f"{completion_stats.get('completion_rate', 0):.1f}%", 'Average field completion'],
                ['Total Fields', f"{completion_stats.get('total_fields', 0):,}", 'Number of form fields'],
                ['Recent Activity (7 days)', f"{recent_activity.get('recent_submissions', 0):,}", 'Submissions in last week'],
                ['Daily Average', f"{recent_activity.get('daily_average', 0):.1f}", 'Average submissions per day'],
            ]
            
            metrics_table = Table(metrics_data, colWidths=[2.5*inch, 1.5*inch, 2.5*inch])
            metrics_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86AB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F0F8FF')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#2E86AB')),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            
            story.append(metrics_table)
            story.append(Spacer(1, 30))
            
        except Exception as e:
            logging.error(f"Error creating metrics overview: {e}")
            story.append(Paragraph("Error creating metrics overview", self.styles['Normal']))
            story.append(Spacer(1, 20))
        
        return story
    
    def _create_submissions_table(self) -> List:
        """Create detailed submissions by date table."""
        story = []
        
        try:
            story.append(Paragraph("📅 Submissions by Date", self.styles['SectionHeader']))
            
            daily_data = self.analytics.get_daily_submissions()
            
            if daily_data.empty:
                story.append(Paragraph("No submission date data available.", self.styles['Normal']))
                return story
            
            # Get last 30 days of data or all data if less than 30 days
            recent_data = daily_data.tail(min(30, len(daily_data)))
            
            # Create table data
            table_data = [['Date', 'Day of Week', 'Submissions', 'Running Total']]
            
            running_total = daily_data['submissions'].sum() - recent_data['submissions'].sum()
            
            for _, row in recent_data.iterrows():
                try:
                    date_str = row['date'].strftime('%Y-%m-%d')
                    day_of_week = row['date'].strftime('%A')
                    submissions = row['submissions']
                    running_total += submissions
                    
                    table_data.append([
                        date_str,
                        day_of_week,
                        str(submissions),
                        str(running_total)
                    ])
                except Exception as e:
                    logging.warning(f"Error processing row in submissions table: {e}")
                    continue
            
            if len(table_data) > 1:  # If we have data beyond just headers
                submissions_table = Table(table_data, colWidths=[1.5*inch, 1.5*inch, 1*inch, 1*inch])
                submissions_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#A23B72')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#FFF0F5')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#A23B72')),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ]))
                
                story.append(submissions_table)
            else:
                story.append(Paragraph("No valid date data found for table.", self.styles['Normal']))
            
            story.append(Spacer(1, 20))
            
        except Exception as e:
            logging.error(f"Error creating submissions table: {e}")
            story.append(Paragraph("Error creating submissions table", self.styles['Normal']))
            story.append(Spacer(1, 20))
        
        return story
    
    def _create_dashboard_charts(self) -> List:
        """Create modern dashboard-style charts with matplotlib warnings."""
        story = []
        
        try:
            story.append(Paragraph("📈 Visual Analysis", self.styles['SectionHeader']))
            
            # Daily submissions chart
            daily_chart = self._create_modern_daily_chart()
            if daily_chart:
                story.append(Paragraph("Daily Submissions Trend", self.styles['MetricHeader']))
                story.append(daily_chart)
                story.append(Spacer(1, 20))
            
            # Weekly pattern chart
            weekly_chart = self._create_weekly_pattern_chart()
            if weekly_chart:
                story.append(Paragraph("Weekly Submission Pattern", self.styles['MetricHeader']))
                story.append(weekly_chart)
                story.append(Spacer(1, 20))
            
            if not daily_chart and not weekly_chart:
                story.append(Paragraph("No chart data available.", self.styles['Normal']))
                story.append(Spacer(1, 20))
            
        except Exception as e:
            logging.error(f"Error creating charts: {e}")
            story.append(Paragraph("Error creating charts", self.styles['Normal']))
            story.append(Spacer(1, 20))
        
        return story
    
    def _create_modern_daily_chart(self) -> Optional[Image]:
        """Create modern daily submissions chart."""
        try:
            daily_data = self.analytics.get_daily_submissions()
            if daily_data.empty:
                logging.warning("No daily data available for chart")
                return None
            
            # Create figure with high DPI
            fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
            fig.patch.set_facecolor('white')
            ax.set_facecolor('white')
            
            dates = daily_data['date']
            submissions = daily_data['submissions']
            
            # Convert dates to proper datetime format to avoid matplotlib warnings
            dates_numeric = [date.toordinal() for date in dates]
            
            # Plot data with numeric dates
            ax.plot(dates, submissions, color='#2E86AB', linewidth=3, marker='o', markersize=6)
            ax.fill_between(dates, submissions, alpha=0.3, color='#2E86AB')
            
            # Styling
            ax.set_title('Daily Submissions Over Time', fontsize=16, fontweight='bold', color='#2E86AB')
            ax.set_xlabel('Date', fontsize=12)
            ax.set_ylabel('Number of Submissions', fontsize=12)
            
            # Format dates
            if len(dates) > 0:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates)//10)))
                plt.xticks(rotation=45)
            
            # Grid and styling
            ax.grid(True, alpha=0.3)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            
            plt.tight_layout()
            
            # Save to buffer with high quality
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            img_buffer.seek(0)
            plt.close(fig)
            
            return Image(img_buffer, width=7*inch, height=3.5*inch)
            
        except Exception as e:
            logging.error(f"Error creating daily chart: {e}")
            return None
    
    def _create_weekly_pattern_chart(self) -> Optional[Image]:
        """Create weekly pattern bar chart with string handling."""
        try:
            weekly_data = self.analytics.get_weekly_trend()
            if not weekly_data or 'weekday_counts' not in weekly_data:
                logging.warning("No weekly data available for chart")
                return None
            
            weekday_counts = weekly_data['weekday_counts']
            if weekday_counts.empty or weekday_counts.sum() == 0:
                logging.warning("No weekly submission data to chart")
                return None
            
            # Create figure with high DPI
            fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
            fig.patch.set_facecolor('white')
            ax.set_facecolor('white')
            
            # Convert weekday names to positions to avoid matplotlib categorical warnings
            weekdays = list(weekday_counts.index)
            values = list(weekday_counts.values)
            positions = range(len(weekdays))
            
            # Create bar chart with numeric positions
            colors_list = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#4CAF50', '#FF9800', '#9C27B0']
            bars = ax.bar(positions, values, 
                         color=colors_list[:len(values)], alpha=0.8)
            
            # Set weekday labels
            ax.set_xticks(positions)
            ax.set_xticklabels(weekdays)
            
            # Add value labels on bars
            for i, (bar, value) in enumerate(zip(bars, values)):
                if value > 0:
                    ax.text(i, value + 0.1, f'{int(value)}', 
                           ha='center', va='bottom', fontweight='bold')
            
            # Styling
            ax.set_title('Submissions by Day of Week', fontsize=16, fontweight='bold', color='#2E86AB')
            ax.set_xlabel('Day of Week', fontsize=12)
            ax.set_ylabel('Number of Submissions', fontsize=12)
            
            # Remove top and right spines
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Save to buffer with high quality
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            img_buffer.seek(0)
            plt.close(fig)
            
            return Image(img_buffer, width=6*inch, height=3.6*inch)
            
        except Exception as e:
            logging.error(f"Error creating weekly chart: {e}")
            return None
    
    def _create_map_visualization(self) -> List:
        """Create map visualization from geopoint data with enhanced error handling."""
        story = []
        
        try:
            # Check if folium is available
            if not HAS_FOLIUM:
                logging.warning("Folium library not available. Maps cannot be generated.")
                story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
                story.append(Spacer(1, 10))
                story.append(Paragraph(
                    "Geographic visualization requires the folium library. "
                    "Please install it with: pip install folium",
                    self.styles['Normal']
                ))
                return story
                
            # Check if we have data
            data = self.analytics.data
            if data.empty:
                logging.warning("No data available for map visualization.")
                return story
                
            # Add debugging log output for column inspection
            logging.info(f"Available columns for geo detection: {', '.join(data.columns)}")
            
            # Initialize map handler with explicit debug mode
            map_handler = MapHandler(debug=True)
            
            # Debug: Check for potential geo columns by printing numeric columns and their ranges
            for col in data.columns:
                try:
                    if data[col].dtype.kind in 'if':  # integer or float
                        values = data[col].dropna()
                        if not values.empty:
                            min_val, max_val = values.min(), values.max()
                            logging.info(f"Numeric column: {col}, Range: {min_val} to {max_val}")
                except Exception as e:
                    logging.debug(f"Could not analyze column {col}: {e}")
                    
            # Try to explicitly identify potential latitude and longitude columns
            potential_lat_cols = []
            potential_lon_cols = []
            
            for col in data.columns:
                col_lower = col.lower()
                if any(term in col_lower for term in ['latitude', 'lat', '_lat']):
                    potential_lat_cols.append(col)
                    logging.info(f"Potential latitude column detected: {col}")
                if any(term in col_lower for term in ['longitude', 'long', 'lon', 'lng', '_lon']):
                    potential_lon_cols.append(col)
                    logging.info(f"Potential longitude column detected: {col}")
            
            # Look for ODK geopoint columns (format: "lat lon alt acc")
            geopoint_cols = []
            for col in data.columns:
                if 'geopoint' in col.lower():
                    geopoint_cols.append(col)
                    logging.info(f"Potential ODK geopoint column detected: {col}")
                    # Sample the data to verify format
                    sample = data[col].dropna().iloc[0] if not data[col].dropna().empty else None
                    if sample:
                        logging.info(f"Sample geopoint data: {sample}")
            
            # First attempt - use automatic detection
            map_html = map_handler.create_map_from_geopoints(data)
            
            # If automatic detection fails, try manual approaches with explicit columns
            if not map_html and potential_lat_cols and potential_lon_cols:
                logging.info(f"Attempting manual lat/lon mapping with: {potential_lat_cols[0]} and {potential_lon_cols[0]}")
                map_html = map_handler.create_map_from_geopoints(
                    data, 
                    lat_column=potential_lat_cols[0], 
                    lon_column=potential_lon_cols[0]
                )
            
            # If that fails and we have geopoint columns, try parsing them
            if not map_html and geopoint_cols:
                # Create temporary parsed columns
                try:
                    logging.info(f"Attempting to parse geopoint column: {geopoint_cols[0]}")
                    gp_col = geopoint_cols[0]
                    
                    # Make a copy to avoid modifying original
                    temp_data = data.copy()
                    
                    # Try to extract lat/lon from space-separated geopoint string
                    temp_data['_temp_lat'] = temp_data[gp_col].astype(str).str.split().str[0]
                    temp_data['_temp_lon'] = temp_data[gp_col].astype(str).str.split().str[1]
                    
                    # Convert to float
                    temp_data['_temp_lat'] = pd.to_numeric(temp_data['_temp_lat'], errors='coerce')
                    temp_data['_temp_lon'] = pd.to_numeric(temp_data['_temp_lon'], errors='coerce')
                    
                    logging.info(f"Created temp columns with {temp_data['_temp_lat'].notna().sum()} valid coordinates")
                    
                    # Try with the temporary columns
                    map_html = map_handler.create_map_from_geopoints(
                        temp_data, 
                        lat_column='_temp_lat', 
                        lon_column='_temp_lon'
                    )
                except Exception as parse_err:
                    logging.error(f"Error parsing geopoint column: {parse_err}")
            
            # If we have map HTML, create the visualization
            if map_html:
                # Add map section header
                story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
                story.append(Spacer(1, 10))
                
                # Add map description
                story.append(Paragraph(
                    "The map below shows the geographical distribution of data collection points. "
                    "Each marker represents a data collection location.",
                    self.styles['Normal']
                ))
                story.append(Spacer(1, 15))
                
                # Convert map to image and add to report
                map_image_path = map_handler.convert_map_to_image(map_html)
                
                if map_image_path and os.path.exists(map_image_path):
                    # Get dimensions in inches (for PDF)
                    width_inches, height_inches = HighQualityImageProcessor.get_image_dimensions_inches(
                        map_image_path, target_dpi=150
                    )
                    
                    # Ensure reasonable size for the map
                    max_width_inches = 6.5  # For A4/Letter page with margins
                    if width_inches > max_width_inches:
                        ratio = max_width_inches / width_inches
                        width_inches = max_width_inches
                        height_inches = height_inches * ratio
                    
                    # Add the map image
                    map_img = Image(map_image_path, width=width_inches*inch, height=height_inches*inch)
                    map_img.hAlign = 'CENTER'
                    story.append(map_img)
                    story.append(Spacer(1, 15))
                    
                    # Add note about interactive map in HTML version
                    story.append(Paragraph(
                        "Note: An interactive version of this map is available in the HTML report.",
                        self.styles['Italic']
                    ))
                else:
                    story.append(Paragraph(
                        "Geographic visualization is available in the HTML report version.",
                        self.styles['Normal']
                    ))
            else:
                # No map could be generated - provide more helpful error info
                story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
                story.append(Spacer(1, 10))
                
                if geopoint_cols or potential_lat_cols:
                    detected_cols = []
                    if geopoint_cols:
                        detected_cols.append(f"Geopoint column(s): {', '.join(geopoint_cols)}")
                    if potential_lat_cols:
                        detected_cols.append(f"Lat column(s): {', '.join(potential_lat_cols)}")
                    if potential_lon_cols:
                        detected_cols.append(f"Lon column(s): {', '.join(potential_lon_cols)}")
                    
                    explanation = (
                        f"Potential geographic data was detected ({', '.join(detected_cols)}), "
                        "but could not be processed. This may be due to invalid coordinates or formatting issues. "
                        "Try the HTML report for interactive maps."
                    )
                    story.append(Paragraph(explanation, self.styles['Normal']))
                else:
                    story.append(Paragraph(
                        "No geographic data was detected in this dataset. "
                        "Geographic visualization requires latitude and longitude coordinates.",
                        self.styles['Normal']
                    ))
            
            story.append(Spacer(1, 20))
                
        except Exception as e:
            logging.error(f"Error creating map visualization: {e}")
            import traceback
            logging.error(traceback.format_exc())
            
            # Add error message to the report
            story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
            story.append(Spacer(1, 10))
            story.append(Paragraph(
                "Error creating geographic visualization. This may be due to invalid coordinates or data format issues.",
                self.styles['Normal']
            ))
            story.append(Spacer(1, 20))
        
        return story
    
def _create_modern_daily_chart(self) -> Optional[Image]:
    """Create modern daily submissions chart."""
    try:
        daily_data = self.analytics.get_daily_submissions()
        if daily_data.empty:
            logging.warning("No daily data available for chart")
            return None
        
        # Create figure with high DPI
        fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
        fig.patch.set_facecolor('white')
        ax.set_facecolor('white')
        
        dates = daily_data['date']
        submissions = daily_data['submissions']
        
        # Convert dates to proper datetime format to avoid matplotlib warnings
        dates_numeric = [date.toordinal() for date in dates]
        
        # Plot data with numeric dates
        ax.plot(dates, submissions, color='#2E86AB', linewidth=3, marker='o', markersize=6)
        ax.fill_between(dates, submissions, alpha=0.3, color='#2E86AB')
        
        # Styling
        ax.set_title('Daily Submissions Over Time', fontsize=16, fontweight='bold', color='#2E86AB')
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Number of Submissions', fontsize=12)
        
        # Format dates
        if len(dates) > 0:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates)//10)))
            plt.xticks(rotation=45)
        
        # Grid and styling
        ax.grid(True, alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        plt.tight_layout()
        
        # Save to buffer with high quality
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        img_buffer.seek(0)
        plt.close(fig)
        
        return Image(img_buffer, width=7*inch, height=3.5*inch)
        
    except Exception as e:
        logging.error(f"Error creating daily chart: {e}")
        return None

def _create_weekly_pattern_chart(self) -> Optional[Image]:
    """Create weekly pattern bar chart with string handling."""
    try:
        weekly_data = self.analytics.get_weekly_trend()
        if not weekly_data or 'weekday_counts' not in weekly_data:
            logging.warning("No weekly data available for chart")
            return None
        
        weekday_counts = weekly_data['weekday_counts']
        if weekday_counts.empty or weekday_counts.sum() == 0:
            logging.warning("No weekly submission data to chart")
            return None
        
        # Create figure with high DPI
        fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
        fig.patch.set_facecolor('white')
        ax.set_facecolor('white')
        
        # Convert weekday names to positions to avoid matplotlib categorical warnings
        weekdays = list(weekday_counts.index)
        values = list(weekday_counts.values)
        positions = range(len(weekdays))
        
        # Create bar chart with numeric positions
        colors_list = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#4CAF50', '#FF9800', '#9C27B0']
        bars = ax.bar(positions, values, 
                     color=colors_list[:len(values)], alpha=0.8)
        
        # Set weekday labels
        ax.set_xticks(positions)
        ax.set_xticklabels(weekdays)
        
        # Add value labels on bars
        for i, (bar, value) in enumerate(zip(bars, values)):
            if value > 0:
                ax.text(i, value + 0.1, f'{int(value)}', 
                       ha='center', va='bottom', fontweight='bold')
        
        # Styling
        ax.set_title('Submissions by Day of Week', fontsize=16, fontweight='bold', color='#2E86AB')
        ax.set_xlabel('Day of Week', fontsize=12)
        ax.set_ylabel('Number of Submissions', fontsize=12)
        
        # Remove top and right spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save to buffer with high quality
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        img_buffer.seek(0)
        plt.close(fig)
        
        return Image(img_buffer, width=6*inch, height=3.6*inch)
        
    except Exception as e:
        logging.error(f"Error creating weekly chart: {e}")
        return None

def _create_map_visualization(self) -> List:
    """Create map visualization from geopoint data with enhanced error handling."""
    story = []
    
    try:
        # Check if folium is available
        if not HAS_FOLIUM:
            logging.warning("Folium library not available. Maps cannot be generated.")
            story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
            story.append(Spacer(1, 10))
            story.append(Paragraph(
                "Geographic visualization requires the folium library. "
                "Please install it with: pip install folium",
                self.styles['Normal']
            ))
            return story
            
        # Check if we have data
        data = self.analytics.data
        if data.empty:
            logging.warning("No data available for map visualization.")
            return story
            
        # Add debugging log output for column inspection
        logging.info(f"Available columns for geo detection: {', '.join(data.columns)}")
        
        # Initialize map handler with explicit debug mode
        map_handler = MapHandler(debug=True)
        
        # Debug: Check for potential geo columns by printing numeric columns and their ranges
        for col in data.columns:
            try:
                if data[col].dtype.kind in 'if':  # integer or float
                    values = data[col].dropna()
                    if not values.empty:
                        min_val, max_val = values.min(), values.max()
                        logging.info(f"Numeric column: {col}, Range: {min_val} to {max_val}")
            except Exception as e:
                logging.debug(f"Could not analyze column {col}: {e}")
                
        # Try to explicitly identify potential latitude and longitude columns
        potential_lat_cols = []
        potential_lon_cols = []
        
        for col in data.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['latitude', 'lat', '_lat']):
                potential_lat_cols.append(col)
                logging.info(f"Potential latitude column detected: {col}")
            if any(term in col_lower for term in ['longitude', 'long', 'lon', 'lng', '_lon']):
                potential_lon_cols.append(col)
                logging.info(f"Potential longitude column detected: {col}")
        
        # Look for ODK geopoint columns (format: "lat lon alt acc")
        geopoint_cols = []
        for col in data.columns:
            if 'geopoint' in col.lower():
                geopoint_cols.append(col)
                logging.info(f"Potential ODK geopoint column detected: {col}")
                # Sample the data to verify format
                sample = data[col].dropna().iloc[0] if not data[col].dropna().empty else None
                if sample:
                    logging.info(f"Sample geopoint data: {sample}")
        
        # First attempt - use automatic detection
        map_html = map_handler.create_map_from_geopoints(data)
        
        # If automatic detection fails, try manual approaches with explicit columns
        if not map_html and (potential_lat_cols and potential_lon_cols):
            logging.info(f"Attempting manual lat/lon mapping with: {potential_lat_cols[0]} and {potential_lon_cols[0]}")
            map_html = map_handler.create_map_from_geopoints(
                data, 
                lat_column=potential_lat_cols[0], 
                lon_column=potential_lon_cols[0]
            )
        
        # If that fails and we have geopoint columns, try parsing them
        if not map_html and geopoint_cols:
            # Create temporary parsed columns
            try:
                logging.info(f"Attempting to parse geopoint column: {geopoint_cols[0]}")
                gp_col = geopoint_cols[0]
                
                # Make a copy to avoid modifying original
                temp_data = data.copy()
                
                # Try to extract lat/lon from space-separated geopoint string
                temp_data['_temp_lat'] = temp_data[gp_col].astype(str).str.split().str[0]
                temp_data['_temp_lon'] = temp_data[gp_col].astype(str).str.split().str[1]
                
                # Convert to float
                temp_data['_temp_lat'] = pd.to_numeric(temp_data['_temp_lat'], errors='coerce')
                temp_data['_temp_lon'] = pd.to_numeric(temp_data['_temp_lon'], errors='coerce')
                
                logging.info(f"Created temp columns with {temp_data['_temp_lat'].notna().sum()} valid coordinates")
                
                # Try with the temporary columns
                map_html = map_handler.create_map_from_geopoints(
                    temp_data, 
                    lat_column='_temp_lat', 
                    lon_column='_temp_lon'
                )
            except Exception as parse_err:
                logging.error(f"Error parsing geopoint column: {parse_err}")
        
        # If we have map HTML, create the visualization
        if map_html:
            # Add map section header
            story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
            story.append(Spacer(1, 10))
            
            # Add map description
            story.append(Paragraph(
                "The map below shows the geographical distribution of data collection points. "
                "Each marker represents a data collection location.",
                self.styles['Normal']
            ))
            story.append(Spacer(1, 15))
            
            # Convert map to image and add to report
            map_image_path = map_handler.convert_map_to_image(map_html)
            
            if map_image_path and os.path.exists(map_image_path):
                # Get dimensions in inches (for PDF)
                width_inches, height_inches = HighQualityImageProcessor.get_image_dimensions_inches(
                    map_image_path, target_dpi=150
                )
                
                # Ensure reasonable size for the map
                max_width_inches = 6.5  # For A4/Letter page with margins
                if width_inches > max_width_inches:
                    ratio = max_width_inches / width_inches
                    width_inches = max_width_inches
                    height_inches = height_inches * ratio
                
                # Add the map image
                map_img = Image(map_image_path, width=width_inches*inch, height=height_inches*inch)
                map_img.hAlign = 'CENTER'
                story.append(map_img)
                story.append(Spacer(1, 15))
                
                # Add note about interactive map in HTML version
                story.append(Paragraph(
                    "Note: An interactive version of this map is available in the HTML report.",
                    self.styles['Italic']
                ))
            else:
                story.append(Paragraph(
                    "Geographic visualization is available in the HTML report version.",
                    self.styles['Normal']
                ))
        else:
            # No map could be generated - provide more helpful error info
            story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
            story.append(Spacer(1, 10))
            
            if geopoint_cols or potential_lat_cols:
                detected_cols = []
                if geopoint_cols:
                    detected_cols.append(f"Geopoint column(s): {', '.join(geopoint_cols)}")
                if potential_lat_cols:
                    detected_cols.append(f"Lat column(s): {', '.join(potential_lat_cols)}")
                if potential_lon_cols:
                    detected_cols.append(f"Lon column(s): {', '.join(potential_lon_cols)}")
                
                explanation = (
                    f"Potential geographic data was detected ({', '.join(detected_cols)}), "
                    "but could not be processed. This may be due to invalid coordinates or formatting issues. "
                    "Try the HTML report for interactive maps."
                )
                story.append(Paragraph(explanation, self.styles['Normal']))
            else:
                story.append(Paragraph(
                    "No geographic data was detected in this dataset. "
                    "Geographic visualization requires latitude and longitude coordinates.",
                    self.styles['Normal']
                ))
        
        story.append(Spacer(1, 20))
            
    except Exception as e:
        logging.error(f"Error creating map visualization: {e}")
        import traceback
        logging.error(traceback.format_exc())
        
        # Add error message to the report
        story.append(Paragraph("🗺️ Geographic Distribution", self.styles['SectionHeader']))
        story.append(Spacer(1, 10))
        story.append(Paragraph(
            "Error creating geographic visualization. This may be due to invalid coordinates or data format issues.",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 20))
    
    return story

# ============================================================================
# Enhanced GUI Application with Image Support
# ============================================================================

class FixedODKDashboardGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Dashboard Reporter")
        self.root.geometry("850x850")
        self.root.resizable(True, True)
        
        # Variables
        self.base_url = tk.StringVar(value="https://")
        self.username = tk.StringVar()
        self.password = tk.StringVar()
        self.remember_password = tk.BooleanVar(value=False)
        self.project_id = tk.StringVar()
        self.form_id = tk.StringVar()
        self.report_title = tk.StringVar(value="ODK Central Dashboard Report")
        self.header_image_path = tk.StringVar()
        
        # Style
        self.style = ttk.Style()
        self.style.theme_use('classic')

        self.setup_ui()
        
    def setup_ui(self):
        # Configure the root window to expand and fill the screen
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)
        
        # Create main container frame that will hold everything
        main_frame = ttk.Frame(self.root)
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        
        # Create canvas with scrollbar
        main_canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=main_canvas.yview)
        
        # Configure canvas
        main_canvas.configure(yscrollcommand=scrollbar.set)
        main_canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Create scrollable frame inside canvas
        scrollable_frame = ttk.Frame(main_canvas)
        
        # Make scrollable_frame expand to fill canvas width
        scrollable_frame.columnconfigure(0, weight=1)
        
        # Create window inside canvas with scrollable_frame
        canvas_window = main_canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        
        # Configure scrolling behavior
        def configure_canvas(event):
            # Update the scrollregion to encompass the scrollable frame
            main_canvas.configure(scrollregion=main_canvas.bbox("all"))
            
            # Make the scrollable frame expand to fill canvas width
            canvas_width = event.width
            main_canvas.itemconfig(canvas_window, width=canvas_width)
        
        scrollable_frame.bind("<Configure>", configure_canvas)
        main_canvas.bind("<Configure>", lambda e: main_canvas.itemconfig(canvas_window, width=e.width))
        
        # Title
        title_frame = ttk.Frame(scrollable_frame)
        title_frame.pack(fill=tk.X, padx=20, pady=20)
        
        title_label = ttk.Label(title_frame, text="🚀 ODK Central Dashboard Reporter",
                               foreground="#810303",
                               font=("Helvetica", 18, "bold"))
        title_label.pack()
     
        # Version and date info
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        info_label = ttk.Label(title_frame, text=f"Version 2.2.1 | {CURRENT_DATETIME} UTC | User: {CURRENT_USER}", 
                              font=("Helvetica", 8), foreground="darkblue")
        info_label.pack(pady=(5, 0))
                
        # ODK Central Settings
        odk_frame = ttk.LabelFrame(scrollable_frame, text="🔐 ODK Central Connection", padding="15")
        odk_frame.pack(fill=tk.X, padx=20, pady=10)
        
        # Server URL
        ttk.Label(odk_frame, text="Server URL:").grid(row=0, column=0, sticky=tk.W, pady=5)
        url_entry = ttk.Entry(odk_frame, textvariable=self.base_url, width=60)
        url_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        # Username
        ttk.Label(odk_frame, text="Username:").grid(row=1, column=0, sticky=tk.W, pady=5)
        user_entry = ttk.Entry(odk_frame, textvariable=self.username, width=60)
        user_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        # Password
        ttk.Label(odk_frame, text="Password:").grid(row=2, column=0, sticky=tk.W, pady=5)
        pass_entry = ttk.Entry(odk_frame, textvariable=self.password, show="*", width=60)
        pass_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        # Add Remember Password checkbox
        self.remember_password = tk.BooleanVar()
        remember_checkbox = ttk.Checkbutton(odk_frame, text="Remember Password", variable=self.remember_password)
        remember_checkbox.grid(row=2, column=2, sticky=tk.W, padx=(10, 0))
        
        # Project ID
        ttk.Label(odk_frame, text="Project ID:").grid(row=3, column=0, sticky=tk.W, pady=5)
        project_entry = ttk.Entry(odk_frame, textvariable=self.project_id, width=60)
        project_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        # Test connection button
        test_btn = ttk.Button(odk_frame, text="🔍 Test Connection", command=self.test_connection)
        test_btn.config(style='Accent.TButton')
        self.style.configure('Accent.TButton',
                              background="#1D15BE",
                              foreground='white',
                              font=('Helvetica', 10, 'bold'))
        self.style.map('Accent.TButton',
                       background=[('active', '#1F5F7A'),
                                   ('pressed', "#7F0909")])

        test_btn.grid(row=4, column=1, sticky=tk.W, pady=10, padx=(10, 0))        
        odk_frame.columnconfigure(1, weight=1)
        
        # Form Selection
        form_frame = ttk.LabelFrame(scrollable_frame, text="📋 Form Selection", padding="15")
        form_frame.pack(fill=tk.X, padx=20, pady=10)
        
        # Configure style for colored text in the label frame
        self.style.configure("ColoredLabel.TLabel", foreground="darkblue")
        
        ttk.Label(form_frame, text="Form ID:").grid(row=0, column=0, sticky=tk.W, pady=5)
        form_entry = ttk.Entry(form_frame, textvariable=self.form_id, width=60)
        form_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        # List forms button
        list_forms_btn = ttk.Button(form_frame, text="📄 List Available Forms", command=self.list_forms)
        list_forms_btn.config(style='Accent.TButton')
        self.style.configure('Accent.TButton',
                              background="#1D15BE",
                              foreground='white',
                              font=('Helvetica', 10, 'bold'))
        self.style.map('Accent.TButton',
                       background=[('active', '#1F5F7A'),
                                   ('pressed', '#0D4F73')])
        list_forms_btn.grid(row=1, column=1, sticky=tk.W, pady=5, padx=(10, 0))
        
        form_frame.columnconfigure(1, weight=1)

        # Header Image Settings
        image_frame = ttk.LabelFrame(scrollable_frame, text="🖼️ Header Image", padding="15")
        image_frame.pack(fill=tk.X, padx=20, pady=10)
        
        ttk.Label(image_frame, text="Header Image:").grid(row=0, column=0, sticky=tk.W, pady=5)
        image_entry = ttk.Entry(image_frame, textvariable=self.header_image_path, width=50)
        image_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 5))
        
        image_browse_btn = ttk.Button(image_frame, text="Browse...", command=self.browse_header_image)
        image_browse_btn.config(style='Accent.TButton')
        self.style.configure('Accent.TButton',
                              background="#1D15BE",
                              foreground='white',
                              font=('Helvetica', 10, 'bold'))
        self.style.map('Accent.TButton',
                       background=[('active', '#1F5F7A'),
                                   ('pressed', '#0D4F73')])
        image_browse_btn.grid(row=0, column=2, pady=5)
        
        # Image quality info
        quality_info = ttk.Label(image_frame, 
                               text="✅ Image Handling",
                               font=("Helvetica", 8), foreground="darkblue")
        quality_info.grid(row=1, column=0, columnspan=3, sticky=tk.W, pady=5)
        
        # Image preview and info frame
        self.image_preview_frame = ttk.Frame(image_frame)
        self.image_preview_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
        
        # Image controls
        controls_frame = ttk.Frame(image_frame)
        controls_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        clear_image_btn = ttk.Button(controls_frame, text="Clear Image", command=self.clear_header_image)
        clear_image_btn.config(style='Accent.TButton')
        self.style.configure('Accent.TButton',
                              background="#1D15BE",
                              foreground='white',
                              font=('Helvetica', 10, 'bold'))
        self.style.map('Accent.TButton',
                       background=[('active', '#1F5F7A'),
                                   ('pressed', '#0D4F73')])
        clear_image_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        
        
        self.image_info_label = ttk.Label(controls_frame, text="", font=("Helvetica", 8))
        self.image_info_label.pack(side=tk.LEFT)
        
        image_frame.columnconfigure(1, weight=1)
        
        # Report Settings
        report_frame = ttk.LabelFrame(scrollable_frame, text="📊 Report Settings", padding="15")
        report_frame.pack(fill=tk.X, padx=20, pady=10)
        
        ttk.Label(report_frame, text="Report Title:").grid(row=0, column=0, sticky=tk.W, pady=5)
        title_entry = ttk.Entry(report_frame, textvariable=self.report_title, width=60)
        title_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        report_frame.columnconfigure(1, weight=1)
        
        # Action Buttons
        action_frame = ttk.Frame(scrollable_frame)
        action_frame.pack(fill=tk.X, padx=20, pady=20)
        
        # Generate button
        generate_btn = ttk.Button(action_frame, text="🚀 Generate Report", 
                                 command=self.generate_dashboard)
        generate_btn.pack(side=tk.LEFT, padx=(0, 10))
        generate_btn.config(style='Accent.TButton')
        
        # Configure a colored style for the generate button
        self.style.configure('Accent.TButton', 
                   background='#2E86AB',
                   foreground='white',
                   font=('Helvetica', 10, 'bold'))
        self.style.map('Accent.TButton',
                  background=[('active', "#750505"),
                    ('pressed', "#780D0D")])
        
        # Generate HTML report button (new)
        html_btn = ttk.Button(action_frame, text="🌐 Generate HTML Report", 
                             command=self.generate_html_report)
        html_btn.pack(side=tk.LEFT, padx=(0, 10))
        html_btn.config(style='Accent.TButton')
        
        # Save settings button
        save_btn = ttk.Button(action_frame, text="💾 Save Settings", command=self.save_settings)
        save_btn.pack(side=tk.LEFT, padx=(0, 10))
        save_btn.config(style='Accent.TButton')

        self.style.map('Accent.TButton',
                  background=[('active', "#890E0E"),
                    ('pressed', "#800A0A")])
        # Load settings button
        load_btn = ttk.Button(action_frame, text="📁 Load Settings", command=self.load_settings)
        load_btn.pack(side=tk.LEFT)
        load_btn.config(style='Accent.TButton')

        self.style.map('Accent.TButton',
                  background=[('active', "#A00606"),
                    ('pressed', "#150BC9")])
        # Progress bar
        self.progress = ttk.Progressbar(scrollable_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, padx=20, pady=10)
        
        # Output Text Area
        output_frame = ttk.LabelFrame(scrollable_frame, text="📄 Output Log", padding="15")
        output_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # Text widget with scrollbar
        text_frame = ttk.Frame(output_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        self.output_text = tk.Text(text_frame, height=10, wrap=tk.WORD, font=("Consolas", 9))
        text_scrollbar = ttk.Scrollbar(text_frame, orient="vertical", command=self.output_text.yview)
        self.output_text.configure(yscrollcommand=text_scrollbar.set)
        
        self.output_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        text_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
                # Add a new frame for variable visualization
        visual_frame = ttk.LabelFrame(self.root, text="Custom Visualization")
        visual_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Create a frame for variable selection and chart type
        var_select_frame = ttk.Frame(visual_frame)
        var_select_frame.pack(fill="x", padx=10, pady=5)
        
        # Variable Selection Dropdown
        ttk.Label(var_select_frame, text="Select Variable:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.variable_selection = ttk.Combobox(var_select_frame, state="readonly", width=50)
        self.variable_selection.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        # Chart Type Selection Dropdown
        ttk.Label(var_select_frame, text="Chart Type:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.chart_type = ttk.Combobox(var_select_frame, state="readonly", width=30, 
                                    values=["Horizontal Bar Chart", "Vertical Bar Chart", "Pie Chart", 
                                            "Line Chart", "Area Chart", "Count Plot"])
        self.chart_type.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        self.chart_type.current(0)  # Default to Horizontal Bar Chart
        
        # Add Chart Button
        add_chart_btn = ttk.Button(var_select_frame, text="Add Chart to Report", 
                                command=self.add_chart_to_report)
        add_chart_btn.grid(row=2, column=0, columnspan=2, pady=10)
        
        # Preview Frame for Chart
        self.chart_preview_frame = ttk.Frame(visual_frame)
        self.chart_preview_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Label for chart preview
        self.preview_label = ttk.Label(self.chart_preview_frame, text="Chart preview will appear here")
        self.preview_label.pack(pady=20)

        def populate_variable_dropdown(self):
            """Populate the variable dropdown with column names from the loaded data."""
            try:
                if hasattr(self, 'analytics') and hasattr(self.analytics, 'data') and not self.analytics.data.empty:
                    # Get column names, exclude system columns
                    columns = [col for col in self.analytics.data.columns 
                            if not col.startswith('_') and col.lower() not in 
                            ['submissiondate', 'instanceid', 'deviceid', 'submission_date']]
                    
                    # Update the dropdown with columns
                    self.variable_selection['values'] = columns
                    
                    if columns:
                        self.variable_selection.current(0)
                        self.log_output(f"✅ Loaded {len(columns)} variables for visualization", "INFO")
                    else:
                        self.log_output("❓ No suitable variables found for visualization", "WARNING")
                else:
                    self.log_output("❗ Load data first to populate variables", "WARNING")
                    self.variable_selection['values'] = []
            except Exception as e:
                self.log_output(f"❌ Error loading variables: {str(e)}", "ERROR")


        def add_chart_to_report(self):
            """Create a chart based on selected variable and chart type and add to report."""
        try:
            # Check if data is loaded
            if not hasattr(self, 'analytics') or not hasattr(self.analytics, 'data') or self.analytics.data.empty:
                self.log_output("❌ No data loaded. Please load data first.", "ERROR")
                return
            
            # Get selections
            selected_var = self.variable_selection.get()
            chart_type = self.chart_type.get()
            
            if not selected_var:
                self.log_output("❌ Please select a variable first", "ERROR")
                return
            
            # Create chart and add to report class
            self.log_output(f"🔍 Analyzing variable: {selected_var}", "INFO")
            
            # Create chart preview
            self.create_chart_preview(selected_var, chart_type)
            
            # Store the chart information for report generation
            if not hasattr(self.analytics, 'custom_charts'):
                self.analytics.custom_charts = []
            
            self.analytics.custom_charts.append({
                'variable': selected_var,
                'chart_type': chart_type
            })
            
            self.log_output(f"✅ Added {chart_type} for '{selected_var}' to report", "SUCCESS")
            
        except Exception as e:
            self.log_output(f"❌ Error adding chart: {str(e)}", "ERROR")

        def create_chart_preview(self, variable, chart_type):
            """Create a preview of the chart."""
            try:
                # Clear previous chart
                for widget in self.chart_preview_frame.winfo_children():
                    widget.destroy()
                
                # Get data for the variable
                data = self.analytics.data
                
                # Check if variable exists in data
                if variable not in data.columns:
                    ttk.Label(self.chart_preview_frame, text=f"Error: Variable '{variable}' not found in data").pack(pady=20)
                    return
                
                # Create figure
                fig = plt.Figure(figsize=(8, 4), dpi=100)
                ax = fig.add_subplot(111)
                
                # Different chart types
                if chart_type == "Horizontal Bar Chart":
                    self._create_horizontal_bar_chart(ax, data, variable)
                elif chart_type == "Vertical Bar Chart":
                    self._create_vertical_bar_chart(ax, data, variable)
                elif chart_type == "Pie Chart":
                    self._create_pie_chart(ax, data, variable)
                elif chart_type == "Line Chart":
                    self._create_line_chart(ax, data, variable)
                elif chart_type == "Area Chart":
                    self._create_area_chart(ax, data, variable)
                elif chart_type == "Count Plot":
                    self._create_count_plot(ax, data, variable)
                
                # Set title
                ax.set_title(f"{chart_type} for {variable}", fontsize=12)
                
                # Adjust layout
                fig.tight_layout()
                
                # Embed in tkinter
                canvas = FigureCanvasTkAgg(fig, self.chart_preview_frame)
                canvas.draw()
                canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
                
                # Add toolbar
                toolbar_frame = ttk.Frame(self.chart_preview_frame)
                toolbar_frame.pack(fill=tk.X)
                toolbar = NavigationToolbar2Tk(canvas, toolbar_frame)
                toolbar.update()
                
            except Exception as e:
                for widget in self.chart_preview_frame.winfo_children():
                    widget.destroy()
                ttk.Label(self.chart_preview_frame, text=f"Error creating chart: {str(e)}").pack(pady=20)
                logging.error(f"Error creating chart preview: {e}")
        ####################################################################
            def _create_horizontal_bar_chart(self, ax, data, variable):
                """Create horizontal bar chart."""
            # Count values for categorical data or bin for numeric
            if data[variable].dtype.kind in 'ifc':  # integer, float, complex
                # Numeric data: create histogram
                counts, bins = np.histogram(data[variable].dropna(), bins=min(10, len(data[variable].unique())))
                bin_labels = [f"{bins[i]:.1f} - {bins[i+1]:.1f}" for i in range(len(bins)-1)]
                y_pos = np.arange(len(bin_labels))
                ax.barh(y_pos, counts, align='center', color='skyblue')
                ax.set_yticks(y_pos)
                ax.set_yticklabels(bin_labels)
            else:
                # Categorical data: value counts
                value_counts = data[variable].value_counts().sort_values()
                # Limit to top 15 categories if too many
                if len(value_counts) > 15:
                    value_counts = value_counts.tail(15)
                    ax.set_title(f"Top 15 values for {variable}")
                value_counts.plot.barh(ax=ax, color='skyblue')
            
            ax.set_xlabel("Count")
            ax.set_ylabel(variable)
            
        def _create_vertical_bar_chart(self, ax, data, variable):
            """Create vertical bar chart."""
            # Count values for categorical data or bin for numeric
            if data[variable].dtype.kind in 'ifc':  # integer, float, complex
                # Numeric data: create histogram
                counts, bins = np.histogram(data[variable].dropna(), bins=min(10, len(data[variable].unique())))
                bin_labels = [f"{bins[i]:.1f} - {bins[i+1]:.1f}" for i in range(len(bins)-1)]
                x_pos = np.arange(len(bin_labels))
                ax.bar(x_pos, counts, align='center', color='skyblue')
                ax.set_xticks(x_pos)
                ax.set_xticklabels(bin_labels, rotation=45, ha='right')
            else:
                # Categorical data: value counts
                value_counts = data[variable].value_counts()
                # Limit to top 15 categories if too many
                if len(value_counts) > 15:
                    value_counts = value_counts.head(15)
                    ax.set_title(f"Top 15 values for {variable}")
                value_counts.plot.bar(ax=ax, color='skyblue')
                plt.xticks(rotation=45, ha='right')
            
            ax.set_ylabel("Count")
            ax.set_xlabel(variable)

        def _create_pie_chart(self, ax, data, variable):
            """Create pie chart."""
            # Get value counts
            value_counts = data[variable].value_counts()
            
            # Limit to top 8 categories + "Others" for readability
            if len(value_counts) > 8:
                top_values = value_counts.head(7)
                others_count = value_counts[7:].sum()
                top_values['Others'] = others_count
                value_counts = top_values
            
            # Plot pie chart
            value_counts.plot.pie(ax=ax, autopct='%1.1f%%', shadow=False, startangle=90)
            ax.set_ylabel('')  # Remove y-label
            ax.set_title(f"Distribution of {variable}")

        def _create_line_chart(self, ax, data, variable):
            """Create line chart (for time-based or ordered data)."""
            if data[variable].dtype.kind in 'ifc':  # integer, float, complex
                # Create a line chart of the distribution (density)
                try:
                    import scipy.stats as stats
                    kde = stats.gaussian_kde(data[variable].dropna())
                    x_range = np.linspace(data[variable].min(), data[variable].max(), 100)
                    density = kde(x_range)
                    ax.plot(x_range, density, 'b-')
                    ax.fill_between(x_range, density, alpha=0.3)
                    ax.set_xlabel(variable)
                    ax.set_ylabel('Density')
                    ax.set_title(f"Distribution Density of {variable}")
                except Exception:
                    # Fallback if KDE doesn't work
                    data[variable].plot.line(ax=ax, color='blue')
            else:
                # For categorical, show a trend of counts
                value_counts = data[variable].value_counts().sort_index()
                value_counts.plot.line(ax=ax, marker='o')
                ax.set_xlabel(variable)
                ax.set_ylabel('Count')
                plt.xticks(rotation=45, ha='right')

        def _create_area_chart(self, ax, data, variable):
            """Create area chart."""
            if data[variable].dtype.kind in 'ifc':  # integer, float, complex
                # Create bins and count
                counts, bins = np.histogram(data[variable].dropna(), bins=min(15, len(data[variable].unique())))
                bin_centers = [(bins[i] + bins[i+1])/2 for i in range(len(bins)-1)]
                ax.fill_between(bin_centers, counts, alpha=0.7, color='skyblue')
                ax.plot(bin_centers, counts, 'b-', alpha=0.7)
                ax.set_xlabel(variable)
                ax.set_ylabel('Count')
            else:
                # For categorical, show counts as area
                value_counts = data[variable].value_counts().sort_index()
                value_counts.plot.area(ax=ax, alpha=0.7, color='skyblue')
                ax.set_xlabel(variable)
                ax.set_ylabel('Count')
                plt.xticks(rotation=45, ha='right')

        def _create_count_plot(self, ax, data, variable):
            """Create count plot with percentages."""
            # Get value counts and percentages
            value_counts = data[variable].value_counts()
            total = len(data[variable].dropna())
            
            # Limit to top 10 categories if too many
            if len(value_counts) > 10:
                value_counts = value_counts.head(10)
                ax.set_title(f"Top 10 values for {variable}")
            
            # Create bar plot
            bars = ax.bar(range(len(value_counts)), value_counts.values, align='center')
            ax.set_xticks(range(len(value_counts)))
            ax.set_xticklabels(value_counts.index, rotation=45, ha='right')
            
            # Add percentage labels on bars
            for i, bar in enumerate(bars):
                percentage = (value_counts.values[i] / total) * 100
                ax.text(i, bar.get_height() + 0.5, f"{percentage:.1f}%", 
                    ha='center', va='bottom', fontweight='bold')
            
            ax.set_xlabel(variable)
            ax.set_ylabel('Count')
        ####################################################################
        #     
        ####################################################################
        # Enhanced mousewheel scrolling for better user experience
        def _on_mousewheel(event):
            # Scroll direction and speed calibration
            scroll_speed = 1
            if event.delta:
                # For Windows and MacOS
                main_canvas.yview_scroll(int(-1 * (event.delta / 120) * scroll_speed), "units")
            elif event.num == 4:
                # For Linux - scroll up
                main_canvas.yview_scroll(-1 * scroll_speed, "units")
            elif event.num == 5:
                # For Linux - scroll down
                main_canvas.yview_scroll(scroll_speed, "units")
                
        # Bind mousewheel for Windows and MacOS
        main_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        # Additional bindings for Linux
        main_canvas.bind_all("<Button-4>", _on_mousewheel)
        main_canvas.bind_all("<Button-5>", _on_mousewheel)
        
        # Load saved settings if available
        self.load_saved_settings()
    
    def browse_header_image(self):
        """Browse for high-quality header image file."""
        filename = filedialog.askopenfilename(
            title="Select High-Quality Header Image",
            filetypes=[
                ("Preferred formats", "*.png *.jpg *.jpeg"),
                ("PNG files", "*.png"),
                ("JPEG files", "*.jpg *.jpeg"),
                ("All image files", "*.png *.jpg *.jpeg *.gif *.bmp *.tiff"),
                ("All files", "*.*")
            ]
        )
        
        if filename:
            self.header_image_path.set(filename)
            self.update_image_preview()
            
            # Get and display image info
            img_info = HighQualityImageProcessor.get_image_info(filename)
            if img_info:
                format_info = img_info.get('format', 'Unknown')
                size_info = f"{img_info.get('width', 0)}x{img_info.get('height', 0)}"
                dpi_info = img_info.get('dpi', (72, 72))[0]
                file_size = img_info.get('file_size', 0) / 1024  # KB
                is_preferred = img_info.get('is_preferred_format', False)
                
                status = "✅ Preferred format" if is_preferred else "⚠️ Will be optimized"
                
                self.log_output(f"Selected: {os.path.basename(filename)}")
                self.log_output(f"Format: {format_info}, Size: {size_info}, DPI: {dpi_info}, {file_size:.1f}KB - {status}")
    
    def clear_header_image(self):
        """Clear the selected header image."""
        self.header_image_path.set("")
        self.clear_image_preview()
        self.image_info_label.config(text="")
        self.log_output("Header image cleared")
    
    def update_image_preview(self):
        """Update the high-quality image preview."""
        try:
            image_path = self.header_image_path.get()
            if not image_path or not os.path.exists(image_path):
                self.clear_image_preview()
                return
            
            # Clear existing preview
            for widget in self.image_preview_frame.winfo_children():
                widget.destroy()
            
            # Get image info
            img_info = HighQualityImageProcessor.get_image_info(image_path)
            
            # Create high-quality preview
            preview_img = HighQualityImageProcessor.create_preview_image(image_path, max_size=(300, 150))
            
            if preview_img:
                # Convert to PhotoImage
                photo = ImageTk.PhotoImage(preview_img)
                
                # Create label with image
                preview_label = ttk.Label(self.image_preview_frame, image=photo)
                preview_label.image = photo  # Keep a reference
                preview_label.pack(pady=5)
                
                    # Create detailed info
                if img_info:
                        format_name = img_info.get('format', 'Unknown')
                        width = img_info.get('width', 0)
                        height = img_info.get('height', 0)
                        dpi = img_info.get('dpi', (72, 72))[0]
                        file_size = img_info.get('file_size', 0) / 1024  # KB
                        has_transparency = img_info.get('has_transparency', False)
                        is_preferred = img_info.get('is_preferred_format', False)
                        
                        quality_indicator = "🟢 Excellent" if is_preferred and dpi >= 150 else "🟡 Good" if is_preferred else "🟠 Will optimize"
                        transparency_info = " (with transparency)" if has_transparency else ""
                        
                        info_text = (f"📏 {width}×{height}px | 🎯 {dpi} DPI | 📁 {file_size:.1f}KB\n"
                                   f"📸 {format_name}{transparency_info} | {quality_indicator}")
                        
                        info_label = ttk.Label(self.image_preview_frame, text=info_text, 
                                             font=("Helvetica", 9), justify=tk.CENTER)
                        info_label.pack(pady=5)
                        
                        # Update the info label
                        self.image_info_label.config(text=f"{os.path.basename(image_path)} - {quality_indicator}")
                
        except Exception as e:
            self.log_output(f"Error loading image preview: {e}", "ERROR")
            self.clear_image_preview()
    
    def clear_image_preview(self):
        """Clear the image preview."""
        for widget in self.image_preview_frame.winfo_children():
            widget.destroy()

        no_image_label = ttk.Label(self.image_preview_frame, text="No image selected\n💡 Image\n✅ Handling")
        no_image_label.pack(pady=20)
        
    def log_output(self, message, level="INFO"):
        """Add message to output text area."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {level}: {message}\n"
        
        self.output_text.insert(tk.END, formatted_message)
        self.output_text.see(tk.END)
        self.root.update()
        
    def validate_inputs(self, check_form=False):
        """Validate user inputs."""
        if not all([self.base_url.get(), self.username.get(), self.password.get(), self.project_id.get()]):
            messagebox.showerror("Error", "Please fill in all ODK Central connection fields")
            return False
        
        try:
            int(self.project_id.get())
        except ValueError:
            messagebox.showerror("Error", "Project ID must be a number")
            return False
        
        if check_form and not self.form_id.get():
            messagebox.showerror("Error", "Please enter a Form ID")
            return False
        
        # Validate header image if provided
        header_image = self.header_image_path.get()
        if header_image and not HighQualityImageProcessor.validate_image(header_image):
            messagebox.showerror("Error", "Selected header image is not valid or does not exist")
            return False
        
        return True
    
    def test_connection(self):
        """Test connection to ODK Central."""
        if not self.validate_inputs():
            return
        
        def run_test():
            try:
                self.progress.start()
                self.log_output("Testing connection to ODK Central...")
                
                client = ODKCentralClient(
                    base_url=self.base_url.get(),
                    username=self.username.get(),
                    password=self.password.get(),
                    project_id=int(self.project_id.get())
                )
                
                if client.authenticate():
                    projects = client.get_projects()
                    self.log_output(f"✅ Connection successful! Found {len(projects)} projects.", "SUCCESS")
                    
                    # Find current project
                    current_project = next((p for p in projects if p.get('id') == int(self.project_id.get())), None)
                    if current_project:
                        self.log_output(f"📁 Current project: {current_project.get('name', 'Unknown')}", "INFO")
                    else:
                        self.log_output("⚠️ Warning: Specified project ID not found in accessible projects", "WARNING")
                else:
                    self.log_output("❌ Authentication failed. Please check your credentials.", "ERROR")
                    
            except Exception as e:
                self.log_output(f"❌ Connection failed: {str(e)}", "ERROR")
            finally:
                self.progress.stop()
        
        thread = threading.Thread(target=run_test)
        thread.daemon = True
        thread.start()
    
    def list_forms(self):
        """List available forms in the project."""
        if not self.validate_inputs():
            return
        
        def run_list():
            try:
                self.progress.start()
                self.log_output("Fetching available forms...")
                
                client = ODKCentralClient(
                    base_url=self.base_url.get(),
                    username=self.username.get(),
                    password=self.password.get(),
                    project_id=int(self.project_id.get())
                )
                
                if client.authenticate():
                    forms = client.get_forms()
                    if forms:
                        self.log_output(f"📋 Found {len(forms)} forms:", "SUCCESS")
                        for form in forms:
                            form_id = form.get('xmlFormId', 'N/A')
                            form_name = form.get('name', 'Unknown')
                            submissions = form.get('submissions', 0)
                            self.log_output(f"  • {form_id} - {form_name} ({submissions} submissions)")
                    else:
                        self.log_output("No forms found in this project.", "WARNING")
                else:
                    self.log_output("❌ Authentication failed.", "ERROR")
                    
            except Exception as e:
                self.log_output(f"❌ Error fetching forms: {str(e)}", "ERROR")
            finally:
                self.progress.stop()
        
        thread = threading.Thread(target=run_list)
        thread.daemon = True
        thread.start()
    
    def generate_dashboard(self):
        """Generate fixed high-quality dashboard report."""
        if not self.validate_inputs(check_form=True):
            return
        
        # Check dependencies
        missing_deps = []
        if not HAS_REPORTLAB:
            missing_deps.append("reportlab")
        if not HAS_MATPLOTLIB:
            missing_deps.append("matplotlib")
        
        if missing_deps:
            messagebox.showerror("Missing Dependencies", 
                               f"Required packages not installed: {', '.join(missing_deps)}\n\n"
                               f"Install with: pip install {' '.join(missing_deps)}")
            return

        def run_generation(self):
            try:
                self.progress.start()
                self.log_output("🚀 Starting dashboard generation...")
                
                # Check header image
                header_image = self.header_image_path.get() if self.header_image_path.get() else None
                if header_image:
                    if HighQualityImageProcessor.validate_image(header_image):
                        img_info = HighQualityImageProcessor.get_image_info(header_image)
                        format_name = img_info.get('format', 'Unknown')
                        is_preferred = img_info.get('is_preferred_format', False)
                        status = "high-quality" if is_preferred else "optimized"
                        self.log_output(f"🖼️ Using {status} header image: {os.path.basename(header_image)} ({format_name})")
                        self.log_output("✅ Fixed file handling - no more temp file errors!")
                    else:
                        self.log_output("⚠️ Warning: Header image invalid, proceeding without it", "WARNING")
                        header_image = None
                #########################################
                self.populate_variable_dropdown()
                #########################################
                # Create client and authenticate
                client = ODKCentralClient(
                    base_url=self.base_url.get(),
                    username=self.username.get(),
                    password=self.password.get(),
                    project_id=int(self.project_id.get())
                )
                
                if not client.authenticate():
                    self.log_output("❌ Authentication failed.", "ERROR")
                    return
                
                self.log_output("✅ Authentication successful")
                
                # Download data
                form_id = self.form_id.get()
                self.log_output(f"📥 Downloading data for form: {form_id}")
                
                data = client.get_submissions(form_id)
                
                if data.empty:
                    self.log_output("❌ No data found for the specified form.", "ERROR")
                    return
                
                self.log_output(f"✅ Downloaded {len(data)} submissions with {len(data.columns)} fields")
                
                # Check for geopoint data
                has_geopoints = False
                for col in data.columns:
                    if 'geopoint' in col.lower() or any(x in col.lower() for x in ['lat', 'lon', 'lng', 'longitude', 'latitude']):
                        has_geopoints = True
                        self.log_output(f"🗺️ Found geographic data in column: {col}")
                        break
                
                # Get form info
                forms = client.get_forms()
                form_info = next((f for f in forms if f.get('xmlFormId') == form_id), {})
                
                # Create analytics
                self.log_output("📊 Analyzing data...")
                analytics = DashboardAnalytics(data, form_info)
                
                # Generate output path
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                reports_dir = Path("./reports")
                reports_dir.mkdir(exist_ok=True)
                
                # Clean filename
                safe_form_id = re.sub(r'[^\w\-_.]', '_', form_id)
                output_path = reports_dir / f"dashboard_fixed_{safe_form_id}_{timestamp}.pdf"

                # Generate high-quality report
                self.log_output(f"📄 Generating PDF report: {output_path}")
                reporter = FixedHighQualityDashboardPDFReporter(analytics, header_image)
                
                if reporter.generate_dashboard_report(str(output_path), self.report_title.get()):
                    self.log_output(f"🎉 Dashboard report generated successfully!", "SUCCESS")
                    self.log_output(f"📍 File saved: {output_path.absolute()}", "SUCCESS")
                    self.log_output("✅ No more temporary file errors with header images", "SUCCESS")
                    self.log_output("✨ Report features stable high-resolution images and optimized quality", "SUCCESS")
                    
                    if header_image:
                        self.log_output("🖼️ High-quality header image included (300 DPI)", "SUCCESS")
                    
                    if has_geopoints and HAS_FOLIUM:
                        self.log_output("🗺️ Geographic data visualization included in report", "SUCCESS")
                        
                        # Generate an HTML report with interactive maps if geopoints are found
                        html_path = reports_dir / f"dashboard_fixed_{safe_form_id}_{timestamp}.html"
                        if reporter.generate_html_report(str(html_path), self.report_title.get()):
                            self.log_output(f"🌐 Interactive HTML report also generated: {html_path.name}", "SUCCESS")
                    
                    # Ask if user wants to open the file
                    if messagebox.askyesno("Success", f"Dashboard report generated successfully!\n\nFile: {output_path.name}\n\nWould you like to open the report?"):
                        import subprocess
                        import platform
                        
                        try:
                            if platform.system() == 'Windows':
                                os.startfile(str(output_path))
                            elif platform.system() == 'Darwin':  # macOS
                                subprocess.run(['open', str(output_path)])
                            else:  # Linux
                                subprocess.run(['xdg-open', str(output_path)])
                        except Exception as e:
                            self.log_output(f"Could not open file automatically: {e}", "WARNING")
                            
                else:
                    self.log_output("❌ Failed to generate dashboard report.", "ERROR")
                    
            except Exception as e:
                self.log_output(f"❌ Error generating report: {str(e)}", "ERROR")
                import traceback
                self.log_output(f"Full error: {traceback.format_exc()}", "ERROR")
            finally:
                self.progress.stop()
        
        thread = threading.Thread(target=run_generation)
        thread.daemon = True
        thread.start()
    
    def generate_html_report(self):
        """Generate HTML report with interactive maps."""
        if not self.validate_inputs(check_form=True):
            return
        
        # Check dependencies
        missing_deps = []
        if not HAS_FOLIUM:
            missing_deps.append("folium")
        
        if missing_deps:
            messagebox.showerror("Missing Dependencies", 
                               f"Required packages not installed: {', '.join(missing_deps)}\n\n"
                               f"Install with: pip install {' '.join(missing_deps)}")
            return
        
        def run_generation(self):
            try:
                self.progress.start()
                self.log_output("🌐 Starting HTML report generation...")
                
                # Create client and authenticate
                client = ODKCentralClient(
                    base_url=self.base_url.get(),
                    username=self.username.get(),
                    password=self.password.get(),
                    project_id=int(self.project_id.get())
                )
                
                if not client.authenticate():
                    self.log_output("❌ Authentication failed.", "ERROR")
                    return
                
                self.log_output("✅ Authentication successful")
                
                # Download data
                form_id = self.form_id.get()
                self.log_output(f"📥 Downloading data for form: {form_id}")
                
                data = client.get_submissions(form_id)
                
                if data.empty:
                    self.log_output("❌ No data found for the specified form.", "ERROR")
                    return
                
                self.log_output(f"✅ Downloaded {len(data)} submissions with {len(data.columns)} fields")
                
                # Get form info
                forms = client.get_forms()
                form_info = next((f for f in forms if f.get('xmlFormId') == form_id), {})
                
                # Create analytics
                self.log_output("📊 Analyzing data...")
                analytics = DashboardAnalytics(data, form_info)
                
                # Generate output path
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                reports_dir = Path("./reports")
                reports_dir.mkdir(exist_ok=True)
                
                # Clean filename
                safe_form_id = re.sub(r'[^\w\-_.]', '_', form_id)
                output_path = reports_dir / f"dashboard_fixed_{safe_form_id}_{timestamp}.html"
                
                # Generate HTML report
                self.log_output(f"🌐 Generating HTML report: {output_path}")
                reporter = FixedHighQualityDashboardPDFReporter(analytics, self.header_image_path.get())
                
                if reporter.generate_html_report(str(output_path), self.report_title.get()):
                    self.log_output(f"🎉 HTML report generated successfully!", "SUCCESS")
                    self.log_output(f"📍 File saved: {output_path.absolute()}", "SUCCESS")
                    
                    # Check for geopoint data
                    has_geopoints = False
                    for col in data.columns:
                        if 'geopoint' in col.lower() or any(x in col.lower() for x in ['lat', 'lon', 'lng', 'longitude', 'latitude']):
                            has_geopoints = True
                            break
                    
                    if has_geopoints:
                        self.log_output("🗺️ Interactive map included with geopoint data", "SUCCESS")
                    
                    # Ask if user wants to open the file
                    if messagebox.askyesno("Success", f"HTML report generated successfully!\n\nFile: {output_path.name}\n\nWould you like to open the report?"):
                        import subprocess
                        import platform
                        
                        try:
                            if platform.system() == 'Windows':
                                os.startfile(str(output_path))
                            elif platform.system() == 'Darwin':  # macOS
                                subprocess.run(['open', str(output_path)])
                            else:  # Linux
                                subprocess.run(['xdg-open', str(output_path)])
                        except Exception as e:
                            self.log_output(f"Could not open file automatically: {e}", "WARNING")
                            
                else:
                    self.log_output("❌ Failed to generate HTML report.", "ERROR")
                    
            except Exception as e:
                self.log_output(f"❌ Error generating HTML report: {str(e)}", "ERROR")
                import traceback
                self.log_output(f"Full error: {traceback.format_exc()}", "ERROR")
            finally:
                self.progress.stop()
        
        thread = threading.Thread(target=run_generation)
        thread.daemon = True
        thread.start()
    
    def save_settings(self):
        """Save current settings to file."""
        try:
            # Encrypt password if remember password is checked
            password_to_save = ""
            if hasattr(self, 'remember_password') and self.remember_password.get():
                try:
                    # Simple encryption - not highly secure but better than plaintext
                    import base64
                    password_to_save = base64.b64encode(self.password.get().encode()).decode()
                except Exception:
                    # If encryption fails, don't save password
                    password_to_save = ""
            
            settings = {
                'base_url': self.base_url.get(),
                'username': self.username.get(),
                'password': password_to_save,
                'remember_password': bool(self.remember_password.get()) if hasattr(self, 'remember_password') else False,
                'project_id': self.project_id.get(),
                'form_id': self.form_id.get(),
                'report_title': self.report_title.get(),
                'header_image_path': self.header_image_path.get()
            }
            
            filename = filedialog.asksaveasfilename(
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
                title="Save Settings"
            )
            
            if filename:
                with open(filename, 'w') as f:
                    json.dump(settings, f, indent=2)
                self.log_output(f"💾 Settings saved to {filename}", "SUCCESS")
                
        except Exception as e:
            self.log_output(f"❌ Error saving settings: {str(e)}", "ERROR")
        
    def load_settings(self):
        """Load settings from file."""
        try:
            filename = filedialog.askopenfilename(
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
                title="Load Settings"
            )
            
            if filename:
                with open(filename, 'r') as f:
                    settings = json.load(f)
                
                self.base_url.set(settings.get('base_url', ''))
                self.username.set(settings.get('username', ''))
                
                # Load password if it was saved
                if 'password' in settings and settings.get('password') and settings.get('remember_password', False):
                    try:
                        # Simple decryption
                        import base64
                        decoded_password = base64.b64decode(settings.get('password').encode()).decode()
                        self.password.set(decoded_password)
                    except Exception:
                        # If decryption fails, don't set password
                        pass
                
                # Set remember password checkbox
                self.remember_password.set(settings.get('remember_password', False))
                
                self.project_id.set(settings.get('project_id', ''))
                self.form_id.set(settings.get('form_id', ''))
                self.report_title.set(settings.get('report_title', 'ODK Central Dashboard Report'))
                self.header_image_path.set(settings.get('header_image_path', ''))
                
                # Update image preview if image path is loaded
                if self.header_image_path.get():
                    self.update_image_preview()
                else:
                    self.clear_image_preview()
                
                self.log_output(f"📁 Settings loaded from {filename}", "SUCCESS")
                
        except Exception as e:
            self.log_output(f"❌ Error loading settings: {str(e)}", "ERROR")
    
    def load_saved_settings(self):
        """Load automatically saved settings."""
        try:
            settings_file = Path.home() / '.odk_dashboard_fixed_settings.json'
            if settings_file.exists():
                with open(settings_file, 'r') as f:
                    settings = json.load(f)
                
                self.base_url.set(settings.get('base_url', 'https://'))
                self.username.set(settings.get('username', ''))
                self.project_id.set(settings.get('project_id', ''))
                self.form_id.set(settings.get('form_id', ''))
                self.report_title.set(settings.get('report_title', 'ODK Central Dashboard Report'))
                self.header_image_path.set(settings.get('header_image_path', ''))
                
                # Update image preview if path exists
                if self.header_image_path.get():
                    self.update_image_preview()
                else:
                    self.clear_image_preview()
                
        except Exception:
            pass  # Ignore errors loading auto-saved settings
    
    def save_auto_settings(self):
        """Automatically save settings including password if enabled."""
        try:
            # Encrypt password if remember password is checked
            password_to_save = ""
            if hasattr(self, 'remember_password') and self.remember_password.get():
                try:
                    # Simple encryption - not highly secure but better than plaintext
                    import base64
                    password_to_save = base64.b64encode(self.password.get().encode()).decode()
                except Exception:
                    # If encryption fails, don't save password
                    password_to_save = ""
            
            settings = {
                'base_url': self.base_url.get(),
                'username': self.username.get(),
                'password': password_to_save,
                'remember_password': bool(self.remember_password.get()) if hasattr(self, 'remember_password') else False,
                'project_id': self.project_id.get(),
                'form_id': self.form_id.get(),
                'report_title': self.report_title.get(),
                'header_image_path': self.header_image_path.get()
            }
            
            settings_file = Path.home() / '.odk_dashboard_fixed_settings.json'
            with open(settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
                
        except Exception as e:
            logging.error(f"Error saving auto settings: {e}")

    def load_saved_settings(self):
        """Load automatically saved settings including password if enabled."""
        try:
            settings_file = Path.home() / '.odk_dashboard_fixed_settings.json'
            if settings_file.exists():
                with open(settings_file, 'r') as f:
                    settings = json.load(f)
                
                self.base_url.set(settings.get('base_url', 'https://'))
                self.username.set(settings.get('username', ''))
                
                # Load password if it was saved
                if 'password' in settings and settings.get('password') and settings.get('remember_password', False):
                    try:
                        # Simple decryption
                        import base64
                        decoded_password = base64.b64decode(settings.get('password').encode()).decode()
                        self.password.set(decoded_password)
                    except Exception:
                        # If decryption fails, don't set password
                        pass
                
                # Set remember password checkbox
                if hasattr(self, 'remember_password'):
                    self.remember_password.set(settings.get('remember_password', False))
                
                self.project_id.set(settings.get('project_id', ''))
                self.form_id.set(settings.get('form_id', ''))
                self.report_title.set(settings.get('report_title', 'ODK Central Dashboard Report'))
                self.header_image_path.set(settings.get('header_image_path', ''))
                
                # Update image preview if path exists
                if self.header_image_path.get():
                    self.update_image_preview()
                else:
                    self.clear_image_preview()
                
        except Exception as e:
            logging.warning(f"Could not load saved settings: {e}")
    
    def on_closing(self):
        """Handle application closing."""
        self.save_auto_settings()
        # Clean up any remaining temp files
        cleanup_temp_files()
        self.root.destroy()
# ============================================================================
# Dependency Checking and Main Application
# ============================================================================

def check_dependencies():
    """Check and report missing dependencies."""
    missing_deps = []
    optional_deps = []
    
    # Required dependencies
    if not HAS_REPORTLAB:
        missing_deps.append("reportlab")
    
    # Check for Pillow specifically
    try:
        import PIL
        PIL_VERSION = PIL.__version__
    except ImportError:
        missing_deps.append("Pillow")
        PIL_VERSION = "Not installed"
    
    # Optional but recommended dependencies
    if not HAS_MATPLOTLIB:
        optional_deps.append("matplotlib")
    if not HAS_YAML:
        optional_deps.append("pyyaml")
    if not HAS_TQDM:
        optional_deps.append("tqdm")
    if not HAS_FOLIUM:
        optional_deps.append("folium")
    
    return missing_deps, optional_deps, PIL_VERSION

def main():
    """Main application entry point."""
    # Update constants with current values
    global CURRENT_USER, CURRENT_DATETIME
    CURRENT_USER = os.getlogin()
    CURRENT_DATETIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('odk_dashboard_fixed.log'),
            logging.StreamHandler()
        ]
    )
    
    # Check dependencies
    missing_deps, optional_deps, pil_version = check_dependencies()
    
    if missing_deps:
        print("❌ Missing required dependencies:")
        for dep in missing_deps:
            print(f"  - {dep}")
        print(f"\nInstall with: pip install {' '.join(missing_deps)}")
        
        # Try to show GUI error if tkinter is available
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()  # Hide the main window
            messagebox.showerror(
                "Missing Dependencies", 
                f"Required packages not installed:\n{', '.join(missing_deps)}\n\n"
                f"Install with: pip install {' '.join(missing_deps)}"
            )
            root.destroy()
        except ImportError:
            pass  # tkinter not available
        except Exception as e:
            print(f"Could not show GUI error: {e}")
        
        return 1
    
    print(f"📦 Image processing: Pillow {pil_version}")
    
    if optional_deps:
        print("⚠️ Optional dependencies missing (features may be limited):")
        for dep in optional_deps:
            print(f"  - {dep}")
        print(f"Install with: pip install {' '.join(optional_deps)}")
        print()
    
    # Create main application
    try:
        # Import tkinter here to ensure it's available
        import tkinter as tk
        root = tk.Tk()
        
        # Set application icon if available
        try:
            # You can add an icon file here
            # root.iconbitmap("icon.ico")
            pass
        except Exception:
            pass
        
        # Create and run the application
        app = FixedODKDashboardGUI(root)
        
        # Handle window closing
        root.protocol("WM_DELETE_WINDOW", app.on_closing)
        
        # Initialize image preview
        app.clear_image_preview()
        
        logging.info("ODK Central Dashboard Reporter started")
        print("\033[92m🚀 ODK Central Dashboard Reporter\033[0m")
        print("✅ Temporary file handling for header images!")
        print("📊 Generate professional dashboard reports from ODK Central data")
        print(f"📅 Version 2.2.1 | {CURRENT_DATETIME} UTC | User: {CURRENT_USER}")
        print()
        
        # Start the application
        root.mainloop()
        
        logging.info("Application closed")
        return 0
        
    except ImportError as e:
        error_msg = f"Failed to import required GUI components: {e}"
        logging.error(error_msg)
        print(f"❌ {error_msg}")
        print("Make sure tkinter is installed. On Ubuntu/Debian: sudo apt-get install python3-tk")
        return 1
        
    except Exception as e:
        error_msg = f"Failed to start application: {e}"
        logging.error(error_msg)
        print(f"❌ {error_msg}")
        
        try:
            import tkinter as tk_error
            from tkinter import messagebox
            root_error = tk_error.Tk()
            root_error.withdraw()
            messagebox.showerror("Startup Error", error_msg)
            root_error.destroy()
        except Exception:
            pass  # Could not show GUI error
        
        return 1

def cli_mode():
    """Command line interface mode for fixed high-quality headless operation."""
    import argparse
    
    # Update constants with current values
    global CURRENT_USER, CURRENT_DATETIME
    CURRENT_USER = os.getlogin()
    CURRENT_DATETIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    parser = argparse.ArgumentParser(description='ODK Central Dashboard Reporter')
    parser.add_argument('--url', required=True, help='ODK Central base URL')
    parser.add_argument('--username', required=True, help='Username')
    parser.add_argument('--password', required=True, help='Password')
    parser.add_argument('--project-id', type=int, required=True, help='Project ID')
    parser.add_argument('--form-id', required=True, help='Form ID')
    parser.add_argument('--output', help='Output PDF path')
    parser.add_argument('--title', default='ODK Central Dashboard Report', help='Report title')
    parser.add_argument('--header-image', help='Path to header image file (PNG/JPG preferred)')
    parser.add_argument('--image-quality', type=int, default=95, choices=range(70, 101), 
                       help='JPEG quality (70-100, default: 95)')
    parser.add_argument('--image-dpi', type=int, default=300, choices=[150, 200, 300, 600],
                       help='Target DPI for images (default: 300)')
    parser.add_argument('--html', action='store_true', help='Generate HTML report with interactive map')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Check dependencies
    missing_deps, _, pil_version = check_dependencies()
    if missing_deps:
        print(f"❌ Missing required dependencies: {', '.join(missing_deps)}")
        print(f"Install with: pip install {' '.join(missing_deps)}")
        return 1
    
    try:
        print("🚀 Starting ODK Central Dashboard Reporter")
        print(f"✅ High-quality image processing (Pillow {pil_version})")
        print(f"🌐 Connecting to: {args.url}")
        print(f"🎯 Image settings: {args.image_dpi} DPI, Quality: {args.image_quality}%")
        
        # Create client and authenticate
        client = ODKCentralClient(args.url, args.username, args.password, args.project_id)
        
        if not client.authenticate():
            print("❌ Authentication failed")
            return 1
        
        print("✅ Authentication successful")
        
        # Download data
        print(f"📥 Downloading data for form: {args.form_id}")
        data = client.get_submissions(args.form_id)
        
        if data.empty:
            print("❌ No data found for the specified form")
            return 1
        
        print(f"✅ Downloaded {len(data)} submissions with {len(data.columns)} fields")
        
        # Check for geopoint data
        has_geopoints = False
        for col in data.columns:
            if 'geopoint' in col.lower() or any(x in col.lower() for x in ['lat', 'lon', 'lng', 'longitude', 'latitude']):
                has_geopoints = True
                print(f"🗺️ Found geographic data in column: {col}")
                break
        
        # Get form info
        forms = client.get_forms()
        form_info = next((f for f in forms if f.get('xmlFormId') == args.form_id), {})
        
        # Create analytics
        print("📊 Analyzing data...")
        analytics = DashboardAnalytics(data, form_info)
        
        # Generate output path
        if args.output:
            output_path = Path(args.output)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_form_id = re.sub(r'[^\w\-_.]', '_', args.form_id)
            output_path = Path(f"dashboard_fixed_{safe_form_id}_{timestamp}.pdf")
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Validate header image if provided
        header_image = None
        if args.header_image:
            if HighQualityImageProcessor.validate_image(args.header_image):
                header_image = args.header_image
                img_info = HighQualityImageProcessor.get_image_info(args.header_image)
                format_name = img_info.get('format', 'Unknown')
                size_info = f"{img_info.get('width', 0)}×{img_info.get('height', 0)}"
                dpi_info = img_info.get('dpi', (72, 72))[0]
                print(f"🖼️ Using header image: {os.path.basename(header_image)}")
                print(f"   📏 Format: {format_name}, Size: {size_info}, DPI: {dpi_info}")
                print("   ✅ Fixed file handling - no temp file errors!")
            else:
                print(f"⚠️ Warning: Header image invalid, proceeding without it")
        
        # Generate report
        print(f"📄 Generating PDF report: {output_path}")
        print("   🔧 Using temporary file handling")
        reporter = FixedHighQualityDashboardPDFReporter(analytics, header_image)
        
        if reporter.generate_dashboard_report(str(output_path), args.title):
            print(f"🎉 Dashboard report generated successfully!")
            print(f"📍 File saved: {output_path.absolute()}")
            print("✅  No more temporary file errors!")
            print("✨  Images and optimized quality!")
            if header_image:
                print(f"🖼️ Header image included at {args.image_dpi} DPI")
            
            # Generate HTML report if requested or geopoints are found
            if args.html or has_geopoints:
                if HAS_FOLIUM:
                    html_path = output_path.with_suffix('.html')
                    print(f"🌐 Generating HTML report with interactive map: {html_path}")
                    
                    if reporter.generate_html_report(str(html_path), args.title):
                        print(f"🎉 HTML report with interactive map generated successfully!")
                        print(f"📍 File saved: {html_path.absolute()}")
                    else:
                        print(f"❌ Failed to generate HTML report")
                else:
                    print(f"⚠️ Folium library not installed. Cannot generate HTML report with maps.")
                    print(f"   Install with: pip install folium")
            
            return 0
        else:
            print("❌ Failed to generate dashboard report")
            return 1
            
    except Exception as e:
        print(f"❌ Error: {e}")
        if args.verbose:
            import traceback
            print(traceback.format_exc())
        return 1

def print_usage_examples():
    """Print usage examples for both GUI and CLI modes."""
    print("📖 Usage Examples:")
    print()
    print("🖥️  GUI Mode:")
    print("   python odk_dashboard_reporter_fixed.py")
    print()
    print("⌨️  CLI Mode:")
    print("   python odk_dashboard_reporter_fixed.py \\")
    print("     --url https://your-odk-central.com \\")
    print("     --username your-email@example.com \\")
    print("     --password your-password \\")
    print("     --project-id 1 \\")
    print("     --form-id your-form-id \\")
    print("     --title 'My Fixed Dashboard Report' \\")
    print("     --header-image /path/to/logo.png \\")
    print("     --image-quality 95 \\")
    print("     --image-dpi 300 \\")
    print("     --output my_fixed_report.pdf \\")
    print("     --html \\")
    print("     --verbose")
    print()
    print("📦 Required Dependencies:")
    print("   pip install reportlab Pillow pandas requests matplotlib seaborn numpy python-dateutil")
    print()
    print("🔧 Optional Dependencies:")
    print("   pip install pyyaml tqdm folium")
    print()
    print("✅ Fixed Issues:")
    print("   • Fixed temporary file deletion causing ReportLab errors")
    print("   • Improved image file stability and persistence")
    print("   • Enhanced error handling for image processing")
    print("   • Fixed matplotlib warnings for categorical data")
    print("   • Added map support for forms with geopoints")
    print()

if __name__ == '__main__':
    import sys
    
    # Update constants with current values
    CURRENT_USER = os.getlogin() 
    CURRENT_DATETIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Print header
    print("=" * 90)
    print("🚀 Dashboard Reporter ")
    print(f"📅 Version 2.2.1 | {CURRENT_DATETIME} UTC | User: {CURRENT_USER}")
    print("=" * 90)
    print()
    
    # Check if running in CLI mode (has command line arguments other than help)
    if len(sys.argv) > 1 and not any(arg in sys.argv for arg in ['--help', '-h']):
        # CLI mode
        try:
            exit_code = cli_mode()
            sys.exit(exit_code)
        except KeyboardInterrupt:
            print("\n⚠️ Operation cancelled by user")
            # Clean up temp files on interrupt
            cleanup_temp_files()
            sys.exit(1)
        except SystemExit:
            raise
        except Exception as e:
            print(f"❌ CLI Error: {e}")
            cleanup_temp_files()
            sys.exit(1)
    elif '--help' in sys.argv or '-h' in sys.argv:
        # Show help
        print_usage_examples()
        cli_mode()  # This will show argparse help
    else:
        # GUI mode
        try:
            exit_code = main()
            sys.exit(exit_code)
        except KeyboardInterrupt:
            print("\n⚠️ Application closed by user")
            cleanup_temp_files()
            sys.exit(0)
        except SystemExit:
            raise
        except Exception as e:
            print(f"❌ GUI Error: {e}")
            import traceback
            traceback.print_exc()
            cleanup_temp_files()
            sys.exit(1)