"""
PDF Report Generator for ODK Central data.
Creates formatted PDF reports with charts, tables, and summaries.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import Color, HexColor
from reportlab.lib.units import inch
from reportlab.lib import colors
from datetime import datetime
import os
import tempfile
import logging
from typing import Dict, Any, List, Optional
import io
import base64


class PDFReportGenerator:
    """Generate formatted PDF reports from ODK Central data."""
    
    def __init__(self, title: str = "ODK Central Data Report", page_size=A4):
        """
        Initialize PDF report generator.
        
        Args:
            title: Report title
            page_size: Page size (default A4)
        """
        self.title = title
        self.page_size = page_size
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
        
    def _setup_custom_styles(self):
        """Setup custom paragraph styles for the report."""
        # Title style
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Title'],
            fontSize=24,
            textColor=HexColor('#2E4057'),
            spaceAfter=30,
            alignment=1  # Center alignment
        ))
        
        # Section header style
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading1'],
            fontSize=16,
            textColor=HexColor('#1B4F72'),
            spaceAfter=12,
            spaceBefore=20
        ))
        
        # Subsection header style
        self.styles.add(ParagraphStyle(
            name='SubsectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            textColor=HexColor('#2874A6'),
            spaceAfter=8,
            spaceBefore=12
        ))
        
        # Info text style
        self.styles.add(ParagraphStyle(
            name='InfoText',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=HexColor('#566573'),
            spaceAfter=6
        ))

    def generate_summary_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics from the dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            Dictionary containing summary statistics
        """
        if df.empty:
            return {}
            
        summary = {
            'total_submissions': len(df),
            'columns_count': len(df.columns),
            'date_range': None,
            'numeric_columns': [],
            'categorical_columns': []
        }
        
        # Try to find date columns and calculate date range
        date_columns = []
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Try to convert to datetime
                    pd.to_datetime(df[col].dropna().head())
                    date_columns.append(col)
                except:
                    pass
        
        if date_columns:
            # Use the first date column found
            date_col = date_columns[0]
            try:
                dates = pd.to_datetime(df[date_col].dropna())
                if not dates.empty:
                    summary['date_range'] = {
                        'start': dates.min().strftime('%Y-%m-%d'),
                        'end': dates.max().strftime('%Y-%m-%d'),
                        'column': date_col
                    }
            except:
                pass
        
        # Identify numeric and categorical columns
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                if not df[col].isna().all():
                    summary['numeric_columns'].append({
                        'name': col,
                        'mean': df[col].mean(),
                        'median': df[col].median(),
                        'std': df[col].std(),
                        'min': df[col].min(),
                        'max': df[col].max(),
                        'count': df[col].count()
                    })
            elif df[col].dtype == 'object':
                value_counts = df[col].value_counts().head(5)
                if not value_counts.empty:
                    summary['categorical_columns'].append({
                        'name': col,
                        'unique_values': df[col].nunique(),
                        'most_common': value_counts.to_dict(),
                        'total_responses': df[col].count()
                    })
        
        return summary

    def create_chart(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: str = None, 
                    title: str = "", figsize=(8, 6)) -> str:
        """
        Create a chart and return it as a base64 encoded string.
        
        Args:
            df: Input dataframe
            chart_type: Type of chart ('bar', 'pie', 'histogram', 'scatter', 'line')
            x_col: X-axis column
            y_col: Y-axis column (for scatter/line plots)
            title: Chart title
            figsize: Figure size tuple
            
        Returns:
            Base64 encoded image string
        """
        try:
            # Set backend to Agg for headless environments
            import matplotlib
            matplotlib.use('Agg')
            
            plt.figure(figsize=figsize)
            
            # Check if column exists
            if x_col not in df.columns:
                logging.warning(f"Column '{x_col}' not found in dataframe")
                return ""
            
            if chart_type == 'bar':
                if y_col and y_col in df.columns:
                    df.groupby(x_col)[y_col].sum().plot(kind='bar')
                else:
                    df[x_col].value_counts().head(10).plot(kind='bar')
                plt.xticks(rotation=45)
                
            elif chart_type == 'pie':
                data = df[x_col].value_counts().head(8)
                plt.pie(data.values, labels=data.index, autopct='%1.1f%%')
                
            elif chart_type == 'histogram':
                df[x_col].hist(bins=20)
                plt.xlabel(x_col)
                plt.ylabel('Frequency')
                
            elif chart_type == 'scatter' and y_col and y_col in df.columns:
                plt.scatter(df[x_col], df[y_col], alpha=0.6)
                plt.xlabel(x_col)
                plt.ylabel(y_col)
                
            elif chart_type == 'line' and y_col and y_col in df.columns:
                df_sorted = df.sort_values(x_col)
                plt.plot(df_sorted[x_col], df_sorted[y_col])
                plt.xlabel(x_col)
                plt.ylabel(y_col)
                
            else:
                # Default to value counts bar chart
                df[x_col].value_counts().head(10).plot(kind='bar')
                plt.xticks(rotation=45)
            
            plt.title(title)
            plt.tight_layout()
            
            # Save to base64 string
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close()
            buffer.close()
            
            return image_base64
            
        except Exception as e:
            logging.error(f"Failed to create chart: {e}")
            plt.close()
            return ""

    def create_data_table(self, df: pd.DataFrame, max_rows: int = 20) -> Table:
        """
        Create a formatted table from dataframe.
        
        Args:
            df: Input dataframe
            max_rows: Maximum number of rows to include
            
        Returns:
            ReportLab Table object
        """
        if df.empty:
            return Table([["No data available"]])
        
        # Limit rows and columns for readability
        display_df = df.head(max_rows)
        
        # Prepare data for table
        table_data = [list(display_df.columns)]
        for _, row in display_df.iterrows():
            table_data.append([str(val)[:50] + ('...' if len(str(val)) > 50 else '') for val in row])
        
        # Create table
        table = Table(table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        return table

    def generate_report(self, df: pd.DataFrame, output_file: str, 
                       report_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        Generate a complete PDF report.
        
        Args:
            df: Input dataframe
            output_file: Path to output PDF file
            report_config: Configuration dictionary for report customization
            
        Returns:
            True if report generated successfully, False otherwise
        """
        try:
            # Default configuration
            config = {
                'include_summary': True,
                'include_charts': True,
                'include_data_table': True,
                'chart_configs': [],
                'max_table_rows': 20
            }
            
            if report_config:
                config.update(report_config)
            
            # Create PDF document
            doc = SimpleDocTemplate(output_file, pagesize=self.page_size)
            story = []
            
            # Title
            story.append(Paragraph(self.title, self.styles['CustomTitle']))
            story.append(Spacer(1, 20))
            
            # Report generation info
            generation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            story.append(Paragraph(f"Generated on: {generation_time}", self.styles['InfoText']))
            story.append(Spacer(1, 20))
            
            # Summary statistics
            if config['include_summary'] and not df.empty:
                story.append(Paragraph("Summary Statistics", self.styles['SectionHeader']))
                
                summary = self.generate_summary_statistics(df)
                
                # Basic stats
                story.append(Paragraph(f"Total Submissions: {summary.get('total_submissions', 0)}", self.styles['Normal']))
                story.append(Paragraph(f"Number of Fields: {summary.get('columns_count', 0)}", self.styles['Normal']))
                
                if summary.get('date_range'):
                    date_range = summary['date_range']
                    story.append(Paragraph(f"Date Range: {date_range['start']} to {date_range['end']}", self.styles['Normal']))
                
                story.append(Spacer(1, 20))
                
                # Numeric columns summary
                if summary.get('numeric_columns'):
                    story.append(Paragraph("Numeric Fields Summary", self.styles['SubsectionHeader']))
                    
                    for col_stats in summary['numeric_columns'][:5]:  # Limit to 5 columns
                        col_name = col_stats['name']
                        story.append(Paragraph(f"<b>{col_name}:</b> Mean={col_stats['mean']:.2f}, "
                                             f"Median={col_stats['median']:.2f}, "
                                             f"Range=[{col_stats['min']:.2f}, {col_stats['max']:.2f}]",
                                             self.styles['Normal']))
                    
                    story.append(Spacer(1, 15))
            
            # Charts section
            if config['include_charts'] and not df.empty:
                story.append(Paragraph("Data Visualizations", self.styles['SectionHeader']))
                
                # Create default charts if no specific configs provided
                chart_configs = config.get('chart_configs', [])
                if not chart_configs:
                    # Auto-generate some charts
                    summary = self.generate_summary_statistics(df)
                    
                    # Add categorical charts
                    for cat_col in summary.get('categorical_columns', [])[:3]:
                        if cat_col['unique_values'] <= 20:  # Only if reasonable number of categories
                            chart_configs.append({
                                'type': 'bar',
                                'x_col': cat_col['name'],
                                'title': f"Distribution of {cat_col['name']}"
                            })
                    
                    # Add numeric histograms
                    for num_col in summary.get('numeric_columns', [])[:2]:
                        chart_configs.append({
                            'type': 'histogram',
                            'x_col': num_col['name'],
                            'title': f"Distribution of {num_col['name']}"
                        })
                
                # Generate charts
                charts_added = 0
                for i, chart_config in enumerate(chart_configs[:4]):  # Limit to 4 charts
                    try:
                        chart_b64 = self.create_chart(
                            df,
                            chart_config.get('type', 'bar'),
                            chart_config.get('x_col', ''),
                            chart_config.get('y_col', None),
                            chart_config.get('title', f'Chart {i+1}')
                        )
                        if chart_b64:
                            # Save chart to temporary file
                            with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp_file:
                                tmp_file.write(base64.b64decode(chart_b64))
                                tmp_file.flush()
                                chart_path = tmp_file.name
                            
                            # Add chart to story
                            if os.path.exists(chart_path):
                                img = Image(chart_path, width=5*inch, height=3*inch)
                                story.append(img)
                                story.append(Spacer(1, 15))
                                charts_added += 1
                                
                                # Clean up temporary file
                                try:
                                    os.unlink(chart_path)
                                except OSError:
                                    pass
                            
                    except Exception as e:
                        logging.error(f"Failed to add chart {i}: {e}")
                        continue
                
                # If no charts were added, add a note
                if charts_added == 0:
                    story.append(Paragraph("Charts could not be generated for this dataset.", self.styles['Normal']))
                    story.append(Spacer(1, 15))
            
            # Data table section
            if config['include_data_table'] and not df.empty:
                story.append(Paragraph("Data Sample", self.styles['SectionHeader']))
                story.append(Paragraph(f"Showing first {min(config['max_table_rows'], len(df))} rows",
                                     self.styles['InfoText']))
                story.append(Spacer(1, 10))
                
                table = self.create_data_table(df, config['max_table_rows'])
                story.append(table)
                story.append(Spacer(1, 20))
            
            # Build PDF
            doc.build(story)
            logging.info(f"PDF report generated successfully: {output_file}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to generate PDF report: {e}")
            return False


def create_default_report_config() -> Dict[str, Any]:
    """Create a default report configuration."""
    return {
        'include_summary': True,
        'include_charts': True,
        'include_data_table': True,
        'max_table_rows': 20,
        'chart_configs': [
            {
                'type': 'bar',
                'x_col': 'status',  # Common field name
                'title': 'Submission Status Distribution'
            }
        ]
    }