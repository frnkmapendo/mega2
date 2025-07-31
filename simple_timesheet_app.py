#!/usr/bin/env python3
"""
Simple Timesheet Management Shiny App
Standalone implementation to demonstrate timesheet functionality
"""

import pandas as pd
import calendar
import random
from datetime import datetime
import openpyxl
from io import BytesIO
from shiny import App, ui, render, reactive
import threading
import webbrowser

# Import the timesheet manager
from timesheet_test import TimesheetManager

# Global timesheet manager instance  
timesheet_manager = TimesheetManager()

# Simple UI
app_ui = ui.page_bootstrap(
    ui.div(
        {"class": "container-fluid"},
        ui.h1("Project Timesheet Manager", {"class": "text-center mt-4 mb-4"}),
        
        ui.div(
            {"class": "row"},
            # Left column - Project Management
            ui.div(
                {"class": "col-md-6"},
                ui.div(
                    {"class": "card"},
                    ui.div(
                        {"class": "card-header"},
                        ui.h5("Project Management")
                    ),
                    ui.div(
                        {"class": "card-body"},
                        ui.input_text("project_name", "Project Name"),
                        ui.input_numeric("project_percentage", "Allocation (%)", 10, min=1, max=100, step=1),
                        ui.input_action_button("add_project", "Add Project"),
                        ui.br(),
                        ui.output_text("project_message"),
                        ui.br(),
                        ui.h6("Current Projects"),
                        ui.output_ui("projects_list")
                    )
                )
            ),
            # Right column - Timesheet Generation
            ui.div(
                {"class": "col-md-6"},
                ui.div(
                    {"class": "card"},
                    ui.div(
                        {"class": "card-header"},
                        ui.h5("Timesheet Generation")
                    ),
                    ui.div(
                        {"class": "card-body"},
                        ui.div(
                            {"class": "row"},
                            ui.div(
                                {"class": "col-md-6"},
                                ui.input_select("timesheet_year", "Year", 
                                               [str(y) for y in range(2020, 2030)],
                                               selected=str(datetime.now().year))
                            ),
                            ui.div(
                                {"class": "col-md-6"},
                                ui.input_select("timesheet_month", "Month",
                                               {str(i): calendar.month_name[i] for i in range(1, 13)},
                                               selected=str(datetime.now().month))
                            )
                        ),
                        ui.input_checkbox("randomize_small", "Randomize projects under 20%", False),
                        ui.input_action_button("generate_timesheet", "Generate Timesheet"),
                        ui.br(),
                        ui.download_button("download_excel", "Download Excel")
                    )
                )
            )
        ),
        
        # Timesheet Display
        ui.div(
            {"class": "mt-4"},
            ui.output_ui("timesheet_display")
        )
    ),
    title="Project Timesheet Manager"
)

def server(input, output, session):
    # Reactive value to store timesheet data
    timesheet_data = reactive.Value(pd.DataFrame())
    
    @reactive.Effect
    @reactive.event(input.add_project)
    def add_project():
        name = input.project_name()
        percentage = input.project_percentage()
        
        if not name or not name.strip():
            ui.notification_show("Project name is required", type="error")
            return
            
        if timesheet_manager.add_project(name.strip(), percentage):
            ui.notification_show(f"Project '{name}' added successfully", type="success")
            # Clear inputs
            ui.update_text("project_name", value="")
            ui.update_numeric("project_percentage", value=10)
        else:
            ui.notification_show("Failed to add project. Check name and percentage allocation.", type="error")
    
    @output
    @render.ui
    def projects_list():
        if not timesheet_manager.projects:
            return ui.p("No projects added yet.")
        
        project_items = []
        total_percentage = 0
        
        for project in timesheet_manager.projects:
            total_percentage += project['percentage']
            project_items.append(
                ui.div(
                    f"{project['name']}: {project['percentage']}% ({project['daily_hours']:.2f} hrs/day)",
                    {"class": "mb-2 p-2 border rounded"}
                )
            )
        
        # Add total percentage display
        percentage_class = "text-success" if total_percentage <= 100 else "text-danger"
        project_items.append(
            ui.div(
                f"Total Allocation: {total_percentage}%",
                {"class": f"mt-3 p-2 border-top {percentage_class}"}
            )
        )
        
        return ui.div(*project_items)
    
    @reactive.Effect
    @reactive.event(input.generate_timesheet)
    def generate_timesheet():
        if not timesheet_manager.projects:
            ui.notification_show("Please add projects first", type="warning")
            return
            
        year = int(input.timesheet_year())
        month = int(input.timesheet_month())
        randomize = input.randomize_small()
        
        try:
            df = timesheet_manager.get_project_hours_daily(year, month, randomize)
            timesheet_data.set(df)
            ui.notification_show("Timesheet generated successfully", type="success")
        except Exception as e:
            ui.notification_show(f"Error generating timesheet: {str(e)}", type="error")
    
    @output
    @render.ui
    def timesheet_display():
        df = timesheet_data.get()
        
        if df.empty:
            return ui.div(
                ui.p("Generate a timesheet to view the daily distribution.", {"class": "text-center"})
            )
        
        # Create HTML table
        table_html = '<table class="table table-striped"><thead><tr>'
        
        # Headers
        headers = ["Day"] + list(df.columns) + ["Daily Total"]
        for header in headers:
            table_html += f'<th class="text-center">{header}</th>'
        table_html += '</tr></thead><tbody>'
        
        # Data rows
        for day, row_data in df.iterrows():
            daily_total = row_data.sum()
            table_html += '<tr>'
            table_html += f'<td class="text-center fw-bold">{day}</td>'
            
            for project_name in df.columns:
                hours = row_data[project_name]
                hours_str = f"{hours:.2f}" if hours > 0 else "-"
                cell_class = "text-center" + (" fw-bold" if hours > 0 else " text-muted")
                table_html += f'<td class="{cell_class}">{hours_str}</td>'
            
            table_html += f'<td class="text-center fw-bold">{daily_total:.2f}</td>'
            table_html += '</tr>'
        
        # Monthly totals row
        table_html += '<tr class="table-info">'
        table_html += '<td class="text-center fw-bold">Monthly Total</td>'
        for project_name in df.columns:
            total_hours = df[project_name].sum()
            table_html += f'<td class="text-center fw-bold">{total_hours:.2f}</td>'
        
        grand_total = df.sum().sum()
        table_html += f'<td class="text-center fw-bold">{grand_total:.2f}</td>'
        table_html += '</tr></tbody></table>'
        
        return ui.div(
            ui.h6("Daily Timesheet"),
            ui.HTML(table_html)
        )
    
    @output
    @render.text
    def project_message():
        total_percentage = sum(p['percentage'] for p in timesheet_manager.projects)
        if total_percentage > 100:
            return "⚠️ Total allocation exceeds 100%"
        elif total_percentage == 100:
            return "✅ Perfect allocation (100%)"
        else:
            return f"Current allocation: {total_percentage}%"
    
    @output
    @render.download(f"timesheet_{datetime.now().year}_{datetime.now().month:02d}.xlsx")
    def download_excel():
        if not timesheet_manager.projects:
            return b""
            
        year = int(input.timesheet_year())
        month = int(input.timesheet_month())
        randomize = input.randomize_small()
        
        try:
            excel_buffer = timesheet_manager.export_to_excel(year, month, randomize)
            return excel_buffer.getvalue()
        except Exception as e:
            ui.notification_show(f"Error generating Excel: {str(e)}", type="error")
            return b""

def open_browser():
    url = "http://127.0.0.1:8000"
    print(f"Opening browser at {url} ...")
    try:
        webbrowser.open(url)
    except Exception as e:
        print(f"Could not open browser automatically: {e}")
        print(f"Please open your browser and go to {url}")

if __name__ == "__main__":
    app = App(app_ui, server)
    print("Starting Timesheet Manager Shiny app on http://127.0.0.1:8000 ...")
    threading.Timer(2.0, open_browser).start()
    app.run(host="127.0.0.1", port=8000)