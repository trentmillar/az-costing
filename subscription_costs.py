from azure.identity import DefaultAzureCredential
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.consumption import ConsumptionManagementClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import calendar
import openpyxl
from openpyxl.styles import Font, PatternFill
from time import sleep
from azure.core.exceptions import HttpResponseError
import random
import requests

def get_subscriptions():
    """Get all subscriptions in the tenant"""
    credential = DefaultAzureCredential()
    subscription_client = SubscriptionClient(credential)
    
    subscriptions = []
    for sub in subscription_client.subscriptions.list():
        if sub.display_name.lower().startswith('sub'):
            subscriptions.append({
                'id': sub.subscription_id,
                'name': sub.display_name
            })
    return subscriptions[0:5]

def get_costs_by_resource_type(subscription_id, start_date, end_date, max_retries=3):
    """Get costs grouped by resource type for a subscription in a date range"""
    credential = DefaultAzureCredential()
    # Get the token for the Azure Management scope
    token = credential.get_token("https://management.azure.com/.default")
    auth_header = f"Bearer {token.token}"
    
    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json"
    }   
    
    for attempt in range(max_retries):
        try:
            print(f"Getting costs for period: {start_date} to {end_date} on {subscription_id}")
            
            url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Consumption/usageDetails"
            params = {
                "api-version": "2019-11-01",
                "startDate": start_date,
                "endDate": end_date
            }
            
            # Add timeout parameter and session with retry configuration
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                max_retries=3,
                pool_connections=10,
                pool_maxsize=10
            )
            session.mount('https://', adapter)
            
            response = session.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            usage = response.json()
            
            # Aggregate costs by resource type
            costs_by_type = {}
            total_cost = 0
            for item in usage.get('value', []):
                resource_type = item.get('properties', {}).get('serviceFamily') or "Unknown"
                cost = float(item.get('properties', {}).get('costInBillingCurrency', 0))
                costs_by_type[resource_type] = costs_by_type.get(resource_type, 0) + cost
                total_cost += cost
            
            print(f"Total cost for period: ${total_cost:,.2f}")
            return costs_by_type
            
        except (requests.exceptions.RequestException, requests.exceptions.ChunkedEncodingError) as e:
            if hasattr(e, 'response') and getattr(e.response, 'status_code', None) == 429:
                if attempt < max_retries - 1:
                    wait_time = 60 + random.uniform(0, 10)
                    print(f"Rate limited. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                    sleep(wait_time)
                    continue
            elif attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)  # Progressive backoff
                print(f"Request failed: {str(e)}. Retrying in {wait_time} seconds... ({attempt + 1}/{max_retries})")
                sleep(wait_time)
                continue
            raise

def create_excel_report(start_period=None, end_period=None):
    """
    Create Excel report with monthly sheets
    Args:
        start_period (dict): Dictionary containing 'year' and 'month' for start period
        end_period (dict): Dictionary containing 'year' and 'month' for end period
    Example:
        create_excel_report(
            start_period={'year': 2024, 'month': 11},
            end_period={'year': 2024, 'month': 12}
        )
    """
    if start_period is None or end_period is None:
        # Default to current month and previous month if no periods specified
        today = datetime.now()
        end_period = {'year': today.year, 'month': today.month}
        start_period = {
            'year': today.year if today.month > 1 else today.year - 1,
            'month': today.month - 1 if today.month > 1 else 12
        }
    
    print(f"Generating cost report from {calendar.month_name[start_period['month']]} {start_period['year']} "
          f"to {calendar.month_name[end_period['month']]} {end_period['year']}")
    
    # Create filename with start and end periods
    start_str = f"{start_period['year']}-{start_period['month']:02d}"
    end_str = f"{end_period['year']}-{end_period['month']:02d}"
    filename = f'azure_costs_{start_str}_to_{end_str}.xlsx'
    
    # Create workbook
    wb = openpyxl.Workbook()
    
    # Store monthly costs for comparison
    monthly_costs = {}  # Format: {(year, month): {sub_id: {service_family: cost}}}
    
    # Create sheets for each month in the range
    start_date = datetime(start_period['year'], start_period['month'], 1)
    end_date = datetime(end_period['year'], end_period['month'], 1)
    
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        month_name = calendar.month_name[month]
        
        print(f"\nProcessing {month_name} {year}")
        sheet = wb.create_sheet(f"{month_name} {year}")
        
        # Set up headers
        sheet['A1'] = 'Subscription Name'
        sheet['A1'].font = Font(bold=True)
        
        # Get start and end dates for the month
        start_date_month = datetime(year, month, 1)
        end_date_month = start_date_month + relativedelta(months=1) - timedelta(days=1)
        
        # Format dates for Azure API
        date_format = '%Y-%m-%d'
        start_date_str = start_date_month.strftime(date_format)
        end_date_str = end_date_month.strftime(date_format)
        
        # Get unique resource types across all subscriptions for the month
        all_resource_types = set()
        for sub in get_subscriptions():
            costs = get_costs_by_resource_type(
                sub['id'], 
                start_date_str,
                end_date_str
            )
            all_resource_types.update(costs.keys())
        
        # Set up resource type columns
        for col, resource_type in enumerate(sorted(all_resource_types), start=2):
            cell = sheet.cell(row=1, column=col)
            cell.value = resource_type
            cell.font = Font(bold=True)
        
        # Add Total column header
        total_col = len(all_resource_types) + 2
        sheet.cell(row=1, column=total_col).value = 'Total'
        sheet.cell(row=1, column=total_col).font = Font(bold=True)
        
        # Initialize monthly costs for this month
        monthly_costs[(year, month)] = {}
        
        # Process each subscription
        for row, sub in enumerate(get_subscriptions(), start=2):
            sheet.cell(row=row, column=1).value = sub['name']
            
            costs = get_costs_by_resource_type(
                sub['id'],
                start_date_str,
                end_date_str
            )
            
            # Store costs for comparison
            monthly_costs[(year, month)][sub['id']] = costs
            
            # Initialize row total
            row_total = 0
            
            # Fill in costs for each resource type
            for col, resource_type in enumerate(sorted(all_resource_types), start=2):
                cell = sheet.cell(row=row, column=col)
                cost = costs.get(resource_type, 0)
                cell.value = cost
                cell.number_format = '$#,##0.00'
                row_total += cost
            
            # Add row total
            total_cell = sheet.cell(row=row, column=total_col)
            total_cell.value = row_total
            total_cell.number_format = '$#,##0.00'
            total_cell.font = Font(bold=True)
        
        # Add column totals at the bottom
        total_row = len(get_subscriptions()) + 2
        sheet.cell(row=total_row, column=1).value = 'Total'
        sheet.cell(row=total_row, column=1).font = Font(bold=True)
        
        # Calculate column totals
        for col in range(2, total_col + 1):
            column_total = sum(sheet.cell(row=r, column=col).value or 0 
                             for r in range(2, total_row))
            cell = sheet.cell(row=total_row, column=col)
            cell.value = column_total
            cell.number_format = '$#,##0.00'
            cell.font = Font(bold=True)
        
        # Auto-adjust column widths
        for column in sheet.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            sheet.column_dimensions[column[0].column_letter].width = adjusted_width
        
        # Move to next month
        current_date = current_date + relativedelta(months=1)
    
    # Create Summary sheet for cost changes
    summary_sheet = wb.create_sheet("Cost Changes", 0)
    
    # Set up summary headers
    summary_sheet['A1'] = 'Subscription'
    summary_sheet['B1'] = 'Service Family'
    summary_sheet['C1'] = 'Previous Month'
    summary_sheet['D1'] = 'Current Month'
    summary_sheet['E1'] = 'Change Amount'
    summary_sheet['F1'] = 'Change Percentage'
    summary_sheet['G1'] = 'Month Pair'
    
    # Apply header formatting
    for cell in summary_sheet[1]:
        cell.font = Font(bold=True)
    
    # Calculate significant changes
    print("\nCalculating cost changes...")
    row = 2
    
    # Get sorted list of (year, month) tuples from monthly_costs
    periods = sorted(monthly_costs.keys())
    
    # Compare each consecutive pair of months
    for i in range(len(periods) - 1):
        prev_year, prev_month = periods[i]
        curr_year, curr_month = periods[i + 1]
        
        prev_month_name = calendar.month_name[prev_month]
        curr_month_name = calendar.month_name[curr_month]
        
        for sub in get_subscriptions():
            prev_costs = monthly_costs.get((prev_year, prev_month), {}).get(sub['id'], {})
            curr_costs = monthly_costs.get((curr_year, curr_month), {}).get(sub['id'], {})
            
            print(f"\nAnalyzing changes for {sub['name']}")
            print(f"Previous month ({prev_month_name} {prev_year}) costs: {prev_costs}")
            print(f"Current month ({curr_month_name} {curr_year}) costs: {curr_costs}")
            
            # Compare service families
            all_services = set(prev_costs.keys()) | set(curr_costs.keys())
            
            for service in all_services:
                prev_cost = prev_costs.get(service, 0)
                curr_cost = curr_costs.get(service, 0)
                
                # Calculate change
                change_amount = curr_cost - prev_cost
                if prev_cost > 0:
                    change_percentage = (change_amount / prev_cost) * 100
                else:
                    change_percentage = float('inf') if curr_cost > 0 else 0
                
                # Debug print
                print(f"Service: {service}")
                print(f"Previous cost: ${prev_cost:,.2f}")
                print(f"Current cost: ${curr_cost:,.2f}")
                print(f"Change: ${change_amount:,.2f} ({change_percentage:.1f}%)")
                
                # Only show significant changes (e.g., >10% and >$100)
                if abs(change_percentage) > 10 and abs(change_amount) > 100:
                    print(f"Significant change detected - adding to summary")
                    summary_sheet[f'A{row}'] = sub['name']
                    summary_sheet[f'B{row}'] = service
                    summary_sheet[f'C{row}'] = prev_cost
                    summary_sheet[f'D{row}'] = curr_cost
                    summary_sheet[f'E{row}'] = change_amount
                    summary_sheet[f'F{row}'] = f"{change_percentage:.1f}%"
                    summary_sheet[f'G{row}'] = f"{prev_month_name} {prev_year} → {curr_month_name} {curr_year}"
                    
                    # Apply conditional formatting
                    if change_amount > 0:
                        summary_sheet[f'E{row}'].fill = PatternFill(start_color="FFB6C1", end_color="FFB6C1", fill_type="solid")
                    else:
                        summary_sheet[f'E{row}'].fill = PatternFill(start_color="90EE90", end_color="90EE90", fill_type="solid")
                    
                    # Format numbers
                    for col in ['C', 'D', 'E']:
                        summary_sheet[f'{col}{row}'].number_format = '$#,##0.00'
                    
                    row += 1
    
    # Auto-adjust column widths for summary sheet
    for column in summary_sheet.columns:
        max_length = 0
        column = [cell for cell in column]
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        summary_sheet.column_dimensions[column[0].column_letter].width = adjusted_width

    # Save with new filename
    wb.save(filename)
    print(f"Report saved as {filename}")

if __name__ == "__main__":
    create_excel_report(
        start_period={'year': 2024, 'month': 12},  # December 2024
        end_period={'year': 2025, 'month': 2}      # February 2025
    ) 
    
