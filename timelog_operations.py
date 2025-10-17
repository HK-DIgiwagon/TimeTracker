import requests
import json
from datetime import datetime
from refresh_token import get_valid_access_token
from models import ZohoTimelogEntry, EmployeeMaster
from database import SessionLocal



def get_user_id_by_email(email):
    """
    Fetch user_id (emp_id) from EmployeeMaster based on email.

    Args:
        email (str): Employee email

    Returns:
        str: Employee ID or None if not found
    """
    try:
        with SessionLocal() as db:  # ensures session is closed automatically
            employee = db.query(EmployeeMaster).filter(EmployeeMaster.email == email).first()
            if employee:
                return employee.id
            else:
                print(f"Warning: No employee found with email: {email}")
                return None
    except Exception as e:
        print(f"Error fetching user by email {email}: {e}")
        return None
    

def fetch_zoho_timelogs(start_date, end_date):
    """
    Fetch timelogs from Zoho Projects API for a given date range.
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
    
    Returns:
        dict: API response containing timelog data
    """
    
    # Get valid access token
    try:
        access_token = get_valid_access_token()
        print(f"Access token obtained successfully")
    except Exception as e:
        print(f"Error getting access token: {e}")
        return None
    
    # API endpoint
    portal_id = "60028771089"
    base_url = f"https://projectsapi.zoho.in/api/v3/portal/{portal_id}/timelogs"
    
    # Query parameters
    params = {
        'module': '{"type":"task"}',
        'start_date': start_date,
        'end_date': end_date,
        'view_type': 'day'
    }
    
    # Headers
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        print(f"\nFetching timelogs from {start_date} to {end_date}...")
        print(f"API URL: {base_url}")
        
        # Make GET request
        response = requests.get(base_url, headers=headers, params=params)
        
        # Check response status
        if response.status_code == 200:
            data = response.json()
            print(f"Request successful!")
            print(f"Response received with {len(data.get('timelogs', []))} timelogs")
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Request error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"✗ JSON decode error: {e}")
        return None




def main():
    """
    Main function to fetch timelogs.
    You can modify the dates here or pass them as arguments.
    """
    
    # Example dates - modify these as needed
    start_date = "2025-10-01"
    end_date = "2025-10-01"
    
    
    # Fetch timelogs
    response_data = fetch_zoho_timelogs(start_date, end_date)
    
    if response_data:
        all_timelogs = response_data.get('time_logs')

        with SessionLocal() as session:  # open session once
            for single_log in all_timelogs:
                single_log_date = single_log.get('date')
                all_logs = single_log.get('log_details', [])
                if not all_logs:
                    continue

                for log in all_logs:
                    useremail = log.get('added_by').get('email')
                    # import pdb; pdb.set_trace()
                    emp_id = get_user_id_by_email(useremail)  # fetch emp_id
                    if not emp_id:
                        print(f"Skipping unknown user: {useremail}")
                        continue

                    project=log.get('project').get('name')
                    task=log.get('module_detail').get('name')
                    start_time=log.get('start_time')
                    end_time=log.get('end_time')
                    logged_hours=log.get('log_hour')


                    if not all([emp_id, project, task, start_time, end_time, logged_hours]):
                        print(f"Skipping incomplete log for user {useremail} on {single_log_date}")
                        continue
                    timelog_entry = ZohoTimelogEntry(
                        emp_id=emp_id,
                        timelog_date=single_log_date,
                        project=project,
                        task=task,
                        start_time=start_time,
                        end_time=end_time,
                        logged_hours=logged_hours
                    )

                    session.add(timelog_entry)  # add to session

            session.commit()  # commit all at once
            print("All timelogs inserted successfully.")





if __name__ == "__main__":
    main()