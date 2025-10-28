import requests
import json
from datetime import datetime, timedelta
from refresh_token import get_valid_access_token
from models import ZohoTimelogEntry, EmployeeMaster
from database import SessionLocal
from logger_config import timelog_logger
from datetime import datetime, timedelta
from logger_config import timelog_logger


def get_user_id_by_email(email):
    """
    Fetch user_id (emp_id) from EmployeeMaster based on email.

    Args:
        email (str): Employee email

    Returns:
        str: Employee ID or None if not found
    """
    try:
        with SessionLocal() as db:
            employee = db.query(EmployeeMaster).filter(EmployeeMaster.email == email).first()
            if employee:
                return employee.id
            else:
                timelog_logger.warning(f"No employee found with email: {email}")
                return None
    except Exception as e:
        timelog_logger.error(f"Error fetching user by email {email}: {e}")
        return None


def fetch_zoho_timelogs_single_day(access_token, portal_id, date_str):
    """
    Fetch timelogs for a single day.
    
    Args:
        access_token (str): Valid Zoho access token
        portal_id (str): Zoho portal ID
        date_str (str): Date in format 'YYYY-MM-DD'
    
    Returns:
        list: List of timelog entries for that day
    """
    base_url = f"https://projectsapi.zoho.in/api/v3/portal/{portal_id}/timelogs"
    
    params = {
        'module': '{"type":"task"}',
        'start_date': date_str,
        'end_date': date_str,
        'view_type': 'day',
        'per_page': 200
    }
    
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('time_logs', [])
        else:
            timelog_logger.error(f"Request failed for {date_str} with status: {response.status_code}")
            timelog_logger.error(f"Response: {response.text}")
            return []
            
    except Exception as e:
        timelog_logger.error(f"Error fetching timelogs for {date_str}: {e}")
        return []


def fetch_zoho_timelogs(start_date, end_date):
    """
    Fetch timelogs from Zoho Projects API for a date range.
    Iterates day by day to ensure all data is retrieved.
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
    
    Returns:
        dict: {'time_logs': list of all timelog entries} or None on failure
    """
    try:
        # Step 1: Get valid access token
        try:
            access_token = get_valid_access_token()
            timelog_logger.info("Access token obtained successfully")
        except Exception as e:
            timelog_logger.error(f"Error getting access token: {e}", exc_info=True)
            return None

        portal_id = "60028771089"

        # Step 2: Validate and convert date inputs
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError as e:
            timelog_logger.error(f"Invalid date format. Expected 'YYYY-MM-DD'. Error: {e}")
            return None

        if start > end:
            timelog_logger.error("Start date cannot be after end date.")
            return None

        all_time_logs = []
        current_date = start

        timelog_logger.info(f"Fetching timelogs from {start_date} to {end_date}...")

        # Step 3: Iterate through each day in the range
        while current_date <= end:
            try:
                date_str = current_date.strftime('%Y-%m-%d')
                timelog_logger.info(f"Fetching data for: {date_str}")

                # Fetch single-day timelogs
                day_logs = fetch_zoho_timelogs_single_day(access_token, portal_id, date_str)

                if day_logs:
                    all_time_logs.extend(day_logs)
                    total_logs_count = sum(len(day.get('log_details', [])) for day in day_logs)
                    timelog_logger.info(f"Found {total_logs_count} log entries for {date_str}")
                else:
                    timelog_logger.info(f"No logs found for {date_str}")

            except Exception as e:
                timelog_logger.error(f"Error fetching data for {date_str}: {e}", exc_info=True)

            current_date += timedelta(days=1)

        # Step 4: Summary logging
        try:
            total_days = (end - start).days + 1
            total_logs = sum(len(day.get('log_details', [])) for day in all_time_logs)
            timelog_logger.info(f"Total days processed: {total_days}")
            timelog_logger.info(f"Total log entries found: {total_logs}")
        except Exception as e:
            timelog_logger.error(f"Error while summarizing log results: {e}", exc_info=True)

        return {'time_logs': all_time_logs}

    except Exception as e:
        timelog_logger.critical(f"Unexpected error in fetch_zoho_timelogs: {e}", exc_info=True)
        return None



def process_and_save_timelogs(response_data):
    """
    Process timelog data and save to the database.
    Updates existing records if task_log_id already exists, otherwise inserts new records.

    Args:
        response_data (dict): Response data containing time_logs

    Returns:
        bool: True if processing completed successfully, False otherwise
    """
    try:
        if not response_data:
            timelog_logger.warning("No response data to process.")
            return False

        all_timelogs = response_data.get('time_logs', [])
        if not all_timelogs:
            timelog_logger.warning("No timelog entries found in response.")
            return False

        total_inserted = 0
        total_updated = 0
        total_skipped = 0

        with SessionLocal() as session:
            for single_log in all_timelogs:
                single_log_date = single_log.get('date')
                all_logs = single_log.get('log_details', [])

                if not all_logs:
                    continue

                for log in all_logs:
                    try:
                        useremail = log.get('added_by', {}).get('email')
                        if not useremail:
                            timelog_logger.warning(f"Skipping log without email on {single_log_date}")
                            total_skipped += 1
                            continue

                        emp_id = get_user_id_by_email(useremail)
                        if not emp_id:
                            timelog_logger.warning(f"Skipping unknown user: {useremail}")
                            total_skipped += 1
                            continue

                        project = log.get('project', {}).get('name')
                        task = log.get('module_detail', {}).get('name')
                        start_time = log.get('start_time') or "00:00:00"
                        end_time = log.get('end_time') or "00:00:00"
                        logged_hours = log.get('log_hour')
                        notes = log.get('notes')
                        task_log_id = log.get('id')

                        if not all([emp_id, project, task, logged_hours, task_log_id]):
                            timelog_logger.warning(f"Skipping incomplete log for {useremail} on {single_log_date}")
                            total_skipped += 1
                            continue

                        # Check if record exists
                        existing_entry = session.query(ZohoTimelogEntry).filter(
                            ZohoTimelogEntry.task_log_id == task_log_id,
                            ZohoTimelogEntry.timelog_date == single_log_date
                        ).first()

                        if existing_entry:
                            existing_entry.project = project
                            existing_entry.task = task
                            existing_entry.notes = notes
                            existing_entry.start_time = start_time
                            existing_entry.end_time = end_time
                            existing_entry.logged_hours = logged_hours
                            total_updated += 1
                        else:
                            timelog_entry = ZohoTimelogEntry(
                                emp_id=emp_id,
                                timelog_date=single_log_date,
                                project=project,
                                task=task,
                                notes=notes,
                                task_log_id=task_log_id,
                                start_time=start_time,
                                end_time=end_time,
                                logged_hours=logged_hours
                            )
                            session.add(timelog_entry)
                            total_inserted += 1

                    except Exception as e:
                        timelog_logger.error(f"Error processing individual log for date {single_log_date}: {e}", exc_info=True)
                        total_skipped += 1

            session.commit()
            timelog_logger.info(f"Inserted {total_inserted} new timelogs.")
            timelog_logger.info(f"Updated {total_updated} existing timelogs.")
            if total_skipped > 0:
                timelog_logger.warning(f"Skipped {total_skipped} timelogs due to errors or missing data.")

        return True

    except Exception as e:
        timelog_logger.error(f"Error in process_and_save_timelogs: {e}", exc_info=True)
        return False



def process_timelogs(start_date, end_date):
    """
    Main function to fetch and save timelogs.
    """
    
    # Fetch timelogs
    response_data = fetch_zoho_timelogs(start_date, end_date)
    
    if response_data:
        # Process and save to database
        result=process_and_save_timelogs(response_data)
        # it return true or false
        if result:
            timelog_logger.info("Timelog data processed and saved successfully.")
            return True
        else:
            timelog_logger.error("Failed to process and save timelog data.")
            return False
    else:
        timelog_logger.error("No timelog data fetched to process")
        return False
