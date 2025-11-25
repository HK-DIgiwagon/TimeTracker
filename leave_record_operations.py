import requests
import time
from datetime import datetime
from refresh_token import get_valid_access_token
from models import EmployeeMaster, EmployeeLeave
from database import SessionLocal
from logger_config import leave_logger


BASE_URL = "https://people.zoho.in/api/v2/leavetracker/leaves/records"
LIMIT = 200


def fetch_leave_records(start_date, end_date):
    """Fetch complete paginated leave data from Zoho People"""

    try:
        access_token = get_valid_access_token()
        leave_logger.info("Access token obtained successfully")
    except Exception as e:
        leave_logger.error(f"Failed to get access token: {e}", exc_info=True)
        return {}   # return empty dict instead of None

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json"
    }

    all_records = {}
    start_index = 0

    try:
        while True:
            params = {
                "from": start_date,
                "to": end_date,
                "limit": LIMIT,
                "startIndex": start_index,
                "dateFormat": "yyyy-MM-dd"
            }

            response = requests.get(BASE_URL, headers=headers, params=params)

            if response.status_code != 200:
                leave_logger.error(f"API failed [{response.status_code}]: {response.text}")
                return {}  # return empty dict on API failure

            data = response.json()
            records = data.get("records", {})

            leave_logger.info(f"Fetched {len(records)} records from index {start_index}")
            all_records |= records

            if len(records) < LIMIT:
                break

            start_index += LIMIT
            time.sleep(1)  # throttle to avoid rate limit

    except Exception as e:
        leave_logger.error(f"Unexpected error while fetching leave records: {e}", exc_info=True)
        return {}  # return empty dict on unexpected exception

    leave_logger.info(f"Total leave records fetched = {len(all_records)}")
    return all_records



def store_leave_records(records):
    """Store leave records into DB with duplicate check and approval filtering"""

    if not records:
        leave_logger.warning("No leave records received to store.")
        return False

    db = SessionLocal()
    try:
        # Cache employee names once
        emp_cache = {e.name.lower(): e.id for e in db.query(EmployeeMaster).all()}
        
        # Cache existing leaves to avoid hitting DB repeatedly
        existing_leaves = {(l.emp_id, l.leave_date,l.leave_type.value) for l in db.query(EmployeeLeave.emp_id, EmployeeLeave.leave_date,EmployeeLeave.leave_type)}
        new_entries = []

        for rec in records.values():

            # Skip non-approved leave records
            if rec.get("ApprovalStatus") != "Approved":
                continue

            emp_name = rec.get("Employee", "").strip().lower()
            emp_id = emp_cache.get(emp_name)

            if not emp_id:
                leave_logger.warning(f"Employee '{emp_name}' not found in database. Skipping.")
                continue

            reason = rec.get("Reason", "No Reason Provided")
            zoho_leave_type = rec.get("Leavetype")
            days = rec.get("Days", {})

            for date_str, single_day in days.items():



                leave_count = float(single_day.get("LeaveCount"))

                if leave_count == 0.5:
                    session = single_day.get("Session")
                    leave_type = "first_half" if session == 1 else "second_half"
                else:
                    leave_type = "full_day"

                # Skip if leave already exists
                if (emp_id, datetime.strptime(date_str, "%Y-%m-%d").date(), leave_type) in existing_leaves:
                    leave_logger.info(f"Leave for Employee ID {emp_id} on {date_str} {leave_type} leave already exists. Skipping.")
                    continue

                new_entries.append(EmployeeLeave(
                    emp_id=emp_id,
                    leave_date=date_str,
                    leave_type=leave_type,
                    zoho_leave_type=zoho_leave_type,
                    reason=reason
                ))

        # Insert only once after processing all records
        if new_entries:
            db.bulk_save_objects(new_entries)
            db.commit()
            leave_logger.info(f"{len(new_entries)} new leave records stored successfully.")
            return True   # success
        else:
            leave_logger.info("No new leave records to store.")
            return True

    except Exception as e:
        db.rollback()
        leave_logger.error(f"Database error while storing leave records: {e}", exc_info=True)
        return False     # on error

    finally:
        db.close()




def process_leave_data(start_date, end_date):

    records = fetch_leave_records(start_date, end_date)

    if not records:   # API failure or token issue
        return {"status": "failed", "message": "Failed to fetch leave records"}

    inserted = store_leave_records(records)
    
    if not inserted:
        return {"status": "failed", "message": "Failed to store leave records"}

    return {"status": "success", "message": f"Leave records stored Successfully"}



