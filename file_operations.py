import pandas as pd
import os
from database import SessionLocal
from models import EmployeeMaster, DailyAttendance
from datetime import timedelta
from dotenv import load_dotenv  
from logger_config import attendance_logger


load_dotenv()  
RAW_FOLDER  = os.getenv("RAW_FOLDER")
PROCESSED_FOLDER  = os.getenv("PROCESSED_FOLDER")



def read_xls_file():
    """
    Automatically finds and reads the first .xls file from the specified folder.
    
    """
    try:
        # Check if folder exists
        if not os.path.exists(RAW_FOLDER):
            raise FileNotFoundError(f"Folder not found: {RAW_FOLDER}")
        
        # Find all .xls files in the folder
        xls_files = [f for f in os.listdir(RAW_FOLDER) if f.endswith('.xls')]
        
        if not xls_files:
            raise FileNotFoundError(f"No .xls files found in folder: {RAW_FOLDER}")
        
        # Use the first .xls file found
        filename = xls_files[0]
        file_path = os.path.join(RAW_FOLDER , filename)

        # Read the Excel file
        df = pd.read_excel(file_path, engine='xlrd')
        
        attendance_logger.info(f"Successfully read {filename}")
        attendance_logger.info(f"Shape: {df.shape}")
        
        if len(xls_files) > 1:
            attendance_logger.warning(f"Note: Multiple .xls files found. Loaded: {filename}")
            attendance_logger.warning(f"Other files: {xls_files[1:]}")
        
        return df, file_path
    
    except FileNotFoundError as e:
        attendance_logger.error(f"File error: {e}")
        return None, None
    except Exception as e:
        attendance_logger.error(f"Error reading file: {e}")
        return None, None



def is_date(value):
    if pd.isna(value):
        return False
    # Check if it's already a datetime object
    if isinstance(value, pd.Timestamp):
        return True
    # Try to parse as date
    try:
        pd.to_datetime(value)
        return True
    except:
        return False


def extract_user_info(full_name):
    """
    Extracts user ID and name from full_name string.
    Format expected: "ID - Name" (e.g., "101 - John Doe")
    
    Returns:
        tuple: (id, name) or (None, None) if parsing fails
    """
    try:
        parts = full_name.split('-')
        if len(parts) < 2:
            return None, None
        id = parts[0].strip()
        name = parts[1].strip()
        return id, name
    except Exception as e:
        attendance_logger.error(f"Error parsing user info from '{full_name}': {e}")
        return None, None


def get_or_create_users_batch(user_names):
    """
    Batch process all unique users from the file.
    Creates users that don't exist in a single database transaction.
    
    Args:
        user_names: List of user name strings ("ID - Name" format)
    
    Returns:
        dict: Mapping of user_name -> user_id
    """
    db = None
    user_id_map = {}
    
    try:
        db = SessionLocal()
        
        # Parse all user names
        user_info = {}
        for user_name in user_names:
            user_id, name = extract_user_info(user_name)
            if user_id and name:
                user_info[user_name] = {'id': user_id, 'name': name.title()}
        
        if not user_info:
            attendance_logger.warning("No valid user names found")
            return user_id_map
        
        # Get all user IDs from parsed data
        user_ids = [info['id'] for info in user_info.values()]
        
        # Fetch all existing users in ONE query
        existing_users = db.query(EmployeeMaster).filter(
            EmployeeMaster.id.in_(user_ids)
        ).all()
        
        # Create mapping of existing users
        existing_ids = {user.id for user in existing_users}
        
        # Build the user_id_map for existing users
        for user_name, info in user_info.items():
            if info['id'] in existing_ids:
                user_id_map[user_name] = info['id']
        
        # Identify users that need to be created
        new_users = []
        for user_name, info in user_info.items():
            if info['id'] not in existing_ids:
                new_users.append(
                    EmployeeMaster(id=info['id'], name=info['name'])
                )
                user_id_map[user_name] = info['id']
        
        # Bulk insert new users
        if new_users:
            db.bulk_save_objects(new_users)
            db.commit()
            attendance_logger.info(f"Created {len(new_users)} new users")
        
        attendance_logger.info(f"Total users processed: {len(user_id_map)}")
        return user_id_map
        
    except Exception as e:
        attendance_logger.error(f"Error in get_or_create_users_batch: {e}")
        if db:
            db.rollback()
        return {}
        
    finally:
        if db:
            db.close()


def clean_data(df):
    """
    Cleans and processes the attendance data from Excel file.
    NOW OPTIMIZED: Creates all users in a single batch operation.
    
    Args:
        df: Raw DataFrame from Excel file
    
    Returns:
        Cleaned DataFrame with user_id, Date, First IN, Last OUT, and Gross Hours
    """
    try:
        # Validate input
        if df is None or df.empty:
            raise ValueError("Input DataFrame is empty or None")
        
        # Set column headers from row 2
        df.columns = df.iloc[2]
        
        # Remove first 4 rows and reset index
        df_cleaned = df.iloc[4:].reset_index(drop=True)
        
        # STEP 1: Extract all unique user names first (no DB calls yet)
        unique_user_names = []
        for index, row in df_cleaned.iterrows():
            date_value = row['Date']
            if pd.notna(date_value) and not is_date(date_value):
                unique_user_names.append(date_value)
        
        # Remove duplicates
        unique_user_names = list(set(unique_user_names))
        attendance_logger.info(f"Found {len(unique_user_names)} unique users in file")
        
        # STEP 2: Batch create/fetch all users in ONE operation
        user_id_map = get_or_create_users_batch(unique_user_names)
        
        if not user_id_map:
            raise ValueError("Failed to process users")
        
        # STEP 3: Now process the data with the pre-loaded user_id_map
        df_cleaned['user_id'] = None
        current_user_id = None
        rows_to_drop = []
        
        for index, row in df_cleaned.iterrows():
            date_value = row['Date']
            
            # Check if the value is not a date and not NaN (indicates user name)
            if pd.notna(date_value) and not is_date(date_value):
                # Get user_id from our pre-loaded map
                current_user_id = user_id_map.get(date_value)
                rows_to_drop.append(index)
                continue
            
            # Skip rows with NaN dates
            if pd.isna(date_value):
                rows_to_drop.append(index)
                continue
            
            # Assign user_id to valid data rows
            df_cleaned.at[index, 'user_id'] = current_user_id
        
        # Drop invalid rows
        df_cleaned = df_cleaned.drop(rows_to_drop).reset_index(drop=True)
        
        # Select required columns
        keep_cols = ['user_id', 'Date', 'First IN', 'Last OUT', 'Gross Hours']
        
        # Check if all required columns exist
        missing_cols = [col for col in keep_cols if col not in df_cleaned.columns]
        if missing_cols:
            raise KeyError(f"Missing required columns: {missing_cols}")
        
        df_cleaned = df_cleaned[keep_cols]
        
        # Remove rows where user_id is still None
        df_cleaned = df_cleaned[df_cleaned['user_id'].notna()]
        
        attendance_logger.info(f"Data cleaned successfully. Shape: {df_cleaned.shape}")
        
        return df_cleaned
    
    except KeyError as e:
        attendance_logger.error(f"Column error: {e}")
        attendance_logger.error(f"Available columns: {list(df.columns)}")
        return None
    except ValueError as e:
        attendance_logger.error(f"Value error: {e}")
        return None
    except Exception as e:
        attendance_logger.error(f"Error cleaning data: {e}")
        import traceback
        attendance_logger.error(traceback.format_exc())
        return None


def load_data_to_db(df):
    """
    Loads attendance data from DataFrame to database.
    Updates existing records or inserts new ones (upsert operation).
    
    Args:
        df: DataFrame with columns: user_id, Date, First IN, Last OUT, Gross Hours
    
    Returns:
        bool: True if successful, False otherwise
    """
    db = None
    try:
        # Validate input
        if df is None or df.empty:
            attendance_logger.error("Error: DataFrame is empty or None")
            return False
        
        # Convert date/time columns
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        df['First IN'] = pd.to_datetime(df['First IN'], errors='coerce').dt.time
        df['Last OUT'] = pd.to_datetime(df['Last OUT'], errors='coerce').dt.time
        df['Gross Hours'] = pd.to_datetime(df['Gross Hours'], errors='coerce').dt.time
        
        # Replace NaT or NaN with None
        df = df.where(pd.notnull(df), None)
        
        # Remove rows with invalid dates
        df = df[df['Date'].notna()]
        
        if df.empty:
            attendance_logger.error("Error: No valid dates found after cleaning")
            return False
        
        df_min_date = df['Date'].min()
        df_max_date = df['Date'].max()
        
        attendance_logger.info(f"Processing data for date range: {df_min_date} to {df_max_date}")
        
        # Database operations
        db = SessionLocal()
        
        # Get all existing records for this date range as a dictionary for fast lookup
        existing_records_query = db.query(DailyAttendance).filter(
            DailyAttendance.attendance_date.between(df_min_date, df_max_date)
        ).all()
        
        # Create a dictionary: (emp_id, date) -> record object
        existing_records_dict = {
            (record.emp_id, record.attendance_date): record 
            for record in existing_records_query
        }
        
        inserted_count = 0
        updated_count = 0
        records_to_insert = []
        
        # Process each row
        for _, row in df.iterrows():
            key = (row['user_id'], row['Date'])
            
            if key in existing_records_dict:
                # Update existing record
                existing_record = existing_records_dict[key]
                existing_record.in_time = row['First IN']
                existing_record.out_time = row['Last OUT']
                existing_record.duration = row['Gross Hours']
                updated_count += 1
            else:
                # Prepare new record for bulk insert
                records_to_insert.append({
                    "emp_id": row['user_id'],
                    "attendance_date": row['Date'],
                    "in_time": row['First IN'],
                    "out_time": row['Last OUT'],
                    "duration": row['Gross Hours']
                })
        
        # Bulk insert new records
        if records_to_insert:
            db.bulk_insert_mappings(DailyAttendance, records_to_insert)
            inserted_count = len(records_to_insert)
        
        # Commit all changes (updates + inserts)
        db.commit()
        
        attendance_logger.info(f"✓ Inserted: {inserted_count} new records")
        attendance_logger.info(f"✓ Updated: {updated_count} existing records")
        attendance_logger.info(f"✓ Total processed: {inserted_count + updated_count} records")
        
        return True
    
    except Exception as e:
        if db:
            db.rollback()
        attendance_logger.error(f"Error loading data to database: {e}")
        import traceback
        attendance_logger.error(traceback.format_exc())
        return False
    
    finally:
        if db:
            db.close()

def move_file(file_path):
    """
    Moves a file to the processed_files directory.
    """
    try:
        # Validate file_path is not None
        if file_path is None:
            attendance_logger.error("Error: file_path is None")
            return False
        
        # Validate file exists
        if not os.path.exists(file_path):
            attendance_logger.error(f"File not found: {file_path}")
            return False
        
        if not PROCESSED_FOLDER:
            attendance_logger.error(f"Processed_Folder not set in .env.")
            return False
        
        # Create processed directory
        os.makedirs(PROCESSED_FOLDER , exist_ok=True)
        
        # Get new path
        filename = os.path.basename(file_path)
        new_path = os.path.join(PROCESSED_FOLDER , filename)
        
        # Handle duplicate filename
        if os.path.exists(new_path):
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            name, ext = os.path.splitext(filename)
            new_path = os.path.join(PROCESSED_FOLDER, f"{name}_{timestamp}{ext}")
        # Move file
        os.rename(file_path, new_path)
        attendance_logger.info(f"File moved to: {new_path}")
        return True
        
    except Exception as e:
        attendance_logger.error(f"Error moving file: {e}")
        return False

def process_file():
    """
    Process attendance file from specified folder.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:        
        # Read file
        df, file_path = read_xls_file()
        if df is None or file_path is None:
            attendance_logger.error("Failed to read file")
            return False
        
        # Clean data
        df_cleaned = clean_data(df)
        if df_cleaned is None or df_cleaned.empty:
            attendance_logger.error("Failed to clean data")
            return False
        
        # Load to database
        if not load_data_to_db(df_cleaned):
            attendance_logger.error("Failed to load data")
            return False
        
        # Move file
        if not move_file(file_path):
            attendance_logger.warning("Data loaded but file not moved")
        
        attendance_logger.info("Process completed successfully!")
        return True
        
    except Exception as e:
        attendance_logger.error(f"Error: {e}")
        return False
