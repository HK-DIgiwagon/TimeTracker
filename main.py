from fastapi import FastAPI, Depends, HTTPException,Query
from sqlalchemy.orm import Session
from database import engine, get_db
import models
from file_operations import process_file
from timelog_operations import process_timelogs
from sqlalchemy import func, and_
from models import ZohoTimelogEntry, DailyAttendance, EmployeeMaster
from datetime import date
from logger_config import attendance_logger, timelog_logger
from sqlalchemy import cast, Time


# Create tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Employee Attendance Demo")

@app.get("/")
def read_root():
    return {"message": "Employee Attendance API is running"}

# Example: Get all employees
@app.get("/employees/")
def get_employees(db: Session = Depends(get_db)):
    return db.query(models.EmployeeMaster).all()


@app.get("/process-attendance")
def process_attendance_file():
    """Process attendance .xls file."""
    attendance_logger.info("Triggered /process-attendance endpoint")
    try:
        success = process_file()
        if success:
            attendance_logger.info("Attendance file processed successfully")
            return {"status": "success", "message": "Attendance file processed successfully"}
        else:
            attendance_logger.error("Failed to process attendance file")
            raise HTTPException(status_code=400, detail="Failed to process attendance file")
    except Exception as e:
        attendance_logger.exception(f"Error processing attendance file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.get("/process-timelog")
def process_timelog(start_date: str = Query(..., description="Start date in YYYY-MM-DD format"), end_date: str = Query(..., description="End date in YYYY-MM-DD format")):
    """Process timelog data from Zoho Projects API."""
    timelog_logger.info(f"Triggered /process-timelog for range {start_date} → {end_date}")
    try:
        success = process_timelogs(start_date, end_date)
        if success:
            timelog_logger.info("Timelog data processed successfully")
            return {"status": "success", "message": "Timelog data processed successfully"}
        else:
            timelog_logger.error("Failed to process timelog data")
            raise HTTPException(status_code=400, detail="Failed to process timelog data")
    except Exception as e:
        timelog_logger.exception(f"Error processing timelog data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")



@app.get("/timelog-summary")
def get_timelog_summary(start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),end_date: str = Query(..., description="End date in YYYY-MM-DD format"),db: Session = Depends(get_db)):
    """
    Fetch total Zoho logged hours per employee per date,
    joined with attendance and employee master.
    """
    results = (
        db.query(
            ZohoTimelogEntry.emp_id,
            EmployeeMaster.name,
            EmployeeMaster.email,
            ZohoTimelogEntry.timelog_date,
            func.sum(ZohoTimelogEntry.logged_hours).label("zoho_hours"),
            DailyAttendance.duration.label("matrix_hours")
        )
        .join(EmployeeMaster, ZohoTimelogEntry.emp_id == EmployeeMaster.id)
        .outerjoin(
            DailyAttendance,
            and_(
                ZohoTimelogEntry.emp_id == DailyAttendance.emp_id,
                ZohoTimelogEntry.timelog_date == DailyAttendance.attendance_date,
            )
        )
        .filter(
            ZohoTimelogEntry.timelog_date.between(start_date, end_date)
        )
        .group_by(
            ZohoTimelogEntry.emp_id,
            EmployeeMaster.name,
            EmployeeMaster.email,
            ZohoTimelogEntry.timelog_date,
            DailyAttendance.duration
        )
        .order_by(
            ZohoTimelogEntry.emp_id,
            ZohoTimelogEntry.timelog_date
        )
        .all()
    )

    # Convert to list of dicts for JSON response
    return [
        {
            "emp_id": r.emp_id,
            "name": r.name,
            "email": r.email,
            "timelog_date": r.timelog_date,
            "zoho_hours": str(r.zoho_hours),
            "matrix_hours": str(r.matrix_hours) if r.matrix_hours else None
        }
        for r in results
    ]



@app.get("/get-late_comers")
def get_late_comers(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    db: Session = Depends(get_db)
):
    """
    Fetch late comers grouped by employee, showing how many times each was late.
    """
    late_comers = (
        db.query(
            DailyAttendance.emp_id,
            EmployeeMaster.name,
            EmployeeMaster.email,
            func.count(DailyAttendance.id).label("late_count")
        )
        .join(EmployeeMaster, DailyAttendance.emp_id == EmployeeMaster.id)
        .filter(
            DailyAttendance.attendance_date.between(start_date, end_date),
            DailyAttendance.in_time >= '10:11:00'  # ✅ Use string comparison for time field
        )
        .group_by(DailyAttendance.emp_id, EmployeeMaster.name, EmployeeMaster.email)
        .order_by(func.count(DailyAttendance.id).desc())
        .all()
    )

    return [
        {
            "emp_id": r.emp_id,
            "name": r.name,
            "email": r.email,
            "late_count": r.late_count
        }
        for r in late_comers
    ]