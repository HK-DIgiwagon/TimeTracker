from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import engine, get_db
import models
from file_operations import process_file

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

# Process attendance file
@app.post("/process-attendance/")
def process_attendance_file():
    """
    Process attendance .xls file from load_file folder.
    Reads, cleans, loads to DB, and moves to processed_files folder.
    """
    try:
        success = process_file()
        
        if success:
            return {
                "status": "success",
                "message": "Attendance file processed successfully"
            }
        else:
            raise HTTPException(
                status_code=400,
                detail="Failed to process attendance file"
            )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing file: {str(e)}"
        )