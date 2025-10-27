from sqlalchemy import Column, Integer, String, Date, Time, ForeignKey, Interval, DateTime, func, Enum
from sqlalchemy.orm import relationship
from database import Base
import enum

# Base model with timestamps
class TimeStampedModel(Base):
    __abstract__ = True
    created = Column(DateTime(timezone=True), server_default=func.now())
    modified = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

# Employee master table
class EmployeeMaster(TimeStampedModel):
    __tablename__ = "employee_master"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=True)
    department = Column(String, nullable=True)
    phone = Column(String, nullable=True)

    # Relationships
    attendances = relationship("DailyAttendance", back_populates="employee")
    leaves = relationship("EmployeeLeave", back_populates="employee")
    timelogs = relationship("ZohoTimelogEntry", back_populates="employee") 

# Daily attendance table
class DailyAttendance(TimeStampedModel):
    __tablename__ = "daily_attendance"

    id = Column(Integer, primary_key=True, index=True)
    emp_id = Column(String, ForeignKey("employee_master.id"), nullable=False)
    attendance_date = Column(Date, nullable=False)
    in_time = Column(Time, nullable=True)
    out_time = Column(Time, nullable=True)
    duration = Column(Time, nullable=True)

    employee = relationship("EmployeeMaster", back_populates="attendances")

# Enum for leave type
class LeaveTypeEnum(str, enum.Enum):
    full_day = "Full Day"
    first_half = "First Half"
    second_half = "Second Half"

# Employee leave table
class EmployeeLeave(TimeStampedModel):
    __tablename__ = "employee_leave"

    id = Column(Integer, primary_key=True, index=True)
    emp_id = Column(String, ForeignKey("employee_master.id"), nullable=False)
    leave_date = Column(Date, nullable=False)
    leave_type = Column(Enum(LeaveTypeEnum), nullable=False)

    employee = relationship("EmployeeMaster", back_populates="leaves")


class ZohoTimelogEntry(TimeStampedModel):
    __tablename__ = "zoho_timelog_entry"

    id = Column(Integer, primary_key=True, index=True)
    emp_id = Column(String, ForeignKey("employee_master.id"), nullable=False)
    timelog_date = Column(Date, nullable=False)
    project = Column(String, nullable=False)
    task = Column(String, nullable=False)
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)
    logged_hours = Column(Time, nullable=False)

    # Relationship to employee
    employee = relationship("EmployeeMaster", back_populates="timelogs")
