# logger_config.py
import logging
import os

# Create logs directory if not exists
os.makedirs("logs", exist_ok=True)

# --- Attendance Logger ---
attendance_logger = logging.getLogger("attendance_logger")
attendance_logger.setLevel(logging.INFO)

attendance_handler = logging.FileHandler("logs/attendance.log")
attendance_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
attendance_handler.setFormatter(attendance_formatter)

if not attendance_logger.hasHandlers():
    attendance_logger.addHandler(attendance_handler)


# --- Timelog Logger ---
timelog_logger = logging.getLogger("timelog_logger")
timelog_logger.setLevel(logging.INFO)

timelog_handler = logging.FileHandler("logs/timelog.log")
timelog_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
timelog_handler.setFormatter(timelog_formatter)

if not timelog_logger.hasHandlers():
    timelog_logger.addHandler(timelog_handler)


# --- Timelog Logger ---
leave_logger = logging.getLogger("leave_logger")
leave_logger.setLevel(logging.INFO)

leave_handler = logging.FileHandler("logs/leave.log")
leave_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
leave_handler.setFormatter(leave_formatter)

if not leave_logger.hasHandlers():
    leave_logger.addHandler(leave_handler)