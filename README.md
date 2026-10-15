# Employee Attendance & Analytics API

FastAPI and MongoDB backend service for employee attendance tracking, manual regularization with audit trails, and reporting analytics powered by native MongoDB aggregation pipelines.

## Prerequisites
- Python 3.11+
- MongoDB 6.0 or newer (tested against MongoDB 7.0/9.0)

## Installation & Setup

1. **Clone and setup virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Environment Variables**:
   Configure `MONGO_URI` and `MONGO_DB` in your environment (or create a local `.env`):
   ```bash
   MONGO_URI="mongodb://localhost:27017"
   MONGO_DB="attendance_db"
   ```
   *(Note: Operating system environment variables always take precedence over `.env`)*

3. **Seed Sample Data (Optional)**:
   ```bash
   python sample_seed.py
   ```

## Running the API

Start the service using Uvicorn from the repository root:
```bash
uvicorn app.main:app --port 8000
```
Interactive OpenAPI documentation will be accessible at `http://localhost:8000/docs`.

All database indexes (unique constraints and compound query indexes) are automatically and idempotently initialized on application startup.

## Features & Implementation Overview

- **Health Probe (`GET /health`)**: Returns 200 when MongoDB answers a ping, else 503.
- **Employee Management (`/employees`)**: Unique `emp_code` enforcement, input validation, and pagination.
- **Punch-in & Punch-out (`/attendance/punch-*`)**: Race-condition safe atomic operations supporting standard and overnight shifts (R1), 10-minute grace period late calculations (R2), overtime calculations with 30-minute threshold (R3), exact decimal half-up work hours (R4), and half-day status (R5).
- **Manual Regularization (`PATCH /attendance/{emp_code}/{date}`)**: Audit trail tracking field-level before/after changes with optimistic concurrency protection.
- **Analytics Aggregations**:
  - `GET /analytics/employees/{emp_code}/monthly`: Mid-month joiner working days (R7) and attendance rates.
  - `GET /analytics/departments/summary`: Cross-collection aggregation accounting for zero-log employees in headcount (R9).
  - `GET /analytics/leaderboard/late`: Window-function standard competition ranking (`$setWindowFields` with `$rank`).
  - `GET /analytics/departments/{department}/trend`: Calendar-gap densification (`$densify`) with rolling 7-day moving averages.
- **Query Optimization (`GET /admin/explain/{endpoint}`)**: Verifies execution plans utilize `IXSCAN` and contain no `COLLSCAN`.

## Project Structure
- `app/main.py`: Complete application code with endpoints, models, and aggregation pipelines.
- `requirements.txt`: Python package dependencies.
- `REVIEW.md`: Comprehensive audit and defect analysis of the starter codebase.
- `DECISIONS.md`: Concise explanation of architectural and database design decisions.
- `README.md`: System documentation and run instructions.
