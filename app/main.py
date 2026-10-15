"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded if present; real env vars take priority)
"""
import calendar
from contextlib import asynccontextmanager
from datetime import date as dt_date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import os
import re
from typing import Annotated, Any, Dict, List, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path, status
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

client: MongoClient = MongoClient(MONGO_URI, tz_aware=True)
db = client[MONGO_DB]

IST = timezone(timedelta(hours=5, minutes=30))


def ensure_indexes(target_db):
    """Create all required indexes idempotently at startup."""
    # employees collection
    target_db.employees.create_index([("emp_code", ASCENDING)], unique=True)
    target_db.employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)])
    target_db.employees.create_index([("joined_on", ASCENDING)])

    # attendance_logs collection
    target_db.attendance_logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True)
    target_db.attendance_logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)])
    target_db.attendance_logs.create_index([("date", ASCENDING), ("late_minutes", ASCENDING)])
    target_db.attendance_logs.create_index([("status", ASCENDING), ("date", DESCENDING)])
    target_db.attendance_logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure indexes at startup
    ensure_indexes(db)
    yield
    client.close()


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def round_half_up(val: Optional[float], decimals: int = 2) -> Optional[float]:
    """R8 Rounding: exact half-up rounding (rates: 4 decimals, numbers: 2 decimals)."""
    if val is None:
        return None
    q = Decimal("10") ** -decimals
    return float(Decimal(str(val)).quantize(q, rounding=ROUND_HALF_UP))


def truncate_ms_to_seconds(ms: int) -> int:
    """R1: Instants are truncated to whole seconds before calculation."""
    return (ms // 1000) * 1000


def epoch_ms_to_dt_utc(ms: int) -> datetime:
    """Convert integer epoch ms to timezone-aware UTC datetime."""
    truncated_ms = truncate_ms_to_seconds(ms)
    return datetime.fromtimestamp(truncated_ms / 1000, tz=timezone.utc)


def dt_to_epoch_ms(dt: Optional[datetime]) -> Optional[int]:
    """Convert datetime to integer epoch milliseconds."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def compute_late_minutes(punch_in_ist: datetime, shift_start: str, date_str: str) -> int:
    """
    R2: Grace period 10 minutes. Late only if punch-in strictly > 10:00 after shift_start.
    Then whole minutes (floor) since shift_start.
    """
    sh, sm = map(int, shift_start.split(":"))
    y, m, d = map(int, date_str.split("-"))
    shift_start_dt_ist = datetime(y, m, d, sh, sm, 0, tzinfo=IST)
    diff_sec = int((punch_in_ist - shift_start_dt_ist).total_seconds())
    if diff_sec > 600:
        return diff_sec // 60
    return 0


def compute_work_hours(punch_in_utc: datetime, punch_out_utc: datetime) -> float:
    """R4: (punch_out - punch_in) in seconds / 3600, rounded to 2 decimals half-up."""
    diff_sec = int((punch_out_utc - punch_in_utc).total_seconds())
    hours_dec = Decimal(diff_sec) / Decimal(3600)
    return float(hours_dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out_utc: datetime, shift_start: str, shift_end: str, date_str: str) -> int:
    """
    R3: Overtime = whole minutes (floor) between shift_end and punch_out,
    counted only when >= 30; otherwise 0. Works across midnight for overnight shifts.
    """
    eh, em = map(int, shift_end.split(":"))
    y, m, d = map(int, date_str.split("-"))
    shift_date = dt_date(y, m, d)
    if shift_end <= shift_start:
        # Overnight shift ends on the next calendar day
        shift_date += timedelta(days=1)
    shift_end_dt_ist = datetime(shift_date.year, shift_date.month, shift_date.day, eh, em, 0, tzinfo=IST)
    punch_out_ist = punch_out_utc.astimezone(IST)
    diff_sec = int((punch_out_ist - shift_end_dt_ist).total_seconds())
    if diff_sec >= 1800:
        return diff_sec // 60
    return 0


def get_attendance_date(punch_in_ist: datetime, shift_start: str, shift_end: str) -> str:
    """
    R1: Attendance date is IST calendar date of punch_in.
    Exception: overnight shifts (shift_end <= shift_start) where punch_in < shift_end
    belongs to the shift starting the previous day.
    """
    ist_date = punch_in_ist.date()
    ist_time = punch_in_ist.strftime("%H:%M")
    if shift_end <= shift_start and ist_time < shift_end:
        ist_date -= timedelta(days=1)
    return ist_date.isoformat()


def serialize_changes_for_mongo(changes: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Changes object stored in MongoDB: punch_in/punch_out as BSON dates."""
    res = {}
    for k, v in changes.items():
        res[k] = {"from": v["from"], "to": v["to"]}
    return res


def serialize_history_entry_for_api(entry: dict) -> dict:
    """Serialize a MongoDB history entry for API output."""
    changes_api = {}
    for field, change in entry.get("changes", {}).items():
        from_val = change.get("from")
        to_val = change.get("to")
        if field in ("punch_in", "punch_out"):
            from_val = dt_to_epoch_ms(from_val) if isinstance(from_val, datetime) else from_val
            to_val = dt_to_epoch_ms(to_val) if isinstance(to_val, datetime) else to_val
        changes_api[field] = {"from": from_val, "to": to_val}

    at_val = entry.get("at")
    if isinstance(at_val, datetime):
        at_ms = dt_to_epoch_ms(at_val)
    elif isinstance(at_val, (int, float)):
        at_ms = int(at_val)
    else:
        at_ms = at_val

    return {
        "at": at_ms,
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": changes_api,
    }


def format_attendance_record(doc: dict) -> dict:
    """Format MongoDB attendance_logs document to AttendanceRecord response schema."""
    history = [serialize_history_entry_for_api(h) for h in doc.get("history", [])]
    return {
        "emp_code": doc.get("emp_code"),
        "date": doc.get("date"),
        "status": doc.get("status"),
        "punch_in": dt_to_epoch_ms(doc.get("punch_in")),
        "punch_out": dt_to_epoch_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes", 0),
        "overtime_minutes": doc.get("overtime_minutes", 0),
        "half_day": doc.get("half_day", False),
        "history": history,
    }


# --------------------------------------------------------------------------- #
# Pydantic Schemas
# --------------------------------------------------------------------------- #
EpochMillis = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
StatusType = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatusType = Literal["PRESENT", "WFH", "ON_DUTY"]


class EmployeeCreate(BaseModel):
    emp_code: Annotated[str, Field(pattern=r"^EMP\d{4,6}$")]
    name: Annotated[str, Field(min_length=1, max_length=100)]
    email: Annotated[str, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, Field(min_length=1, max_length=50)]
    shift_start: Annotated[str, Field(default="09:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]
    shift_end: Annotated[str, Field(default="18:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]
    joined_on: Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")]

    @model_validator(mode="after")
    def check_shift(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        # Validate date validity
        try:
            dt_date.fromisoformat(self.joined_on)
        except ValueError:
            raise ValueError("joined_on must be a valid calendar date YYYY-MM-DD")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: PresenceStatusType = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None


class RegularizeRequest(BaseModel):
    status: Optional[StatusType] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None
    reason: Annotated[str, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[str, Field(min_length=1, max_length=50)]


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health", tags=["system"])
def health():
    """Readiness probe: 200 only if MongoDB answers a ping, otherwise 503."""
    try:
        client.admin.command("ping")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable")


@app.post("/employees", status_code=status.HTTP_201_CREATED, tags=["employees"])
def create_employee(body: EmployeeCreate):
    """Create an employee. Unique emp_code enforced by database unique index."""
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    doc = body.model_dump()
    doc["created_at"] = now_utc
    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="emp_code already exists")

    doc.pop("_id", None)
    doc["created_at"] = dt_to_epoch_ms(doc["created_at"])
    return doc


@app.get("/employees", tags=["employees"])
def list_employees(
    department: Optional[str] = Query(None, description="Exact, case-sensitive match."),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List employees sorted by emp_code ascending."""
    q: Dict[str, Any] = {}
    if department is not None:
        q["department"] = department

    total = db.employees.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip(skip).limit(page_size)
    items = []
    for doc in cursor:
        doc["created_at"] = dt_to_epoch_ms(doc.get("created_at"))
        items.append(doc)

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=status.HTTP_201_CREATED, tags=["attendance"])
def punch_in(body: PunchInRequest):
    """Record punch-in for employee and create day's record (R1, R2). Race-safe."""
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")

    if body.punched_at is not None:
        p_ms = truncate_ms_to_seconds(body.punched_at)
        p_dt_utc = datetime.fromtimestamp(p_ms / 1000, tz=timezone.utc)
    else:
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        p_ms = int(now_dt.timestamp() * 1000)
        p_dt_utc = now_dt

    p_dt_ist = p_dt_utc.astimezone(IST)
    att_date = get_attendance_date(p_dt_ist, emp["shift_start"], emp["shift_end"])
    late_minutes = compute_late_minutes(p_dt_ist, emp["shift_start"], att_date)

    doc = {
        "emp_code": body.emp_code,
        "date": att_date,
        "status": body.status,
        "punch_in": p_dt_utc,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": late_minutes,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="already punched in for this date")

    return format_attendance_record(doc)


@app.post("/attendance/punch-out", status_code=status.HTTP_200_OK, tags=["attendance"])
def punch_out(body: PunchOutRequest):
    """
    Record punch-out. Closes employee's most recent record whose punch_in <= punched_at.
    Computes work_hours (R4), overtime_minutes (R3), and half_day (R5). Race-safe.
    """
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")

    if body.punched_at is not None:
        p_ms = truncate_ms_to_seconds(body.punched_at)
        p_dt_utc = datetime.fromtimestamp(p_ms / 1000, tz=timezone.utc)
    else:
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        p_ms = int(now_dt.timestamp() * 1000)
        p_dt_utc = now_dt

    # Find most recent punch_in <= punched_at
    record = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": p_dt_utc, "$ne": None}},
        sort=[("punch_in", DESCENDING)],
    )
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No punch-in found")

    if record.get("punch_out") is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Record already punched out")

    punch_in_utc = record["punch_in"]
    if punch_in_utc.tzinfo is None:
        punch_in_utc = punch_in_utc.replace(tzinfo=timezone.utc)

    diff_sec = (p_dt_utc - punch_in_utc).total_seconds()
    if diff_sec <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["body", "punched_at"], "msg": "punched_at must be after punch_in", "type": "value_error"}],
        )
    if diff_sec > 86400:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["body", "punched_at"], "msg": "punch_out cannot be more than 24 hours after punch_in", "type": "value_error"}],
        )

    work_hours = compute_work_hours(punch_in_utc, p_dt_utc)
    overtime_minutes = compute_overtime(p_dt_utc, emp["shift_start"], emp["shift_end"], record["date"])
    half_day = bool(work_hours < 4.50)

    # Atomic update guaranteeing that only one concurrent punch-out succeeds
    updated = db.attendance_logs.find_one_and_update(
        {
            "emp_code": body.emp_code,
            "date": record["date"],
            "punch_out": None,
        },
        {
            "$set": {
                "punch_out": p_dt_utc,
                "work_hours": work_hours,
                "overtime_minutes": overtime_minutes,
                "half_day": half_day,
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Record already punched out")

    return format_attendance_record(updated)


@app.get("/attendance", tags=["attendance"])
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status_filter: Optional[StatusType] = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List attendance records sorted by date descending, then emp_code ascending."""
    if date_from and date_to and date_from > date_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["query", "date_from"], "msg": "date_from must be <= date_to", "type": "value_error"}],
        )

    q: Dict[str, Any] = {}
    if emp_code:
        q["emp_code"] = emp_code
    if status_filter:
        q["status"] = status_filter
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to

    total = db.attendance_logs.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.attendance_logs.find(q).sort([("date", DESCENDING), ("emp_code", ASCENDING)]).skip(skip).limit(page_size)

    items = [format_attendance_record(doc) for doc in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize_attendance(
    emp_code: str,
    date: str = Path(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    body: RegularizeRequest = ...,
):
    """Regularize (manually correct) one day's record with audit trail."""
    emp = db.employees.find_one({"emp_code": emp_code})
    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if not emp or not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee or attendance record not found")

    provided_fields = body.model_dump(exclude_unset=True)
    if "status" not in provided_fields and "punch_in" not in provided_fields and "punch_out" not in provided_fields:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["body"], "msg": "At least one mutable field must be provided", "type": "value_error"}],
        )

    curr_status = record.get("status")
    curr_punch_in = record.get("punch_in")
    if curr_punch_in and curr_punch_in.tzinfo is None:
        curr_punch_in = curr_punch_in.replace(tzinfo=timezone.utc)
    curr_punch_out = record.get("punch_out")
    if curr_punch_out and curr_punch_out.tzinfo is None:
        curr_punch_out = curr_punch_out.replace(tzinfo=timezone.utc)
    curr_work_hours = record.get("work_hours")
    curr_late_minutes = record.get("late_minutes", 0)
    curr_overtime_minutes = record.get("overtime_minutes", 0)
    curr_half_day = record.get("half_day", False)

    new_status = body.status if body.status is not None else curr_status

    if new_status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["body"], "msg": "Supplying punch_in or punch_out with ABSENT or LEAVE is not allowed", "type": "value_error"}],
            )
        new_punch_in_dt = None
        new_punch_out_dt = None
        new_work_hours = None
        new_late_minutes = 0
        new_overtime_minutes = 0
        new_half_day = False
    else:
        # Presence status: requires a punch_in (supplied or already stored)
        if body.punch_in is not None:
            p_in_ms = truncate_ms_to_seconds(body.punch_in)
            new_punch_in_dt = datetime.fromtimestamp(p_in_ms / 1000, tz=timezone.utc)
        else:
            new_punch_in_dt = curr_punch_in

        if new_punch_in_dt is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["body", "punch_in"], "msg": "Presence status requires punch_in", "type": "value_error"}],
            )

        # R1: punch_in must stay on the record's attendance date
        p_in_ist = new_punch_in_dt.astimezone(IST)
        calc_date = get_attendance_date(p_in_ist, emp["shift_start"], emp["shift_end"])
        if calc_date != date:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["body", "punch_in"], "msg": "punch_in must stay on the record's attendance date", "type": "value_error"}],
            )

        if body.punch_out is not None:
            p_out_ms = truncate_ms_to_seconds(body.punch_out)
            new_punch_out_dt = datetime.fromtimestamp(p_out_ms / 1000, tz=timezone.utc)
        else:
            new_punch_out_dt = curr_punch_out

        if new_punch_out_dt is not None:
            diff_sec = (new_punch_out_dt - new_punch_in_dt).total_seconds()
            if diff_sec <= 0:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=[{"loc": ["body", "punch_out"], "msg": "punch_out must be after punch_in", "type": "value_error"}],
                )
            if diff_sec > 86400:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=[{"loc": ["body", "punch_out"], "msg": "punch_out cannot be more than 24 hours after punch_in", "type": "value_error"}],
                )
            new_work_hours = compute_work_hours(new_punch_in_dt, new_punch_out_dt)
            new_overtime_minutes = compute_overtime(new_punch_out_dt, emp["shift_start"], emp["shift_end"], date)
            new_half_day = bool(new_work_hours < 4.50)
        else:
            new_work_hours = None
            new_overtime_minutes = 0
            new_half_day = False

        new_late_minutes = compute_late_minutes(p_in_ist, emp["shift_start"], date)

    # Calculate exact changes
    changes: Dict[str, Dict[str, Any]] = {}
    if curr_status != new_status:
        changes["status"] = {"from": curr_status, "to": new_status}
    if curr_punch_in != new_punch_in_dt:
        changes["punch_in"] = {"from": curr_punch_in, "to": new_punch_in_dt}
    if curr_punch_out != new_punch_out_dt:
        changes["punch_out"] = {"from": curr_punch_out, "to": new_punch_out_dt}
    if curr_work_hours != new_work_hours:
        changes["work_hours"] = {"from": curr_work_hours, "to": new_work_hours}
    if curr_late_minutes != new_late_minutes:
        changes["late_minutes"] = {"from": curr_late_minutes, "to": new_late_minutes}
    if curr_overtime_minutes != new_overtime_minutes:
        changes["overtime_minutes"] = {"from": curr_overtime_minutes, "to": new_overtime_minutes}
    if curr_half_day != new_half_day:
        changes["half_day"] = {"from": curr_half_day, "to": new_half_day}

    if not changes:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["body"], "msg": "A request that changes nothing is 422", "type": "value_error"}],
        )

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    history_entry = {
        "at": now_utc,
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": serialize_changes_for_mongo(changes),
    }

    curr_history_len = len(record.get("history", []))

    # Optimistic locking on history length to prevent concurrent lost updates
    updated = db.attendance_logs.find_one_and_update(
        {
            "emp_code": emp_code,
            "date": date,
            "history": {"$size": curr_history_len},
        },
        {
            "$set": {
                "status": new_status,
                "punch_in": new_punch_in_dt,
                "punch_out": new_punch_out_dt,
                "work_hours": new_work_hours,
                "late_minutes": new_late_minutes,
                "overtime_minutes": new_overtime_minutes,
                "half_day": new_half_day,
            },
            "$push": {
                "history": history_entry,
            },
        },
        return_document=ReturnDocument.AFTER,
    )

    if not updated:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Concurrent modification conflict")

    return format_attendance_record(updated)


@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def employee_monthly(
    emp_code: str,
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
):
    """Monthly attendance summary for one employee (MongoDB aggregation pipeline)."""
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")

    year, m_int = map(int, month.split("-"))
    num_days = calendar.monthrange(year, m_int)[1]
    start_date = f"{month}-01"
    end_date = f"{month}-{num_days:02d}"

    # R7: Working days Mon-Fri from joined_on for mid-month joiners
    joined_on = emp["joined_on"]
    working_days = 0
    if joined_on <= end_date:
        eff_start = max(start_date, joined_on)
        curr = dt_date.fromisoformat(eff_start)
        last = dt_date.fromisoformat(end_date)
        while curr <= last:
            if curr.weekday() < 5:
                working_days += 1
            curr += timedelta(days=1)

    pipeline = [
        {
            "$match": {
                "emp_code": emp_code,
                "date": {"$gte": start_date, "$lte": end_date},
            }
        },
        {
            "$project": {
                "date": 1,
                "status": 1,
                "late_minutes": {"$ifNull": ["$late_minutes", 0]},
                "overtime_minutes": {"$ifNull": ["$overtime_minutes", 0]},
                "half_day": {"$ifNull": ["$half_day", False]},
                "dayOfWeek": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
            }
        },
        {
            "$group": {
                "_id": None,
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$dayOfWeek", [2, 3, 4, 5, 6]]},
                                    {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                ]
                            },
                            {"$cond": ["$half_day", 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "leave_days": {
                    "$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}
                },
                "total_late_minutes": {"$sum": "$late_minutes"},
                "total_overtime_minutes": {"$sum": "$overtime_minutes"},
            }
        },
    ]

    agg_result = list(db.attendance_logs.aggregate(pipeline))
    if agg_result:
        row = agg_result[0]
        present_days = float(row.get("present_days", 0.0))
        leave_days = int(row.get("leave_days", 0))
        late_count = int(row.get("late_count", 0))
        total_late_minutes = int(row.get("total_late_minutes", 0))
        total_overtime_minutes = int(row.get("total_overtime_minutes", 0))
    else:
        present_days = 0.0
        leave_days = 0
        late_count = 0
        total_late_minutes = 0
        total_overtime_minutes = 0

    if working_days > 0:
        attendance_pct = round_half_up((present_days / working_days) * 100, 2)
    else:
        attendance_pct = None

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late_minutes,
        "total_overtime_minutes": total_overtime_minutes,
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary", tags=["analytics"])
def department_summary(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = Query(None, description="Restrict to one department (exact match)."),
):
    """
    Department summary for a month via MongoDB aggregation across both collections.
    Headcount per R9 includes employees with 0 logs who joined on or before month end.
    """
    year, m_int = map(int, month.split("-"))
    num_days = calendar.monthrange(year, m_int)[1]
    start_date = f"{month}-01"
    end_date = f"{month}-{num_days:02d}"

    match_stage: Dict[str, Any] = {"joined_on": {"$lte": end_date}}
    if department:
        match_stage["department"] = department

    pipeline = [
        {"$match": match_stage},
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"emp": "$emp_code"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$emp_code", "$$emp"]},
                                    {"$gte": ["$date", start_date]},
                                    {"$lte": ["$date", end_date]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "date": 1,
                            "status": 1,
                            "work_hours": 1,
                            "late_minutes": {"$ifNull": ["$late_minutes", 0]},
                            "half_day": {"$ifNull": ["$half_day", False]},
                            "dayOfWeek": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
                        }
                    },
                ],
                "as": "logs",
            }
        },
        {
            "$project": {
                "department": 1,
                "emp_code": 1,
                "emp_present_days": {
                    "$sum": {
                        "$map": {
                            "input": "$logs",
                            "as": "l",
                            "in": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$in": ["$$l.dayOfWeek", [2, 3, 4, 5, 6]]},
                                            {"$in": ["$$l.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                        ]
                                    },
                                    {"$cond": ["$$l.half_day", 0.5, 1.0]},
                                    0.0,
                                ]
                            },
                        }
                    }
                },
                "emp_work_hours": {
                    "$filter": {
                        "input": {
                            "$map": {
                                "input": "$logs",
                                "as": "l",
                                "in": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$in": ["$$l.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                                {"$ne": ["$$l.work_hours", None]},
                                            ]
                                        },
                                        "$$l.work_hours",
                                        None,
                                    ]
                                },
                            }
                        },
                        "as": "wh",
                        "cond": {"$ne": ["$$wh", None]},
                    }
                },
                "emp_late_count": {
                    "$size": {
                        "$filter": {
                            "input": "$logs",
                            "as": "l",
                            "cond": {"$gt": ["$$l.late_minutes", 0]},
                        }
                    }
                },
                "emp_total_late_minutes": {"$sum": "$logs.late_minutes"},
                "emp_leave_count": {
                    "$size": {
                        "$filter": {
                            "input": "$logs",
                            "as": "l",
                            "cond": {"$eq": ["$$l.status", "LEAVE"]},
                        }
                    }
                },
                "emp_on_duty_count": {
                    "$size": {
                        "$filter": {
                            "input": "$logs",
                            "as": "l",
                            "cond": {"$eq": ["$$l.status", "ON_DUTY"]},
                        }
                    }
                },
            }
        },
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": "$emp_present_days"},
                "all_work_hours": {"$push": "$emp_work_hours"},
                "late_count": {"$sum": "$emp_late_count"},
                "total_late_minutes": {"$sum": "$emp_total_late_minutes"},
                "leave_count": {"$sum": "$emp_leave_count"},
                "on_duty_count": {"$sum": "$emp_on_duty_count"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": 1,
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
                "flat_work_hours": {
                    "$reduce": {
                        "input": "$all_work_hours",
                        "initialValue": [],
                        "in": {"$concatArrays": ["$$value", "$$this"]},
                    }
                },
            }
        },
        {
            "$project": {
                "department": 1,
                "headcount": 1,
                "present_days": 1,
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": [{"$size": "$flat_work_hours"}, 0]},
                        {"$round": [{"$avg": "$flat_work_hours"}, 2]},
                        None,
                    ]
                },
            }
        },
        {"$sort": {"department": ASCENDING}},
    ]

    items = list(db.employees.aggregate(pipeline))
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
def late_leaderboard(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    """
    Late leaderboard for a month using standard competition ranking ($rank).
    Limit applies after ranking (rank <= limit).
    """
    year, m_int = map(int, month.split("-"))
    num_days = calendar.monthrange(year, m_int)[1]
    start_date = f"{month}-01"
    end_date = f"{month}-{num_days:02d}"

    match_stage = {
        "date": {"$gte": start_date, "$lte": end_date},
        "late_minutes": {"$gt": 0},
    }

    pipeline = [
        {"$match": match_stage},
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "emp",
            }
        },
        {"$unwind": "$emp"},
    ]

    if department:
        pipeline.append({"$match": {"emp.department": department}})

    pipeline.extend([
        {
            "$project": {
                "_id": 0,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"total_late_minutes": DESCENDING},
                "output": {
                    "rank": {"$rank": {}}
                },
            }
        },
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": DESCENDING, "emp_code": ASCENDING}},
    ])

    items = list(db.attendance_logs.aggregate(pipeline))
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
def department_trend(
    department: str = Path(...),
    from_date: str = Query(..., alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    to_date: str = Query(..., alias="to", pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    """
    Daily attendance trend for department with gap filling ($densify) and 7-day moving average.
    """
    # 404 if department has no employees
    if db.employees.count_documents({"department": department}) == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown department")

    try:
        f_date = dt_date.fromisoformat(from_date)
        t_date = dt_date.fromisoformat(to_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["query"], "msg": "Invalid date format", "type": "value_error"}],
        )

    if t_date < f_date:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["query", "to"], "msg": "to cannot be earlier than from", "type": "value_error"}],
        )

    if (t_date - f_date).days > 92:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"loc": ["query"], "msg": "Date range cannot exceed 92 days", "type": "value_error"}],
        )

    from_dt = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
    to_dt = (datetime.fromisoformat(to_date) + timedelta(days=1)).replace(tzinfo=timezone.utc)

    pipeline = [
        {
            "$match": {
                "date": {"$gte": from_date, "$lte": to_date},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "as": "emp",
            }
        },
        {"$unwind": "$emp"},
        {
            "$match": {
                "emp.department": department,
            }
        },
        {
            "$group": {
                "_id": {"$dateFromString": {"dateString": "$date"}},
                "present_count": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                            {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "late_count": {
                    "$sum": {
                        "$cond": [
                            {"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]},
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$densify": {
                "field": "_id",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": [from_dt, to_dt],
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$_id"}},
                "present_count": {"$ifNull": ["$present_count", 0.0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "is_working_day": {
                    "$in": [{"$dayOfWeek": "$_id"}, [2, 3, 4, 5, 6]]
                },
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "let": {"current_day": "$date"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$department", department]},
                                    {"$lte": ["$joined_on", "$$current_day"]},
                                ]
                            }
                        }
                    },
                    {"$count": "count"},
                ],
                "as": "hc",
            }
        },
        {
            "$addFields": {
                "headcount": {"$ifNull": [{"$first": "$hc.count"}, 0]}
            }
        },
        {
            "$addFields": {
                "attendance_rate": {
                    "$cond": [
                        {
                            "$and": [
                                "$is_working_day",
                                {"$gt": ["$headcount", 0]},
                            ]
                        },
                        {
                            "$round": [
                                {"$divide": ["$present_count", "$headcount"]},
                                4,
                            ]
                        },
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"_id": ASCENDING},
                "output": {
                    "window_rates": {
                        "$push": "$attendance_rate",
                        "window": {
                            "documents": [-6, 0],
                        },
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "non_null_rates": {
                    "$filter": {
                        "input": "$window_rates",
                        "as": "r",
                        "cond": {"$ne": ["$$r", None]},
                    }
                },
            }
        },
        {
            "$project": {
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": {
                    "$cond": [
                        {"$gt": [{"$size": "$non_null_rates"}, 0]},
                        {"$round": [{"$avg": "$non_null_rates"}, 4]},
                        None,
                    ]
                },
            }
        },
        {"$sort": {"date": ASCENDING}},
    ]

    items = list(db.attendance_logs.aggregate(pipeline))
    return {"department": department, "items": items}


@app.get("/admin/explain/{endpoint}", tags=["admin"])
def explain_endpoint(
    endpoint: Literal[
        "attendance_list",
        "employee_monthly",
        "department_summary",
        "late_leaderboard",
        "department_trend",
    ],
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status_filter: Optional[StatusType] = Query(None, alias="status"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    MongoDB explain output with executionStats verbosity for an endpoint's main query or pipeline.
    """
    if endpoint == "attendance_list":
        if date_from and date_to and date_from > date_to:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query", "date_from"], "msg": "date_from must be <= date_to", "type": "value_error"}],
            )
        q: Dict[str, Any] = {}
        if emp_code:
            q["emp_code"] = emp_code
        if status_filter:
            q["status"] = status_filter
        if date_from or date_to:
            q["date"] = {}
            if date_from:
                q["date"]["$gte"] = date_from
            if date_to:
                q["date"]["$lte"] = date_to

        skip = (page - 1) * page_size
        cmd = {
            "explain": {
                "find": "attendance_logs",
                "filter": q,
                "sort": {"date": DESCENDING, "emp_code": ASCENDING},
                "skip": skip,
                "limit": page_size,
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(json_util.dumps(res)),
        }

    elif endpoint == "employee_monthly":
        if not emp_code:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query", "emp_code"], "msg": "emp_code is required", "type": "missing"}],
            )
        if not month or not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", month):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query", "month"], "msg": "month is required (YYYY-MM)", "type": "missing"}],
            )

        year, m_int = map(int, month.split("-"))
        num_days = calendar.monthrange(year, m_int)[1]
        start_date = f"{month}-01"
        end_date = f"{month}-{num_days:02d}"

        pipeline = [
            {
                "$match": {
                    "emp_code": emp_code,
                    "date": {"$gte": start_date, "$lte": end_date},
                }
            },
            {
                "$project": {
                    "date": 1,
                    "status": 1,
                    "late_minutes": {"$ifNull": ["$late_minutes", 0]},
                    "overtime_minutes": {"$ifNull": ["$overtime_minutes", 0]},
                    "half_day": {"$ifNull": ["$half_day", False]},
                    "dayOfWeek": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
                }
            },
            {
                "$group": {
                    "_id": None,
                    "present_days": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$in": ["$dayOfWeek", [2, 3, 4, 5, 6]]},
                                        {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    ]
                                },
                                {"$cond": ["$half_day", 0.5, 1.0]},
                                0.0,
                            ]
                        }
                    },
                    "leave_days": {
                        "$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
                    },
                    "late_count": {
                        "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}
                    },
                    "total_late_minutes": {"$sum": "$late_minutes"},
                    "total_overtime_minutes": {"$sum": "$overtime_minutes"},
                }
            },
        ]
        cmd = {
            "explain": {
                "aggregate": "attendance_logs",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(json_util.dumps(res)),
        }

    elif endpoint == "department_summary":
        if not month or not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", month):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query", "month"], "msg": "month is required (YYYY-MM)", "type": "missing"}],
            )

        year, m_int = map(int, month.split("-"))
        num_days = calendar.monthrange(year, m_int)[1]
        start_date = f"{month}-01"
        end_date = f"{month}-{num_days:02d}"

        match_stage = {"joined_on": {"$lte": end_date}}
        if department:
            match_stage["department"] = department

        pipeline = [
            {"$match": match_stage},
            {
                "$lookup": {
                    "from": "attendance_logs",
                    "let": {"emp": "$emp_code"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$emp_code", "$$emp"]},
                                        {"$gte": ["$date", start_date]},
                                        {"$lte": ["$date", end_date]},
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "date": 1,
                                "status": 1,
                                "work_hours": 1,
                                "late_minutes": {"$ifNull": ["$late_minutes", 0]},
                                "half_day": {"$ifNull": ["$half_day", False]},
                                "dayOfWeek": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
                            }
                        },
                    ],
                    "as": "logs",
                }
            },
            {
                "$project": {
                    "department": 1,
                    "emp_code": 1,
                    "emp_present_days": {
                        "$sum": {
                            "$map": {
                                "input": "$logs",
                                "as": "l",
                                "in": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$in": ["$$l.dayOfWeek", [2, 3, 4, 5, 6]]},
                                                {"$in": ["$$l.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                            ]
                                        },
                                        {"$cond": ["$$l.half_day", 0.5, 1.0]},
                                        0.0,
                                    ]
                                },
                            }
                        }
                    },
                    "emp_work_hours": {
                        "$filter": {
                            "input": {
                                "$map": {
                                    "input": "$logs",
                                    "as": "l",
                                    "in": {
                                        "$cond": [
                                            {
                                                "$and": [
                                                    {"$in": ["$$l.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                                    {"$ne": ["$$l.work_hours", None]},
                                                ]
                                            },
                                            "$$l.work_hours",
                                            None,
                                        ]
                                    },
                                }
                            },
                            "as": "wh",
                            "cond": {"$ne": ["$$wh", None]},
                        }
                    },
                    "emp_late_count": {
                        "$size": {
                            "$filter": {
                                "input": "$logs",
                                "as": "l",
                                "cond": {"$gt": ["$$l.late_minutes", 0]},
                            }
                        }
                    },
                    "emp_total_late_minutes": {"$sum": "$logs.late_minutes"},
                    "emp_leave_count": {
                        "$size": {
                            "$filter": {
                                "input": "$logs",
                                "as": "l",
                                "cond": {"$eq": ["$$l.status", "LEAVE"]},
                            }
                        }
                    },
                    "emp_on_duty_count": {
                        "$size": {
                            "$filter": {
                                "input": "$logs",
                                "as": "l",
                                "cond": {"$eq": ["$$l.status", "ON_DUTY"]},
                            }
                        }
                    },
                }
            },
            {
                "$group": {
                    "_id": "$department",
                    "headcount": {"$sum": 1},
                    "present_days": {"$sum": "$emp_present_days"},
                    "all_work_hours": {"$push": "$emp_work_hours"},
                    "late_count": {"$sum": "$emp_late_count"},
                    "total_late_minutes": {"$sum": "$emp_total_late_minutes"},
                    "leave_count": {"$sum": "$emp_leave_count"},
                    "on_duty_count": {"$sum": "$emp_on_duty_count"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "department": "$_id",
                    "headcount": 1,
                    "present_days": 1,
                    "late_count": 1,
                    "total_late_minutes": 1,
                    "leave_count": 1,
                    "on_duty_count": 1,
                    "flat_work_hours": {
                        "$reduce": {
                            "input": "$all_work_hours",
                            "initialValue": [],
                            "in": {"$concatArrays": ["$$value", "$$this"]},
                        }
                    },
                }
            },
            {
                "$project": {
                    "department": 1,
                    "headcount": 1,
                    "present_days": 1,
                    "late_count": 1,
                    "total_late_minutes": 1,
                    "leave_count": 1,
                    "on_duty_count": 1,
                    "avg_work_hours": {
                        "$cond": [
                            {"$gt": [{"$size": "$flat_work_hours"}, 0]},
                            {"$round": [{"$avg": "$flat_work_hours"}, 2]},
                            None,
                        ]
                    },
                }
            },
            {"$sort": {"department": ASCENDING}},
        ]
        cmd = {
            "explain": {
                "aggregate": "employees",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "employees",
            "explain": json.loads(json_util.dumps(res)),
        }

    elif endpoint == "late_leaderboard":
        if not month or not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", month):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query", "month"], "msg": "month is required (YYYY-MM)", "type": "missing"}],
            )

        year, m_int = map(int, month.split("-"))
        num_days = calendar.monthrange(year, m_int)[1]
        start_date = f"{month}-01"
        end_date = f"{month}-{num_days:02d}"

        match_stage = {
            "date": {"$gte": start_date, "$lte": end_date},
            "late_minutes": {"$gt": 0},
        }

        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": "$emp_code",
                    "total_late_minutes": {"$sum": "$late_minutes"},
                    "late_count": {"$sum": 1},
                }
            },
            {
                "$lookup": {
                    "from": "employees",
                    "localField": "_id",
                    "foreignField": "emp_code",
                    "as": "emp",
                }
            },
            {"$unwind": "$emp"},
        ]

        if department:
            pipeline.append({"$match": {"emp.department": department}})

        pipeline.extend([
            {
                "$project": {
                    "_id": 0,
                    "emp_code": "$_id",
                    "name": "$emp.name",
                    "department": "$emp.department",
                    "total_late_minutes": 1,
                    "late_count": 1,
                }
            },
            {
                "$setWindowFields": {
                    "sortBy": {"total_late_minutes": DESCENDING},
                    "output": {
                        "rank": {"$rank": {}}
                    },
                }
            },
            {"$match": {"rank": {"$lte": limit}}},
            {"$sort": {"total_late_minutes": DESCENDING, "emp_code": ASCENDING}},
        ])
        cmd = {
            "explain": {
                "aggregate": "attendance_logs",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(json_util.dumps(res)),
        }

    elif endpoint == "department_trend":
        if not department:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query", "department"], "msg": "department is required", "type": "missing"}],
            )
        if not from_date or not to_date:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query"], "msg": "from and to are required", "type": "missing"}],
            )

        try:
            f_date = dt_date.fromisoformat(from_date)
            t_date = dt_date.fromisoformat(to_date)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query"], "msg": "Invalid date format", "type": "value_error"}],
            )

        if t_date < f_date:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query", "to"], "msg": "to cannot be earlier than from", "type": "value_error"}],
            )

        if (t_date - f_date).days > 92:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[{"loc": ["query"], "msg": "Date range cannot exceed 92 days", "type": "value_error"}],
            )

        from_dt = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
        to_dt = (datetime.fromisoformat(to_date) + timedelta(days=1)).replace(tzinfo=timezone.utc)

        pipeline = [
            {
                "$match": {
                    "date": {"$gte": from_date, "$lte": to_date},
                }
            },
            {
                "$lookup": {
                    "from": "employees",
                    "localField": "emp_code",
                    "foreignField": "emp_code",
                    "as": "emp",
                }
            },
            {"$unwind": "$emp"},
            {
                "$match": {
                    "emp.department": department,
                }
            },
            {
                "$group": {
                    "_id": {"$dateFromString": {"dateString": "$date"}},
                    "present_count": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1.0]},
                                0.0,
                            ]
                        }
                    },
                    "late_count": {
                        "$sum": {
                            "$cond": [
                                {"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]},
                                1,
                                0,
                            ]
                        }
                    },
                }
            },
            {
                "$densify": {
                    "field": "_id",
                    "range": {
                        "step": 1,
                        "unit": "day",
                        "bounds": [from_dt, to_dt],
                    },
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$_id"}},
                    "present_count": {"$ifNull": ["$present_count", 0.0]},
                    "late_count": {"$ifNull": ["$late_count", 0]},
                    "is_working_day": {
                        "$in": [{"$dayOfWeek": "$_id"}, [2, 3, 4, 5, 6]]
                    },
                }
            },
            {
                "$lookup": {
                    "from": "employees",
                    "let": {"current_day": "$date"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$department", department]},
                                        {"$lte": ["$joined_on", "$$current_day"]},
                                    ]
                                }
                            }
                        },
                        {"$count": "count"},
                    ],
                    "as": "hc",
                }
            },
            {
                "$addFields": {
                    "headcount": {"$ifNull": [{"$first": "$hc.count"}, 0]}
                }
            },
            {
                "$addFields": {
                    "attendance_rate": {
                        "$cond": [
                            {
                                "$and": [
                                    "$is_working_day",
                                    {"$gt": ["$headcount", 0]},
                                ]
                            },
                            {
                                "$round": [
                                    {"$divide": ["$present_count", "$headcount"]},
                                    4,
                                ]
                            },
                            None,
                        ]
                    }
                }
            },
            {
                "$setWindowFields": {
                    "sortBy": {"_id": ASCENDING},
                    "output": {
                        "window_rates": {
                            "$push": "$attendance_rate",
                            "window": {
                                "documents": [-6, 0],
                            },
                        }
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "date": 1,
                    "is_working_day": 1,
                    "headcount": 1,
                    "present_count": 1,
                    "late_count": 1,
                    "attendance_rate": 1,
                    "non_null_rates": {
                        "$filter": {
                            "input": "$window_rates",
                            "as": "r",
                            "cond": {"$ne": ["$$r", None]},
                        }
                    },
                }
            },
            {
                "$project": {
                    "date": 1,
                    "is_working_day": 1,
                    "headcount": 1,
                    "present_count": 1,
                    "late_count": 1,
                    "attendance_rate": 1,
                    "moving_avg_7d": {
                        "$cond": [
                            {"$gt": [{"$size": "$non_null_rates"}, 0]},
                            {"$round": [{"$avg": "$non_null_rates"}, 4]},
                            None,
                        ]
                    },
                }
            },
            {"$sort": {"date": ASCENDING}},
        ]
        cmd = {
            "explain": {
                "aggregate": "attendance_logs",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(json_util.dumps(res)),
        }
