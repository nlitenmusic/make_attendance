# app.py
import os
from pymongo import MongoClient
import io
import re
import uuid
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify, abort

from attendance_logic import convert_import_to_internal_schema, build_attendance, export_attendance_sheets

client = None
db = None

def get_db():
    global client, db
    if client is None:
        mongo_uri = os.getenv("MONGO_URL")
        if not mongo_uri:
            raise RuntimeError("MONGO_URL environment variable is not set")
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        try:
            client.admin.command("ping")
            print("MongoDB CONNECTED")
        except Exception as e:
            print("MongoDB ERROR:", e)
        db = client["attendance_db"]
    return db

secret = os.getenv("FLASK_SECRET_KEY")
if not secret:
    raise RuntimeError("FLASK_SECRET_KEY environment variable is not set")
app = Flask(__name__)
app.secret_key = secret

ATTENDANCE_DIR = Path("attendance_sheets")
ATTENDANCE_DIR.mkdir(exist_ok=True)

SESSION_NAME = os.getenv("SESSION_NAME", "Fall 2025")

def pretty_filename(filename: str) -> str:
    base = filename.replace(".csv", "")
    if "_" not in base:
        return base

    day, clinic = base.split("_", 1)

    # Insert spaces before capital letters: "GreenBallClinic" → "Green Ball Clinic"
    clinic_readable = re.sub(r"(?<!^)([A-Z])", r" \1", clinic)

    return f"{day} {clinic_readable}"

app.jinja_env.globals.update(pretty_filename=pretty_filename)

# Helper: extract sheet id and optional gid from provided URL
def extract_ids(sheet_url: str):
    sheet_match = re.search(r"/d/([a-zA-Z0-9_-]+)", sheet_url)
    gid_match = re.search(r"gid=([0-9]+)", sheet_url)
    sheet_id = sheet_match.group(1) if sheet_match else None
    gid = gid_match.group(1) if gid_match else "0"
    return sheet_id, gid


def fetch_csv_from_google(sheet_url: str) -> pd.DataFrame:
    sheet_id, gid = extract_ids(sheet_url)
    if not sheet_id:
        raise ValueError("Invalid Google Sheet URL")
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    r = requests.get(export_url)
    r.raise_for_status()
    text = r.text
    # Use pandas to read; ensure dtype=str to avoid unintended NaNs
    return pd.read_csv(io.StringIO(text), dtype=str).fillna("")


# Sorting helper for index listing
DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
CLINIC_TOKENS = ["RedBallClinic", "OrangeBallClinic", "GreenBallClinic", "YellowBallClinic", "HighPerformanceClinic"]

CLINIC_ORDER = [
    {"day": "Monday", "clinic": "Orange Ball Clinic"},
    {"day": "Monday", "clinic": "Green Ball Clinic"},
    {"day": "Monday", "clinic": "Yellow Ball Clinic"},
    {"day": "Tuesday", "clinic": "Red Ball Clinic"},
    {"day": "Tuesday", "clinic": "Orange Ball Clinic"},
    {"day": "Tuesday", "clinic": "High Performance Clinic"},
    {"day": "Wednesday", "clinic": "Orange Ball Clinic"},
    {"day": "Wednesday", "clinic": "Green Ball Clinic"},
    {"day": "Wednesday", "clinic": "Yellow Ball Clinic"},
    {"day": "Thursday", "clinic": "Red Ball Clinic"},
    {"day": "Thursday", "clinic": "Green Ball Clinic"},
    {"day": "Thursday", "clinic": "High Performance Clinic"},
    {"day": "Friday", "clinic": "Orange Ball Clinic"},
    {"day": "Friday", "clinic": "Green Ball Clinic"},
    {"day": "Friday", "clinic": "Yellow Ball Clinic"},
]

def sort_key(filename: str):
    base = filename.replace(".csv", "")
    parts = base.split(" ", 1)

    if len(parts) != 2:
        return (999, 999, filename)

    day, clinic = parts

    # Day index
    day_idx = DAY_ORDER.index(day) if day in DAY_ORDER else 999

    # Clinic index — match the exact clinic name
    clinic_idx = next(
        (i for i, c in enumerate(CLINIC_TOKENS) if c == clinic),
        999
    )

    return (day_idx, clinic_idx, filename)


def list_saved_sheets_sorted():
    files = [f.name for f in ATTENDANCE_DIR.glob("*.csv")]
    return sorted(files, key=sort_key)

# --- MongoDB helpers (per-upload collections) ---

def _slug(s: str) -> str:
    s = (s or "default").strip()
    s = s.lower()
    s = re.sub(r"[^0-9a-z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "default"

def collection_name_for_sheet(sheet_id: str, session: str) -> str:
    """Canonical collection name for an uploaded Google Sheet (sheet_id + session)."""
    sid = _slug(sheet_id)
    sess = _slug(session or "default")
    return f"sheet__{sid}__{sess}"

def collection_for_upload_by_name(cname: str):
    return get_db()[cname]

def collection_for_upload_from_ids(sheet_id: str, session: str):
    cname = collection_name_for_sheet(sheet_id, session)
    return collection_for_upload_by_name(cname)

def _legacy_collection():
    return get_db()["sheets"]

def _all_relevant_collections():
    """Yield legacy then all per-upload sheet collections (sheet__*)."""
    db = get_db()
    if "sheets" in db.list_collection_names():
        yield db["sheets"]
    # support both old 'sheets__' and new 'sheet__' patterns for compatibility
    for name in db.list_collection_names():
        if name.startswith("sheet__") or name.startswith("sheets__"):
            yield db[name]

def save_sheet_upload(sheet_id: str, filename: str, rows, dynamic_columns, session=SESSION_NAME, sheet_url=None):
    """Save the whole uploaded Google Sheet into one collection identified by sheet_id + session."""
    col = collection_for_upload_from_ids(sheet_id, session)
    doc = {
        "filename": filename,
        "sheet_id": sheet_id,
        "sheet_url": sheet_url,
        "rows": rows,
        "dynamic_columns": dynamic_columns,
        "session": session,
        "created_at": datetime.utcnow()
    }
    # single document per upload inside this collection (use filename as key)
    col.replace_one({"filename": filename}, doc, upsert=True)

def save_sheet_to_db(filename, rows, dynamic_columns, session=SESSION_NAME, sheet_url=None):
    """Compatibility wrapper used by routes that expect save_sheet_to_db(filename, ...).

    Derive a stable sheet_id from the filename stem and delegate to save_sheet_upload so
    existing per-upload collection semantics are preserved.
    """
    # derive a sheet id from the filename stem (strip extension)
    sheet_id = Path(filename).stem
    save_sheet_upload(sheet_id, filename, rows, dynamic_columns, session=session, sheet_url=sheet_url)

def get_all_sheets():
    """Return all sheet documents across legacy + per-sheet collections, deduped by filename newest-first."""
    docs = []
    for col in _all_relevant_collections():
        docs.extend(list(col.find({})))
    # dedupe by filename keeping newest created_at
    seen = {}
    for d in docs:
        fname = d.get("filename")
        if not fname:
            continue
        existing = seen.get(fname)
        if not existing or d.get("created_at", datetime.min) > existing.get("created_at", datetime.min):
            seen[fname] = d
    out = list(seen.values())
    out.sort(key=lambda d: d.get("created_at", datetime.min), reverse=True)
    return out

def get_sheet_by_filename(filename):
    """Find a sheet document by filename across all collections."""
    for col in _all_relevant_collections():
        doc = col.find_one({"filename": filename})
        if doc:
            return doc
    return None

def get_sheet_for_clinic(day, clinic, session=None):
    """Return the newest document across all collections that contains Day+Clinic."""
    candidates = []
    for col in _all_relevant_collections():
        if session:
            doc = col.find_one({"session": session, "rows": {"$elemMatch": {"Day": day, "Clinic": clinic}}})
            if doc:
                candidates.append(doc)
                continue
        doc = col.find_one({"rows": {"$elemMatch": {"Day": day, "Clinic": clinic}}})
        if doc:
            candidates.append(doc)
    if not candidates:
        return None
    candidates.sort(key=lambda d: d.get("created_at", datetime.min), reverse=True)
    return candidates[0]

def _find_collection_for_filename(filename):
    """Return collection object that contains the given filename (search all relevant collections)."""
    db = get_db()
    if "sheets" in db.list_collection_names() and db["sheets"].find_one({"filename": filename}):
        return db["sheets"]
    for name in db.list_collection_names():
        if not (name.startswith("sheet__") or name.startswith("sheets__")):
            continue
        col = db[name]
        if col.find_one({"filename": filename}):
            return col
    return None

def update_sheet_rows(filename, rows):
    col = _find_collection_for_filename(filename)
    if col:
        col.update_one({"filename": filename}, {"$set": {"rows": rows}})

def update_sheet_dynamic_columns(filename, dynamic_columns, rows):
    col = _find_collection_for_filename(filename)
    if col:
        col.update_one({"filename": filename}, {"$set": {"dynamic_columns": dynamic_columns, "rows": rows}})

def add_row_to_sheet(filename, new_row):
    col = _find_collection_for_filename(filename)
    if col:
        col.update_one({"filename": filename}, {"$push": {"rows": new_row}})

def delete_sheet(filename):
    """Drop the per-upload collection for this filename if present, and remove any legacy doc."""
    db = get_db()
    # find collection containing filename
    col = _find_collection_for_filename(filename)
    if col:
        # if collection name follows new pattern and contains only this upload, drop it
        if col.name.startswith("sheet__") or col.name.startswith("sheets__"):
            db.drop_collection(col.name)
            return
    # fallback: remove any doc in legacy collection
    if "sheets" in db.list_collection_names():
        db["sheets"].delete_one({"filename": filename})

def export_all_sheets_to_csv():
    all_rows = []
    for col in _all_relevant_collections():
        for sheet in col.find():
            for row in sheet.get("rows", []):
                rc = dict(row)
                rc["filename"] = sheet.get("filename")
                rc["session"] = sheet.get("session")
                all_rows.append(rc)
    if not all_rows:
        return ""
    df = pd.DataFrame(all_rows)
    return df.to_csv(index=False)

def find_sheet_by_prefix(prefix_re: str):
    """Return newest sheet doc whose filename matches prefix_re across all collections."""
    candidates = []
    db = get_db()
    for col in _all_relevant_collections():
        try:
            doc = col.find_one({"filename": {"$regex": prefix_re}}, sort=[("created_at", -1)])
        except Exception:
            doc = None
        if doc:
            candidates.append(doc)
    if not candidates:
        return None
    candidates.sort(key=lambda d: d.get("created_at", datetime.min), reverse=True)
    return candidates[0]

# --- Routes ---

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        sheet_url = request.form.get("sheet_url", "").strip()
        if not sheet_url:
            flash("Please paste a Google Sheet URL.")
            return redirect(url_for("index"))
        try:
            # fetch CSV, convert and build attendance as before
            df_raw = fetch_csv_from_google(sheet_url)
            df_clean = convert_import_to_internal_schema(df_raw)
            attendance_df = build_attendance(df_clean)

            # convert dataframe to rows and compute dynamic columns
            rows = attendance_df.to_dict(orient="records")
            known_fields = {"row_id", "Name", "Age", "MemberName", "Comments", "Fee"}
            dynamic_columns = [col for col in attendance_df.columns if col not in known_fields]

            # session provided by user (form) or default
            form_session = request.form.get("session", "").strip() or SESSION_NAME

            # use sheet_id (from URL) to derive collection name so one collection per uploaded Google Sheet
            sheet_id, gid = extract_ids(sheet_url)
            if not sheet_id:
                raise ValueError("Invalid Google Sheet URL (no sheet id)")

            # canonical filename for this upload (helps UI)
            date_tag = datetime.utcnow().strftime("%Y%m%d")
            filename = f"{sheet_id}_{date_tag}.csv"

            # Save entire sheet into one collection identified by sheet_id + session
            save_sheet_upload(sheet_id, filename, rows, dynamic_columns, session=form_session, sheet_url=sheet_url)

            flash("Attendance sheet saved to database as one collection.")
        except Exception as e:
            flash(f"Error: {e}")
        return redirect(url_for("index"))

    # Fetch full documents (including rows)
    sheets = get_all_sheets()

    # Build grouped_sessions: session -> ordered clinics -> entries
    sessions = []
    seen_sessions = set()
    for s in sheets:
        sess = (s.get("session") or "Default Session")
        if sess not in seen_sessions:
            seen_sessions.add(sess)
            sessions.append(sess)

    # prefer configured SESSION_NAME first
    def session_sort_key(s):
        return (0 if s == SESSION_NAME else 1, s.lower())

    sessions_sorted = sorted(sessions, key=session_sort_key)

    grouped_sessions = []
    for sess in sessions_sorted:
        clinics_out = []
        for cfg in CLINIC_ORDER:
            items = []
            seen = set()
            for sheet in sheets:
                if (sheet.get("session") or "Default Session") != sess:
                    continue
                filename = sheet.get("filename")
                rows = sheet.get("rows", []) or []

                sheet_has_match = any(
                    ((r.get("Day") or "").strip().casefold() == cfg["day"].casefold() and
                     (r.get("Clinic") or "").strip().casefold() == cfg["clinic"].casefold())
                    for r in rows
                )
                if not sheet_has_match:
                    continue

                for idx, r in enumerate(rows):
                    row_day = (r.get("Day") or "").strip()
                    row_clinic = (r.get("Clinic") or "").strip()
                    if (row_day.casefold() == cfg["day"].casefold() and row_clinic.casefold() == cfg["clinic"].casefold()) \
                       or (not row_day and not row_clinic):
                        rid = r.get("row_id") or f"{filename}:{idx}"
                        key = (rid, filename)
                        if key in seen:
                            continue
                        seen.add(key)
                        items.append({"filename": filename, "row": r})

            if items:
                clinics_out.append({"day": cfg["day"], "clinic": cfg["clinic"], "entries": items})

        if clinics_out:
            grouped_sessions.append({"session": sess, "clinics": clinics_out})

    # Backwards-compatible flat list (older templates may still expect grouped_clinics)
    grouped_clinics = []
    for gs in grouped_sessions:
        for c in gs["clinics"]:
            grouped_clinics.append(c)

    # render using grouped_sessions; grouped_clinics remains for compatibility
    return render_template("index.html", grouped_clinics=grouped_clinics, grouped_sessions=grouped_sessions, pretty_filename=pretty_filename)

@app.route("/results", methods=["GET"])
def results():
    day = request.args.get("day")
    clinic = request.args.get("clinic")
    if not day or not clinic:
        flash("Missing clinic information.")
        return redirect(url_for("index"))

    # Prefer the single sheet doc that contains this Day+Clinic so we always render the same doc
    sheet = get_sheet_for_clinic(day, clinic)
    matched_rows = []
    dynamic_dates = []

    if sheet:
        filename = sheet.get("filename")
        app.logger.info("results: using sheet filename=%s", filename)
        rows = sheet.get("rows", [])
        # Include rows that match Day+Clinic OR blank rows in the same sheet (newly added rows)
        matched_rows = [
            r for r in rows
            if (r.get("Day") == day and r.get("Clinic") == clinic) or (not r.get("Day") and not r.get("Clinic"))
        ]
        dynamic_dates = sorted(sheet.get("dynamic_columns", []))
    else:
        # fallback: aggregate across all sheets (existing behavior)
        sheets = list(get_db()["sheets"].find().sort("created_at", -1))
        dynamic_dates_set = set()
        for s in sheets:
            for r in s.get("rows", []):
                if r.get("Day") == day and r.get("Clinic") == clinic:
                    matched_rows.append(r)
            for c in s.get("dynamic_columns", []):
                dynamic_dates_set.add(c)
        dynamic_dates = sorted(dynamic_dates_set)

    app.logger.info("results: matched_rows count=%s ids=%s", len(matched_rows), [r.get("row_id") for r in matched_rows])
    return render_template(
        "results.html",
        data=matched_rows,
        day=day,
        clinic=clinic,
        dynamic_dates=dynamic_dates,
        pretty_filename=pretty_filename
    )

@app.route("/save_attendance", methods=["POST"])
def save_attendance():
    day = request.form.get("day")
    clinic = request.form.get("clinic")
    app.logger.info("save_attendance called: day=%s clinic=%s", day, clinic)
    if not day or not clinic:
        flash("Missing clinic information.")
        return redirect(url_for("index"))

    names = request.form.getlist("player_name")
    ages = request.form.getlist("age")
    parents = request.form.getlist("parent")
    comments = request.form.getlist("comments")
    fees = request.form.getlist("fee")
    row_ids = request.form.getlist("row_id")
    delete_flags = request.form.getlist("delete_flag")

    # union of dynamic columns across DB
    all_sheets = get_all_sheets()
    dynamic_cols = set()
    for s in all_sheets:
        for c in s.get("dynamic_columns", []):
            dynamic_cols.add(c)
    dynamic_cols = list(dynamic_cols)

    # collect dynamic values per submitted row
    dynamic_values = {}
    n = len(names)
    for col in dynamic_cols:
        key = f"date__{col}"
        if key in request.form:
            vals = request.form.getlist(key)
            if len(vals) < n:
                vals += [""] * (n - len(vals))
            dynamic_values[col] = vals

    target = get_sheet_for_clinic(day, clinic)
    if target:
        filename = target["filename"]
        col = _find_collection_for_filename(filename)
        app.logger.info("save_attendance: target sheet filename=%s", filename)
        # ensure dynamic_columns merged if new columns present
        existing_dyn = set(target.get("dynamic_columns", []))
        merged_dyn = sorted(existing_dyn.union(dynamic_cols))
        if merged_dyn != sorted(existing_dyn):
            if col:
                res = col.update_one({"filename": filename}, {"$set": {"dynamic_columns": merged_dyn}})
                app.logger.info("Updated dynamic_columns on %s matched=%s modified=%s", filename, res.matched_count, res.modified_count)

        for i in range(n):
            ...
            # existing update/insert logic unchanged
            ...
    else:
        app.logger.info("save_attendance: no target sheet found for %s — %s; aborting save", day, clinic)
        flash("No uploaded Google Sheet found for this clinic/day. Please import the Google Sheet first.")
        return redirect(url_for("index"))

    flash("Attendance saved for %s — %s" % (day, clinic))
    return redirect(url_for("results", day=day, clinic=clinic))

@app.route("/add_row", methods=["POST"])
def add_row():
    day = request.form.get("day")
    clinic = request.form.get("clinic")
    app.logger.info("add_row called: day=%s clinic=%s", day, clinic)
    if not day or not clinic:
        flash("Missing clinic information.")
        return redirect(url_for("index"))

    safe_clinic = re.sub(r"\s+", "", clinic)

    target = get_sheet_for_clinic(day, clinic)
    if not target:
        prefix_re = f"^{re.escape(day)}_{re.escape(safe_clinic)}"
        target = find_sheet_by_prefix(prefix_re)

    new_row = {
        "row_id": str(uuid.uuid4()),
        "Day": day,
        "Clinic": clinic,
        "Name": "",
        "Age": "",
        "MemberName": "",
        "Comments": "",
        "Fee": ""
    }

    if target:
        filename = target["filename"]
        dynamic_columns = target.get("dynamic_columns", [])
        for col in dynamic_columns:
            new_row[col] = ""
        add_row_to_sheet(filename, new_row)
        col = _find_collection_for_filename(filename)
        found = False
        if col:
            found = bool(col.find_one({"filename": filename, "rows.row_id": new_row["row_id"]}))
        app.logger.info("add_row: pushed row_id=%s into %s found=%s", new_row["row_id"], filename, found)
    else:
        app.logger.info("add_row: no target sheet found for %s — %s; aborting add_row", day, clinic)
        flash("No uploaded Google Sheet found for this clinic/day. Please import the Google Sheet first.")
        return redirect(url_for("index"))

    flash("Row added.")
    return redirect(url_for("results", day=day, clinic=clinic))

@app.route("/add_date_column", methods=["POST"])
def add_date_column():
    day = request.form.get("day")
    clinic = request.form.get("clinic")
    new_col = request.form.get("new_date", "").strip()
    if not day or not clinic or not new_col:
        flash("Missing parameters.")
        return redirect(url_for("index"))

    sheet = get_sheet_for_clinic(day, clinic)
    if not sheet:
        flash("Sheet not found for that clinic.")
        return redirect(url_for("results", day=day, clinic=clinic))

    filename = sheet["filename"]
    dynamic_columns = sheet.get("dynamic_columns", [])
    if new_col in dynamic_columns:
        flash("Date column already exists.")
        return redirect(url_for("results", day=day, clinic=clinic))

    dynamic_columns.append(new_col)
    rows = sheet.get("rows", [])
    for row in rows:
        row[new_col] = row.get(new_col, "")
    update_sheet_dynamic_columns(filename, dynamic_columns, rows)
    flash("Date column added.")
    return redirect(url_for("results", day=day, clinic=clinic))


@app.route("/delete_row", methods=["POST"])
def delete_row():
    row_id = request.form.get("row_id")
    day = request.form.get("day")
    clinic = request.form.get("clinic")
    app.logger.info("delete_row called: row_id=%s day=%s clinic=%s", row_id, day, clinic)
    if not row_id:
        flash("Missing row_id.")
        return redirect(url_for("index"))
    total_matched = total_modified = 0
    for col in _all_relevant_collections():
        res = col.update_many({}, {"$pull": {"rows": {"row_id": row_id}}})
        total_matched += getattr(res, "matched_count", 0)
        total_modified += getattr(res, "modified_count", 0)
    app.logger.info("delete_row: total matched=%s total modified=%s", total_matched, total_modified)
    flash("Row deleted.")
    if day and clinic:
        return redirect(url_for("results", day=day, clinic=clinic))
    return redirect(url_for("index"))

@app.route("/delete_sheet", methods=["POST"])
def delete_sheet_route():
    # Support deleting specific (day, clinic, time) across all sheet documents
    filename = request.form.get("filename")
    day = request.form.get("day")
    clinic = request.form.get("clinic")
    time = request.form.get("time")

    if day and clinic and time:
        # remove matching rows from every document across all collections
        sheets = get_all_sheets()
        for sheet in sheets:
            filename_doc = sheet.get("filename")
            rows = sheet.get("rows", [])
            new_rows = [r for r in rows if not (r.get("Day") == day and r.get("Clinic") == clinic and r.get("Time") == time)]
            if len(new_rows) != len(rows):
                update_sheet_rows(filename_doc, new_rows)
        flash(f"Deleted {clinic} {time} on {day}.")
    elif filename:
        delete_sheet(filename)
        flash(f"Deleted {filename} from database.")
    else:
        flash("Missing deletion parameters.")
    return redirect(url_for("index"))

@app.route("/export_all", methods=["GET"])
def export_all():
    csv_data = export_all_sheets_to_csv()
    return (
        csv_data,
        200,
        {
            "Content-Type": "text/csv",
            "Content-Disposition": "attachment; filename=all_attendance.csv"
        }
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
