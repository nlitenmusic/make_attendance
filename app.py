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

# --- MongoDB helpers ---

def save_sheet_to_db(filename, rows, dynamic_columns, session=SESSION_NAME):
    doc = {
        "filename": filename,
        "rows": rows,
        "dynamic_columns": dynamic_columns,
        "session": session,
        "created_at": datetime.utcnow()
    }
    get_db()["sheets"].replace_one({"filename": filename}, doc, upsert=True)

def get_all_sheets():
    return list(get_db()["sheets"].find({}, {"filename": 1, "created_at": 1}).sort("created_at", -1))

def get_sheet_by_filename(filename):
    return get_db()["sheets"].find_one({"filename": filename})

def get_sheet_for_clinic(day, clinic):
    """Return the first sheet document that contains rows for the given day+clinic, or None."""
    # use $elemMatch for a robust match against array elements
    return get_db()["sheets"].find_one({"rows": {"$elemMatch": {"Day": day, "Clinic": clinic}}})

def update_sheet_rows(filename, rows):
    get_db()["sheets"].update_one({"filename": filename}, {"$set": {"rows": rows}})

def update_sheet_dynamic_columns(filename, dynamic_columns, rows):
    get_db()["sheets"].update_one({"filename": filename}, {"$set": {"dynamic_columns": dynamic_columns, "rows": rows}})

def add_row_to_sheet(filename, new_row):
    get_db()["sheets"].update_one({"filename": filename}, {"$push": {"rows": new_row}})

def delete_sheet(filename):
    get_db()["sheets"].delete_one({"filename": filename})

def export_all_sheets_to_csv():
    sheets = list(get_db()["sheets"].find())
    all_rows = []
    for sheet in sheets:
        for row in sheet.get("rows", []):
            row_copy = dict(row)
            row_copy["filename"] = sheet["filename"]
            all_rows.append(row_copy)
    if not all_rows:
        return ""
    df = pd.DataFrame(all_rows)
    return df.to_csv(index=False)

# --- Routes ---

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        sheet_url = request.form.get("sheet_url", "").strip()
        if not sheet_url:
            flash("Please paste a Google Sheet URL.")
            return redirect(url_for("index"))
        try:
            df_raw = fetch_csv_from_google(sheet_url)
            df_clean = convert_import_to_internal_schema(df_raw)
            attendance_df = build_attendance(df_clean)

            # convert dataframe to rows
            rows = attendance_df.to_dict(orient="records")

            # known static fields -> dynamic columns are everything else
            known_fields = {"row_id", "Name", "Age", "MemberName", "Comments", "Fee"}
            dynamic_columns = [col for col in attendance_df.columns if col not in known_fields]

            # Group incoming rows by (Day, Clinic) so we save one document per clinic/day
            from collections import defaultdict
            groups = defaultdict(list)
            for r in rows:
                day = r.get("Day")
                clinic = r.get("Clinic")
                if not day or not clinic:
                    # skip rows missing grouping keys
                    continue
                groups[(day, clinic)].append(r)

            # For each (day,clinic) replace existing document (if any) so there is only one sheet per clinic/day
            for (day, clinic), group_rows in groups.items():
                # create a stable canonical filename for this clinic/day (helps UX)
                safe_clinic = re.sub(r"\s+", "", clinic)
                date_tag = datetime.utcnow().strftime("%Y%m%d")
                filename = f"{day}_{safe_clinic}_{date_tag}.csv"

                # determine session: prefer explicit form value, otherwise keep any existing doc session, else default
                form_session = request.form.get("session", "").strip()
                if form_session:
                    session_to_use = form_session
                else:
                    existing_doc = get_db()["sheets"].find_one({"rows.Day": day, "rows.Clinic": clinic})
                    session_to_use = (existing_doc.get("session") if existing_doc and existing_doc.get("session") else SESSION_NAME)

                doc = {
                    "filename": filename,
                    "rows": group_rows,
                    "dynamic_columns": dynamic_columns,
                    "session": session_to_use,
                    "created_at": datetime.utcnow()
                }

                # Replace any existing document that contains rows for the same Day+Clinic.
                # This ensures exactly one saved sheet per clinic per day.
                get_db()["sheets"].replace_one(
                    {"rows.Day": day, "rows.Clinic": clinic},
                    doc,
                    upsert=True
                )

            flash("Attendance sheet(s) saved to database (one per clinic/day).")
        except Exception as e:
            flash(f"Error: {e}")
        return redirect(url_for("index"))

    # Fetch full documents (including rows)
    sheets = list(get_db()["sheets"].find().sort("created_at", -1))

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
    all_sheets = list(get_db()["sheets"].find())
    dynamic_cols = set()
    for s in all_sheets:
        for c in s.get("dynamic_columns", []):
            dynamic_cols.add(c)
    dynamic_cols = list(dynamic_cols)

    # collect dynamic values per submitted row
    # Only include dynamic columns that were actually submitted in the form.
    # If a dynamic column is not present in the POST, we must NOT overwrite it with empty strings.
    dynamic_values = {}
    # number of rows submitted (used to pad lists when needed)
    n = len(names)
    for col in dynamic_cols:
        key = f"date__{col}"
        if key in request.form:
            vals = request.form.getlist(key)
            # pad to length n if client sent fewer values
            if len(vals) < n:
                vals += [""] * (n - len(vals))
            dynamic_values[col] = vals

    target = get_sheet_for_clinic(day, clinic)
    if target:
        filename = target["filename"]
        app.logger.info("save_attendance: target sheet filename=%s", filename)
        # ensure dynamic_columns merged if new columns present
        existing_dyn = set(target.get("dynamic_columns", []))
        merged_dyn = sorted(existing_dyn.union(dynamic_cols))
        if merged_dyn != sorted(existing_dyn):
            res = get_db()["sheets"].update_one({"filename": filename}, {"$set": {"dynamic_columns": merged_dyn}})
            app.logger.info("Updated dynamic_columns on %s matched=%s modified=%s", filename, res.matched_count, res.modified_count)

        for i in range(n):
            rid = row_ids[i] if i < len(row_ids) else None
            # deletion via delete_flag
            if rid and i < len(delete_flags) and delete_flags[i] == "1":
                res = get_db()["sheets"].update_one({"filename": filename}, {"$pull": {"rows": {"row_id": rid}}})
                app.logger.info("Pulled row_id=%s from %s matched=%s modified=%s", rid, filename, res.matched_count, res.modified_count)
                continue

            if rid:
                # prepare update fields for the element with matching row_id
                update_fields = {}
                update_fields["rows.$[elem].Name"] = names[i] if i < len(names) else ""
                update_fields["rows.$[elem].Age"] = ages[i] if i < len(ages) else ""
                update_fields["rows.$[elem].MemberName"] = parents[i] if i < len(parents) else ""
                update_fields["rows.$[elem].Comments"] = comments[i] if i < len(comments) else ""
                update_fields["rows.$[elem].Fee"] = fees[i] if i < len(fees) else ""
                for col, vals in dynamic_values.items():
                    update_fields[f"rows.$[elem].{col}"] = vals[i] if i < len(vals) else ""
                contains = get_db()["sheets"].count_documents({"filename": filename, "rows.row_id": rid}) > 0
                if contains:
                    res = get_db()["sheets"].update_one(
                        {"filename": filename},
                        {"$set": update_fields},
                        array_filters=[{"elem.row_id": rid}]
                    )
                    app.logger.info("Updated row_id=%s in %s matched=%s modified=%s", rid, filename, res.matched_count, res.modified_count)
                else:
                    new_row = {
                        "row_id": rid,
                        "Day": day,
                        "Clinic": clinic,
                        "Name": names[i] if i < len(names) else "",
                        "Age": ages[i] if i < len(ages) else "",
                        "MemberName": parents[i] if i < len(parents) else "",
                        "Comments": comments[i] if i < len(comments) else "",
                        "Fee": fees[i] if i < len(fees) else ""
                    }
                    for col, vals in dynamic_values.items():
                        new_row[col] = vals[i] if i < len(vals) else ""
                    res = get_db()["sheets"].update_one({"filename": filename}, {"$push": {"rows": new_row}})
                    app.logger.info("Pushed new row_id=%s into %s matched=%s modified=%s", rid, filename, res.matched_count, res.modified_count)
            else:
                # create new row and push
                new_row = {
                    "row_id": str(uuid.uuid4()),
                    "Day": day,
                    "Clinic": clinic,
                    "Name": names[i] if i < len(names) else "",
                    "Age": ages[i] if i < len(ages) else "",
                    "MemberName": parents[i] if i < len(parents) else "",
                    "Comments": comments[i] if i < len(comments) else "",
                    "Fee": fees[i] if i < len(fees) else ""
                }
                for col, vals in dynamic_values.items():
                    new_row[col] = vals[i] if i < len(vals) else ""
                res = get_db()["sheets"].update_one({"filename": filename}, {"$push": {"rows": new_row}})
                app.logger.info("Pushed generated row_id=%s into %s matched=%s modified=%s", new_row["row_id"], filename, res.matched_count, res.modified_count)
    else:
        app.logger.info("save_attendance: no target sheet found for %s — %s; creating new sheet", day, clinic)
        # create a new sheet doc for this clinic/day
        new_rows = []
        for i in range(n):
            if i < len(delete_flags) and delete_flags[i] == "1":
                continue
            rid = row_ids[i] if i < len(row_ids) and row_ids[i] else str(uuid.uuid4())
            r = {
                "row_id": rid,
                "Day": day,
                "Clinic": clinic,
                "Name": names[i] if i < len(names) else "",
                "Age": ages[i] if i < len(ages) else "",
                "MemberName": parents[i] if i < len(parents) else "",
                "Comments": comments[i] if i < len(comments) else "",
                "Fee": fees[i] if i < len(fees) else ""
            }
            for col, vals in dynamic_values.items():
                r[col] = vals[i] if i < len(vals) else ""
            new_rows.append(r)
        safe_clinic = re.sub(r"\s+", "", clinic)
        date_tag = datetime.utcnow().strftime("%Y%m%d")
        filename = f"{day}_{safe_clinic}_{date_tag}.csv"
        save_sheet_to_db(filename, new_rows, dynamic_cols)
        app.logger.info("Created new sheet %s with %d rows", filename, len(new_rows))

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
        target = get_db()["sheets"].find_one({"filename": {"$regex": prefix_re}}, sort=[("created_at", -1)])

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
        # verify insertion
        found = get_db()["sheets"].find_one({"filename": filename, "rows.row_id": new_row["row_id"]})
        app.logger.info("add_row: pushed row_id=%s into %s found=%s", new_row["row_id"], filename, bool(found))
    else:
        dynamic_columns = []
        date_tag = datetime.utcnow().strftime("%Y%m%d")
        filename = f"{day}_{safe_clinic}_{date_tag}.csv"
        save_sheet_to_db(filename, [new_row], dynamic_columns)
        app.logger.info("add_row: created new sheet %s with row_id=%s", filename, new_row["row_id"])

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
    res = get_db()["sheets"].update_many({}, {"$pull": {"rows": {"row_id": row_id}}})
    app.logger.info("delete_row: update_many matched=%s modified=%s", res.matched_count, res.modified_count)
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
        # remove matching rows from every document that contains them
        sheets = list(get_db()["sheets"].find())
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
