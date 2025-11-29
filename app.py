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
from flask import Flask, render_template, request, redirect, url_for, flash, send_file

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

def save_sheet_to_db(filename, rows, dynamic_columns):
    doc = {
        "filename": filename,
        "rows": rows,
        "dynamic_columns": dynamic_columns,
        "created_at": datetime.utcnow()
    }
    get_db()["sheets"].replace_one({"filename": filename}, doc, upsert=True)

def get_all_sheets():
    return list(get_db()["sheets"].find({}, {"filename": 1, "created_at": 1}).sort("created_at", -1))

def get_sheet_by_filename(filename):
    return get_db()["sheets"].find_one({"filename": filename})

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

                doc = {
                    "filename": filename,
                    "rows": group_rows,
                    "dynamic_columns": dynamic_columns,
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

    # Group by (day, clinic) following CLINIC_ORDER and deduplicate entries
    grouped = []
    for cfg in CLINIC_ORDER:
        items = []
        seen = set()  # dedupe by (day,clinic,time,filename) to avoid duplicates showing
        for sheet in sheets:
            filename = sheet.get("filename")
            for row in sheet.get("rows", []):
                if row.get("Day") == cfg["day"] and row.get("Clinic") == cfg["clinic"]:
                    key = (row.get("Day"), row.get("Clinic"), row.get("Time"), filename)
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append({
                        "filename": filename,
                        "row": row
                    })
        grouped.append({
            "day": cfg["day"],
            "clinic": cfg["clinic"],
            "entries": items
        })

    return render_template("index.html", grouped_clinics=grouped, pretty_filename=pretty_filename)

@app.route("/results", methods=["GET"])
def results():
    filename = request.args.get("filename")
    if not filename:
        flash("No attendance file specified.")
        return redirect(url_for("index"))
    sheet = get_sheet_by_filename(filename)
    if not sheet:
        flash("Attendance sheet not found in database.")
        return redirect(url_for("index"))
    rows = sheet.get("rows", [])
    dynamic_dates = sheet.get("dynamic_columns", [])
    return render_template("results.html", data=rows, filename=filename, dynamic_dates=dynamic_dates, pretty_filename=pretty_filename)

@app.route("/save_attendance", methods=["POST"])
def save_attendance():
    filename = request.form.get("filename")
    if not filename:
        flash("Missing filename.")
        return redirect(url_for("index"))
    sheet = get_sheet_by_filename(filename)
    if not sheet or not sheet.get("rows"):
        flash("Sheet not found in database.")
        return redirect(url_for("index"))
    dynamic_cols = sheet.get("dynamic_columns", [])
    names = request.form.getlist("player_name")
    ages = request.form.getlist("age")
    parents = request.form.getlist("parent")
    comments = request.form.getlist("comments")
    fees = request.form.getlist("fee")
    row_ids = request.form.getlist("row_id")
    delete_flags = request.form.getlist("delete_flag")

    dynamic_values = {}
    for col in dynamic_cols:
        key = f"date__{col}"
        dynamic_values[col] = request.form.getlist(key) if key in request.form else [""] * len(names)

    rows = []
    n = len(names)
    for i in range(n):
        if i < len(delete_flags) and delete_flags[i] == "1":
            continue
        row = {
            "row_id": row_ids[i] if i < len(row_ids) else str(uuid.uuid4()),
            "Name": names[i] if i < len(names) else "",
            "Age": ages[i] if i < len(ages) else "",
            "MemberName": parents[i] if i < len(parents) else "",
            "Comments": comments[i] if i < len(comments) else "",
            "Fee": fees[i] if i < len(fees) else ""
        }
        for col, vals in dynamic_values.items():
            row[col] = vals[i] if i < len(vals) else ""
        rows.append(row)

    update_sheet_rows(filename, rows)
    flash("Attendance saved to database.")
    return redirect(url_for("results", filename=filename))

@app.route("/add_date_column", methods=["POST"])
def add_date_column():
    filename = request.form.get("filename")
    new_col = request.form.get("new_date", "").strip()
    if not filename or not new_col:
        return redirect(url_for("index"))
    sheet = get_sheet_by_filename(filename)
    if not sheet or not sheet.get("rows"):
        flash("Sheet not found in database.")
        return redirect(url_for("index"))
    dynamic_columns = sheet.get("dynamic_columns", [])
    if new_col in dynamic_columns:
        flash("Date column already exists.")
        return redirect(url_for("results", filename=filename))
    dynamic_columns.append(new_col)
    rows = sheet["rows"]
    for row in rows:
        row[new_col] = ""
    update_sheet_dynamic_columns(filename, dynamic_columns, rows)
    flash("Date column added.")
    return redirect(url_for("results", filename=filename))

@app.route("/add_row", methods=["POST"])
def add_row():
    filename = request.form.get("filename")
    if not filename:
        return redirect(url_for("index"))
    sheet = get_sheet_by_filename(filename)
    if not sheet or not sheet.get("rows"):
        flash("Sheet not found in database.")
        return redirect(url_for("index"))
    dynamic_columns = sheet.get("dynamic_columns", [])
    new_row = {
        "row_id": str(uuid.uuid4()),
        "Name": "",
        "Age": "",
        "MemberName": "",
        "Comments": "",
        "Fee": ""
    }
    for col in dynamic_columns:
        new_row[col] = ""
    add_row_to_sheet(filename, new_row)
    flash("Row added.")
    return redirect(url_for("results", filename=filename))

@app.route("/delete_sheet", methods=["POST"])
def delete_sheet_route():
    filename = request.form.get("filename")
    day = request.form.get("day")
    clinic = request.form.get("clinic")
    time = request.form.get("time")
    if day and clinic and time:
        sheet = get_sheet_by_filename(filename)
        if sheet:
            new_rows = [row for row in sheet["rows"] if not (
                row.get("Day") == day and row.get("Clinic") == clinic and row.get("Time") == time
            )]
            update_sheet_rows(filename, new_rows)
            flash(f"Deleted {clinic} {time} on {day} from {filename}.")
    else:
        delete_sheet(filename)
        flash(f"Deleted {filename} from database.")
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
