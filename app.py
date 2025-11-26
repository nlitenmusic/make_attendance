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

mongo_uri = os.getenv("MONGO_URL")
client = MongoClient(mongo_uri)
db = client["attendance_db"]

app = Flask(__name__)
app.secret_key = "supersecretkey"

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

def save_sheet_to_db(filename, attendance):
    doc = {
        "filename": filename,
        "uploaded_at": datetime.utcnow(),
        "attendance": attendance  # list of dicts
    }
    db.sheets.replace_one({"filename": filename}, doc, upsert=True)

def get_all_sheets():
    return list(db.sheets.find().sort("uploaded_at", -1))

def get_sheet_by_filename(filename):
    return db.sheets.find_one({"filename": filename})

def update_sheet_attendance(filename, attendance):
    db.sheets.update_one({"filename": filename}, {"$set": {"attendance": attendance}})

def delete_sheet(filename):
    db.sheets.delete_one({"filename": filename})

def export_all_sheets_to_csv():
    sheets = get_all_sheets()
    all_rows = []
    for sheet in sheets:
        for row in sheet.get("attendance", []):
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
            filename = request.form.get("filename") or f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
            attendance_dict = attendance_df.to_dict(orient="records")
            save_sheet_to_db(filename, attendance_dict)
            flash("Attendance sheet saved to database.")
        except Exception as e:
            flash(f"Error: {e}")
        return redirect(url_for("index"))

    sheets = get_all_sheets()
    return render_template("index.html", sheets=sheets, pretty_filename=pretty_filename)

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
    data = sheet.get("attendance", [])
    binder_cols = ["row_id", "Name", "Age", "MemberName", "Comments", "Fee"]
    dynamic_dates = [k for k in data[0].keys() if k not in binder_cols and k != "filename"] if data else []
    return render_template("results.html", data=data, filename=filename, dynamic_dates=dynamic_dates, pretty_filename=pretty_filename)

@app.route("/save_attendance", methods=["POST"])
def save_attendance():
    filename = request.form.get("filename")
    if not filename:
        flash("Missing filename.")
        return redirect(url_for("index"))
    sheet = get_sheet_by_filename(filename)
    if not sheet or not sheet.get("attendance"):
        flash("Sheet not found in database.")
        return redirect(url_for("index"))
    data = sheet["attendance"]
    binder_cols = ["row_id", "Name", "Age", "MemberName", "Comments", "Fee"]
    dynamic_cols = [k for k in data[0].keys() if k not in binder_cols and k != "filename"] if data else []

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

    update_sheet_attendance(filename, rows)
    flash("Attendance saved to database.")
    return redirect(url_for("results", filename=filename))

@app.route("/add_date_column", methods=["POST"])
def add_date_column():
    filename = request.form.get("filename")
    new_col = request.form.get("new_date", "").strip()
    if not filename or not new_col:
        return redirect(url_for("index"))
    sheet = get_sheet_by_filename(filename)
    if not sheet or not sheet.get("attendance"):
        flash("Sheet not found in database.")
        return redirect(url_for("index"))
    data = sheet["attendance"]
    for row in data:
        row[new_col] = ""
    update_sheet_attendance(filename, data)
    flash("Date column added.")
    return redirect(url_for("results", filename=filename))

@app.route("/add_row", methods=["POST"])
def add_row():
    filename = request.form.get("filename")
    if not filename:
        return redirect(url_for("index"))
    sheet = get_sheet_by_filename(filename)
    if not sheet or not sheet.get("attendance"):
        flash("Sheet not found in database.")
        return redirect(url_for("index"))
    data = sheet["attendance"]
    if data:
        new_row = {c: "" for c in data[0].keys()}
    else:
        new_row = {"row_id": str(uuid.uuid4())}
    new_row["row_id"] = str(uuid.uuid4())
    data.append(new_row)
    update_sheet_attendance(filename, data)
    flash("Row added.")
    return redirect(url_for("results", filename=filename))

@app.route("/delete_sheet", methods=["POST"])
def delete_sheet_route():
    filename = request.form.get("filename")
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
