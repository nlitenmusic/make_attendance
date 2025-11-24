# app.py
import os
import io
import re
import uuid
from pathlib import Path

import pandas as pd
import requests
from flask import Flask, render_template, request, redirect, url_for, flash

from attendance_logic import convert_import_to_internal_schema, build_attendance, export_attendance_sheets

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

# -------------------- Routes --------------------

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
            attendance = build_attendance(df_clean)
            export_attendance_sheets(attendance, ATTENDANCE_DIR)
            flash("Attendance sheets generated successfully.")
        except Exception as e:
            flash(f"Error: {e}")
        return redirect(url_for("index"))

    saved_sheets = list_saved_sheets_sorted()
    return render_template("index.html", saved_sheets=saved_sheets, pretty_filename=pretty_filename)


@app.route("/results", methods=["GET"])
def results():
    filename = request.args.get("filename")
    if not filename:
        flash("No attendance file specified.")
        return redirect(url_for("index"))
    path = ATTENDANCE_DIR / filename
    if not path.exists():
        flash("Attendance file not found.")
        return redirect(url_for("index"))

    df = pd.read_csv(path, dtype=str).fillna("")
    # ensure binder columns exist
    for c in ["row_id", "Name", "Age", "MemberName", "Comments", "Fee"]:
        if c not in df.columns:
            df[c] = ""

    # dynamic dates are any columns not in binder set
    dynamic_dates = [c for c in df.columns if c not in ["row_id", "Name", "Age", "MemberName", "Comments", "Fee"]]

    # convert rows to list of dicts for template
    data = df.to_dict("records")
    return render_template("results.html", data=data, filename=filename, dynamic_dates=dynamic_dates, pretty_filename=pretty_filename)


@app.route("/save_attendance", methods=["POST"])
def save_attendance():
    filename = request.form.get("filename")
    if not filename:
        flash("Missing filename.")
        return redirect(url_for("index"))
    path = ATTENDANCE_DIR / filename
    if not path.exists():
        flash("File not found.")
        return redirect(url_for("index"))

    # load current sheet to get column ordering and dynamic columns
    df = pd.read_csv(path, dtype=str).fillna("")
    dynamic_cols = [c for c in df.columns if c not in ["row_id", "Name", "Age", "MemberName", "Comments", "Fee"]]

    # get arrays from form: the template will submit array-like fields
    names = request.form.getlist("player_name")
    ages = request.form.getlist("age")
    parents = request.form.getlist("parent")
    comments = request.form.getlist("comments")
    fees = request.form.getlist("fee")
    row_ids = request.form.getlist("row_id")
    delete_flags = request.form.getlist("delete_flag")

    # dynamic date columns posted as date__<colname>
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
        # attach dynamic dates
        for col, vals in dynamic_values.items():
            row[col] = vals[i] if i < len(vals) else ""
        rows.append(row)

    df_new = pd.DataFrame(rows)
    # ensure binder columns order + dynamic cols
    cols = ["row_id", "Name", "Age", "MemberName"] + dynamic_cols + ["Comments", "Fee"]
    # fill missing dynamic cols if none present
    for c in cols:
        if c not in df_new.columns:
            df_new[c] = ""
    df_new = df_new[cols]
    df_new = df_new.fillna("")
    df_new.to_csv(path, index=False)

    flash("Attendance saved.")
    return redirect(url_for("results", filename=filename))


@app.route("/add_date_column", methods=["POST"])
def add_date_column():
    filename = request.form.get("filename")
    new_col = request.form.get("new_date", "").strip()
    if not filename or not new_col:
        return redirect(url_for("index"))
    path = ATTENDANCE_DIR / filename
    if not path.exists():
        flash("File not found.")
        return redirect(url_for("index"))
    df = pd.read_csv(path, dtype=str).fillna("")
    if new_col in df.columns:
        flash("Date column already exists.")
        return redirect(url_for("results", filename=filename))
    df[new_col] = ""
    df.to_csv(path, index=False)
    flash("Date column added.")
    return redirect(url_for("results", filename=filename))


@app.route("/add_row", methods=["POST"])
def add_row():
    filename = request.form.get("filename")
    if not filename:
        return redirect(url_for("index"))
    path = ATTENDANCE_DIR / filename
    if not path.exists():
        flash("File not found.")
        return redirect(url_for("index"))
    df = pd.read_csv(path, dtype=str).fillna("")
    new_row = {c: "" for c in df.columns}
    new_row["row_id"] = str(uuid.uuid4())
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(path, index=False)
    flash("Row added.")
    return redirect(url_for("results", filename=filename))


@app.route("/delete_sheet", methods=["POST"])
def delete_sheet():
    filename = request.form.get("filename")
    path = ATTENDANCE_DIR / filename
    if path.exists():
        path.unlink()
        flash(f"Deleted {filename}")
    else:
        flash("File not found.")
    return redirect(url_for("index"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
