# app.py
import io
import re
import os
from pathlib import Path

import pandas as pd
import requests
from flask import Flask, render_template, request, redirect, url_for, flash

from attendance_logic import build_attendance  # keep your central logic here

app = Flask(__name__)
app.secret_key = "supersecretkey"  # required for flashing messages

OUTPUT_DIR = Path("attendance_sheets")
OUTPUT_DIR.mkdir(exist_ok=True)


# ---------- helpers ----------
def extract_sheet_id(sheet_url: str) -> str | None:
    """Extract the Google Sheet ID from a full URL."""
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url)
    return match.group(1) if match else None


def fetch_csv_from_google(sheet_url: str) -> pd.DataFrame:
    """Fetch a CSV from a Google Sheet URL."""
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        raise ValueError("Invalid Google Sheet URL.")
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    r = requests.get(export_url)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def save_attendance_csv(grouped_data: dict, filename="master_attendance.csv") -> Path:
    """Save grouped attendance data from form submission to a CSV file."""
    records = []
    for label, rows in grouped_data.items():
        for row in rows:
            records.append({
                "Day": row.get("Day", ""),
                "Clinic": row.get("Clinic", ""),
                "Time": row.get("Time", ""),
                "Player Name": row.get("Player Name", ""),
                "Player Age": row.get("Player Age", ""),
                "Parent/Guardian Name": row.get("Parent/Guardian Name", ""),
                "Parent/Guardian Email": row.get("Parent/Guardian Email", ""),
                "Parent/Guardian Phone": row.get("Parent/Guardian Phone", "")
            })
    df = pd.DataFrame(records)
    filepath = OUTPUT_DIR / filename
    df.to_csv(filepath, index=False)
    return filepath


# ---------- routes ----------
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        sheet_url = request.form["sheet_url"].strip()
        return redirect(url_for("results", sheet_url=sheet_url))
    return render_template("index.html")


@app.route("/results", methods=["GET"])
def results():
    sheet_url = request.args.get("sheet_url")
    if not sheet_url:
        flash("No Google Sheet URL provided.")
        return redirect(url_for("index"))

    df = fetch_csv_from_google(sheet_url)
    attendance = build_attendance(df)

    # Group by Day and Clinic for rendering
    grouped = attendance.groupby(["Day", "Clinic"])
    data = {}
    for (day, clinic), group in grouped:
        label = f"{day} - {clinic}"
        # Convert to dicts
        data[label] = group.to_dict("records")

    return render_template("results.html", data=data, sheet_url=sheet_url)


@app.route("/save_attendance", methods=["POST"])
def save_attendance():
    """
    Process the submitted form from results.html,
    reconstruct the grouped data, and save it to CSV.
    """
    form_data = request.form.to_dict(flat=False)
    sheet_url = request.form.get("sheet_url", "")

    # Gather table labels
    table_labels = form_data.get("table_label", [])

    # Make sure each input list exists and is the same length
    fields = ["player_name", "player_age", "parent_name",
              "parent_email", "parent_phone", "time"]

    for field in fields:
        if field not in form_data:
            flash(f"Missing form data for {field}")
            return redirect(url_for("results", sheet_url=sheet_url))

    grouped_data = {}
    for i, label in enumerate(table_labels):
        if label not in grouped_data:
            grouped_data[label] = []

        grouped_data[label].append({
            "Player Name": form_data["player_name"][i],
            "Player Age": form_data["player_age"][i],
            "Parent/Guardian Name": form_data["parent_name"][i],
            "Parent/Guardian Email": form_data["parent_email"][i],
            "Parent/Guardian Phone": form_data["parent_phone"][i],
            "Time": form_data["time"][i],
            "Day": label.split(" - ")[0],
            "Clinic": label.split(" - ")[1]
        })

    saved_path = save_attendance_csv(grouped_data)
    flash(f"Attendance saved to {saved_path}")
    return redirect(url_for("results", sheet_url=sheet_url))


# ---------- run ----------
if __name__ == "__main__":
    app.run(debug=True)
