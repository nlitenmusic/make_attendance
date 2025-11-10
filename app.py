# app.py
import io
import re
import requests
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for
from attendance_logic import build_attendance, export_attendance_sheets  # import functions

app = Flask(__name__)

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

# ---------- routes ----------
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        sheet_url = request.form["sheet_url"].strip()
        return redirect(url_for("results", sheet_url=sheet_url))
    return render_template("index.html")

@app.route("/results")
def results():
    sheet_url = request.args.get("sheet_url")
    df = fetch_csv_from_google(sheet_url)
    
    # Use the central logic from attendance_logic.py
    attendance = build_attendance(df)

    # Optional: export offline sheets if you want
    # export_attendance_sheets(attendance, "attendance_sheets")

    # Group by Day and Clinic for rendering
    grouped = attendance.groupby(["Day", "Clinic"])
    data = {f"{day} - {clinic}": group.to_dict("records")
            for (day, clinic), group in grouped}

    return render_template("results.html", data=data, sheet_url=sheet_url)

# ---------- run ----------
if __name__ == "__main__":
    app.run(debug=True)
