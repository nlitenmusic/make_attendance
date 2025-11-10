# attendance_logic.py
import pandas as pd
from pathlib import Path

CLINIC_COLUMNS = {
    "Red Ball Clinic": "Red Ball Clinic - Day & Time",
    "Orange Ball Clinic": "Orange Ball Clinic - Day & Time",
    "Green Ball Clinic": "Green Ball Clinic - Day & Time",
    "Yellow Ball Clinic": "Yellow Ball Clinic - Day & Time",
    "High Performance Clinic": "High Performance Clinic - Day & Time",
}

DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
CLINIC_ORDER = [
    "Red Ball Clinic",
    "Orange Ball Clinic",
    "Green Ball Clinic",
    "Yellow Ball Clinic",
    "High Performance Clinic"
]

def build_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a raw sign-up DataFrame into a structured attendance DataFrame.
    Adds a blank 'Present' column for check-off purposes.
    """
    records = []

    for clinic_name, column in CLINIC_COLUMNS.items():
        for _, row in df.iterrows():
            times_field = row.get(column)
            if pd.isna(times_field):
                continue

            sessions = [s.strip() for s in str(times_field).split(",") if s.strip()]

            for session in sessions:
                parts = [p.strip() for p in session.split("-")]
                if len(parts) != 3:
                    continue
                day, start, end = parts
                records.append({
                    "Day": day,
                    "Clinic": clinic_name,
                    "Time": f"{start} - {end}",
                    "Player Name": row["Player Name (First and Last)"],
                    "Player Age": row["Player Age"],
                    "Parent/Guardian Name": row["Parent/Guardian Name"],
                    "Parent/Guardian Email": row["Parent/Guardian Email"],
                    "Parent/Guardian Phone": row["Parent/Guardian Phone Number"],
                    "Present": ""
                })

    attendance = pd.DataFrame(records)
    attendance["Day"] = pd.Categorical(attendance["Day"], categories=DAY_ORDER, ordered=True)
    attendance["Clinic"] = pd.Categorical(attendance["Clinic"], categories=CLINIC_ORDER, ordered=True)
    return attendance.sort_values(["Day", "Clinic", "Time", "Player Name"])


def export_attendance_sheets(attendance_df: pd.DataFrame, output_dir: str | Path) -> None:
    """
    Save separate CSVs for each Day/Clinic combination. 
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    for (clinic, day), group in attendance_df.groupby(["Clinic", "Day"]):
        if pd.isna(day):
            continue
        clean_filename = f"{day}_{clinic.replace(' ', '')}.csv"
        filepath = output_dir / clean_filename
        group_sorted = group.sort_values(by=["Time", "Player Name"])
        group_sorted.to_csv(filepath, index=False)
        print(f"✅ Saved: {filepath}")
