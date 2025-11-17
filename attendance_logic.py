# attendance_logic.py
import pandas as pd
import uuid
from pathlib import Path

# Clinic columns expected in the Google Sheet
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

# Final binder CSV columns (Option A)
BINDER_COLUMNS = ["row_id", "Name", "Age", "MemberName", "Comments", "Fee"]


def _choose_name_column(df: pd.DataFrame) -> str | None:
    """
    Return whichever column in df that most likely contains the player's name.
    We prioritize the exact header you confirmed, but accept others as fallback.
    """
    candidates = [
        "Player Name (First and Last)",
        "Player Name",
        "Name",
        "Player",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    # fallback: any column that contains 'name' (case-insensitive)
    for c in df.columns:
        if "name" in c.lower():
            return c
    return None


def convert_import_to_internal_schema(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the raw Google Sheet to a minimal internal DataFrame that still
    contains the clinic scheduling columns. We return the df with:
      - mapped name column -> 'Name'
      - Age -> 'Age' if present
      - Parent/Guardian Name -> 'MemberName' if present
      - original clinic columns preserved (so build_attendance can read them)
    Comments and Fee are added blank.
    """
    df = df_raw.copy()

    name_col = _choose_name_column(df)
    if name_col:
        df.rename(columns={name_col: "Name"}, inplace=True)

    # map known columns if they exist
    map_cols = {}
    if "Player Age" in df.columns:
        map_cols["Player Age"] = "Age"
    if "Parent/Guardian Name" in df.columns:
        map_cols["Parent/Guardian Name"] = "MemberName"
    # keep Email/Phone if present, but they will not be exported to binder CSVs
    if "Parent/Guardian Email" in df.columns:
        map_cols["Parent/Guardian Email"] = "Email"
    if "Parent/Guardian Phone Number" in df.columns:
        map_cols["Parent/Guardian Phone Number"] = "Phone"

    if map_cols:
        df.rename(columns=map_cols, inplace=True)

    # Ensure minimal columns exist so code later doesn't break
    for col in ("Name", "Age", "MemberName"):
        if col not in df.columns:
            df[col] = ""

    # Add blank Comments/Fee for binder
    df["Comments"] = ""
    df["Fee"] = ""

    # Ensure row_id for raw rows (not strictly necessary for export)
    if "row_id" not in df.columns:
        df["row_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    # remove NaN values now to avoid 'nan' in saved CSVs
    df = df.fillna("")

    return df


def build_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand each signup row into one or more attendance records (Day/Clinic/Time).
    Returns a long-form DataFrame with Day, Clinic, Time, Name, Age, MemberName, Email, Phone, Comments, Fee, row_id.
    """
    records = []

    for clinic_name, column in CLINIC_COLUMNS.items():
        if column not in df.columns:
            continue

        for _, row in df.iterrows():
            times_field = row.get(column)
            if pd.isna(times_field) or times_field == "":
                continue

            sessions = [s.strip() for s in str(times_field).split(",") if s.strip()]
            for session in sessions:
                parts = [p.strip() for p in session.split("-")]
                if len(parts) != 3:
                    # skip malformed
                    continue
                day, start, end = parts
                records.append({
                    "Day": day,
                    "Clinic": clinic_name,
                    "Time": f"{start} - {end}",
                    "Name": row.get("Name", ""),
                    "Age": row.get("Age", ""),
                    "MemberName": row.get("MemberName", ""),
                    "Email": row.get("Email", ""),
                    "Phone": row.get("Phone", ""),
                    "Comments": "",
                    "Fee": "",
                    "row_id": str(uuid.uuid4())
                })

    attendance = pd.DataFrame(records)
    if attendance.empty:
        # return empty DataFrame with columns we expect
        cols = ["Day", "Clinic", "Time", "Name", "Age", "MemberName", "Email", "Phone", "Comments", "Fee", "row_id"]
        return pd.DataFrame(columns=cols)

    attendance["Day"] = pd.Categorical(attendance["Day"], categories=DAY_ORDER, ordered=True)
    attendance["Clinic"] = pd.Categorical(attendance["Clinic"], categories=CLINIC_ORDER, ordered=True)

    return attendance.sort_values(["Day", "Clinic", "Time", "Name"])


def export_attendance_sheets(attendance_df: pd.DataFrame, output_dir: str | Path):
    """
    For each (Day, Clinic) export a binder-style CSV with only:
      row_id, Name, Age, MemberName, Comments, Fee
    (dynamic date columns are intentionally NOT created here — Option A)
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if attendance_df.empty:
        return

    for (day, clinic), group in attendance_df.groupby(["Day", "Clinic"]):
        if pd.isna(day):
            continue

        clean_filename = f"{day}_{clinic.replace(' ', '')}.csv"
        filepath = out / clean_filename

        binder_df = group[["row_id", "Name", "Age", "MemberName", "Comments", "Fee"]].copy()
        # make sure NaN -> ""
        binder_df = binder_df.fillna("")
        binder_df.to_csv(filepath, index=False)
