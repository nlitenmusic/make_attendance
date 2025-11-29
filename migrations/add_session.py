import os
from pymongo import MongoClient

MONGO_URL = os.getenv("MONGO_URL")
if not MONGO_URL:
    raise RuntimeError("Set MONGO_URL environment variable before running migration")

SESSION_NAME = os.getenv("SESSION_NAME", "Fall 2025")

client = MongoClient(MONGO_URL)
db = client.get_database("attendance_db")

# only set session on docs that don't already have one
res = db["sheets"].update_many(
    {"session": {"$exists": False}},
    {"$set": {"session": SESSION_NAME}}
)
print(f"Matched: {res.matched_count}, Modified: {res.modified_count}")
client.close()