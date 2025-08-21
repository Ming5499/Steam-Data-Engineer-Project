import os
import json
from pymongo import MongoClient

def load_json_safe(path):
    """Try to load JSON array from file, fallback to line-by-line recovery."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"⚠ JSON error in {path}, attempting recovery...")
        records = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip().rstrip(",")  
                if not line or line in ("[", "]"):
                    continue
                try:
                    rec = json.loads(line)
                    records.append(rec)
                except json.JSONDecodeError:
                    pass  
        return records

# Connect to MongoDB in Docker
client = MongoClient("mongodb://admin:password@localhost:27018/steam?authSource=admin")
db = client["steam"]
collection = db["reviews"]

folder = "data/raw/review"

for filename in sorted(os.listdir(folder)):
    if filename.startswith("reviews_part") and filename.endswith(".json"):
        path = os.path.join(folder, filename)
        print(f"📥 Importing {path}...")
        data = load_json_safe(path)
        if data:
            collection.insert_many(data)

print("✅ All files imported.")

