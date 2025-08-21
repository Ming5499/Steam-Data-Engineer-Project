import os
import json
import time
import logging
from pymongo import MongoClient

logger = logging.getLogger("MongoImporter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

MONGO_URI = "mongodb://localhost:27018"
DB_NAME = "steam"
COLLECTION_NAME = "reviews"
REVIEW_DIR = "data/raw/review"

def load_to_mongo():
    logger.info("📥 Starting Mongo importer (watch mode)...")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    checkpoints = db["etl_checkpoints"]

    processed_files = set(checkpoints.distinct("filename"))

    new_files = sorted([
        f for f in os.listdir(REVIEW_DIR)
        if f.startswith("reviews_part") and f.endswith(".json") and f not in processed_files
    ])

    if not new_files:
        logger.info("⏹ No new files detected, stopping Mongo importer.")
        return

    total_inserted = 0
    for filename in new_files:
        file_path = os.path.join(REVIEW_DIR, filename)
        logger.info(f"📥 Importing {file_path}...")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not data:
                logger.info(f"⏭ Skipping empty file {filename}")
                continue

            collection.insert_many(data, ordered=False)
            checkpoints.insert_one({"filename": filename})
            inserted = len(data)
            total_inserted += inserted
            logger.info(f"✅ Inserted {inserted} reviews from {filename}")

        except json.JSONDecodeError:
            logger.warning(f"⚠ JSON error in {filename}, skipping (possibly still being written).")

    logger.info(f"🏁 Mongo import finished: {total_inserted} reviews inserted from {len(new_files)} new files.")
