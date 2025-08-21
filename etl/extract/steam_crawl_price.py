import requests
import csv
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime

STEAM_API_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
STEAMSPY_API_URL = "https://steamspy.com/api.php"

MAX_WORKERS = 20
SLEEP_BETWEEN_BATCHES = 0.5
CSV_FILE = f"data/raw/price/price_raw{datetime.now().strftime('%d%m%Y')}.csv"
BATCH_SIZE = 50  

COLUMNS = ["appid", "discount", "price", ]  

lock = Lock()
buffer_rows = [] 


def retry_request(url, params=None, timeout=10, retries=3):
    """Retry GET request with exponential backoff."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt)  
    return None


def get_all_steam_apps():
    print("Fetching app list from Steam...")
    start = time.time()
    r = retry_request(STEAM_API_URL)
    if not r:
        raise RuntimeError("Failed to fetch Steam app list.")
    apps = r.json()["applist"]["apps"]
    print(f"Retrieved {len(apps)} apps in {time.time() - start:.2f} seconds.")
    return apps


def get_steamspy_details(appid):
    """Fetch app details from SteamSpy."""
    details = {col: "" for col in COLUMNS}
    details["appid"] = appid

    spy_resp = retry_request(STEAMSPY_API_URL, params={"request": "appdetails", "appid": appid})
    if spy_resp:
        spy_data = spy_resp.json()
        for col in ["discount", "price",]:  
            details[col] = spy_data.get(col, "")

    return details


def save_batch_to_csv(rows):
    """Save a batch of rows to CSV."""
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    
def crawl_price():
    start_time = time.time()

    apps = get_all_steam_apps()
    app_ids = [a["appid"] for a in apps if a["appid"] > 0]

    # Resume support
    processed_ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed_ids.add(int(row["appid"]))
        print(f"Resuming... already have {len(processed_ids)} entries.")

    remaining_ids = [aid for aid in app_ids if aid not in processed_ids]
    total = len(remaining_ids)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_steamspy_details, aid): aid for aid in remaining_ids}

        try:
            for i, future in enumerate(as_completed(futures), start=1):
                appid = futures[future]
                try:
                    data = future.result()
                    if data:
                        with lock:
                            buffer_rows.append(data)
                            processed_ids.add(appid)

                            # Write batch when buffer is full
                            if len(buffer_rows) >= BATCH_SIZE:
                                save_batch_to_csv(buffer_rows)
                                buffer_rows.clear()

                    # Progress log
                    if i % (MAX_WORKERS * 5) == 0 or i == total:
                        percent = (i / total) * 100
                        print(f"Processed {i}/{total} apps ({percent:.2f}%)")
                        time.sleep(SLEEP_BETWEEN_BATCHES)

                except Exception as e:
                    print(f"Error fetching appid {appid}: {e}")

        except KeyboardInterrupt:
            print("\nStopping early... saving remaining buffer.")


    if buffer_rows:
        save_batch_to_csv(buffer_rows)

    elapsed = time.time() - start_time
    print(f"Finished processing. Total collected: {len(processed_ids)} records.")
    print(f"Total time: {elapsed/60:.2f} minutes")
    
crawl_price()