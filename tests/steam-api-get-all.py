import requests
import csv
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

STEAM_API_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
STEAMSPY_API_URL = "https://steamspy.com/api.php"

MAX_WORKERS = 20          
SLEEP_BETWEEN_BATCHES = 1  
CSV_FILE = "steam_full_data.csv"

def get_all_steam_apps():
    """Fetch all apps from Steam official API."""
    print("Fetching app list from Steam...")
    start = time.time()
    r = requests.get(STEAM_API_URL)
    r.raise_for_status()
    apps = r.json()["applist"]["apps"]
    elapsed = time.time() - start
    print(f"Retrieved {len(apps)} apps in {elapsed:.2f} seconds.")
    return apps

def get_steamspy_details(appid):
    """Fetch details for one app from SteamSpy."""
    params = {"request": "appdetails", "appid": appid}
    try:
        r = requests.get(STEAMSPY_API_URL, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data and "name" in data and data["name"]:
                return data
    except requests.RequestException:
        return None
    return None

def save_to_csv_row(row, keys):
    """Append a single game record to CSV."""
    file_exists = os.path.exists( )
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

if __name__ == "__main__":
    start_time = time.time()

    # Load full app list
    apps = get_all_steam_apps()
    app_ids = [a["appid"] for a in apps if a["appid"] > 0]

    # Load already processed IDs for resume
    processed_ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed_ids.add(int(row["appid"]))
        print(f"Resuming... already have {len(processed_ids)} entries.")

    # Prepare keys for CSV
    csv_keys = set()

    total = len(app_ids)
    remaining_ids = [aid for aid in app_ids if aid not in processed_ids]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_steamspy_details, aid): aid for aid in remaining_ids}

        try:
            for i, future in enumerate(as_completed(futures), start=1):
                appid = futures[future]
                try:
                    data = future.result()
                    if data:
                        csv_keys.update(data.keys())
                        save_to_csv_row(data, sorted(csv_keys))
                        processed_ids.add(appid)
                except Exception as e:
                    print(f"Error fetching appid {appid}: {e}")

                if i % (MAX_WORKERS * 5) == 0:
                    print(f"Processed {len(processed_ids)}/{total} apps, collected {len(processed_ids)} games.")
                    time.sleep(SLEEP_BETWEEN_BATCHES)

        except KeyboardInterrupt:
            print("\nStopping early... saving progress so you can resume later.")

    elapsed = time.time() - start_time
    print(f"Finished processing. Total collected: {len(processed_ids)} games.")
    print(f"Total time: {elapsed/60:.2f} minutes")
