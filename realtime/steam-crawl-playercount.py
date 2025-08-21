import requests
import csv
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------- CONFIG --------
API_ALL_APPS = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
API_PLAYER_COUNT = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

DISCOVERY_CSV = "all_games_player_counts_test_1.csv"
TOP2000_CSV = "top2000.csv"
REALTIME_CSV = "realtime_player_counts.csv"

# Phase 1 (fast scan) settings
MAX_WORKERS = 12
BATCH_SIZE = 100
SLEEP_BETWEEN_BATCHES = 60  # seconds

# Real-time update settings
UPDATE_INTERVAL_MINUTES = 10   # How often to run updates


# -------- FETCH FUNCTIONS --------
def fetch_all_appids():
    print("[INFO] Fetching all appids from Steam...")
    resp = requests.get(API_ALL_APPS, timeout=15)
    data = resp.json()
    return data.get("applist", {}).get("apps", [])


def fetch_player_count(app):
    appid = app["appid"]
    name = app["name"]
    try:
        resp = requests.get(API_PLAYER_COUNT, params={"appid": appid}, timeout=10)
        count = resp.json().get("response", {}).get("player_count", 0)
        return appid, name, count
    except Exception:
        return appid, name, 0


# -------- PHASE 1: FAST DISCOVERY --------
def phase1_discovery_fast():
    apps = fetch_all_appids()
    total = len(apps)
    print(f"[INFO] Found {total} apps.")

    with open(DISCOVERY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["appid", "name", "player_count"])

        for batch_start in range(0, total, BATCH_SIZE):
            batch = apps[batch_start: batch_start + BATCH_SIZE]
            print(f"[INFO] Processing batch {batch_start // BATCH_SIZE + 1} "
                  f"({batch_start}-{batch_start + len(batch)})...")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(fetch_player_count, app) for app in batch]

                for future in as_completed(futures):
                    result = future.result()
                    writer.writerow(result)

            print("[INFO] Batch done. Sleeping to respect API limits...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

    print(f"[INFO] Discovery complete → {DISCOVERY_CSV}")


# -------- PHASE 2: CREATE TOP2000 --------
def create_top2000():
    rows = []
    with open(DISCOVERY_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rows.append((int(row["appid"]),  int(row["player_count"])))
            except ValueError:
                continue
    
    rows.sort(key=lambda x: x[2], reverse=True)
    top2000 = rows[:2000]

    with open(TOP2000_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["appid", "player_count"])
        writer.writerows(top2000)
    
    print(f"[INFO] Top 2000 saved → {TOP2000_CSV}")


# -------- PHASE 3: REAL-TIME UPDATES (LOOP) --------
def realtime_update_loop():
    top2000 = []
    with open(TOP2000_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            top2000.append((int(row["appid"]), row["name"]))

    while True:
        timestamp = datetime.utcnow().isoformat()
        print(f"[INFO] Starting real-time update at {timestamp}")

        with open(REALTIME_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["timestamp", "appid", "name", "current_players"])

            for appid, name in top2000:
                count = fetch_player_count({"appid": appid, "name": name})
                writer.writerow([timestamp, appid, name, count])
                time.sleep(0.6)  # Safe delay between requests

        print(f"[INFO] Update complete, sleeping {UPDATE_INTERVAL_MINUTES} minutes...\n")
        time.sleep(UPDATE_INTERVAL_MINUTES * 60)


# -------- MAIN --------
if __name__ == "__main__":
    # Uncomment the step(s) you want to run:
    
    # Step 1: Fast discovery of all games (takes ~3-4h)
    #phase1_discovery_fast()
    
    # Step 2: Create top2000 list from discovery CSV
    create_top2000()
    
    # Step 3: Real-time update loop (runs forever)
    realtime_update_loop()
