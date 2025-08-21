import requests
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== CONFIG =====================
THREADS = 20
BATCH_SIZE = 200
SLEEP_TIME = 1  # per batch
APP_LIST_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
PLAYER_COUNT_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails"
# ===================================================

def get_all_appids():
    r = requests.get(APP_LIST_URL, timeout=15)
    return [app['appid'] for app in r.json()['applist']['apps']]

# ------------------- PHASE 1 ----------------------
def get_player_count(appid):
    try:
        r = requests.get(PLAYER_COUNT_URL, params={"appid": appid}, timeout=5)
        return [appid, r.json().get("response", {}).get("player_count", None)]
    except:
        return [appid, None]

def phase1_player_counts(appids):
    print("[PHASE 1] Starting player count fetch...")
    first_batch = True
    for i in range(0, len(appids), BATCH_SIZE):
        batch_appids = appids[i:i+BATCH_SIZE]
        batch_data = []
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(get_player_count, appid) for appid in batch_appids]
            for f in as_completed(futures):
                batch_data.append(f.result())
        save_csv("phase1_player_counts.csv", batch_data, first_batch, ["appid", "player_count"])
        first_batch = False
        print(f"[PHASE 1] Batch {i//BATCH_SIZE+1} done ({len(batch_data)} records).")
        time.sleep(SLEEP_TIME)
    print("[PHASE 1] Done.")

# ------------------- PHASE 2 ----------------------
def get_app_details(appid):
    try:
        r = requests.get(APP_DETAILS_URL, params={"appids": appid}, timeout=5)
        data = r.json().get(str(appid), {}).get("data", {})
        return [
            appid,
            data.get("name"),
            data.get("type"),
            data.get("release_date", {}).get("date"),
            data.get("release_date", {}).get("coming_soon")
        ]
    except:
        return [appid, None, None, None, None]

def phase2_app_info(appids):
    print("[PHASE 2] Starting app info fetch...")
    first_batch = True
    for i in range(0, len(appids), BATCH_SIZE):
        batch_appids = appids[i:i+BATCH_SIZE]
        batch_data = []
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(get_app_details, appid) for appid in batch_appids]
            for f in as_completed(futures):
                batch_data.append(f.result())
        save_csv("phase2_app_info.csv", batch_data, first_batch, ["appid", "name", "type", "release_date", "coming_soon"])
        first_batch = False
        print(f"[PHASE 2] Batch {i//BATCH_SIZE+1} done ({len(batch_data)} records).")
        time.sleep(SLEEP_TIME)
    print("[PHASE 2] Done.")

# ------------------- PHASE 3 ----------------------
def get_extra_data(appid):
    try:
        r = requests.get(APP_DETAILS_URL, params={"appids": appid}, timeout=5)
        data = r.json().get(str(appid), {}).get("data", {})
        return [
            appid,
            data.get("price_overview", {}).get("final_formatted"),
            ",".join([g["description"] for g in data.get("genres", [])])
        ]
    except:
        return [appid, None, None]

def phase3_extra_data(appids):
    print("[PHASE 3] Starting extra data fetch...")
    first_batch = True
    for i in range(0, len(appids), BATCH_SIZE):
        batch_appids = appids[i:i+BATCH_SIZE]
        batch_data = []
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(get_extra_data, appid) for appid in batch_appids]
            for f in as_completed(futures):
                batch_data.append(f.result())
        save_csv("phase3_extra_data.csv", batch_data, first_batch, ["appid", "price", "genres"])
        first_batch = False
        print(f"[PHASE 3] Batch {i//BATCH_SIZE+1} done ({len(batch_data)} records).")
        time.sleep(SLEEP_TIME)
    print("[PHASE 3] Done.")

# ------------------- CSV SAVE ---------------------
def save_csv(filename, data, first_batch, header):
    mode = "w" if first_batch else "a"
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if first_batch:
            writer.writerow(header)
        writer.writerows(data)

# ------------------- MAIN -------------------------
if __name__ == "__main__":
    print("[INFO] Fetching all app IDs...")
    appids = get_all_appids()
    print(f"[INFO] Found {len(appids)} app IDs.")

    # Run phases in parallel
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.submit(phase1_player_counts, appids)
        executor.submit(phase2_app_info, appids)
        executor.submit(phase3_extra_data, appids)

    print("[INFO] All phases started in parallel.")
