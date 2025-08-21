import requests
import csv
import time
import os
import random
import sys
import signal
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/steam_game_more_info.log'
)

# --- Config ---
STEAM_API_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
STEAM_STORE_API_URL = "https://store.steampowered.com/api/appdetails"
STEAM_STORE_PAGE = "https://store.steampowered.com/app/{}"

MAX_WORKERS = 10
MAX_RETRIES = 1
CSV_FILE = "data/game/steam_game_more_info.csv"
ERROR_LOG = "logs/errors/steam_game_more_info_error.log"
COLUMNS = ["appid", "windows_req", "mac_req", "linux_req", "required_age", "awards"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9"
}

STOP = False

def strip_html_tags(text):
    if not text:
        return ""
    return BeautifulSoup(text, "html.parser").get_text(separator=" ", strip=True)

def signal_handler(sig, frame):
    global STOP
    logging.info("Ctrl+C detected. Will stop after current requests finish...")
    STOP = True

signal.signal(signal.SIGINT, signal_handler)

def safe_get_minimum(req_data):
    """Safely extract and strip 'minimum' field from requirement data."""
    if isinstance(req_data, dict):
        return strip_html_tags(req_data.get("minimum", ""))
    elif isinstance(req_data, list):
        return strip_html_tags(" ".join([str(r) for r in req_data]))
    elif isinstance(req_data, str):
        return strip_html_tags(req_data)
    return ""

def log_error(appid, code):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"{appid},{code}\n")
    logging.error(f"{appid} - {code}")

def get_award_flag(appid):
    """Return 'Award' if #awardsTable exists on the app's store page, else ''."""
    try:
        url = STEAM_STORE_PAGE.format(appid)
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        if soup.find("div", id="awardsTable"):
            return "Award"
        return ""
    except Exception:
        return ""

def get_storefront_details(appid):
    """Fetch app details with retries for 403/429. Returns dict row or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            params = {"appids": appid}
            r = requests.get(STEAM_STORE_API_URL, params=params, headers=HEADERS, timeout=12)

            if r.status_code in (403, 429):
                wait = random.uniform(15, 25)
                logging.warning(f"{appid} - {r.status_code} (attempt {attempt}) sleeping {wait:.1f}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            data = r.json()
            entry = data.get(str(appid))
            if not entry:
                log_error(appid, "no_entry")
                return None
            if not entry.get("success", False):
                log_error(appid, "success_false")
                return None

            d = entry["data"]

            if d.get("type") != "game":
                log_error(appid, "not_a_game")
                return None

            windows_req = safe_get_minimum(d.get("pc_requirements", {}))
            mac_req = safe_get_minimum(d.get("mac_requirements", {}))
            linux_req = safe_get_minimum(d.get("linux_requirements", {}))
            required_age = d.get("required_age", "")
            awards = get_award_flag(appid)

            time.sleep(random.uniform(0.8, 1.5))

            return {
                "appid": appid,
                "windows_req": windows_req,
                "mac_req": mac_req,
                "linux_req": linux_req,
                "required_age": required_age,
                "awards": awards
            }

        except requests.RequestException as e:
            logging.error(f"{appid} - request error: {e}")
            time.sleep(random.uniform(5, 10))
            continue

    log_error(appid, f"failed_after_{MAX_RETRIES}")
    return None

def save_to_csv_row(row):
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def load_error_appids():
    if not os.path.exists(ERROR_LOG):
        return set()
    appids = set()
    with open(ERROR_LOG, "r", encoding="utf-8") as f:
        for line in f:
            try:
                appid = int(line.strip().split(",")[0])
                appids.add(appid)
            except:
                pass
    return appids

def main():
    start_time = time.time()
    logging.info("Fetching app list from Steam...")
    r = requests.get(STEAM_API_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    apps = r.json().get("applist", {}).get("apps", [])
    app_ids = [a["appid"] for a in apps if a.get("appid", 0) > 0]
    total = len(app_ids)
    logging.info(f"Retrieved {total} apps.")

    processed_ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    processed_ids.add(int(row["appid"]))
                except:
                    pass
        logging.info(f"Resuming... already have {len(processed_ids)} entries.")

    error_ids = load_error_appids()
    remaining_ids = [aid for aid in app_ids if aid not in processed_ids and aid not in error_ids]
    logging.info(f"Remaining apps to process: {len(remaining_ids)}")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(get_storefront_details, aid): aid for aid in remaining_ids}

            for i, future in enumerate(as_completed(futures), start=1):
                if STOP:
                    logging.info("Stop flag set — cancelling remaining work...")
                    break

                aid = futures[future]
                try:
                    row = future.result()
                except Exception as e:
                    log_error(aid, f"worker_exception:{e}")
                    row = None

                if row:
                    save_to_csv_row(row)
                    processed_ids.add(aid)
                    logging.info(f"{aid} saved (age={row['required_age']})")

                if i % (MAX_WORKERS * 5) == 0:
                    time.sleep(random.uniform(3, 5))

            if STOP:
                for fut in futures:
                    if not fut.done():
                        fut.cancel()

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt — stopping early and saving progress...")

    elapsed = time.time() - start_time
    logging.info(f"Finished run. Collected: {len(processed_ids)} records. Time: {elapsed/60:.2f} minutes")

if __name__ == "__main__":
    main()
