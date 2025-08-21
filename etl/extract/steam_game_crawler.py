import requests
import csv
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import os
import random
from datetime import datetime
import threading

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("log/steam_game_more_info.log"),
        logging.StreamHandler()
    ]
)

error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler("log/errors/steam_game_more_info_error.log")
error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)


CSV_FILE = f"steam_game_data{datetime.now().strftime('%d%m%Y')}.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}


csv_lock = threading.Lock()

def init_csv():
    if not os.path.exists(CSV_FILE):
        with csv_lock:
            with open(CSV_FILE, "w", encoding="utf-8", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=[
                    "appid", "title", "description", "developer", "publisher",
                    "release_date", "genres", "languages", "content"
                ])
                writer.writeheader()

def fetch_app_list():
    logging.info("Fetching app list from Steam...")
    resp = requests.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
    resp.raise_for_status()
    data = resp.json()
    apps = data.get("applist", {}).get("apps", [])
    logging.info(f"Retrieved {len(apps)} apps.")
    return apps

def fetch_game_details(appid, max_retries=2):
    retries = 0
    while retries <= max_retries:
        try:
            time.sleep(1)  
            url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=en"
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json().get(str(appid), {})

            if not data.get("success", False):
                error_logger.error(f"{appid} - success_false")
                return None

            details = data.get("data", {})
            content_type = details.get("type", "").strip().lower()

            # Skip non-game content (e.g., DLC, demo)
            if content_type != "game":
                logging.info(f"{appid} - Skipped (type: {content_type})")
                return None

            # Extract fields
            title = details.get("name", "").strip()
            description = details.get("short_description", "").strip()
            developer = ", ".join(details.get("developers", [])) if details.get("developers") else ""
            publisher = ", ".join(details.get("publishers", [])) if details.get("publishers") else ""
            release_date = details.get("release_date", {}).get("date", "").strip()

            genres_list = [g.get("description", "").strip() for g in details.get("genres", []) if g.get("description")]
            languages_list = []
            langs_raw = details.get("supported_languages", "")
            if langs_raw:
                soup = BeautifulSoup(langs_raw, "html.parser")
                cleaned = soup.get_text(separator=",")
                languages_list = [lang.strip() for lang in cleaned.split(",") if lang.strip()]

            return {
                "appid": appid,
                "title": title,
                "description": description,
                "developer": developer,
                "publisher": publisher,
                "release_date": release_date,
                "genres": genres_list,
                "languages": languages_list,
                "content": content_type
            }

        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                retries += 1
                wait_time = (1 ** retries) + random.uniform(0, 1) 
                error_logger.error(f"{appid} - 429 Too Many Requests (retry {retries}/{max_retries}), sleeping {wait_time:.2f}s")
                time.sleep(wait_time)
                continue
            else:
                error_logger.error(f"{appid} - {str(e)}")
                return None
        except Exception as e:
            error_logger.error(f"{appid} - {str(e)}")
            return None

    error_logger.error(f"{appid} - failed after {max_retries} retries")
    return None

def save_to_csv(data):
    with csv_lock:
        with open(CSV_FILE, "a", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[
                "appid", "title", "description", "developer", "publisher",
                "release_date", "genres", "languages", "content"
            ])
            writer.writerow(data)

def main():
    init_csv()
    apps = fetch_app_list()
    logging.info(f"Remaining apps to process: {len(apps)}")

    with ThreadPoolExecutor(max_workers=20) as executor: 
        futures = {executor.submit(fetch_game_details, app.get("appid")): app.get("appid") for app in apps if app.get("appid")}

        for future in as_completed(futures):
            appid = futures[future]
            result = future.result()
            if result:
                save_to_csv(result)
                logging.info(f"{appid} saved")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting gracefully...")