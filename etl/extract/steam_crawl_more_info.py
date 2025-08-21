import aiohttp
import asyncio
import csv
import logging
from bs4 import BeautifulSoup
import os
from datetime import datetime
import re

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("steam_game_more_info.log"),
        logging.StreamHandler()
    ]
)

error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler("steam_game_more_info_error.log")
error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# Set CSV filename with current date (DDMMYYYY)
CSV_FILE = f"steam_game_data_123{datetime.now().strftime('%d%m%Y')}.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

# Lock for thread-safe CSV writing
csv_lock = asyncio.Lock()

def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[
                "appid", "title", "description", "developer", "publisher",
                "release_date", "genres", "languages", "content"
            ])
            writer.writeheader()

async def fetch_app_list(session):
    logging.info("Fetching app list from Steam...")
    async with session.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/") as resp:
        resp.raise_for_status()
        data = await resp.json()
        apps = data.get("applist", {}).get("apps", [])
        logging.info(f"Retrieved {len(apps)} apps.")
        
        # Pre-filter apps based on name heuristics
        exclude_patterns = re.compile(r"(?i)\b(DLC|Demo|Soundtrack|OST|Beta|Test|Pack|Addon|Expansion)\b")
        filtered_apps = [
            app for app in apps 
            if app.get("appid") and isinstance(app.get("appid"), int) and app.get("appid") > 0
            and app.get("name") and not exclude_patterns.search(app.get("name", ""))
        ]
        logging.info(f"Filtered to {len(filtered_apps)} apps after excluding likely non-games and invalid appids.")
        return filtered_apps

async def fetch_game_details(session, appid):
    try:
        await asyncio.sleep(1)  # Base delay for rate limit compliance
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=en"
        async with session.get(url, headers=HEADERS, timeout=10) as r:
            r.raise_for_status()
            data = await r.json()
            
            app_data = data.get(str(appid), {})
            if not app_data.get("success", False):
                error_logger.error(f"{appid} - success_false, url={url}")
                return None

            details = app_data.get("data", {})
            content_type = details.get("type", "").strip().lower()

            # Skip non-game content
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

    except Exception as e:
        error_logger.error(f"{appid} - {str(e)}, url={url}")
        return None

async def save_to_csv(data):
    if data:
        async with csv_lock:
            with open(CSV_FILE, "a", encoding="utf-8", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=[
                    "appid", "title", "description", "developer", "publisher",
                    "release_date", "genres", "languages", "content"
                ])
                writer.writerow(data)

async def main():
    init_csv()
    async with aiohttp.ClientSession() as session:
        apps = await fetch_app_list(session)
        logging.info(f"Remaining apps to process: {len(apps)}")

        tasks = []
        for app in apps:
            appid = app.get("appid")
            if appid:
                tasks.append(fetch_game_details(session, appid))

        for future in asyncio.as_completed(tasks):
            result = await future
            if result:
                await save_to_csv(result)
                logging.info(f"{result['appid']} saved")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting gracefully...")