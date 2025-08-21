import os
import sys
import csv
import json
import time
import math
import random
import signal
import logging
import argparse
import requests
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process, Queue, set_start_method, freeze_support, current_process

# =========================
# CONFIGURABLE PARAMETERS
# =========================

# Proxy provider API (replace with your proxy API if needed)
PROXY_API_URL = "https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=10000&country=all"

# Validate proxies against this Steam endpoint (lightweight)
STEAM_VALIDATION_URL = "https://store.steampowered.com/api/appdetails"
STEAM_VALIDATION_APPID = "570"  # Dota2 as test appid

# Per-game review limit (user requested 100 per game)
MAX_REVIEWS_PER_GAME = 10

# Per-part file record limit (user requested 50k per file)
RECORDS_PER_PART = 50_000

# Random delay range between page requests (seconds)
RANDOM_DELAY_RANGE = (0.5, 1.5)

# Number of worker processes
PROCESS_COUNT = 4

# Concurrency inside each worker (how many apps processed concurrently per process)
CONCURRENT_APPS_PER_PROCESS = 10

# HTTP request settings
REQUEST_TIMEOUT = 20
RETRY_MAX = 5

# When a proxy fails this many times consecutively inside a worker, drop it
PROXY_CONSECUTIVE_FAILURES_TO_DROP = 3

today = datetime.now().strftime("%d%m%Y")

OUTPUT_DIR = f"data/raw/review_{today}"        # chứa reviews_partXXXXX.json
STATE_DIR = "logs/crawler/state"                # chứa processed_*.txt
LOG_DIR = "logs/crawler"                   # chứa log files

# Proxy validation behavior (new)
VALIDATION_CONCURRENCY = 200      # how many concurrent validation coroutines
VALIDATION_TIMEOUT = 2            # seconds per proxy validation (user requested 2s)
VALIDATION_MAX_GOOD = 5000        # stop after this many good proxies found
VALIDATION_LOG_EVERY = 500        # log progress every N proxies checked

# Steam review endpoint format (cursor-based)
STEAM_REVIEW_URL_FMT = "https://store.steampowered.com/appreviews/{appid}"
PAGE_SIZE = 100  # max per Steam endpoint

# =========================
# Utilities and setup
# =========================

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

from prefect import get_run_logger

def setup_logger(name, log_file=None, level=None):
    """
    Instead of normal Python logging, use Prefect logger
    so logs show in Prefect UI in real time.
    """
    return get_run_logger()

def read_games(csv_path: str) -> List[Dict[str, str]]:
    """
    Read input CSV with columns: appid,name
    Ensures appid and name are strings.
    """
    df = pd.read_csv(csv_path, dtype={"appid": str, "name": str})
    df["appid"] = df["appid"].astype(str)
    df["name"] = df["name"].fillna("")
    return df.to_dict(orient="records")

def read_processed_appids() -> set:
    """
    Read all processed appids from state/processed_*.txt for resume.
    """
    done = set()
    if not os.path.exists(STATE_DIR):
        return done
    for fn in os.listdir(STATE_DIR):
        if fn.startswith("processed_") and fn.endswith(".txt"):
            path = os.path.join(STATE_DIR, fn)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        s = line.strip()
                        if s:
                            done.add(s)
            except Exception:
                pass
    return done

def normalize_proxy(p: str) -> str:
    """
    Normalize proxy format: ensure scheme present.
    If proxy string like 'ip:port' or 'user:pass@ip:port', prepend 'http://'.
    """
    p = p.strip()
    if not p:
        return ""
    if p.startswith(("http://", "https://", "socks5://")):
        return p
    return "http://" + p

# =========================
# Old synchronous validator (kept for reference but not used)
# =========================

def test_proxy_for_steam(proxy: str, timeout=8) -> bool:
    """
    Synchronously test whether a proxy can access Steam validation endpoint.
    Return True if valid JSON response received and status OK.
    (Note: replaced by async validator; kept for reference)
    """
    try:
        proxy_url = normalize_proxy(proxy)
        proxies = {"http": proxy_url, "https": proxy_url}
        resp = requests.get(STEAM_VALIDATION_URL, params={"appids": STEAM_VALIDATION_APPID}, proxies=proxies, timeout=timeout)
        if resp.status_code != 200:
            return False
        data = resp.json()
        if isinstance(data, dict) and STEAM_VALIDATION_APPID in data:
            return True
        return False
    except Exception:
        return False

# =========================
# Async Proxy validation (new)
# =========================

async def _validate_one_proxy(session: aiohttp.ClientSession, raw_proxy: str, logger: logging.Logger) -> Optional[str]:
    """
    Validate single proxy against Steam validation endpoint.
    - Uses VALIDATION_TIMEOUT seconds total.
    - No retries.
    - Returns normalized proxy (with scheme) if valid, otherwise None.
    """
    proxy_norm = normalize_proxy(raw_proxy)
    # aiohttp expects proxy like "http://ip:port"
    # but if proxy_norm already contains credentials, it's fine.
    params = {"appids": STEAM_VALIDATION_APPID}
    try:
        # per-proxy timeout
        timeout = aiohttp.ClientTimeout(total=VALIDATION_TIMEOUT)
        async with session.get(STEAM_VALIDATION_URL, params=params, proxy=proxy_norm, timeout=timeout) as resp:
            if resp.status == 200:
                try:
                    data = await resp.json()
                    if isinstance(data, dict) and STEAM_VALIDATION_APPID in data:
                        return proxy_norm
                except Exception:
                    # parse error -> invalid
                    return None
            return None
    except Exception:
        return None

async def validate_proxies_from_api_async(proxy_api_url: str, logger: logging.Logger,
                                          max_workers: int = VALIDATION_CONCURRENCY,
                                          max_good: int = VALIDATION_MAX_GOOD) -> List[str]:
    """
    Fetch raw proxy list from PROXY_API_URL and validate each proxy against Steam asynchronously.
    - 2-second timeout per proxy (VALIDATION_TIMEOUT)
    - no retries
    - stops after max_good proxies found
    - logs live progress every VALIDATION_LOG_EVERY proxies checked
    """
    try:
        r = requests.get(proxy_api_url, timeout=20)
        r.raise_for_status()
        lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    except Exception as e:
        logger.warning(f"Failed to fetch proxies from API: {e}")
        return []

    total = len(lines)
    if total == 0:
        logger.warning("Proxy API returned empty list.")
        return []

    logger.info(f"Fetched {total} raw proxies from API. Validating against Steam asynchronously...")

    # shared state
    good_proxies: List[str] = []
    checked = 0
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(min(max_workers, 1000))  # safety upper bound

    # connector with limited total connections
    connector = aiohttp.TCPConnector(limit_per_host=max_workers, limit=max_workers, enable_cleanup_closed=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        # create coroutine for each proxy, but we will iterate with as_completed to stop early
        coros = []

        for raw in lines:
            # we pass raw; normalization is done in _validate_one_proxy
            async def _wrap(rp=raw):
                nonlocal good_proxies, checked
                async with sem:
                    res = await _validate_one_proxy(session, rp, logger)
                    async with lock:
                        checked += 1
                        if res:
                            good_proxies.append(res)
                            # log when we find a good one
                            logger.info(f"VALID PROXY [{len(good_proxies)}/{max_good}] {res} (checked {checked}/{total})")
                        # periodic progress log
                        if checked % VALIDATION_LOG_EVERY == 0 or checked == total:
                            logger.info(f"Proxy validation progress: checked={checked}/{total} good={len(good_proxies)}")
                        # if reached max_good, nothing more needed (main loop will handle cancel)
                    return res

            coros.append(_wrap())

        # schedule all tasks
        tasks = [asyncio.create_task(c) for c in coros]

        # iterate tasks as they complete so we can cancel early when enough good proxies collected
        try:
            for fut in asyncio.as_completed(tasks):
                res = await fut
                # check stop condition
                if len(good_proxies) >= max_good:
                    logger.info(f"Reached target of {max_good} good proxies; cancelling remaining validations.")
                    # cancel remaining tasks
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Exception during async proxy validation: {e}")
        finally:
            # ensure all tasks are cleaned up
            for t in tasks:
                if not t.done():
                    try:
                        t.cancel()
                    except Exception:
                        pass
            # wait a tiny bit for cancellations to settle
            await asyncio.sleep(0.01)

    logger.info(f"Validation complete: {len(good_proxies)} proxies valid for Steam (requested max {max_good}).")
    return good_proxies

# Backwards compatible wrapper so existing main() call site can remain simple
def validate_proxies_from_api(proxy_api_url: str, logger: logging.Logger, max_workers: int = VALIDATION_CONCURRENCY) -> List[str]:
    """
    Synchronous wrapper that runs the async validator.
    """
    return asyncio.run(validate_proxies_from_api_async(proxy_api_url, logger, max_workers=max_workers, max_good=VALIDATION_MAX_GOOD))

# =========================
# JSON PART WRITER PROCESS
# =========================

class JsonPartWriter:
    """
    Manage JSON part files. Each part is a JSON array:
      [ {..}, {..}, ... ]
    We open one part at a time, append records, and when count hits RECORDS_PER_PART,
    close it and open the next part. We choose to start new parts (not append to existing parts)
    to simplify resume/consistency.
    """
    def __init__(self, output_dir: str, records_per_part: int, logger: logging.Logger):
        self.output_dir = output_dir
        self.records_per_part = records_per_part
        self.logger = logger
        self.part_idx = self._next_part_index()
        self.fh = None
        self.first_in_file = True
        self.written = 0

    def _next_part_index(self) -> int:
        existing = []
        for fn in os.listdir(self.output_dir):
            if fn.startswith("reviews_part") and fn.endswith(".json"):
                try:
                    idx = int(fn.replace("reviews_part", "").replace(".json", ""))
                    existing.append(idx)
                except Exception:
                    pass
        return max(existing) + 1 if existing else 1

    def _open(self):
        if self.fh:
            self._close()
        name = f"reviews_part{self.part_idx:05d}.json"
        path = os.path.join(self.output_dir, name)
        self.fh = open(path, "w", encoding="utf-8", newline="")
        self.fh.write("[\n")
        self.first_in_file = True
        self.written = 0
        self.logger.info(f"Opened new part file: {name}")

    def _close(self):
        if self.fh:
            self.fh.write("\n]\n")
            self.fh.flush()
            self.fh.close()
            self.logger.info(f"Closed part file index={self.part_idx:05d} written={self.written}")
            self.fh = None
            self.part_idx += 1

    def write(self, rec: Dict[str, Any]):
        if self.fh is None:
            self._open()
        if not self.first_in_file:
            self.fh.write(",\n")
        json.dump(rec, self.fh, ensure_ascii=False)
        self.first_in_file = False
        self.written += 1
        if self.written >= self.records_per_part:
            self._close()
            self._open()

    def finalize(self):
        self._close()

def writer_process(queue: Queue, output_dir: str, records_per_part: int):
    """
    Writer process receives:
      - ("INIT", total_workers)
      - list of records (a list of dicts) from workers
      - ("END", worker_idx) when a worker finishes
    When number of END matches total_workers, it finalizes and exits.
    """
    logger = setup_logger("writer", "writer.log")
    ensure_dirs()
    writer = JsonPartWriter(output_dir, records_per_part, logger)
    total_workers = None
    ended = 0
    logger.info("Writer started, waiting for INIT...")
    while True:
        msg = queue.get()
        if isinstance(msg, tuple) and msg[0] == "INIT":
            total_workers = int(msg[1])
            logger.info(f"Writer received INIT: total_workers={total_workers}")
            continue
        if isinstance(msg, tuple) and msg[0] == "END":
            ended += 1
            logger.info(f"Writer received END: {ended}/{total_workers}")
            if total_workers is not None and ended >= total_workers:
                break
            continue
        # DATA expected to be list of dicts
        if isinstance(msg, list):
            for rec in msg:
                writer.write(rec)
        else:
            logger.warning(f"Writer got unexpected message type: {type(msg)}")
    writer.finalize()
    logger.info("Writer finished.")

# =========================
# Worker: async crawler per group
# =========================

async def fetch_one_page(session: aiohttp.ClientSession, url: str, params: dict,
                         proxy: Optional[str], logger: logging.Logger, attempt: int = 0) -> Optional[dict]:
    """
    Fetch a single page with retry & exponential backoff + jitter.
    Uses aiohttp session and optional proxy (string with scheme).
    Returns parsed JSON dict or None on permanent failure.
    """
    backoff = min(2 ** attempt + random.random(), 20.0)
    try:
        # aiohttp expects proxy param as a URL string
        async with session.get(url, params=params, proxy=proxy, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status in (200, 206):
                return await resp.json()
            elif resp.status in (429, 500, 502, 503, 504):
                logger.info(f"HTTP {resp.status} -> backoff {backoff:.1f}s")
                await asyncio.sleep(backoff)
                if attempt < RETRY_MAX:
                    return await fetch_one_page(session, url, params, proxy, logger, attempt + 1)
                return None
            else:
                text = await resp.text()
                logger.warning(f"Bad status {resp.status} from proxy->steam: {text[:200]}")
                return None
    except asyncio.TimeoutError:
        logger.info(f"Timeout -> backoff {backoff:.1f}s")
        await asyncio.sleep(backoff)
        if attempt < RETRY_MAX:
            return await fetch_one_page(session, url, params, proxy, logger, attempt + 1)
        return None
    except Exception as e:
        logger.info(f"Request exception: {e} -> backoff {backoff:.1f}s")
        await asyncio.sleep(backoff)
        if attempt < RETRY_MAX:
            return await fetch_one_page(session, url, params, proxy, logger, attempt + 1)
        return None

async def fetch_reviews_for_app(appid: str, name: str, session: aiohttp.ClientSession,
                                logger: logging.Logger, proxies: List[str],
                                proxy_fail_counts: Dict[str, int]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Fetch up to MAX_REVIEWS_PER_GAME for one app.
    - Returns tuple (records, proxies_to_remove)
    - proxies_to_remove is a list of proxies that should be removed by the worker because they failed repeatedly.
    """
    url = STEAM_REVIEW_URL_FMT.format(appid=appid)
    cursor = "*"
    collected: List[Dict[str, Any]] = []
    proxies_to_remove = []

    while len(collected) < MAX_REVIEWS_PER_GAME:
        params = {
            "json": 1,
            "filter": "recent",
            "language": "all",
            "purchase_type": "all",
            "num_per_page": PAGE_SIZE,
            "cursor": cursor
        }
        proxy = random.choice(proxies) if proxies else None

        data = await fetch_one_page(session, url, params, proxy, logger)
        # If the chosen proxy resulted in immediate failure (data is None), increment its fail count
        if proxy and data is None:
            # increment fail count
            proxy_fail_counts[proxy] = proxy_fail_counts.get(proxy, 0) + 1
            if proxy_fail_counts[proxy] >= PROXY_CONSECUTIVE_FAILURES_TO_DROP:
                proxies_to_remove.append(proxy)
                logger.info(f"Marking proxy for removal due to repeated failures: {proxy}")
            # try another proxy for next iteration
            # small random backoff to avoid tight loop
            await asyncio.sleep(random.uniform(0.5, 1.0))
            continue

        if not data or "reviews" not in data or not data["reviews"]:
            break

        # convert each review into the required fields
        batch = []
        for r in data["reviews"]:
            rec = {
                "appid": appid,
                "name": name,
                "author_steamid": r.get("author", {}).get("steamid"),
                "review": r.get("review"),
                "timestamp_created": r.get("timestamp_created"),
                "language": r.get("language"),
            }
            batch.append(rec)

        # trim if exceeding per-app limit
        remaining = MAX_REVIEWS_PER_GAME - len(collected)
        if len(batch) > remaining:
            batch = batch[:remaining]

        collected.extend(batch)

        cursor = data.get("cursor", cursor)

        # random delay between pages to reduce detectability
        await asyncio.sleep(random.uniform(*RANDOM_DELAY_RANGE))

        # if less than page size, likely no more reviews
        if len(data["reviews"]) < PAGE_SIZE:
            break

    logger.info(f"App {appid}: collected {len(collected)} reviews.")
    return collected, proxies_to_remove

async def worker_async(games: List[Dict[str, str]], queue: Queue,
                       processed_state_path: str, proxies: List[str], logger: logging.Logger):
    """
    Asynchronous worker routine: processes a list of games (appid/name).
    - Uses an aiohttp session
    - Uses a Semaphore to limit concurrent app tasks to CONCURRENT_APPS_PER_PROCESS
    - Sends finished app review lists to writer via queue
    - Records processed appids to processed_state_path for resume
    - Maintains local proxy list and fail counts (removes proxies that fail repeatedly)
    """
    sem = asyncio.Semaphore(CONCURRENT_APPS_PER_PROCESS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_APPS_PER_PROCESS, enable_cleanup_closed=True)

    headers = {
        "Accept": "application/json, text/javascript,*/*;q=0.01",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SteamReviewsCrawler/1.0",
    }

    processed_local = set()
    if os.path.exists(processed_state_path):
        with open(processed_state_path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    processed_local.add(s)

    # use a mutable local proxy pool so we can remove bad proxies
    local_proxies = list(proxies) if proxies else []
    proxy_fail_counts: Dict[str, int] = {}

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:

        async def run_one(app: Dict[str, str]):
            appid = str(app["appid"])
            name = str(app.get("name", ""))

            if appid in processed_local:
                return

            async with sem:
                nonlocal local_proxies, proxy_fail_counts
                try:
                    # if no proxy left, still continue without proxy
                    collected, to_remove = await fetch_reviews_for_app(appid, name, session, logger, local_proxies, proxy_fail_counts)
                    if collected:
                        queue.put(collected)
                    # update local proxies: remove any with too many failures
                    if to_remove:
                        for p in to_remove:
                            if p in local_proxies:
                                try:
                                    local_proxies.remove(p)
                                    logger.info(f"Worker removed bad proxy: {p} (remaining {len(local_proxies)})")
                                except ValueError:
                                    pass
                                proxy_fail_counts.pop(p, None)
                    # mark processed (even if 0 reviews) so resume skips next time
                    with open(processed_state_path, "a", encoding="utf-8") as sf:
                        sf.write(appid + "\n")
                    processed_local.add(appid)
                except Exception as e:
                    logger.exception(f"Worker exception processing app {appid}: {e}")

        tasks = [asyncio.create_task(run_one(g)) for g in games]
        await asyncio.gather(*tasks)

def worker_process(idx: int, games: List[Dict[str, str]], queue: Queue, proxies: List[str]):
    """
    Worker process entrypoint. Creates logger, runs worker_async, then notifies writer with ("END", idx).
    """
    logger = setup_logger(f"worker_{idx}", f"worker_{idx}.log")
    ensure_dirs()
    state_path = os.path.join(STATE_DIR, f"processed_{idx}.txt")
    logger.info(f"Worker {idx} starting. games={len(games)} proxies={len(proxies)}")

    # handle termination signals gracefully
    stop_flag = {"stop": False}
    def _sig(signum, frame):
        logger.info("Worker received termination signal.")
        stop_flag["stop"] = True
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    try:
        asyncio.run(worker_async(games, queue, state_path, proxies, logger))
    except Exception as e:
        logger.exception(f"Worker {idx} crashed: {e}")
    finally:
        queue.put(("END", idx))
        logger.info(f"Worker {idx} finished.")

async def validate_proxy(proxy):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://store.steampowered.com/appreviews/730?json=1&num_per_page=1",
                proxy=f"http://{proxy}",
                timeout=aiohttp.ClientTimeout(total=2)  # fast fail
            ) as resp:
                return resp.status == 200
    except:
        return False

# =========================
# Master orchestration
# =========================

def split_into_groups(seq: List[Any], n_groups: int) -> List[List[Any]]:
    """Split list into n_groups roughly equal parts."""
    if n_groups <= 0:
        return [seq]
    size = math.ceil(len(seq) / n_groups)
    return [seq[i:i+size] for i in range(0, len(seq), size)]

def run_crawler(csv_path: str):
    ensure_dirs()
    master_log = setup_logger("master", "crawl.log")
    master_log.info("=== START Steam Reviews Crawler (validated proxies) ===")

    # 1) read games
    games = read_games(csv_path)
    master_log.info(f"Loaded {len(games)} games from CSV.")

    # 2) resume: skip already processed
    processed_appids = read_processed_appids()
    remaining_games = [g for g in games if str(g["appid"]) not in processed_appids]
    master_log.info(f"{len(remaining_games)} games remain after applying resume filter.")

    if not remaining_games:
        master_log.info("Nothing to do. Exiting.")
        return

    # 3) proxy validation
    working_proxies = validate_proxies_from_api(PROXY_API_URL, master_log, max_workers=VALIDATION_CONCURRENCY)
    if not working_proxies:
        master_log.warning("No working proxies available. Proceeding without proxies.")
    else:
        master_log.info(f"Using {len(working_proxies)} validated proxies for crawling.")

    # 4) split
    groups = split_into_groups(remaining_games, PROCESS_COUNT)
    proxy_groups = split_into_groups(working_proxies, len(groups)) if working_proxies else [[] for _ in groups]

    # 5) writer
    q: Queue = Queue()
    writer = Process(target=writer_process, args=(q, OUTPUT_DIR, RECORDS_PER_PART), daemon=False)
    writer.start()
    q.put(("INIT", len(groups)))

    # 6) workers
    procs: List[Process] = []
    for i, group in enumerate(groups):
        p = Process(target=worker_process, args=(i, group, q, proxy_groups[i] if i < len(proxy_groups) else []), daemon=False)
        p.start()
        procs.append(p)
        master_log.info(f"Spawned worker {i} (games={len(group)})")

    # 7) wait
    for p in procs:
        p.join()

    writer.join()

    master_log.info("=== DONE Steam Reviews Crawler ===")
    master_log.info(f"Output files in folder: {OUTPUT_DIR}")


if __name__ == "__main__":
    freeze_support()
    try:
        set_start_method("spawn", force=True)
    except RuntimeError:
        pass
    parser = argparse.ArgumentParser()
    parser.add_argument("csv")
    args = parser.parse_args()
    run_crawler(args.csv)