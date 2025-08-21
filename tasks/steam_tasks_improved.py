# tasks/steam_tasks_improved.py
from prefect import task
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import requests
import csv
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import json
from typing import List, Dict, Any, Optional, Set

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from config.settings import config
from utils.common import CheckpointManager, DatabaseManager, ProcessedTracker, get_processed_ids_from_csv, get_date_string, ensure_directory
from monitoring.metrics import metrics, track_task_performance, track_api_request

# Global locks and buffers
lock = Lock()
buffer_rows = []

class SteamAPIClient:
    """Steam API client with retry logic and monitoring"""
    
    @staticmethod
    @track_api_request('steam')
    def get_app_list() -> List[Dict[str, Any]]:
        """Get all Steam apps with retry logic"""
        print("Fetching app list from Steam...")
        start = time.time()
        
        for attempt in range(config.api.max_retries):
            try:
                resp = requests.get(config.api.steam_api_url, timeout=config.api.request_timeout)
                if resp.status_code == 200:
                    apps = resp.json()["applist"]["apps"]
                    print(f"Retrieved {len(apps)} apps in {time.time() - start:.2f} seconds.")
                    return apps
            except requests.RequestException as e:
                print(f"Steam API request attempt {attempt + 1} failed: {e}")
                if attempt < config.api.max_retries - 1:
                    time.sleep(config.api.retry_delay ** attempt)
        
        raise RuntimeError("Failed to fetch Steam app list after all retries.")
    
    @staticmethod
    @track_api_request('steamspy')
    def get_steamspy_details(appid: int) -> Dict[str, Any]:
        """Get SteamSpy details for an app with retry logic"""
        details = {"appid": appid, "discount": "", "price": ""}
        
        for attempt in range(config.api.max_retries):
            try:
                with metrics.time_api_request('steamspy'):
                    resp = requests.get(
                        config.api.steamspy_api_url,
                        params={"request": "appdetails", "appid": appid},
                        timeout=config.api.request_timeout
                    )
                    if resp.status_code == 200:
                        spy_data = resp.json()
                        for col in ["discount", "price"]:
                            details[col] = spy_data.get(col, "")
                        metrics.increment_api_requests('steamspy', 'success')
                        return details
            except requests.RequestException as e:
                if attempt == config.api.max_retries - 1:
                    metrics.increment_api_requests('steamspy', 'failed')
                    print(f"SteamSpy API failed for appid {appid}: {e}")
                else:
                    time.sleep(config.api.retry_delay ** attempt)
        
        return details

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def crawl_price_task():
    """Crawl Steam prices with fault tolerance and monitoring"""
    
    with metrics.time_task('crawl_price'):
        metrics.set_pipeline_status('crawl', 1)
        
        try:
            date_str = get_date_string()
            csv_file = f"{config.files.raw_dir}/price_raw{date_str}.csv"
            output_file = f"{config.files.processed_dir}/price_processed_{date_str}.csv"
            
            # # Check if already completed
            # if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            #     print(f"Skipping crawl: {output_file} already exists and has data.")
            #     return
            
            ensure_directory(csv_file)
            
            # Initialize tracker for fault tolerance
            tracker = ProcessedTracker('crawl_price')
            
            # Get all Steam apps
            apps = SteamAPIClient.get_app_list()
            app_ids = [a["appid"] for a in apps if a["appid"] > 0]
            
            # Get already processed IDs from CSV (for additional safety)
            csv_processed_ids = get_processed_ids_from_csv(csv_file)
            all_processed_ids = tracker.processed_ids.union(csv_processed_ids)
            
            # Calculate remaining work
            remaining_ids = [aid for aid in app_ids if aid not in all_processed_ids]
            failed_ids = tracker.get_retry_ids()
            
            # Prioritize failed IDs for retry
            work_ids = list(failed_ids) + [aid for aid in remaining_ids if aid not in failed_ids]
            total = len(work_ids)
            
            if total == 0:
                print("No work to do - all apps already processed.")
                return
            
            print(f"Processing {total} apps ({len(failed_ids)} retries, {len(remaining_ids)} new)")
            
            # Set up monitoring
            metrics.set_queue_size('pending', total)
            metrics.set_queue_size('failed', len(failed_ids))
            
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=config.crawl.max_workers) as executor:
                metrics.set_active_workers(config.crawl.max_workers)
                
                # Submit all tasks
                futures = {executor.submit(SteamAPIClient.get_steamspy_details, aid): aid for aid in work_ids}
                
                processed_count = 0
                failed_count = 0
                
                try:
                    for future in as_completed(futures):
                        appid = futures[future]
                        try:
                            data = future.result()
                            if data and (data.get("price") or data.get("discount")):
                                with lock:
                                    buffer_rows.append(data)
                                    tracker.mark_processed(appid)
                                    processed_count += 1
                                    
                                    # Batch save to CSV
                                    if len(buffer_rows) >= config.crawl.batch_size:
                                        save_batch_to_csv(csv_file, buffer_rows)
                                        buffer_rows.clear()
                            else:
                                tracker.mark_failed(appid)
                                failed_count += 1
                                
                        except Exception as e:
                            print(f"Error processing appid {appid}: {e}")
                            tracker.mark_failed(appid)
                            failed_count += 1
                        
                        # Update monitoring
                        current_processed = processed_count + failed_count
                        if current_processed % (config.crawl.max_workers * 5) == 0 or current_processed == total:
                            percent = (current_processed / total) * 100
                            print(f"Processed {current_processed}/{total} apps ({percent:.2f}%) - Success: {processed_count}, Failed: {failed_count}")
                            
                            metrics.set_queue_size('pending', total - current_processed)
                            metrics.set_queue_size('failed', failed_count)
                            
                            time.sleep(config.crawl.sleep_between_batches)
                            
                except KeyboardInterrupt:
                    print("\nStopping early... saving remaining buffer.")
                
                # Save remaining buffer
                if buffer_rows:
                    save_batch_to_csv(csv_file, buffer_rows)
                    buffer_rows.clear()
            
            elapsed = time.time() - start_time
            print(f"Crawl completed. Processed: {processed_count}, Failed: {failed_count}")
            print(f"Total time: {elapsed/60:.2f} minutes")
            
            # Clear checkpoint if successful
            if failed_count == 0:
                tracker.clear()
                
            # Push metrics
            metrics.push_metrics("_crawl")
            
        except Exception as e:
            metrics.set_pipeline_status('crawl', 0)
            raise e
        finally:
            metrics.set_pipeline_status('crawl', 0)

def save_batch_to_csv(csv_file: str, rows: List[Dict[str, Any]]):
    """Save batch of rows to CSV file"""
    file_exists = os.path.exists(csv_file)
    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["appid", "discount", "price"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def transform_price_task():
    """Transform raw price data from CSV with continuous polling until crawl completes"""
    with metrics.time_task('transform_price'):
        metrics.set_pipeline_status('transform', 1)
        
        try:
            date_str = get_date_string()
            raw_file = f"{config.files.raw_dir}/price_raw{date_str}.csv"
            processed_file = f"{config.files.processed_dir}/price_processed_{date_str}.csv"
            
            ensure_directory(processed_file)
            
            max_wait_time = 3600  # Tối đa 1 giờ (tùy chỉnh theo thời gian crawl)
            elapsed_time = 0
            polling_interval = 5  # Polling mỗi 5 giây
            last_size = 0
            
            while elapsed_time < max_wait_time:
                if os.path.exists(raw_file) and os.path.getsize(raw_file) > 0:
                    current_size = os.path.getsize(raw_file)
                    if current_size > last_size:
                        print(f"📂 Detected new data in {raw_file}, size changed to {current_size} bytes, transforming...")
                        df = pd.read_csv(raw_file)
                        if 'discount' in df.columns and 'price' in df.columns:
                            df['initial_price'] = df.apply(
                                lambda row: row['price'] / (1 - row['discount'] / 100) if row['discount'] > 0 else row['price'],
                                axis=1
                            )
                        else:
                            print("⚠️ Warning: Missing 'discount' or 'price' columns, using price as initial_price")
                            df['initial_price'] = df.get('price', 0)
                        df.to_csv(processed_file, index=False)
                        print(f"✅ Transformed data saved to {processed_file} with {len(df)} rows")
                        last_size = current_size
                    else:
                        print(f"⏳ No new data in {raw_file}, size {current_size} bytes, waiting...")
                else:
                    print(f"⏳ Waiting for raw file {raw_file}, elapsed {elapsed_time}s...")
                
                time.sleep(polling_interval)
                elapsed_time += polling_interval
            
            if elapsed_time >= max_wait_time:
                print(f"⏱️ Max wait time ({max_wait_time}s) reached, stopping transform")
                raise Exception("Max wait time exceeded while waiting for crawl data")
            
            metrics.push_metrics("_transform")
            
        except Exception as e:
            metrics.set_pipeline_status('transform', 0)
            raise e
        finally:
            metrics.set_pipeline_status('transform', 0)

def process_new_rows(input_file: str, output_file: str, tracker: ProcessedTracker) -> int:
    """Process new rows from input CSV with fault tolerance"""
    new_rows = []
    
    try:
        with open(input_file, mode='r', encoding='utf-8', newline='') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                appid = row.get('appid')
                if not appid or tracker.is_processed(int(appid)):
                    continue
                
                try:
                    # Transform data
                    discount = parse_discount(row.get('discount'))
                    price_cents = to_cents(row.get('price'))
                    init_cents = initial_price_cents(price_cents, discount)
                    
                    price_out = format_cents(price_cents)
                    init_out = "0" if (init_cents == 0) else format_cents(init_cents)
                    
                    new_rows.append({
                        'appid': appid,
                        'discount': str(discount).rstrip('0').rstrip('.') if discount % 1 == 0 else str(discount),
                        'initial_price': init_out,
                        'price': price_out,
                    })
                    
                    tracker.mark_processed(int(appid))
                    metrics.increment_games_processed('transform', 'success')
                    
                except Exception as e:
                    print(f"Error transforming appid {appid}: {e}")
                    tracker.mark_failed(int(appid))
                    metrics.increment_games_processed('transform', 'failed')
                    
    except FileNotFoundError:
        return 0
    
    if new_rows:
        # Save to CSV
        file_exists = os.path.exists(output_file)
        with open(output_file, mode='a', encoding='utf-8', newline='') as outfile:
            fieldnames = ['appid', 'discount', 'initial_price', 'price']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {len(new_rows)} new rows.")
    
    return len(new_rows)

def is_crawl_running() -> bool:
    """Check if crawl task is still running by looking at checkpoint"""
    checkpoint = CheckpointManager.load_checkpoint('crawl_price')
    return checkpoint is not None

# Transform utility functions
def to_cents(value):
    """Convert value to cents"""
    if value is None:
        return None
    s = str(value).strip()
    if s == '':
        return None
    if ',' in s and '.' not in s:
        s = s.replace('.', '').replace(',', '.')
    if '.' in s:
        try:
            return int(round(float(s) * 100))
        except ValueError:
            return None
    try:
        return int(s)
    except ValueError:
        return None

def format_cents(cents):
    """Format cents to currency string"""
    if cents is None:
        return ''
    euros = cents // 100
    cents_part = cents % 100
    return f"{euros},{cents_part:02d}"

def parse_discount(value):
    """Parse discount percentage"""
    if value is None:
        return 0.0
    s = str(value).strip().replace('%', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0

def initial_price_cents(current_price_cents, discount_percent):
    """Calculate initial price from current price and discount"""
    if current_price_cents is None:
        return None
    if current_price_cents <= 0:
        return 0
    d = float(discount_percent)
    if d <= 0:
        return int(current_price_cents)
    denom = 1.0 - (d / 100.0)
    if denom <= 0:
        return int(current_price_cents)
    return int(round(current_price_cents / denom))

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def publish_to_kafka_task():
    """Publish transformed price data to Kafka with continuous polling"""
    with metrics.time_task('publish_to_kafka'):
        metrics.set_pipeline_status('publish', 1)
        
        try:
            date_str = get_date_string()
            processed_file = f"{config.files.processed_dir}/price_processed_{date_str}.csv"
            
            max_wait_time = 3600  # Tối đa 1 giờ
            elapsed_time = 0
            polling_interval = 5  # Polling mỗi 5 giây
            last_size = 0
            
            while elapsed_time < max_wait_time:
                if os.path.exists(processed_file) and os.path.getsize(processed_file) > 0:
                    current_size = os.path.getsize(processed_file)
                    if current_size > last_size:
                        print(f"📂 Detected new data in {processed_file}, size changed to {current_size} bytes, publishing...")
                        df = pd.read_csv(processed_file)
                        producer = KafkaProducer(bootstrap_servers=config.kafka.bootstrap_servers)
                        for _, row in df.iterrows():
                            message = {
                                "type": "price",
                                "appid": row.get("appid", 0),
                                "discount": row.get("discount", 0),
                                "price": row.get("price", 0),
                                "initial_price": row.get("initial_price", row.get("price", 0)),
                                "timestamp": datetime.now().timestamp()
                            }
                            producer.send(config.kafka.steam_dynamic_topic, json.dumps(message).encode('utf-8'))
                        producer.flush()
                        print(f"✅ Published data to Kafka topic {config.kafka.steam_dynamic_topic}")
                        last_size = current_size
                    else:
                        print(f"⏳ No new data in {processed_file}, size {current_size} bytes, waiting...")
                else:
                    print(f"⏳ Waiting for processed file {processed_file}, elapsed {elapsed_time}s...")
                
                time.sleep(polling_interval)
                elapsed_time += polling_interval
            
            if elapsed_time >= max_wait_time:
                print(f"⏱️ Max wait time ({max_wait_time}s) reached, stopping publish")
                raise Exception("Max wait time exceeded while waiting for transformed data")
            
            metrics.push_metrics("_publish")
            
        except Exception as e:
            metrics.set_pipeline_status('publish', 0)
            raise e
        finally:
            metrics.set_pipeline_status('publish', 0)

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def consume_prices_task():
    """Consume prices from Kafka and store in MySQL with monitoring"""
    
    with metrics.time_task('consume_prices'):
        metrics.set_pipeline_status('kafka_consumer', 1)
        
        try:
            # Set up consumer
            consumer = KafkaConsumer(
                config.kafka.steam_dynamic_topic,
                bootstrap_servers=config.kafka.bootstrap_servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=config.kafka.consumer_timeout_ms
            )
            
            processed_count = 0
            failed_count = 0
            
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                print(f"Starting to consume from {config.kafka.steam_dynamic_topic}...")
                
                for message in consumer:
                    try:
                        data = message.value
                        if data.get('type') == 'price':
                            game_id = data.get('appid')
                            price = data.get('price')
                            discount = data.get('discount')
                            initial_price = data.get('initial_price')
                            timestamp = datetime.fromtimestamp(data.get('timestamp'))
                            
                            if not all([game_id, price is not None, discount is not None, initial_price is not None]):
                                print(f"Invalid data for game_id {game_id}: {data}")
                                failed_count += 1
                                continue
                            
                            # Ensure game exists in games table
                            cursor.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
                            if not cursor.fetchone():
                                cursor.execute("INSERT IGNORE INTO games (game_id) VALUES (%s)", (game_id,))
                                print(f"Added new game_id {game_id} to games table")
                            
                            # Insert price data
                            cursor.execute("""
                                INSERT INTO prices (game_id, price, discount, initial_price, timestamp)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (game_id, price, discount, initial_price, timestamp))
                            
                            # Update crawl state
                            cursor.execute("""
                                INSERT INTO crawl_state (game_appid, last_price_timestamp)
                                VALUES (%s, %s)
                                ON DUPLICATE KEY UPDATE last_price_timestamp = %s
                            """, (game_id, timestamp, timestamp))
                            
                            conn.commit()
                            processed_count += 1
                            metrics.increment_kafka_messages(config.kafka.steam_dynamic_topic, 'consumed')
                            
                            # Log progress
                            if processed_count % 100 == 0:
                                print(f"Processed {processed_count} price records")
                        
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        failed_count += 1
                        conn.rollback()
            
            print(f"Consumer completed. Processed: {processed_count}, Failed: {failed_count}")
                # Thêm flag khi hoàn thành
            with open(f"{config.files.raw_dir}/crawl_complete.txt", "w") as f:
                f.write("done")
            metrics.push_metrics("_crawl")
        except Exception as e:
            metrics.set_pipeline_status('crawl', 0)
            raise e
        finally:
            metrics.set_pipeline_status('crawl', 0)
            if 'consumer' in locals():
                consumer.close()

# File để lưu trạng thái đã xử lý
CDC_STATE_FILE = "data/cdc_state.json"

def load_cdc_state():
    if os.path.exists(CDC_STATE_FILE):
        with open(CDC_STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_cdc_state(state):
    try:
        with open(CDC_STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print(f"Error saving CDC state: {e}")

processed_cdc = load_cdc_state()

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def discord_notifier_task():
    """Discord notification task with monitoring, fault tolerance, and offset tracking"""
    
    if not config.discord.enable_notifications or not config.discord.webhook_url:
        print("Discord notifications disabled")
        return
    
    with metrics.time_task('discord_notifier'):
        metrics.set_pipeline_status('discord_notifier', 1)
        
        try:
            # Set up consumer with offset tracking
            consumer = KafkaConsumer(
                config.kafka.mysql_cdc_topic,
                bootstrap_servers=config.kafka.bootstrap_servers,
                auto_offset_reset='latest',  # Chỉ lấy message mới
                enable_auto_commit=False,    # Kiểm soát commit offset thủ công
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            notification_count = 0
            failed_notifications = 0
            
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                print("Starting Discord notifier...")
                
                for message in consumer:
                    try:
                        value = message.value
                        if not value or 'payload' not in value or 'after' not in value['payload']:
                            continue
                        
                        payload = value['payload']
                        after = payload.get('after', {})
                        before = payload.get('before', {})
                        
                        game_id = after.get('game_id')
                        if not game_id:
                            continue
                        
                        cdc_timestamp = after.get('timestamp', 0)
                        
                        # Kiểm tra trạng thái đã xử lý
                        if game_id in processed_cdc and processed_cdc[game_id] >= cdc_timestamp:
                            print(f"⏩ Skipping duplicate CDC for game_id {game_id}, timestamp {cdc_timestamp}")
                            consumer.commit()  # Commit offset để bỏ qua message cũ
                            continue
                        
                        new_data = {
                            'discount': after.get('discount', 0),
                            'price': after.get('price', 0),
                            'initial_price': after.get('initial_price', 0),
                            'timestamp': datetime.fromtimestamp(after.get('timestamp', 0) / 1000) if after.get('timestamp') else datetime.now()
                        }
                        
                        old_data = {
                            'discount': before.get('discount', 0),
                            'price': before.get('price', 0),
                            'initial_price': before.get('initial_price', 0)
                        } if before else None
                        
                        if old_data and (
                            new_data['discount'] != old_data['discount'] or
                            new_data['price'] != old_data['price'] or
                            new_data['initial_price'] != old_data['initial_price']
                        ):
                            if send_discord_notification(game_id, old_data, new_data):
                                processed_cdc[game_id] = cdc_timestamp
                                save_cdc_state(processed_cdc)
                                consumer.commit()  # Commit offset sau khi xử lý thành công
                                notification_count += 1
                                metrics.increment_discord_notifications('success')
                            else:
                                failed_notifications += 1
                                metrics.increment_discord_notifications('failed')
                        else:
                            consumer.commit()  # Commit nếu không có thay đổi đáng kể
                        
                    except Exception as e:
                        print(f"Error processing CDC message: {e}")
                        failed_notifications += 1
                        metrics.increment_discord_notifications('failed')
                        conn.rollback()
                
        except Exception as e:
            metrics.set_pipeline_status('discord_notifier', 0)
            raise e
        finally:
            metrics.set_pipeline_status('discord_notifier', 0)
            if 'consumer' in locals():
                consumer.close()

def process_cdc_message(message, cursor, conn) -> bool:
    """Process CDC message and send Discord notification if needed with debounce"""
    try:
        value = message.value
        if not value or 'payload' not in value or 'after' not in value['payload']:
            return False
        
        payload = value['payload']
        after = payload.get('after', {})
        before = payload.get('before', {})
        
        game_id = after.get('game_id')
        if not game_id:
            return False
        
        # Lấy timestamp từ after (nếu có)
        cdc_timestamp = after.get('timestamp', 0)
        
        # Kiểm tra nếu đã xử lý CDC này
        if game_id in processed_cdc and processed_cdc[game_id] >= cdc_timestamp:
            print(f"⏩ Skipping duplicate CDC for game_id {game_id}, timestamp {cdc_timestamp}")
            return False
        
        new_data = {
            'discount': after.get('discount', 0),
            'price': after.get('price', 0),
            'initial_price': after.get('initial_price', 0),
            'timestamp': datetime.fromtimestamp(after.get('timestamp', 0) / 1000) if after.get('timestamp') else datetime.now()
        }
        
        old_data = {
            'discount': before.get('discount', 0),
            'price': before.get('price', 0),
            'initial_price': before.get('initial_price', 0)
        } if before else None
        
        # So sánh thay đổi
        if old_data and (
            new_data['discount'] != old_data['discount'] or
            new_data['price'] != old_data['price'] or
            new_data['initial_price'] != old_data['initial_price']
        ):
            if send_discord_notification(game_id, old_data, new_data):
                # Cập nhật trạng thái đã xử lý
                processed_cdc[game_id] = cdc_timestamp
                return True
        
        # Cập nhật crawl_state
        cursor.execute("""
            INSERT INTO crawl_state (game_appid, last_price_timestamp)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE last_price_timestamp = %s
        """, (game_id, new_data['timestamp'], new_data['timestamp']))
        conn.commit()
        
        return True
        
    except Exception as e:
        print(f"Error processing CDC message: {e}")
        return False

def send_discord_notification(game_id: int, old_data: Dict, new_data: Dict) -> bool:
    """Send Discord notification for price changes"""
    try:
        message_text = (
            f"🎮 **Price Update for Game `{game_id}`**\n"
            f"🔽 Discount: `{old_data['discount']}%` → `{new_data['discount']}%`\n"
            f"💰 Price: `{old_data['price']}` → `{new_data['price']}`\n"
            f"📊 Initial Price: `{old_data['initial_price']}` → `{new_data['initial_price']}`"
        )
        
        payload = {
            "content": message_text,
            "embeds": [{
                "color": 0x00ff00 if new_data['discount'] > old_data['discount'] else 0xff0000,
                "fields": [
                    {"name": "AppID", "value": str(game_id), "inline": True},
                    {"name": "Old Discount", "value": f"{old_data['discount']}%", "inline": True},
                    {"name": "New Discount", "value": f"{new_data['discount']}%", "inline": True},
                    {"name": "Old Price", "value": f"{old_data['price']}", "inline": True},
                    {"name": "New Price", "value": f"{new_data['price']}", "inline": True},
                    {"name": "Old Initial Price", "value": f"{old_data['initial_price']}", "inline": True},
                    {"name": "New Initial Price", "value": f"{new_data['initial_price']}", "inline": True}
                ]
            }]
        }
        
        response = requests.post(
            config.discord.webhook_url, 
            data=json.dumps(payload), 
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 204:
            print(f"Notification sent to Discord for game_id {game_id}")
            return True
        else:
            print(f"Failed to send Discord notification for game_id {game_id}: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Error sending Discord notification: {e}")
        return False