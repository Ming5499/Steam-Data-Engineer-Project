# tasks/steam_review_task_improved.py
from prefect import task
import os
import time
import json
import requests
import csv
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from typing import List, Dict, Any, Optional, Set
import pandas as pd
import threading
import sys
from bson import ObjectId



sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import config
from utils.common import CheckpointManager, DatabaseManager, ProcessedTracker, get_processed_ids_from_csv, get_date_string, ensure_directory
from monitoring.metrics import metrics, track_task_performance, track_api_request

# Global locks and buffers
lock = Lock()
buffer_reviews = []

import random, time, requests

class SteamReviewAPIClient:
    def __init__(self, proxies=None, max_retries=5, backoff_factor=1.5, timeout=10):
        self.proxies = proxies or []
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.proxy_index = 0  

    def _get_proxy(self):
        if not self.proxies:
            return None
        proxy = self.proxies[self.proxy_index % len(self.proxies)]
        self.proxy_index += 1
        return {"http": proxy, "https": proxy}

    def get_reviews(self, appid, cursor="*"):
        url = f"https://store.steampowered.com/appreviews/{appid}"
        params = {
            "json": 1,
            "filter": "recent",
            "language": "all",
            "day_range": 9223372036854775807,
            "review_type": "all",
            "purchase_type": "all",
            "cursor": cursor,
        }

        for attempt in range(1, self.max_retries + 1):
            proxy = self._get_proxy()
            try:
                resp = requests.get(
                    url, params=params, proxies=proxy, timeout=self.timeout
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success") == 1:
                        reviews = data.get("reviews", [])
                        print(f"App {appid}: collected {len(reviews)} reviews.")
                        return {"reviews": reviews, "cursor": data.get("cursor", cursor)}
                    else:
                        print(f"⚠️ App {appid}: success={data.get('success')}, no reviews")
                        return None
                else:
                    print(f"❌ App {appid}: HTTP {resp.status_code}")
            except requests.exceptions.Timeout:
                wait = self.backoff_factor * attempt
                print(f"⏱️ Timeout for app {appid} -> backoff {wait:.1f}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                wait = self.backoff_factor * attempt
                print(f"❌ Request exception for app {appid}: {e} -> backoff {wait:.1f}s")
                time.sleep(wait)

        print(f"App {appid}: failed after {self.max_retries} retries")
        return None


class MongoDBManager:
    """MongoDB connection manager"""
    
    def __init__(self):
        self.client = None
        self.db = None
        
    def connect(self):
        """Connect to MongoDB"""
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.client = MongoClient(mongo_uri)
        self.db = self.client[os.getenv("MONGO_DB", "steam")]
        
    def get_collection(self, collection_name: str):
        if self.db is None:
            self.connect()
        return self.db[collection_name]
        
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()


@task(retries=3, retry_delay_seconds=5, log_prints=True)
def crawl_reviews_task():
    with metrics.time_task('crawl_reviews'):
        metrics.set_pipeline_status('crawl_reviews', 1)

        try:
            date_str = get_date_string()
            output_dir = f"{config.files.raw_dir}/review"
            ensure_directory(output_dir)

            output_file = os.path.join(output_dir, f"reviews_{date_str}.jsonl")

            if not os.path.exists(output_file):
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write("")

            # Tracker
            tracker = ProcessedTracker('crawl_reviews')

            # 🔹 Load app IDs CSV
            csv_file = "data/game/steam_appid_name.csv"
            if not os.path.exists(csv_file):
                print(f"❌ CSV file {csv_file} not found")
                return

            df = pd.read_csv(csv_file)
            if "appid" not in df.columns:
                print(f"❌ CSV file {csv_file} missing 'appid' column")
                return

            app_ids = df["appid"].dropna().astype(int).tolist()
            remaining_ids = [aid for aid in app_ids if not tracker.is_processed(aid)]
            failed_ids = tracker.get_retry_ids()
            work_ids = list(failed_ids) + [aid for aid in remaining_ids if aid not in failed_ids]
            total = len(work_ids)

            if total == 0:
                print("No work to do - all apps already processed.")
                return

            print(f"Processing reviews for {total} apps "
                  f"({len(failed_ids)} retries, {len(remaining_ids)} new)")

            client = SteamReviewAPIClient(
                proxies=[],
                max_retries=5,
                backoff_factor=1.5,
                timeout=10,
            )

            lock = threading.Lock()
            total_reviews = 0
            processed_count, failed_count = 0, 0

            with ThreadPoolExecutor(max_workers=config.crawl.max_workers) as executor:
                futures = {executor.submit(client.get_reviews, aid): aid for aid in work_ids}

                for future in as_completed(futures):
                    appid = futures[future]
                    try:
                        review_data = future.result()
                        if review_data and review_data.get('reviews'):
                            reviews = review_data['reviews']
                            for review in reviews:
                                review['appid'] = appid
                                review['crawl_date'] = datetime.now().isoformat()

                            # Append trực tiếp vào file JSONL
                            with lock, open(output_file, "a", encoding="utf-8") as f:
                                for review in reviews:
                                    f.write(json.dumps(review, ensure_ascii=False) + "\n")

                            total_reviews += len(reviews)
                            tracker.mark_processed(appid)
                            processed_count += 1
                            print(f"💾 Appended {len(reviews)} reviews (appid={appid}) to {output_file}")
                        else:
                            tracker.mark_failed(appid)
                            failed_count += 1

                    except Exception as e:
                        print(f"Error processing reviews for appid {appid}: {e}")
                        tracker.mark_failed(appid)
                        failed_count += 1

            print(f"✅ Review crawl completed. Success={processed_count}, Failed={failed_count}, "
                  f"Total reviews={total_reviews}")

            if failed_count == 0:
                tracker.clear()

            metrics.push_metrics("_crawl_reviews")

        except Exception as e:
            metrics.set_pipeline_status('crawl_reviews', 0)
            raise e
        finally:
            metrics.set_pipeline_status('crawl_reviews', 0)

def save_reviews_batch(reviews):
    today = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(config.files.raw_dir, f"reviews_{today}.jsonl")

    with open(output_file, "a", encoding="utf-8") as f:
        for review in reviews:
            f.write(json.dumps(review, ensure_ascii=False) + "\n")

    print(f"💾 Appended {len(reviews)} reviews to {output_file}")
    return output_file


def clean_mongo_doc(doc):
    if "_id" in doc and isinstance(doc["_id"], ObjectId):
        doc["_id"] = str(doc["_id"])
    return doc


@task(retries=3, retry_delay_seconds=10, log_prints=True)
def load_to_mongo_task_watch():
    metrics.set_pipeline_status("mongo_loader", 1)

    try:
        # MongoDB connection
        client = MongoClient(config.mongo.uri)
        db = client[config.mongo.database]
        reviews_collection = db["reviews"]

        input_dir = f"{config.files.raw_dir}/review"
        ensure_directory(input_dir)

        processed_files = set()
        total_inserted = 0

        print("📥 Starting MongoDB import (watch mode)...")

        while True:
            # Detect new JSON review files
            json_files = sorted(glob.glob(os.path.join(input_dir, "reviews_part*.json")))

            new_files = [f for f in json_files if f not in processed_files]
            if new_files:
                for file_path in new_files:
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            reviews = json.load(f)

                        if reviews:
                            result = reviews_collection.insert_many(reviews, ordered=False)
                            inserted = len(result.inserted_ids)
                            total_inserted += inserted
                            print(f"✅ Inserted {inserted} reviews from {os.path.basename(file_path)}")
                        else:
                            print(f"⚠️ File {os.path.basename(file_path)} empty, skipped")

                        processed_files.add(file_path)

                    except Exception as e:
                        print(f"❌ Error loading {file_path}: {e}")

            # Check if crawl is done
            flag_file = os.path.join(input_dir, "_CRAWL_DONE.flag")
            if os.path.exists(flag_file):
                print("✅ Crawl completed flag detected, stopping MongoDB watcher.")
                break
            time.sleep(5)  

        print(f"🏁 MongoDB import finished: {total_inserted} reviews inserted.")

    except Exception as e:
        metrics.set_pipeline_status("mongo_loader", 0)
        raise e
    finally:
        metrics.set_pipeline_status("mongo_loader", 0)


def clean_for_kafka(doc: dict):
    safe_doc = dict(doc) 
    if "_id" in safe_doc:
        safe_doc["_id"] = str(safe_doc["_id"])
    return safe_doc


@task(retries=3, retry_delay_seconds=10, log_prints=True)
def load_and_publish_reviews_task_watch():

    date_str = get_date_string()
    target_file = os.path.join(config.files.raw_dir, "review", f"reviews_{date_str}.jsonl")
    print(f"👀 Starting Mongo+Kafka streaming watcher on {target_file}")

    try:
        # MongoDB
        client = MongoClient(config.mongo.uri)
        db = client["steam"]
        collection = db["reviews"]
        print("✅ Connected to MongoDB collection: steam.reviews")

        # Kafka
        producer = KafkaProducer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        )
        print(f"✅ Connected to Kafka: {config.kafka.bootstrap_servers}")
        print(f"✅ Publishing to Kafka topic: {config.kafka.steam_review_topic}")

        # Read fỏ streaming
        if not os.path.exists(target_file):
            print(f"⚠️ File {target_file} not found, waiting for crawl...")
            while not os.path.exists(target_file):
                time.sleep(5)

        print(f"📂 Streaming from file: {target_file}")
        with open(target_file, "r", encoding="utf-8") as f:
            f.seek(0, os.SEEK_END)  

            while True:
                line = f.readline()
                if not line:
                    time.sleep(2)
                    continue

                try:
                    review = json.loads(line.strip())
                    
                    # Insert Mongo
                    result = collection.insert_one(review)
                    review['_id'] = str(result.inserted_id)

                    # Publish Kafka
                    producer.send(config.kafka.steam_review_topic, review)

                except Exception as e:
                    print(f"❌ Error processing line: {e}")
                    
    except Exception as e:
        print(f"❌ Watcher error: {e}")
        raise e

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def publish_reviews_to_kafka_task():
    
    with metrics.time_task('publish_kafka_reviews'):
        metrics.set_pipeline_status('kafka_producer_reviews', 1)
        
        try:
            # Initialize MongoDB manager
            mongo_manager = MongoDBManager()
            mongo_manager.connect()
            reviews_collection = mongo_manager.get_collection("reviews")
            
            # Initialize Kafka producer
            producer = KafkaProducer(
                bootstrap_servers=config.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            published_count = 0
            failed_count = 0
            
            # Get reviews from MongoDB (recent ones)
            current_time = datetime.now()
            query = {
                "crawl_date": {
                    "$gte": current_time.replace(hour=0, minute=0, second=0).isoformat()
                }
            }
            
            reviews_cursor = reviews_collection.find(query)
            
            for review in reviews_cursor:
                try:
                    # Convert MongoDB document to Kafka message
                    review_data = {
                        "type": "review",
                        "appid": review.get("appid"),
                        "recommendationid": review.get("recommendationid"),
                        "author": review.get("author", {}).get("steamid"),
                        "language": review.get("language"),
                        "review": review.get("review"),
                        "timestamp_created": review.get("timestamp_created"),
                        "timestamp_updated": review.get("timestamp_updated"),
                        "voted_up": review.get("voted_up"),
                        "votes_up": review.get("votes_up"),
                        "votes_funny": review.get("votes_funny"),
                        "weighted_vote_score": review.get("weighted_vote_score"),
                        "crawl_timestamp": float(current_time.timestamp())
                    }
                    
                    # Remove None values
                    review_data = {k: v for k, v in review_data.items() if v is not None}
                    
                    producer.send('steam-reviews', value=review_data)
                    published_count += 1
                    
                    if published_count % 100 == 0:
                        print(f"Published {published_count} reviews to Kafka")
                        
                except Exception as e:
                    print(f"Error publishing review {review.get('recommendationid', 'unknown')}: {e}")
                    failed_count += 1
            
            producer.flush()
            
            print(f"✅ Published {published_count} reviews to Kafka. Failed: {failed_count}")
            
            # Push metrics
            metrics.push_metrics("_kafka_producer_reviews")
            
        except Exception as e:
            metrics.set_pipeline_status('kafka_producer_reviews', 0)
            raise e
        finally:
            metrics.set_pipeline_status('kafka_producer_reviews', 0)
            if 'producer' in locals():
                producer.close()
            if 'mongo_manager' in locals():
                mongo_manager.close()


@task(retries=3, retry_delay_seconds=5, log_prints=True)
def consume_reviews_task():
    """Consume reviews from Kafka and process with monitoring"""
    
    with metrics.time_task('consume_reviews'):
        metrics.set_pipeline_status('kafka_consumer_reviews', 1)
        
        try:
            # Set up consumer
            consumer = KafkaConsumer(
                'steam-reviews',
                bootstrap_servers=config.kafka.bootstrap_servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=config.kafka.consumer_timeout_ms
            )
            
            processed_count = 0
            failed_count = 0
            
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                print("Starting to consume reviews from Kafka...")
                
                for message in consumer:
                    try:
                        data = message.value
                        if data.get('type') == 'review':
                            appid = data.get('appid')
                            recommendationid = data.get('recommendationid')
                            author = data.get('author')
                            language = data.get('language')
                            review_text = data.get('review')
                            voted_up = data.get('voted_up')
                            votes_up = data.get('votes_up', 0)
                            votes_funny = data.get('votes_funny', 0)
                            weighted_vote_score = data.get('weighted_vote_score', 0)
                            timestamp_created = datetime.fromtimestamp(data.get('timestamp_created', 0))
                            
                            if not all([appid, recommendationid]):
                                print(f"Invalid review data: {data}")
                                failed_count += 1
                                continue
                            
                            # Insert review summary into MySQL
                            cursor.execute("""
                                INSERT IGNORE INTO review_summaries 
                                (appid, recommendationid, author_steamid, language, voted_up, 
                                 votes_up, votes_funny, weighted_vote_score, timestamp_created, processed_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (appid, recommendationid, author, language, voted_up, 
                                  votes_up, votes_funny, weighted_vote_score, 
                                  timestamp_created, datetime.now()))
                            
                            conn.commit()
                            processed_count += 1
                            
                            # Log progress
                            if processed_count % 50 == 0:
                                print(f"Processed {processed_count} review records")
                        
                    except Exception as e:
                        print(f"Error processing review message: {e}")
                        failed_count += 1
                        conn.rollback()
            
            print(f"Review consumer completed. Processed: {processed_count}, Failed: {failed_count}")
            
            # Push metrics
            metrics.push_metrics("_kafka_consumer_reviews")
            
        except Exception as e:
            metrics.set_pipeline_status('kafka_consumer_reviews', 0)
            raise e
        finally:
            metrics.set_pipeline_status('kafka_consumer_reviews', 0)
            if 'consumer' in locals():
                consumer.close()