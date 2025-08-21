# utils/your_module.py
import csv
import time
import os
from threading import Lock
from datetime import datetime
import pandas as pd
import mysql.connector
from kafka import KafkaProducer
import json
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv('config/.env')


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        return super().default(obj)


date_str = datetime.now().strftime('%d%m%Y')
RAW_CSV = f"data/raw/price/price_raw{date_str}.csv"
PROCESSED_CSV = f"data/processed/price_processed_{date_str}.csv"
COLUMNS = ["appid", "discount", "price"]
lock = Lock()
buffer_rows = []
last_update_time = time.time() 

# Crawl functions
def save_batch_to_csv(rows, csv_file):
    file_exists = os.path.exists(csv_file)
    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    global last_update_time
    last_update_time = time.time()

def crawl_price(csv_file):
    global buffer_rows, last_update_time
    start_time = time.time()
    sample_data = [
        {"appid": "300", "discount": "0", "price": "999"},
        {"appid": "584", "discount": "0", "price": "0"},
        *[{"appid": f"{i}", "discount": "0", "price": str(i % 1000)} for i in range(3000, 3010)]
    ]
    processed_ids = set()
    if os.path.exists(csv_file):
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed_ids.add(str(row["appid"]))

    remaining_data = [d for d in sample_data if str(d["appid"]) not in processed_ids]
    total = len(remaining_data)

    if not remaining_data:
        print("No new data to crawl. Crawl completed.")
        return

    for i, data in enumerate(remaining_data, 1):
        with lock:
            buffer_rows.append(data)
            processed_ids.add(str(data["appid"]))
            if len(buffer_rows) >= 5:
                save_batch_to_csv(buffer_rows, csv_file)
                buffer_rows.clear()
        time.sleep(0.1)  
        print(f"Processed {i}/{total} apps ({(i/total)*100:.2f}%)")

    if buffer_rows:
        save_batch_to_csv(buffer_rows, csv_file)

    elapsed = time.time() - start_time
    print(f"Finished crawl. Total collected: {len(processed_ids)} records in {elapsed:.2f} seconds.")
    last_update_time = 0  

# Transform functions
def to_cents(value):
    if value is None or str(value).strip() == '':
        return None
    s = str(value).strip()
    if ',' in s and '.' not in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return int(round(float(s) * 100))
    except ValueError:
        return None

def format_cents(cents):
    if cents is None:
        return ''
    euros = cents // 100
    cents_part = cents % 100
    return f"{euros},{cents_part:02d}"

def parse_discount(value):
    if value is None:
        return 0.0
    s = str(value).strip().replace('%', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0

def initial_price_cents(current_price_cents, discount_percent):
    if current_price_cents is None or current_price_cents <= 0:
        return 0
    d = float(discount_percent)
    if d <= 0:
        return int(current_price_cents)
    denom = 1.0 - (d / 100.0)
    if denom <= 0:
        return int(current_price_cents)
    return int(round(current_price_cents / denom))

def preprocess_data(input_file, output_file):
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        return
    df = pd.read_csv(input_file, delimiter=',')
    df = df.drop_duplicates(subset=['appid'], keep='last')
    df['timestamp'] = datetime.now().timestamp()
    df.to_csv(output_file, index=False, sep=',')
    if os.path.getsize(output_file) > 0:
        print(f"Preprocessed data saved to {output_file}")
    else:
        print(f"Error: Output file {output_file} is empty")

def publish_to_kafka(input_file):
    try:
        
        mysql_conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB")
        )
        cursor = mysql_conn.cursor(dictionary=True)

        df = pd.read_csv(input_file, delimiter=',')
        producer = KafkaProducer(
            bootstrap_servers=[os.getenv("KAFKA_SERVER")],
            value_serializer=lambda v: json.dumps(v, cls=NumpyEncoder).encode('utf-8')
        )

        last_timestamps = {}
        for appid in df['appid'].unique():
            cursor.execute("SELECT last_price_timestamp FROM crawl_state WHERE game_appid = %s", (appid,))
            result = cursor.fetchone()
            last_timestamps[appid] = result['last_price_timestamp'] if result else None

        current_time = datetime.now()
        for index, row in df.iterrows():
            appid = row['appid']
            last_timestamp = last_timestamps.get(appid)
            if last_timestamp is None or current_time > last_timestamp:
                price_data = {
                    "type": "price",
                    "appid": int(float(row['appid'])),
                    "discount": int(float(row['discount'])),
                    "initial_price": float(row['initial_price']),
                    "price": float(row['price']),
                    "timestamp": float(current_time.timestamp())
                }
                producer.send('steam-dynamic', value=price_data)

        producer.flush()
        print(f"Successfully sent {len(df)} price records to Kafka topic 'steam-dynamic'")
    except Exception as e:
        print(f"Error in publish_to_kafka: {e}")
    finally:
        if 'mysql_conn' in locals() and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            

