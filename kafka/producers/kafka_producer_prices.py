from kafka import KafkaProducer
import pandas as pd
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Kafka producer config
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    # Connect MySQL
    mysql_conn = mysql.connector.connect(
        host="localhost",
        port=3307,
        user="root",
        password="root",
        database="steam_db"
    )
    cursor = mysql_conn.cursor(dictionary=True)

    # Dynamic CSV filename
    CSV_FILE = f"data/processed/price_processed_{datetime.now().strftime('%d%m%Y')}.csv"
    df = pd.read_csv(CSV_FILE)

    # Clean and convert price columns 
    for col in ['initial_price', 'price']:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(',', '.', regex=False)
            .replace({'': '0', 'nan': '0'})
            .astype(float)
        )

    # Ensure discount and appid are ints
    df['appid'] = df['appid'].astype(str) 
    df['discount'] = df['discount'].astype(str)

    # Get appids from CSV
    appids = df['appid'].unique()

    # Fetch last_price_timestamp for each appid
    last_timestamps = {}
    for appid in appids:
        cursor.execute("SELECT last_price_timestamp FROM crawl_state WHERE game_appid = %s", (appid,))
        result = cursor.fetchone()
        last_timestamps[appid] = result['last_price_timestamp'] if result else None

    # Send price updates to Kafka
    current_time = datetime.now()
    for _, row in df.iterrows():
        appid = int(row['appid'])  
        last_timestamp = last_timestamps.get(appid)

        if last_timestamp is None or current_time > last_timestamp:
            price_data = {
                "type": "price",
                "appid": appid,
                "discount": int(row['discount']), 
                "initial_price": float(row['initial_price']), 
                "price": float(row['price']),  
                "timestamp": float(current_time.timestamp())  
            }
            producer.send('steam-dynamic', value=price_data)

    producer.flush()
    print("✅ Prices sent to Kafka topic 'steam-dynamic' successfully.")

except Error as e:
    print(f"MySQL Error: {e}")
except FileNotFoundError:
    print(f"File {CSV_FILE} not found")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'mysql_conn' in locals() and mysql_conn.is_connected():
        cursor.close()
        mysql_conn.close()
