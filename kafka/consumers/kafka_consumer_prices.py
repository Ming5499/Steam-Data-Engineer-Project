
from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import Error
import requests
from datetime import datetime


# Config consumer
consumer = KafkaConsumer('steam-dynamic-price',
                         bootstrap_servers=['localhost:29092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))


try:
    conn = mysql.connector.connect(
        host="localhost",
        port=3307,
        user="root",
        password="root",
        database="steam_db"
    )
    cursor = conn.cursor(dictionary=True)

    # Consume messages 
    for message in consumer:
        data = message.value
        if 'price' in data:  #  price messages
            appid = data['appid']
            price = data['price']
            discount = data['discount']
            initial_price = data['initial_price']
            timestamp = datetime.fromtimestamp(data['timestamp'])

            cursor.execute("""
                SELECT price, discount, initial_price FROM prices
                WHERE game_id = %s AND timestamp < %s
                ORDER BY timestamp DESC LIMIT 1
            """, (appid, timestamp))
            old_price = cursor.fetchone()


            cursor.execute("""
                INSERT INTO prices (game_id, price, discount, initial_price, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (appid, price, discount, initial_price, timestamp))

            # Update crawl_state
            cursor.execute("""
                INSERT INTO crawl_state (game_appid, last_price_timestamp)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE last_price_timestamp = %s
            """, (appid, timestamp, timestamp))

            conn.commit()

            # Check notify Discord 
            if old_price:
                if price != old_price['price'] or discount != old_price['discount'] or initial_price != old_price['initial_price']:
                    message = f"Price change for game {appid}: New price={price}, discount={discount}, initial_price={initial_price} (Old: price={old_price['price']}, discount={old_price['discount']}, initial_price={old_price['initial_price']})"
                    webhook_url = "YOUR_DISCORD_WEBHOOK_URL"
                    requests.post(webhook_url, json={"content": message})
                    print(f"Sent Discord notify: {message}")
            else:
                print(f"No old price for {appid}, inserted new")

            print(f"Processed price for appid: {appid}")

except Error as e:
    print(f"Error: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
