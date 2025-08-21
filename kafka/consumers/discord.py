
from kafka import KafkaConsumer
import requests
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Configuration
DISCORD_WEBHOOK_URL = "YOURWEBHOOK"
KAFKA_SERVER = "localhost:29092"
TOPIC_NAME = "steamdb.steam_db.prices"

def send_discord_notification(appid, old_data, new_data):
    """Send formatted notification to Discord"""
    old_discount = old_data['discount'] if old_data else 0
    new_discount = new_data['discount']
    old_price = old_data['price'] if old_data else 0
    new_price = new_data['price']
    old_initial_price = old_data['initial_price'] if old_data else 0
    new_initial_price = new_data['initial_price']

    message = (
        f"🎮 **Price Update for Game `{appid}`**\n"
        f"🔽 Discount: `{old_discount}%` → `{new_discount}%`\n"
        f"💰 Price: `{old_price}` → `{new_price}`\n"
        f"📊 Initial Price: `{old_initial_price}` → `{new_initial_price}`"
    )
    
    payload = {
        "content": message,
        "embeds": [{
            "color": 0x00ff00 if new_discount > old_discount else 0xff0000,
            "fields": [
                {"name": "AppID", "value": str(appid), "inline": True},
                {"name": "Old Discount", "value": f"{old_discount}%", "inline": True},
                {"name": "New Discount", "value": f"{new_discount}%", "inline": True},
                {"name": "Old Price", "value": f"{old_price}", "inline": True},
                {"name": "New Price", "value": f"{new_price}", "inline": True},
                {"name": "Old Initial Price", "value": f"{old_initial_price}", "inline": True},
                {"name": "New Initial Price", "value": f"{new_initial_price}", "inline": True}
            ]
        }]
    }
    
    headers = {'Content-Type': 'application/json'}
    response = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers=headers)
    return response.status_code == 204

def process_message(message, cursor, conn):
    """Process Debezium message and detect price changes"""
    try:
        value = message.value
        if not value or 'payload' not in value or 'after' not in value['payload']:
            print("Invalid Debezium message format")
            return

        payload = value['payload']
        after = payload.get('after', {})
        before = payload.get('before', {})

        appid = after.get('game_id')
        if not appid:
            print("No game_id in message")
            return

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

        # Check nếu có thay đổi
        if old_data and (
            new_data['discount'] != old_data['discount'] or
            new_data['price'] != old_data['price'] or
            new_data['initial_price'] != old_data['initial_price']
        ):
            if send_discord_notification(appid, old_data, new_data):
                print(f"Notification sent to Discord for appid {appid}")
            else:
                print(f"Failed to send Discord notification for appid {appid}")

        # Update last_price_timestamp  crawl_state
        cursor.execute("""
            INSERT INTO crawl_state (game_appid, last_price_timestamp)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE last_price_timestamp = %s
        """, (appid, new_data['timestamp'], new_data['timestamp']))
        conn.commit()

        print(f"Processed price for appid: {appid}")

    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    print("Starting Discord Notifier Consumer...")
    
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3307,
            user="root",
            password="123456",
            database="steam_db"
        )
        cursor = conn.cursor(dictionary=True)

        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000
        )

        try:
            for message in consumer:
                process_message(message, cursor, conn)
        except KeyboardInterrupt:
            print("Shutting down consumer...")
        finally:
            consumer.close()
            if conn.is_connected():
                cursor.close()
                conn.close()
            print("Connections closed")

    except Error as e:
        print(f"MySQL Error: {e}")

if __name__ == "__main__":
    main()
