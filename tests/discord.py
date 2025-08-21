from kafka import KafkaConsumer
import requests
import json

# Configuration
DISCORD_WEBHOOK_URL = ""
KAFKA_SERVER = "localhost:29092"  
TOPIC_NAME = "steamdb.steam_db.prices"

def send_discord_notification(appid, old_discount, new_discount, new_price):
    """Send formatted notification to Discord"""
    message = (
        f"🎮 **Price Discount!** Game `{appid}`\n"
        f"🔽 Discount changed from `{old_discount}%` to `{new_discount}%`\n"
        f"💰 New price: `{new_price}`"
    )
    
    payload = {
        "content": message,
        "embeds": [{
            "color": 0x00ff00 if new_discount > old_discount else 0xff0000,
            "fields": [
                {"name": "AppID", "value": str(appid), "inline": True},
                {"name": "Old Discount", "value": f"{old_discount}%", "inline": True},
                {"name": "New Discount", "value": f"{new_discount}%", "inline": True},
                {"name": "Price", "value": new_price, "inline": False}
            ]
        }]
    }
    
    headers = {'Content-Type': 'application/json'}
    response = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers=headers)
    return response.status_code == 204

def process_message(message):
    """Process Kafka message and detect discount changes"""
    try:
        value = message.value
        appid = value['payload']['appid']
        new_discount = value['payload']['discount']
        new_price = value['payload']['price']
        
        # Track previous discount for this appid
        if not hasattr(process_message, 'last_discounts'):
            process_message.last_discounts = {}
        
        old_discount = process_message.last_discounts.get(appid, new_discount)
        
        # Only notify if discount actually changed
        if old_discount != new_discount:
            print(f"Detected discount change for appid {appid}: {old_discount}% -> {new_discount}%")
            if send_discord_notification(appid, old_discount, new_discount, new_price):
                print("Notification sent to Discord successfully")
            else:
                print("Failed to send Discord notification")
        
        # Update last known discount
        process_message.last_discounts[appid] = new_discount
        
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    print("Starting Discord Notifier Consumer...")
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    
    try:
        for message in consumer:
            process_message(message)
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()