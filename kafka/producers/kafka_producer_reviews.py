from kafka import KafkaProducer
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Config consumer
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


try:
    mysql_conn = mysql.connector.connect(
        host="localhost",
        port=3307,
        user="root",
        password="123456",
        database="steam_db"
    )
    cursor = mysql_conn.cursor(dictionary=True)


    with open('testdata.json', 'r', encoding='utf-8') as f:
        reviews = json.load(f)

    appids = list(set(review['appid'] for review in reviews))


    last_timestamps = {}
    for appid in appids:
        cursor.execute("SELECT last_review_timestamp FROM crawl_state WHERE game_appid = %s", (appid,))
        result = cursor.fetchone()
        last_timestamps[appid] = result['last_review_timestamp'] if result else None

    for review in reviews:
        appid = review['appid']
        timestamp_created = datetime.fromtimestamp(review['timestamp_created'])


        if last_timestamps.get(appid) is None or timestamp_created > last_timestamps[appid]:
            review_data = {
                "appid": review['appid'],
                "name": review['name'],
                "author_steamid": review['author_steamid'],
                "review": review['review'],
                "timestamp_created": review['timestamp_created'],
                "language": review['language']
            }
            producer.send('steam-dynamic', value=review_data)

    producer.flush()
    print("Reviews sent to Kafka topic 'steam-dynamic' successfully")

except Error as e:
    print(f"MySQL Error: {e}")

finally:
    if mysql_conn.is_connected():
        cursor.close()
        mysql_conn.close()

