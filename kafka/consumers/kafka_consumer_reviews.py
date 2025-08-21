from kafka import KafkaConsumer
import pymongo
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import json

# Config consumer
consumer = KafkaConsumer('steam-dynamic',
                         bootstrap_servers=['localhost:29092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))




try:
    # MongoDB
    client = pymongo.MongoClient(
        'mongodb://admin:password@localhost:27018/',
        authSource='admin')
    db = client['steam']
    collection = db['test']
    collection.create_index([('appid', 1), ('author_steamid', 1)], unique=True)

    # MySQL
    mysql_conn = mysql.connector.connect(
        host="localhost",
        port=3307,
        user="root",
        password="123456",
        database="steam_db"
    )
    mysql_cursor = mysql_conn.cursor()

    # Consume messages
    processed_count = 0
    max_messages = 40 

    for message in consumer:
        data = message.value
        appid = data['appid']


        mysql_cursor.execute("SELECT game_id FROM games WHERE game_id = %s", (appid,))
        if not mysql_cursor.fetchone():
            print(f"Skipping crawl_state update for appid {appid}: Not found in games table")
            continue


        review_doc = {
            'appid': appid,
            'name': data['name'],
            'author_steamid': data['author_steamid'],
            'review': data['review'],
            'timestamp_created': data['timestamp_created'],
            'language': data['language']
        }


        result = collection.update_one(
            {'appid': appid, 'author_steamid': data['author_steamid']},
            {'$set': review_doc},
            upsert=True
        )


        timestamp = datetime.fromtimestamp(data['timestamp_created'])
        mysql_cursor.execute("""
            INSERT INTO crawl_state (game_appid, last_review_timestamp)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE last_review_timestamp = GREATEST(last_review_timestamp, %s)
        """, (appid, timestamp, timestamp))
        mysql_conn.commit()

        processed_count += 1
        print(f"Processed review for appid: {appid}, author: {data['author_steamid']}, modified: {result.modified_count}, upserted: {result.upserted_id}")

        if processed_count >= max_messages:
            break


    total_docs = collection.count_documents({})
    print(f"Total documents in reviews collection: {total_docs}")

except Exception as e:
    print(f"Error: {e}")

finally:
    client.close()
    if mysql_conn.is_connected():
        mysql_cursor.close()
        mysql_conn.close()
    consumer.close()
    print("Connections closed")