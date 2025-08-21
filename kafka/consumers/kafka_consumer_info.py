from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import Error

# Cấu hình consumer
consumer = KafkaConsumer('steam-static',
                         bootstrap_servers=['localhost:29092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Kết nối MySQL
try:
    conn = mysql.connector.connect(
        host="localhost",     
        port="3306",          
        user="root",          
        password="root",    
        database="steam_db"   
    )
    cursor = conn.cursor()

# Consume messages
    for message in consumer:
        data = message.value
        game_id = data['appid']  


        title = data['title'] if data['title'] and data['title'] != 'nan' else ''
        description = data['description'] if data['description'] and data['description'] != 'nan' else ''
        windows_req = data['windows_req'] if data['windows_req'] and data['windows_req'] != 'nan' else ''
        mac_req = data['mac_req'] if data['mac_req'] and data['mac_req'] != 'nan' else ''
        linux_req = data['linux_req'] if data['linux_req'] and data['linux_req'] != 'nan' else ''
        awards = data['awards'] if data['awards'] and data['awards'] != 'nan' else ''
        required_age = int(data['required_age']) if data['required_age'] is not None else 0
        release_date = data['release_date'] if data['release_date'] and data['release_date'] != 'nan' else None


        cursor.execute("""
            INSERT INTO games (game_id, title, description, release_date, windows_req, mac_req, linux_req, required_age, awards)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                title=VALUES(title), 
                description=VALUES(description), 
                release_date=VALUES(release_date),
                windows_req=VALUES(windows_req),
                mac_req=VALUES(mac_req),
                linux_req=VALUES(linux_req),
                required_age=VALUES(required_age),
                awards=VALUES(awards)
        """, (game_id, title, description, release_date, windows_req, mac_req, linux_req, required_age, awards))
        
        # Insert developers (N-N)
        for dev_name in data['developers']:
            if dev_name and dev_name != 'nan':
                cursor.execute("""
                    INSERT IGNORE INTO developers (name)
                    VALUES (%s)
                """, (dev_name,))
                cursor.execute("SELECT dev_id FROM developers WHERE name = %s", (dev_name,))
                result = cursor.fetchone()
                if result:
                    dev_id = result[0]
                    cursor.execute("""
                        INSERT INTO game_developers (game_id, dev_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE game_id=game_id
                    """, (game_id, dev_id))
        
        # Insert publishers (N-N)
        for pub_name in data['publishers']:
            if pub_name and pub_name != 'nan':
                cursor.execute("""
                    INSERT IGNORE INTO publishers (name)
                    VALUES (%s)
                """, (pub_name,))
                cursor.execute("SELECT pub_id FROM publishers WHERE name = %s", (pub_name,))
                result = cursor.fetchone()
                if result:
                    pub_id = result[0]
                    cursor.execute("""
                        INSERT INTO game_publishers (game_id, pub_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE game_id=game_id
                    """, (game_id, pub_id))
        
        # Insert genres (N-N)
        for genre in data['genres']:
            if genre and genre != 'nan':
                cursor.execute("""
                    INSERT IGNORE INTO genres (name)
                    VALUES (%s)
                """, (genre,))
                cursor.execute("SELECT genre_id FROM genres WHERE name = %s", (genre,))
                result = cursor.fetchone()
                if result:
                    genre_id = result[0]
                    cursor.execute("""
                        INSERT INTO game_genres (game_id, genre_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE game_id=game_id
                    """, (game_id, genre_id))

        # Insert languages (N-N)
        for lang in data['languages']:
            if lang and lang != 'nan':
                cursor.execute("""
                    INSERT IGNORE INTO languages (name)
                    VALUES (%s)
                """, (lang,))
                cursor.execute("SELECT lang_id FROM languages WHERE name = %s", (lang,))
                result = cursor.fetchone()
                if result:
                    lang_id = result[0]
                    cursor.execute("""
                        INSERT INTO game_languages (game_id, lang_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE game_id=game_id
                    """, (game_id, lang_id))

        conn.commit()
        print(f"Processed game_id: {game_id}")

except Error as e:
    print(f"Error: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()