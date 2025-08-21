import pandas as pd
import mysql.connector
from mysql.connector import Error


# Config MySQL
MYSQL_HOST = "localhost"   
MYSQL_PORT = 3307          
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB = "steam_db"


# Connect MySQL
def get_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )


# Helpers
def safe_val(v):
    return None if pd.isna(v) else v

def safe_int(v, default=0):
    try:
        if v is None or pd.isna(v):
            return default
        return int(v)
    except:
        return default

def get_or_create(cursor, table, name_field, value):
    if value is None or value == "":
        return None
    
    id_columns = {
        "developers": "dev_id",
        "publishers": "pub_id",
        "genres": "genre_id",
        "languages": "lang_id"
    }
    
    id_col = id_columns.get(table)
    if not id_col:
        raise ValueError(f"Table {table} không có mapping id column!")

    cursor.execute(f"SELECT {id_col} FROM {table} WHERE name = %s", (value,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(f"INSERT INTO {table}(name) VALUES (%s)", (value,))
    print(f"  ➕ Insert {table[:-1]}: {value}")
    return cursor.lastrowid


# Main
def main():
    df_more = pd.read_csv("data/game/steam_game_more_info.csv")
    df_clean = pd.read_csv("data/game/steam_game_clean.csv")
    df = pd.merge(df_clean, df_more, on="appid", how="left")
    df = df.where(pd.notnull(df), None)

    conn = get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            game_id = int(row["appid"])
            title = safe_val(row["title"])
            description = safe_val(row["description"])
            release_date = safe_val(row["release_date"])
            windows_req = safe_val(row.get("windows_req"))
            mac_req = safe_val(row.get("mac_req"))
            linux_req = safe_val(row.get("linux_req"))
            required_age = safe_int(row.get("required_age"), 0)
            awards = safe_val(row.get("awards"))

            # Insert games
            cursor.execute("""
                INSERT INTO games (game_id, title, description, release_date, windows_req, mac_req, linux_req, required_age, awards)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE 
                    title=VALUES(title), description=VALUES(description), release_date=VALUES(release_date),
                    windows_req=VALUES(windows_req), mac_req=VALUES(mac_req), linux_req=VALUES(linux_req),
                    required_age=VALUES(required_age), awards=VALUES(awards)
            """, (game_id, title, description, release_date, windows_req, mac_req, linux_req, required_age, awards))

            print(f"✅ Game {game_id} - {title} inserted/updated")

            # Developers
            if row["developer"]:
                for dev in str(row["developer"]).split(","):
                    dev = dev.strip()
                    if dev:
                        dev_id = get_or_create(cursor, "developers", "name", dev)
                        if dev_id:
                            cursor.execute("INSERT IGNORE INTO game_developers (game_id, dev_id) VALUES (%s, %s)", (game_id, dev_id))

            # Publishers
            if row["publisher"]:
                for pub in str(row["publisher"]).split(","):
                    pub = pub.strip()
                    if pub:
                        pub_id = get_or_create(cursor, "publishers", "name", pub)
                        if pub_id:
                            cursor.execute("INSERT IGNORE INTO game_publishers (game_id, pub_id) VALUES (%s, %s)", (game_id, pub_id))

            # Genres
            if row["genres"]:
                for genre in str(row["genres"]).split(","):
                    genre = genre.strip()
                    if genre:
                        genre_id = get_or_create(cursor, "genres", "name", genre)
                        if genre_id:
                            cursor.execute("INSERT IGNORE INTO game_genres (game_id, genre_id) VALUES (%s, %s)", (game_id, genre_id))

            # Languages
            if row["languages"]:
                for lang in str(row["languages"]).split(","):
                    lang = lang.strip()
                    if lang:
                        lang_id = get_or_create(cursor, "languages", "name", lang)
                        if lang_id:
                            cursor.execute("INSERT IGNORE INTO game_languages (game_id, lang_id) VALUES (%s, %s)", (game_id, lang_id))

        except Exception as e:
            print(f"❌ Lỗi khi xử lý game {row.get('appid')}, {row.get('title')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("🎉 Insert dữ liệu hoàn tất!")

if __name__ == "__main__":
    main()
