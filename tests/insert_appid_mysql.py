import pandas as pd
import pymysql


MYSQL_HOST = "localhost"   
MYSQL_PORT = 3307
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB = "steam_db"

# CSV

csv_file = "E:/Project-DE/Steam-Project/tests/steam_appid_name.csv"
df = pd.read_csv(csv_file)
appids = [(int(x),) for x in df.iloc[:, 0].dropna().unique()]


conn = pymysql.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    charset="utf8mb4"
)
cursor = conn.cursor()

sql = "INSERT IGNORE INTO games (game_id) VALUES (%s)"
cursor.executemany(sql, appids)

conn.commit()
cursor.close()
conn.close()

print(f"✅ Đã insert {len(appids)} game_id vào bảng games.")


