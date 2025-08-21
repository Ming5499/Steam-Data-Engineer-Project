from kafka import KafkaProducer
import pandas as pd
import json
import numpy as np

# Config producer
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'))


df_main = pd.read_csv("data/game/steam_game_clean.csv")
df_extra = pd.read_csv("data/game/steam_game_more_info.csv")
merged_df = pd.merge(df_main, df_extra, on='appid', how='outer')

#  NaN
merged_df = merged_df.fillna({
    'title': '',
    'description': '',
    'developer': '',
    'publisher': '',
    'genres': '',
    'languages': '',
    'windows_req': '',
    'mac_req': '',
    'linux_req': '',
    'required_age': 0,
    'awards': ''
})

for index, row in merged_df.iterrows():
    data = {
        "appid": int(row['appid']) if not np.isnan(row['appid']) else 0,  
        "title": row['title'],
        "description": row['description'],
        "release_date": row['release_date'] if not pd.isna(row['release_date']) else None,
        "genres": row['genres'].split(", ") if isinstance(row['genres'], str) and row['genres'] else [],
        "languages": row['languages'].split(", ") if isinstance(row['languages'], str) and row['languages'] else [],
        "developers": [row['developer']] if row['developer'] else [],
        "publishers": [row['publisher']] if row['publisher'] else [],
        "windows_req": row['windows_req'],
        "mac_req": row['mac_req'],
        "linux_req": row['linux_req'],
        "required_age": int(row['required_age']),
        "awards": row['awards']
    }

    producer.send('steam-static', value=data)


producer.flush()
print("Data sent to Kafka topic 'steam-static' successfully")