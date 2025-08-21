import pandas as pd
from datetime import datetime


df = pd.read_csv('data/game/steam_game.csv')


def clean_array_string(text, is_language=False):
    if isinstance(text, str):
        text = text.replace('[', '').replace(']', '').replace("'", "")

        if is_language:
            text = text.replace('*', '')
            items = [item.strip() for item in text.split(',') if item.strip()]
            text = ', '.join(items)
        else:
            text = ' '.join(text.split())
    return text

# Convert release_date to datetime
df['release_date'] = pd.to_datetime(df['release_date'], format='%d %b, %Y', errors='coerce')

# Clean columns
df['genres'] = df['genres'].apply(clean_array_string)
df['languages'] = df['languages'].apply(lambda x: clean_array_string(x, is_language=True))

# Save to new CSV file
df.to_csv('data/game/steam_game_clean.csv', index=False)

print("Data cleaning complete. Saved to 'steam_game_clean.csv'")