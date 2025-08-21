import csv
from datetime import datetime
import os
import time
import sys  

# parsing & formatting helpers 

def to_cents(value):
    if value is None:
        return None
    s = str(value).strip()
    if s == '':
        return None
    if ',' in s and '.' not in s:
        s = s.replace('.', '')  
        s = s.replace(',', '.')
        try:
            return int(round(float(s) * 100))
        except ValueError:
            return None
    if '.' in s:
        try:
            return int(round(float(s) * 100))
        except ValueError:
            return None
    try:
        return int(s)
    except ValueError:
        return None

def format_cents(cents):
    if cents is None:
        return ''
    euros = cents // 100
    cents_part = cents % 100
    return f"{euros},{cents_part:02d}"

def parse_discount(value):
    if value is None:
        return 0.0
    s = str(value).strip().replace('%', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0



def initial_price_cents(current_price_cents, discount_percent):
    if current_price_cents is None:
        return None
    if current_price_cents <= 0:
        return 0
    d = float(discount_percent)
    if d <= 0:
        return int(current_price_cents)
    # initial = current / (1 - d/100)
    denom = 1.0 - (d / 100.0)
    if denom <= 0:
        # fall back to current price
        return int(current_price_cents)
    return int(round(current_price_cents / denom))



def process_new_rows(input_file, output_file, processed_appids):
    new_rows = []

    try:
        with open(input_file, mode='r', encoding='utf-8', newline='') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                appid = row.get('appid')
                if not appid or appid in processed_appids:
                    continue

                discount = parse_discount(row.get('discount'))
                price_cents = to_cents(row.get('price'))

                init_cents = initial_price_cents(price_cents, discount)

                # Format outputs
                price_out = format_cents(price_cents)
                init_out = "0" if (init_cents == 0) else format_cents(init_cents)

                new_rows.append({
                    'appid': appid,
                    'discount': str(discount).rstrip('0').rstrip('.') if discount % 1 == 0 else str(discount),
                    'initial_price': init_out,
                    'price': price_out,
                })

                processed_appids.add(appid)

    except FileNotFoundError:
        return processed_appids

    if not new_rows:
        return processed_appids

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    file_exists = os.path.exists(output_file)

    with open(output_file, mode='a', encoding='utf-8', newline='') as outfile:
        fieldnames = ['appid', 'discount', 'initial_price', 'price']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {len(new_rows)} new rows.")
    return processed_appids

if __name__ == "__main__":
    input_csv = f"data/raw/price/price_raw{datetime.now().strftime('%d%m%Y')}.csv"
    output_csv = f"data/processed/price_processed_{datetime.now().strftime('%d%m%Y')}.csv"

    processed_appids = set()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--one-pass':
        print("Running one-pass transform for Airflow...")
        process_new_rows(input_csv, output_csv, processed_appids)
    else:
        print("📡 Starting real-time processing (every 3s)...")
        while True:
            processed_appids = process_new_rows(input_csv, output_csv, processed_appids)
            time.sleep(3)