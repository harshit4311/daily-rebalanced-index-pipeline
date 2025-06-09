''' 
- fetching ohlcv for top X tokens in a given month 
- importing fetch_ohlcv.py to fetch data using moralis (done to write cleaner code) 
'''

import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

from moralis_fetch_ohlcv import fetch_ohlcv, process_and_save

# --- Moralis + Dexscreener ---
MORALIS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjIyNzkyMDU5LTUyY2QtNGEwZC1hM2Q3LTRlMDY3MWE3NjAzMiIsIm9yZ0lkIjoiNDUyMTEwIiwidXNlcklkIjoiNDY1MTg0IiwidHlwZUlkIjoiNzJlNWQ1ZDctNjk2MC00ZjI1LWIwMTktY2MzZTIwNjM5ZTQ5IiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDk0NjM5OTMsImV4cCI6NDkwNTIyMzk5M30.5hQpK13gHJ0zGlS0XsYzZjf9uxsegby-P9uLo9SOSaA"
HEADERS = {"X-API-Key": MORALIS_API_KEY}
DEX_API = "https://api.dexscreener.com/tokens/v1/ethereum"

RAW_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/automated-memetoken-index-pipeline/data/raw"
DF_DIR = Path("../dataframes")

month_map = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}


def list_json_files():
    return sorted([f for f in os.listdir(RAW_DIR) if f.endswith("_fetched_tokens.json")])


def load_tokens_from_file(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def get_pair_addresses(token_address):
    url = f"{DEX_API}/{token_address}"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        raise Exception(f"Dexscreener failed: {resp.status_code}")
    data = resp.json()
    return [pool["pairAddress"] for pool in data]


def fetch_ohlcv(pair_address, from_date, to_date):
    url = f"https://deep-index.moralis.io/api/v2.2/pairs/{pair_address}/ohlcv"
    params = {
        "chain": "eth",
        "timeframe": "1d",
        "currency": "usd",
        "fromDate": from_date,
        "toDate": to_date,
        "limit": 1000
    }
    print(f"Requesting OHLCV with params: {params}")
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        raise Exception(f"Moralis error: {resp.status_code} - {resp.text}")

    data = resp.json()
    results = data.get("result", [])

    if results:
        earliest = min([item['timestamp'] for item in results])
        print(f"Earliest timestamp in response: {earliest}")
    else:
        print("No results returned by API")

    return results


def process_and_save(df, symbol, month):
    if df.empty:
        print(f"No data for {symbol}")
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    launch_date = pd.to_datetime(df["timestamp"].min(), utc=True)
    df["days_since_launch"] = (df["timestamp"] - launch_date).dt.days

    df = df[df["days_since_launch"] <= 60].copy()
    if df.empty:
        print(f"⚠️ Skipping {symbol}: no records in first 60 days.")
        return None

    df.set_index("timestamp", inplace=True)
    df['return'] = df['close'].pct_change()
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    df['cumulative_return'] = (1 + df['return']).cumprod() - 1
    df['sharpe_ratio'] = (df['return'].mean() / df['return'].std()) * np.sqrt(365)
    df['cum_max'] = df['close'].cummax()
    df['drawdown'] = df['close'] / df['cum_max'] - 1
    df['turnover'] = df['volume'] / df['close']
    df.dropna(subset=["return"], inplace=True)

    output_dir = os.path.join(os.path.dirname(__file__), "..", "dataframes", month)
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{symbol}.csv")
    df.to_csv(out_path)
    return out_path


def parse_month_label(label):
    """Converts 'mar24' to (2024, 3)"""
    month_str = label[:3].lower()
    year_suffix = int(label[3:])
    if month_str not in month_map:
        raise ValueError(f"Invalid month abbreviation in label: {label}")
    return 2000 + year_suffix, month_map[month_str]


def main():
    print("📁 Available Token Files:")
    files = list_json_files()
    for idx, f in enumerate(files):
        print(f"{idx + 1}. {f}")
    choice = int(input("\nSelect a file: ")) - 1
    file = files[choice]

    month_label = file.split("_")[0]
    try:
        expected_year, expected_month = parse_month_label(month_label)
    except Exception as e:
        print(f"⚠️ Failed to parse month label '{month_label}': {e}")
        return

    data = load_tokens_from_file(os.path.join(RAW_DIR, file))

    top_n = int(input("How many top tokens to fetch OHLCV for? "))
    buffer = 30
    from_date = "2021-01-01"
    to_date = input("To Date (YYYY-MM-DD): ").strip()

    selected_tokens = list(data.items())[:top_n + buffer]

    saved_files = []
    success_count = 0

    for token_id, token_info in selected_tokens:
        if success_count >= top_n:
            break

        symbol = token_info["symbol"]
        print(f"\n🔍 {symbol} | {token_id}")
        try:
            pairs = get_pair_addresses(token_id)
            print(f"Found {len(pairs)} pairs for {symbol}: {pairs}")
            if not pairs:
                print("❌ No LP pairs found.")
                continue

            for pair in pairs:
                raw = fetch_ohlcv(pair, from_date, to_date)
                df = pd.DataFrame(raw)

                if df.empty:
                    print("⚠️ No data returned.")
                    continue

                first_timestamp = pd.to_datetime(df["timestamp"].min(), utc=True)
                first_year = first_timestamp.year
                first_month = first_timestamp.month

                if (first_year, first_month) != (expected_year, expected_month):
                    print(f"🚫 Skipping {symbol}: first OHLCV is in {first_year}-{first_month:02d}, not in {month_label}")
                    continue

                saved_path = process_and_save(df, symbol, month_label)
                if saved_path:
                    print(f"✅ Saved {symbol} OHLCV to {saved_path}")
                    saved_files.append(saved_path)
                    success_count += 1
                else:
                    print(f"❌ No data to save for {symbol}")
                break

        except Exception as e:
            print(f"⚠️ {symbol} failed: {e}")


if __name__ == "__main__":
    main()
