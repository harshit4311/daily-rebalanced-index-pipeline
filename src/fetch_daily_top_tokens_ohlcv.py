import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
import requests

# --- Directories ---
BASE_DIR = os.path.dirname(__file__)
TOP_TOKENS_DIR = os.path.join(BASE_DIR, "..", "data", "top_tokens_daily")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "dataframes", "daily")

# --- Moralis + Dexscreener ---
MORALIS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjM0MDlmY2YyLWM5Y2ItNDcxYy04MDQ1LTY2ZmQ5MjdmMTc5MyIsIm9yZ0lkIjoiNDQ2NDI2IiwidXNlcklkIjoiNDU5MzEwIiwidHlwZUlkIjoiNjNmZjY2MDUtNTRhYS00NTMyLWE5NWMtOTMwNTIyMjMxNzRiIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDY5NDM5MzUsImV4cCI6NDkwMjcwMzkzNX0._LVE0RJNvv7vKwmbSmQ4U1NSvTStVaAeZB_qSC6_roY"
HEADERS = {"X-API-Key": MORALIS_API_KEY}
DEX_API = "https://api.dexscreener.com/tokens/v1/ethereum"


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
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        raise Exception(f"Moralis error: {resp.status_code} - {resp.text}")
    return resp.json().get("result", [])

def process_and_save(df, symbol, out_dir, file_label):
    if df.empty:
        print(f"⚠️ No data found for {symbol}")
        return
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)

    df['return'] = df['close'].pct_change()
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    df['cumulative_return'] = (1 + df['return']).cumprod() - 1
    df['sharpe_ratio'] = (df['return'].mean() / df['return'].std()) * np.sqrt(365)
    df['cum_max'] = df['close'].cummax()
    df['drawdown'] = df['close'] / df['cum_max'] - 1
    df['turnover'] = df['volume'] / df['close']
    df.dropna(subset=["return"], inplace=True)

    day_output_dir = os.path.join(out_dir, file_label)
    os.makedirs(day_output_dir, exist_ok=True)
    out_path = os.path.join(day_output_dir, f"{symbol}.csv")
    df.to_csv(out_path)
    print(f"✅ Saved {symbol} OHLCV to {out_path}")


def fetch_and_save_daily_ohlcv(month_label, top_n=5):
    files = sorted([
        f for f in os.listdir(TOP_TOKENS_DIR)
        if f.endswith("_top_tokens.json") and f.split("_")[1] == month_label
    ])

    if not files:
        print(f"❌ No files found for month: {month_label}")
        return

    for file in files:
        file_path = os.path.join(TOP_TOKENS_DIR, file)
        file_label = file.replace("_top_tokens.json", "")  # e.g. 1_feb25
        print(f"\n📅 Processing {file_label}...")

        try:
            token_data = load_tokens_from_file(file_path)
            top_tokens = list(token_data.items())[:top_n]

            for token_address, token_info in top_tokens:
                symbol = token_info["symbol"]
                print(f"🔍 Fetching {symbol} ({token_address})")

                try:
                    pairs = get_pair_addresses(token_address)
                    if not pairs:
                        print(f"⚠️ No LP pairs for {symbol}")
                        continue

                    pair = pairs[0]
                    from_date = "2021-01-01"
                    to_date = "2025-12-31"  # fetch full life cycle, filter in backtest

                    raw = fetch_ohlcv(pair, from_date, to_date)
                    df = pd.DataFrame(raw)

                    if df.empty:
                        print(f"⚠️ No OHLCV data for {symbol}")
                        continue

                    process_and_save(df, symbol, OUTPUT_DIR, file_label)

                except Exception as e:
                    print(f"❌ Error fetching {symbol}: {e}")

        except Exception as e:
            print(f"❌ Failed to process file {file}: {e}")

if __name__ == "__main__":
    fetch_and_save_daily_ohlcv("feb25", top_n=5)
