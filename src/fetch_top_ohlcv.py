import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# --- Moralis + Dexscreener ---
MORALIS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjM0MDlmY2YyLWM5Y2ItNDcxYy04MDQ1LTY2ZmQ5MjdmMTc5MyIsIm9yZ0lkIjoiNDQ2NDI2IiwidXNlcklkIjoiNDU5MzEwIiwidHlwZUlkIjoiNjNmZjY2MDUtNTRhYS00NTMyLWE5NWMtOTMwNTIyMjMxNzRiIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDY5NDM5MzUsImV4cCI6NDkwMjcwMzkzNX0._LVE0RJNvv7vKwmbSmQ4U1NSvTStVaAeZB_qSC6_roY"
HEADERS = {"X-API-Key": MORALIS_API_KEY}
DEX_API = "https://api.dexscreener.com/tokens/v1/ethereum"

RAW_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/automated-memetoken-index-pipeline/data/raw"
DF_DIR = Path("../dataframes")


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
        "limit": 400
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        raise Exception(f"Moralis error: {resp.status_code} - {resp.text}")
    return resp.json().get("result", [])


def process_and_save(df, symbol, month):
    if df.empty:
        print(f"No data for {symbol}")
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

    output_dir = DF_DIR / month
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / f"{symbol}.csv")
    print(f"✅ Saved {symbol} to {output_dir / f'{symbol}.csv'}")


def main():
    print("📁 Available Token Files:")
    files = list_json_files()
    for idx, f in enumerate(files):
        print(f"{idx + 1}. {f}")
    choice = int(input("\nSelect a file: ")) - 1
    file = files[choice]

    month_label = file.split("_")[0]
    data = load_tokens_from_file(os.path.join(RAW_DIR, file))

    top_n = int(input("How many top tokens to fetch OHLCV for? "))
    from_date = input("From Date (YYYY-MM-DD): ").strip()
    to_date = input("To Date (YYYY-MM-DD): ").strip()

    selected_tokens = list(data.items())[:top_n]

    for token_id, token_info in selected_tokens:
        symbol = token_info["symbol"]
        print(f"\n🔍 {symbol} | {token_id}")
        try:
            pairs = get_pair_addresses(token_id)
            if not pairs:
                print("❌ No LP pairs found.")
                continue
            
            # Print all pair addresses fetched from Dexscreener for this token
            print("🔗 Pair Addresses:")
            for pair_addr in pairs:
                print(f"   - {pair_addr}")

            pair = pairs[0]  # or add logic to choose based on liquidity
            raw = fetch_ohlcv(pair, from_date, to_date)
            df = pd.DataFrame(raw)
            process_and_save(df, symbol, month_label)
        except Exception as e:
            print(f"⚠️ {symbol} failed: {e}")



if __name__ == "__main__":
    main()
    