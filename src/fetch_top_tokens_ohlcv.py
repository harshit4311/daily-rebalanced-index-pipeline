import os
import json
import pandas as pd
from moralis_fetch_ohlcv import get_pair_addresses, fetch_ohlcv, process_and_save

# --- Directories ---
BASE_DIR = os.path.dirname(__file__)
TOP_TOKENS_DIR = os.path.join(BASE_DIR, "..", "data", "top_tokens_daily")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "dataframes", "daily")

def load_tokens_from_file(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

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
                    to_date = "2025-12-31"

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
