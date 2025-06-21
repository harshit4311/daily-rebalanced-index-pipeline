import os
import json
import pandas as pd
from datetime import datetime, timedelta
from moralis_fetch_ohlcv import get_pair_addresses, fetch_ohlcv, process_and_save

# --- Directories ---
BASE_DIR = os.path.dirname(__file__)
TOP_TOKENS_DIR = os.path.join(BASE_DIR, "..", "data", "top_tokens_daily")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "dataframes", "daily")


def load_tokens_from_file(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def fetch_and_save_daily_ohlcv(month_label, top_n=5, buffer=20):
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
            # derive target date from label
            day_str, mon_str = file_label.split("_")
            day = int(day_str)
            year = 2025  # hardcoded for now — you can generalize this
            month_abbr = mon_str[:3]  # extract "feb" from "feb25"
            month = datetime.strptime(month_abbr, "%b").month
            year = 2025

            from_date = datetime(year, month, day)
            to_date = from_date + timedelta(days=1)

            token_data = load_tokens_from_file(file_path)
            all_tokens = list(token_data.items())[:top_n + buffer]
            success_count = 0

            for token_address, token_info in all_tokens:
                if success_count >= top_n:
                    break

                symbol = token_info["symbol"]
                print(f"🔍 Fetching {symbol} ({token_address})")

                try:
                    pairs = get_pair_addresses(token_address)
                    if not pairs:
                        print(f"⚠️ No LP pairs for {symbol}")
                        continue

                    pair = pairs[0]

                    raw = fetch_ohlcv(
                        pair,
                        from_date.strftime("%Y-%m-%d"),
                        to_date.strftime("%Y-%m-%d")
                    )
                    df = pd.DataFrame(raw)

                    if df.empty:
                        print(f"⚠️ No OHLCV data for {symbol}")
                        continue

                    process_and_save(df, symbol, OUTPUT_DIR, file_label)
                    success_count += 1

                except Exception as e:
                    print(f"❌ Error fetching {symbol}: {e}")

            if success_count < top_n:
                print(f"⚠️ Only saved {success_count}/{top_n} tokens for {file_label}")

        except Exception as e:
            print(f"❌ Failed to process file {file}: {e}")
            

if __name__ == "__main__":
    fetch_and_save_daily_ohlcv("feb25", top_n=5)
