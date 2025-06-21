import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime

# --- Moralis API ---
MORALIS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjM0MDlmY2YyLWM5Y2ItNDcxYy04MDQ1LTY2ZmQ5MjdmMTc5MyIsIm9yZ0lkIjoiNDQ2NDI2IiwidXNlcklkIjoiNDU5MzEwIiwidHlwZUlkIjoiNjNmZjY2MDUtNTRhYS00NTMyLWE5NWMtOTMwNTIyMjMxNzRiIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDY5NDM5MzUsImV4cCI6NDkwMjcwMzkzNX0._LVE0RJNvv7vKwmbSmQ4U1NSvTStVaAeZB_qSC6_roY"
HEADERS = {"X-API-Key": MORALIS_API_KEY}


def fetch_ohlcv(pair_address, from_date, to_date, limit=1000):
    url = f"https://deep-index.moralis.io/api/v2.2/pairs/{pair_address}/ohlcv"
    params = {
        "chain": "eth",
        "timeframe": "1d",
        "currency": "usd",
        "fromDate": from_date,
        "toDate": to_date,
        "limit": limit  # Use the function parameter here
    }

    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed: {response.status_code} - {response.text}")
    return response.json().get("result", [])


BASE_DATAFRAMES_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/automated-memetoken-index-pipeline/dataframes"

def process_and_save(df, symbol, month):
    if df.empty:
        print(f"No data found for {symbol}")
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
    
    output_dir = os.path.join(os.path.dirname(__file__), "..", "dataframes", month)
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{symbol}.csv")
    df.to_csv(out_path)
    print(f"✅ Saved {symbol} OHLCV to {out_path}")

def main():
    symbol = input("Token Symbol (eg. PEPE): ").strip().upper()
    pair_address = input("Pair Address (Uniswap/Dexscreener): ").strip()
    from_date = input("From Date (YYYY-MM-DD): ").strip()
    to_date = input("To Date (YYYY-MM-DD): ").strip()
    month = input("Month label for output (e.g., 2024-06): ").strip()

    print(f"\nFetching OHLCV for {symbol} from {from_date} to {to_date}...")

    try:
        data = fetch_ohlcv(pair_address, from_date, to_date, 1000)
        df = pd.DataFrame(data)
        
        if df.empty:
            print(f"⚠️ No data found for {symbol}. Skipping.")
            return

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        inception_date = df['timestamp'].min()
        target_year, target_month = map(int, month.split('-'))

        if inception_date.year == target_year and inception_date.month == target_month:
            process_and_save(df, symbol, month)
        else:
            print(f"⚠️ Skipping {symbol}: inception date is {inception_date.date()}, not in {month}")
    
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
