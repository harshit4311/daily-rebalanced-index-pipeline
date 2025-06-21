import os
import pandas as pd
import numpy as np
import requests
from datetime import datetime

# --- Constants ---
MORALIS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjM0MDlmY2YyLWM5Y2ItNDcxYy04MDQ1LTY2ZmQ5MjdmMTc5MyIsIm9yZ0lkIjoiNDQ2NDI2IiwidXNlcklkIjoiNDU5MzEwIiwidHlwZUlkIjoiNjNmZjY2MDUtNTRhYS00NTMyLWE5NWMtOTMwNTIyMjMxNzRiIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDY5NDM5MzUsImV4cCI6NDkwMjcwMzkzNX0._LVE0RJNvv7vKwmbSmQ4U1NSvTStVaAeZB_qSC6_roY"
HEADERS = {"X-API-Key": MORALIS_API_KEY}
DEX_API = "https://api.dexscreener.com/tokens/v1/ethereum"


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
        "timeframe": "10min",
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
