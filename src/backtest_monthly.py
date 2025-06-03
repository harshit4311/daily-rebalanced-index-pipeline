import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
import sys




# === CONFIGURATION ===
BASE_PATH = "/Users/harshit/Downloads/Research-Commons-Quant/memetoken-index-RC5/dataframes/"
LOOKAHEAD_START, LOOKAHEAD_END = 0, 29
NO_LOOKAHEAD_START, NO_LOOKAHEAD_END = 30, 59
if len(sys.argv) > 1:
    MONTH = sys.argv[1]
else:
    MONTH = "oct_23"  
MONTH = "oct23" 

# === LOAD DATA ===
files = glob.glob(f"{BASE_PATH}/{MONTH}/*.csv")
price_data = {}
volume_data = {}

print("🔍 Loading files...")
for file in files:
    token = os.path.basename(file).replace(".csv", "")
    try:
        df = pd.read_csv(file)
        df.columns = df.columns.str.lower()
        if 'close' not in df.columns or 'volume' not in df.columns:
            print(f"⚠️ Skipping {token}: missing 'close' or 'volume'")
            continue
        df = df.sort_values(by='timestamp').reset_index(drop=True)
        df['day'] = df.index
        price_data[token] = df.set_index('day')['close']
        volume_data[token] = df.set_index('day')['volume']
    except Exception as e:
        print(f"❌ Error loading {token}: {e}")

print(f"📦 Loaded {len(price_data)} tokens.")

# === TOTAL VOLUME RANKING ===
volume_df = pd.DataFrame(volume_data)
if volume_df.empty:
    print("❌ No volume data available. Check folder path or CSV format.")
    exit()

total_volume = volume_df.loc[LOOKAHEAD_START:LOOKAHEAD_END].sum()
top_tokens = total_volume.sort_values(ascending=False).head(10).index.tolist()
print(f"📊 Top tokens by volume in {MONTH}: {top_tokens}")

# === FILTER TOKENS WITH DATA IN WINDOW ===
def filter_tokens_with_valid_window(tokens, df_dict, start_day, end_day):
    return [
        token for token in tokens
        if token in df_dict and df_dict[token].loc[start_day:end_day].notna().all()
    ]

lookahead_tokens = filter_tokens_with_valid_window(top_tokens, price_data, LOOKAHEAD_START, LOOKAHEAD_END)
nobias_tokens = filter_tokens_with_valid_window(top_tokens, price_data, NO_LOOKAHEAD_START, NO_LOOKAHEAD_END)

print(f"\n🎯 Tokens with valid data for Lookahead Bias (Oct {LOOKAHEAD_START}–{LOOKAHEAD_END}): {lookahead_tokens}")
print(f"🎯 Tokens with valid data for No Lookahead Bias (Nov {NO_LOOKAHEAD_START}–{NO_LOOKAHEAD_END}): {nobias_tokens}")

# === BACKTESTING ===
def backtest(tokens, start_day, end_day):
    prices = pd.DataFrame({t: price_data[t].loc[start_day:end_day] for t in tokens})
    returns = prices.pct_change().dropna()
    weights = np.ones(len(tokens)) / len(tokens)
    daily_returns = returns.dot(weights)
    cum_returns = (1 + daily_returns).cumprod()

    # Metrics
    total_return = cum_returns.iloc[-1] - 1
    ann_return = (cum_returns.iloc[-1])**(365 / len(cum_returns)) - 1
    volatility = daily_returns.std()
    sharpe = daily_returns.mean() / volatility * np.sqrt(365)
    max_dd = (cum_returns / cum_returns.cummax() - 1).min()

    metrics = {
        "Total Return": f"{total_return * 100:.2f}%",
        "Annualized Return": f"{ann_return * 100:.2f}%",
        "Sharpe Ratio": f"{sharpe:.2f}",
        "Max Drawdown": f"{max_dd * 100:.2f}%"
    }

    return cum_returns, daily_returns, metrics

# === RUN BACKTESTS ===
results = {}

if lookahead_tokens:
    cr_look, dr_look, metrics_look = backtest(lookahead_tokens, LOOKAHEAD_START, LOOKAHEAD_END)
    results['Lookahead Bias'] = (cr_look, dr_look, metrics_look)
else:
    print("❌ No tokens with full data for Lookahead Bias window.")

if nobias_tokens:
    cr_nobias, dr_nobias, metrics_nobias = backtest(nobias_tokens, NO_LOOKAHEAD_START, NO_LOOKAHEAD_END)
    results['No Lookahead Bias'] = (cr_nobias, dr_nobias, metrics_nobias)
else:
    print("❌ No tokens with full data for No Lookahead Bias window.")

# === PLOT ===
if results:
    plt.figure(figsize=(10, 5))
    for label, (cum_returns, _, _) in results.items():
        plt.plot(cum_returns.values, label=label)
    plt.title("📈 Portfolio Performance Comparison")
    plt.xlabel("Day")
    plt.ylabel("Cumulative Return")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# === METRICS ===
for label, (_, _, metrics) in results.items():
    print(f"\n📊 {label} Portfolio:")
    for k, v in metrics.items():
        print(f"{k:>20}: {v}")
