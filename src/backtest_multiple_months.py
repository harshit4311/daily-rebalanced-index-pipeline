# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import os
# import glob

# BASE_PATH = "/Users/harshit/Downloads/Research-Commons-Quant/automated-memetoken-index-pipeline/dataframes"

# # Define the monthly folders you want to backtest
# months = sorted([f for f in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, f))])

# print(f"📅 Backtesting months found: {months}")

# def load_month_data(month):
#     files = glob.glob(f"{BASE_PATH}/{month}/*.csv")
#     price_data = {}
#     volume_data = {}

#     for file in files:
#         token = os.path.basename(file).replace(".csv", "")
#         try:
#             df = pd.read_csv(file)
#             df.columns = df.columns.str.lower()
#             if 'close' not in df.columns or 'volume' not in df.columns:
#                 print(f"⚠️ Skipping {token} in {month}: missing 'close' or 'volume'")
#                 continue
#             df = df.sort_values(by='timestamp').reset_index(drop=True)
#             df['day'] = df.index
#             price_data[token] = df.set_index('day')['close']
#             volume_data[token] = df.set_index('day')['volume']
#         except Exception as e:
#             print(f"❌ Error loading {token} in {month}: {e}")

#     return price_data, volume_data

# def backtest(tokens, price_data):
#     # Monthly backtest: full range of days for that month in the data (0 to max)
#     max_day = min([len(price_data[t]) for t in tokens])
#     prices = pd.DataFrame({t: price_data[t].iloc[:max_day] for t in tokens})
#     returns = prices.pct_change().dropna()
#     weights = np.ones(len(tokens)) / len(tokens)
#     daily_returns = returns.dot(weights)
#     cum_returns = (1 + daily_returns).cumprod()

#     # Metrics
#     total_return = cum_returns.iloc[-1] - 1
#     ann_return = (cum_returns.iloc[-1])**(365 / len(cum_returns)) - 1
#     volatility = daily_returns.std()
#     sharpe = daily_returns.mean() / volatility * np.sqrt(365)
#     max_dd = (cum_returns / cum_returns.cummax() - 1).min()

#     metrics = {
#         "Total Return": total_return,
#         "Annualized Return": ann_return,
#         "Sharpe Ratio": sharpe,
#         "Max Drawdown": max_dd
#     }

#     return cum_returns, daily_returns, metrics

# # Store monthly summary for CSV output
# monthly_metrics = []
# # Store monthly cumulative returns to chain together multi-month return curve
# all_daily_returns = []

# for month in months:
#     print(f"\n🔍 Processing month: {month}")
#     price_data, volume_data = load_month_data(month)

#     if not price_data or not volume_data:
#         print(f"❌ No valid data for {month}, skipping...")
#         continue

#     volume_df = pd.DataFrame(volume_data)
#     total_volume = volume_df.sum()
#     top_tokens = total_volume.sort_values(ascending=False).head(10).index.tolist()
#     print(f"📊 Top tokens by volume in {month}: {top_tokens}")

#     # Filter tokens with full data
#     tokens = [t for t in top_tokens if len(price_data[t]) > 30]  # arbitrary min length for safety

#     if len(tokens) < 2:
#         print(f"❌ Not enough tokens with full data for {month}, skipping...")
#         continue

#     cum_returns, daily_returns, metrics = backtest(tokens, price_data)
#     print(f"✅ Finished backtest for {month}")
#     print(f"Metrics: Total Return={metrics['Total Return']*100:.2f}%, Sharpe={metrics['Sharpe Ratio']:.2f}")

#     # Append metrics for CSV
#     monthly_metrics.append({
#         "Month": month,
#         **{k: v for k, v in metrics.items()}
#     })

#     # Save daily returns with month prefix to keep track
#     daily_returns.name = month
#     all_daily_returns.append(daily_returns)

# # Combine daily returns from all months assuming monthly rebalancing
# if all_daily_returns:
#     combined_daily_returns = pd.concat(all_daily_returns).reset_index(drop=True)
#     combined_cum_returns = (1 + combined_daily_returns).cumprod()

#     # Save metrics to CSV
#     metrics_df = pd.DataFrame(monthly_metrics)
#     metrics_df.to_csv("monthly_backtest_metrics.csv", index=False)
#     print("\n📁 Monthly metrics saved to monthly_backtest_metrics.csv")

#     # Plot multi-month cumulative returns
#     plt.figure(figsize=(12, 6))
#     plt.plot(combined_cum_returns, label="Rebalanced Portfolio")
#     plt.title("Portfolio Performance Over Multiple Months (Monthly Rebalanced)")
#     plt.xlabel("Days")
#     plt.ylabel("Cumulative Return")
#     plt.grid(True)
#     plt.legend()
#     plt.tight_layout()
#     plt.show()
# else:
#     print("❌ No monthly data found for backtesting.")


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob

BASE_PATH = "/Users/harshit/Downloads/Research-Commons-Quant/automated-memetoken-index-pipeline/dataframes"

# Define the monthly folders you want to backtest
months = sorted([f for f in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, f))])

print(f"📅 Backtesting months found: {months}")

LOOKAHEAD_START, LOOKAHEAD_END = 0, 29
NO_LOOKAHEAD_START, NO_LOOKAHEAD_END = 30, 59

def load_month_data(month):
    files = glob.glob(f"{BASE_PATH}/{month}/*.csv")
    price_data = {}
    volume_data = {}

    for file in files:
        token = os.path.basename(file).replace(".csv", "")
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.lower()
            if 'close' not in df.columns or 'volume' not in df.columns:
                print(f"⚠️ Skipping {token} in {month}: missing 'close' or 'volume'")
                continue
            df = df.sort_values(by='timestamp').reset_index(drop=True)
            df['day'] = df.index
            price_data[token] = df.set_index('day')['close']
            volume_data[token] = df.set_index('day')['volume']
        except Exception as e:
            print(f"❌ Error loading {token} in {month}: {e}")

    return price_data, volume_data

def filter_tokens_with_valid_window(tokens, price_data, start_day, end_day):
    # Filter tokens that have non-NaN close prices for the full window length
    filtered = [
        t for t in tokens 
        if t in price_data and len(price_data[t]) > end_day and price_data[t].iloc[start_day:end_day+1].notna().all()
    ]
    return filtered

def backtest_window(tokens, price_data, start_day, end_day):
    prices = pd.DataFrame({t: price_data[t].iloc[start_day:end_day+1] for t in tokens})
    returns = prices.pct_change().dropna()
    weights = np.ones(len(tokens)) / len(tokens)
    daily_returns = returns.dot(weights)
    cum_returns = (1 + daily_returns).cumprod()

    total_return = cum_returns.iloc[-1] - 1
    ann_return = (cum_returns.iloc[-1])**(365 / len(cum_returns)) - 1
    volatility = daily_returns.std()
    sharpe = daily_returns.mean() / volatility * np.sqrt(365) if volatility != 0 else 0
    max_dd = (cum_returns / cum_returns.cummax() - 1).min()

    metrics = {
        "Total Return": total_return,
        "Annualized Return": ann_return,
        "Sharpe Ratio": sharpe,
        "Max Drawdown": max_dd
    }

    return cum_returns, daily_returns, metrics

# Store monthly summary for CSV output
monthly_metrics = []
# Store daily returns from all months (separately for lookahead and no-lookahead)
all_daily_returns_lookahead = []
all_daily_returns_nolookahead = []

for month in months:
    print(f"\n🔍 Processing month: {month}")
    price_data, volume_data = load_month_data(month)

    if not price_data or not volume_data:
        print(f"❌ No valid data for {month}, skipping...")
        continue

    volume_df = pd.DataFrame(volume_data)
    total_volume = volume_df.sum()
    top_tokens = total_volume.sort_values(ascending=False).head(10).index.tolist()
    print(f"📊 Top tokens by volume in {month}: {top_tokens}")

    # Filter tokens valid for each window
    lookahead_tokens = filter_tokens_with_valid_window(top_tokens, price_data, LOOKAHEAD_START, LOOKAHEAD_END)
    nolookahead_tokens = filter_tokens_with_valid_window(top_tokens, price_data, NO_LOOKAHEAD_START, NO_LOOKAHEAD_END)

    print(f"🎯 Tokens valid for Lookahead Bias: {lookahead_tokens}")
    print(f"🎯 Tokens valid for No Lookahead Bias: {nolookahead_tokens}")

    # Backtest Lookahead Bias portfolio if possible
    if len(lookahead_tokens) >= 1:
        cum_returns_la, daily_returns_la, metrics_la = backtest_window(
            lookahead_tokens, price_data, LOOKAHEAD_START, LOOKAHEAD_END
        )
        print(f"✅ Lookahead Bias backtest done for {month}")
        print(f"Metrics: Total Return={metrics_la['Total Return']*100:.2f}%, Sharpe={metrics_la['Sharpe Ratio']:.2f}")

        monthly_metrics.append({
            "Month": month,
            "Bias": "Lookahead Bias",
            **metrics_la
        })
        daily_returns_la.name = f"{month}_Lookahead"
        all_daily_returns_lookahead.append(daily_returns_la)
    else:
        print(f"❌ Not enough tokens for Lookahead Bias in {month}, skipping...")

    # Backtest No Lookahead Bias portfolio if possible
    if len(nolookahead_tokens) >= 1:
        cum_returns_nola, daily_returns_nola, metrics_nola = backtest_window(
            nolookahead_tokens, price_data, NO_LOOKAHEAD_START, NO_LOOKAHEAD_END
        )
        print(f"✅ No Lookahead Bias backtest done for {month}")
        print(f"Metrics: Total Return={metrics_nola['Total Return']*100:.2f}%, Sharpe={metrics_nola['Sharpe Ratio']:.2f}")

        monthly_metrics.append({
            "Month": month,
            "Bias": "No Lookahead Bias",
            **metrics_nola
        })
        daily_returns_nola.name = f"{month}_NoLookahead"
        all_daily_returns_nolookahead.append(daily_returns_nola)
    else:
        print(f"❌ Not enough tokens for No Lookahead Bias in {month}, skipping...")

# Combine daily returns from all months assuming monthly rebalancing, separately by bias
if all_daily_returns_lookahead:
    combined_daily_returns_la = pd.concat(all_daily_returns_lookahead).reset_index(drop=True)
    combined_cum_returns_la = (1 + combined_daily_returns_la).cumprod()
else:
    combined_cum_returns_la = None

if all_daily_returns_nolookahead:
    combined_daily_returns_nola = pd.concat(all_daily_returns_nolookahead).reset_index(drop=True)
    combined_cum_returns_nola = (1 + combined_daily_returns_nola).cumprod()
else:
    combined_cum_returns_nola = None

# Save metrics to CSV
if monthly_metrics:
    metrics_df = pd.DataFrame(monthly_metrics)
    metrics_df.to_csv("monthly_backtest_metrics.csv", index=False)
    print("\n📁 Monthly metrics saved to monthly_backtest_metrics.csv")

# Plot multi-month cumulative returns for both portfolios if available
plt.figure(figsize=(12, 6))
if combined_cum_returns_la is not None:
    plt.plot(combined_cum_returns_la, label="Lookahead Bias Portfolio")
if combined_cum_returns_nola is not None:
    plt.plot(combined_cum_returns_nola, label="No Lookahead Bias Portfolio")

plt.title("Portfolio Performance Over Multiple Months (Monthly Rebalanced)")
plt.xlabel("Days")
plt.ylabel("Cumulative Return")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

if not monthly_metrics:
    print("❌ No monthly data found for backtesting.")
