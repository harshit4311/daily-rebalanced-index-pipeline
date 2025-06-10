''' fetch all new pools created in a specific month + filter large coins/stablecoins/major coins '''

import requests
import time
from collections import defaultdict
import yaml
import os
import json
from collections import OrderedDict


# Load settings
config_path = os.path.join(os.path.dirname(__file__), "/Users/harshit/Downloads/Research-Commons-Quant/automated-memetoken-index-pipeline/config/", "settings.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

API_KEY = config["graph"]["api_key"]
SUBGRAPH_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{config['graph']['subgraph_id']}"

START_TS = config["fetch_pools"]["start_timestamp"]
END_TS = config["fetch_pools"]["end_timestamp"]
MONTH_LABEL = config["fetch_pools"]["label"]


def run_query(query):
    headers = {"Authorization": f"Bearer {API_KEY}"}
    response = requests.post(SUBGRAPH_URL, json={"query": query}, headers=headers)
    if response.status_code == 200:
        resp_json = response.json()
        if "errors" in resp_json:
            raise Exception(f"GraphQL errors: {resp_json['errors']}")
        return resp_json["data"]
    else:
        raise Exception(f"Query failed with status code {response.status_code}: {response.text}")


def fetch_pools_created_in_month():
    pools = []
    skip = 0
    batch_size = 1000

    while True:
        query = f"""
        {{
          pools(
            first: {batch_size},
            skip: {skip},
            orderBy: createdAtTimestamp,
            orderDirection: asc,
            where: {{
              createdAtTimestamp_gte: {START_TS},
              createdAtTimestamp_lt: {END_TS}
            }}
          ) {{
            id
            createdAtTimestamp
            token0 {{
              id
              symbol
              name
              decimals
            }}
            token1 {{
              id
              symbol
              name
              decimals
            }}
          }}
        }}
        """
        data = run_query(query)
        batch = data["pools"]
        pools.extend(batch)
        print(f"Fetched {len(batch)} pools, total so far: {len(pools)}")
        if len(batch) < batch_size:
            break
        skip += batch_size
        time.sleep(0.25)
    return pools


def extract_unique_tokens_from_pools(pools):
    tokens = {}
    for pool in pools:
        for token_key in ["token0", "token1"]:
            token = pool[token_key]
            tokens[token["id"]] = {
                "symbol": token["symbol"],
                "name": token["name"],
                "decimals": int(token["decimals"]),
                "address": token["id"],
            }
    return tokens


def fetch_pool_day_data(pool_ids):
    pool_volumes = defaultdict(float)
    batch_size = 50

    for i in range(0, len(pool_ids), batch_size):
        batch_ids = pool_ids[i:i + batch_size]
        ids_string = ','.join(f'"{pid}"' for pid in batch_ids)

        query = f"""
        {{
          poolDayDatas(
            where: {{
              pool_in: [{ids_string}],
              date_gte: {START_TS},
              date_lt: {END_TS}
            }},
            first: 1000
          ) {{
            pool {{
              id
              token0 {{
                id
                symbol
              }}
              token1 {{
                id
                symbol
              }}
            }}
            volumeUSD
            date
          }}
        }}
        """
        data = run_query(query)
        day_data = data["poolDayDatas"]

        for record in day_data:
            pool = record["pool"]
            vol = float(record["volumeUSD"])
            pool_volumes[pool["token0"]["id"]] += vol / 2
            pool_volumes[pool["token1"]["id"]] += vol / 2

        print(f"Processed batch {i} to {i+batch_size}, day data: {len(day_data)}")
        time.sleep(0.3)

    return pool_volumes


EXCLUDE_SYMBOLS = {
    # Stablecoins
    "USDC", "USDT", "DAI", "FRAX", "LUSD", "TUSD", "THUSD", "GUSD", "CRVUSD",
    
    # Wrapped tokens
    "WETH", "WBTC", "WSTETH", "RETH", "MATICWETH", "ARBITRUMWETH", "cbBTC", "CBBTC", "USD0", "USD0++", "USD0++ ", "YUSD", 

    # Major blue-chip tokens
    "ETH", "BTC", "WBTC", "AAVE", "CRV", "LINK", "UNI", "COMP", "MKR", "SNX", "BAL",

    # LSD & restaking tokens (not meme-related)
    "STETH", "CBETH", "SWETH", "OSMO", "JUNO", "ATOM", "FXS", "ezETH", "EZETH",

    # Others to exclude
    "MORPHO", "LDO", "ENS", "1INCH", "GRT"
}


def main():
    print(f"🔍 Fetching Uniswap V3 pools created in {MONTH_LABEL}...")
    pools = fetch_pools_created_in_month()
    print(f"✅ Total pools fetched: {len(pools)}")

    tokens = extract_unique_tokens_from_pools(pools)
    print(f"✅ Total unique tokens found: {len(tokens)}")

    pool_ids = [pool["id"] for pool in pools]
    volumes = fetch_pool_day_data(pool_ids)
    print(f"✅ Monthly volumes calculated for {len(volumes)} tokens.")

    # Sort volumes descending (list of tuples (token_id, volume))
    sorted_tokens = sorted(volumes.items(), key=lambda x: x[1], reverse=True)

    # ✅ Filter tokens by symbol
    filtered_sorted_tokens = [
        (token_id, vol)
        for token_id, vol in sorted_tokens
        if token_id in tokens and tokens[token_id]["symbol"].upper() not in EXCLUDE_SYMBOLS
    ]

    # Build an OrderedDict of tokens sorted by volume for saving
    sorted_tokens_dict = OrderedDict()
    for token_id, _ in filtered_sorted_tokens:
        sorted_tokens_dict[token_id] = tokens[token_id]

    # ✅ Save sorted token data to data/raw
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f"{MONTH_LABEL}_fetched_tokens.json")
    with open(output_path, "w") as f:
        json.dump(sorted_tokens_dict, f, indent=2)

    print(f"\n💾 Saved {len(sorted_tokens_dict)} tokens to {output_path}")

    print(f"\n📊 Top 20 tokens by volume in {MONTH_LABEL}:")
    for token_id, vol in filtered_sorted_tokens[:20]:
        symbol = tokens[token_id]["symbol"]
        print(f"{symbol} ({token_id}): ${vol:,.2f}")

if __name__ == "__main__":
    main()
