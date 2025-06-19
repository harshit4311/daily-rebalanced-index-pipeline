import requests
import time
from collections import defaultdict, OrderedDict
import yaml
import os
import json

# Load settings
config_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

API_KEY = config["graph"]["api_key"]
SUBGRAPH_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{config['graph']['subgraph_id']}"

EXCLUDE_SYMBOLS = {
    "USDC", "USDT", "DAI", "FRAX", "WETH", "WBTC", "ETH", "STETH", "CBETH", "RETH",
    "AAVE", "CRV", "UNI", "LINK", "MKR", "COMP", "LDO", "1INCH", "SNX", "GRT", "FXS"
}


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


def fetch_pools_created(start_ts, end_ts):
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
              createdAtTimestamp_gte: {start_ts},
              createdAtTimestamp_lt: {end_ts}
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


def extract_tokens(pools):
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


def fetch_volumes(pool_ids, start_ts, end_ts):
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
              date_gte: {start_ts},
              date_lt: {end_ts}
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

        print(f"Processed batch {i} to {i + batch_size}, records: {len(day_data)}")
        time.sleep(0.3)

    return pool_volumes


def run_fetch_pools(label, start_ts, end_ts, save_dir="data/raw"):
    print(f"🔍 Fetching Uniswap V3 pools for {label}...")
    pools = fetch_pools_created(start_ts, end_ts)
    print(f"✅ Total pools fetched: {len(pools)}")

    tokens = extract_tokens(pools)
    print(f"✅ Unique tokens extracted: {len(tokens)}")

    pool_ids = [p["id"] for p in pools]
    volumes = fetch_volumes(pool_ids, start_ts, end_ts)
    print(f"✅ Volume stats for {len(volumes)} tokens")

    sorted_tokens = sorted(volumes.items(), key=lambda x: x[1], reverse=True)
    filtered = [
        (token_id, vol) for token_id, vol in sorted_tokens
        if token_id in tokens and tokens[token_id]["symbol"].upper() not in EXCLUDE_SYMBOLS
    ]

    sorted_tokens_dict = OrderedDict()
    for token_id, _ in filtered:
        sorted_tokens_dict[token_id] = tokens[token_id]

    full_save_path = os.path.join(os.path.dirname(__file__), "..", save_dir)
    os.makedirs(full_save_path, exist_ok=True)
    out_path = os.path.join(full_save_path, f"{label}.json")

    with open(out_path, "w") as f:
        json.dump(sorted_tokens_dict, f, indent=2)

    print(f"\n💾 Saved {len(sorted_tokens_dict)} tokens → {out_path}")
    print(f"\n📊 Top 10 tokens for {label}:")
    for token_id, vol in filtered[:10]:
        print(f"{tokens[token_id]['symbol']} ({token_id}): ${vol:,.2f}")


# Optional direct test mode using old config values (for standalone testing)
if __name__ == "__main__":
    START_TS = config["fetch_pools"]["start_timestamp"]
    END_TS = config["fetch_pools"]["end_timestamp"]
    LABEL = config["fetch_pools"]["label"]
    run_fetch_pools(label=LABEL, start_ts=START_TS, end_ts=END_TS, save_dir="data/raw/jan25")
