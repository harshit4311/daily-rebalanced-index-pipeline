'''fetch all pools in a given month, day-by-day using this batch-fetch script '''

import os
from datetime import datetime, timedelta
import importlib.util

# Load fetch_pools.py as a module
fetch_pools_path = os.path.join(os.path.dirname(__file__), "fetch_pools.py")
spec = importlib.util.spec_from_file_location("fetch_pools", fetch_pools_path)
fetch_pools = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_pools)


def get_label_and_timestamps(day, month, year):
    date = datetime(year, month, day)
    next_day = date + timedelta(days=1)
    label = f"{day}_{date.strftime('%b').lower()}{str(year)[2:]}"
    return label, int(date.timestamp()), int(next_day.timestamp())


def run_batch(start_day, end_day, month, year, save_subdir):
    for day in range(start_day, end_day + 1):
        label, start_ts, end_ts = get_label_and_timestamps(day, month, year)
        print(f"\n📆 Running fetch_pools for {label}...")

        try:
            fetch_pools.run_fetch_pools(
                label=label,
                start_ts=start_ts,
                end_ts=end_ts,
                save_dir=f"data/raw/{save_subdir}"
            )
        except Exception as e:
            print(f"❌ Error on {label}: {e}")


if __name__ == "__main__":
    # Edit this section to change the batch month/year
    run_batch(
        start_day=1,
        end_day=31,
        month=6,
        year=2025,
        save_subdir="jun25"
    )
