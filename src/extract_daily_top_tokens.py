import os
import json

# Set paths
RAW_BASE_DIR = "data/raw"  
OUT_DIR = "data/top_tokens_daily"
TOP_N = 50


def extract_top_tokens_from_day(json_path, top_n=50):
    with open(json_path, "r") as f:
        data = json.load(f)

    sorted_tokens = sorted(
        data.items(),
        key=lambda item: item[1].get("volume", 0),
        reverse=True
    )
    top_tokens = {k: v for k, v in sorted_tokens[:top_n]}
    return top_tokens


def run():
    os.makedirs(OUT_DIR, exist_ok=True)

    for month in sorted(os.listdir(RAW_BASE_DIR)):
        month_path = os.path.join(RAW_BASE_DIR, month)
        if not os.path.isdir(month_path):
            continue

        print(f"\n📅 Month: {month}")
        for filename in sorted(os.listdir(month_path)):
            if not filename.endswith(".json"):
                continue

            day_label = filename.replace(".json", "")
            input_path = os.path.join(month_path, filename)
            top_tokens = extract_top_tokens_from_day(input_path, TOP_N)

            output_path = os.path.join(OUT_DIR, f"{day_label}_top_tokens.json")
            with open(output_path, "w") as f:
                json.dump(top_tokens, f, indent=2)

            print(f"✅ {day_label}: saved top {TOP_N} tokens → {output_path}")


if __name__ == "__main__":
    run()
