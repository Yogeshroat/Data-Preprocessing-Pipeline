#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

# Ensure imports from scripts package
sys.path.insert(0, str(Path(__file__).parent))
from initial_cleanup.initial_cleanup import load_and_clean_data


def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    data_csv = project_root / "data" / "cmo_videos_names.csv"
    outputs = project_root / "outputs"
    outputs.mkdir(parents=True, exist_ok=True)

    if not data_csv.exists():
        print(f"❌ Missing input: {data_csv}")
        return 1

    df = load_and_clean_data(data_csv)

    # Preserve original input order for downstream steps
    if 'Original Order' not in df.columns:
        df.insert(0, 'Original Order', range(len(df)))

    out_csv = outputs / "final_cleaned_data.csv"
    df.to_csv(out_csv, index=False)
    print(f"✓ Wrote {out_csv} with {len(df)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
