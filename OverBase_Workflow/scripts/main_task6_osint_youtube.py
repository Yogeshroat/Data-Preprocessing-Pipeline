#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task6_youtube_osint import task6_youtube_osint as t6


def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    outputs = project_root / "outputs"

    # Prefer richest dataset if present
    candidates = [
        outputs / 'senior_execs_with_emails.csv',
        outputs / 'senior_execs_web_verified.csv',
        outputs / 'senior_execs_validated.csv',
    ]
    src = next((p for p in candidates if p.exists()), None)
    if not src:
        print("❌ Missing input: none of the expected inputs found (senior_execs_with_emails.csv / senior_execs_web_verified.csv / senior_execs_validated.csv).")
        return 1

    df = pd.read_csv(src)
    t6(df)  # writes outputs/step6_osint_scored.csv and outputs/step6_osint_top15.csv

    # Optional: also export non-verified scored list for convenience
    scored = outputs / 'step6_osint_scored.csv'
    if scored.exists():
        sc = pd.read_csv(scored)
        out = sc[sc['Employment Verified'].astype(str).str.lower() != 'yes'].copy()
        out.to_csv(outputs / 'final_osint_scored_nonverified.csv', index=False)

    print("✓ OSINT completed: step6_osint_scored.csv, step6_osint_top15.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
