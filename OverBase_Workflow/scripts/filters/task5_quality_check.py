#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import re

# ----------------------------------------------------------------------------
# Project paths
# ----------------------------------------------------------------------------
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent
PROJECT_ROOT = scripts_dir.parent.resolve()
OUTPUT_DIR = PROJECT_ROOT / "outputs"
LOGS_DIR = OUTPUT_DIR / "logs"
for p in [OUTPUT_DIR, LOGS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "workflow.log"


def log(message: str):
    with open(LOG_FILE, "a") as f:
        f.write(f"[task5_quality_check] {message}\n")


# ----------------------------------------------------------------------------
# TASK 5: QUALITY CHECK & FINAL OUTPUT
# ----------------------------------------------------------------------------
def task5_quality_check(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 70)
    print("▶ Task 5: Quality Check & Final Output")
    print("=" * 70)

    print(f"Loaded {len(df)} executives for quality check")

    quality_issues = []
    TARGET_COUNT = 50

    required_columns = [
        'Name', 'Title', 'Company', 'Company Website',
        'Source', 'Candidate Email 1', 'Candidate Email 2'
    ]
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        quality_issues.append(f"Missing columns: {', '.join(missing_columns)}")

    df_quality = df.copy()

    # Ensure verification columns exist
    for col in [
        'Employment Verified', 'Verification Source',
        'Verified At', 'LinkedIn Search URL',
        'Domain Notes', 'Confidence'
    ]:
        if col not in df_quality.columns:
            df_quality[col] = ''

    # ------------------------------------------------------------------------
    # Quality scoring
    # ------------------------------------------------------------------------
    senior_title_pattern = (
        r'Chief|CEO|COO|CMO|CFO|CTO|CRO|CPO|'
        r'President|SVP|EVP|Managing Director|Founder'
    )

    df_quality['_quality_score'] = (
        (df_quality['Source'].str.contains('https', na=False)).astype(int) * 2 +
        (df_quality['Company Website'].str.contains('https', na=False)).astype(int) * 2 +
        (df_quality['Title'].str.contains(senior_title_pattern, case=False, na=False)).astype(int) +
        (df_quality['Employment Verified'].str.lower().eq('yes')).astype(int) * 3 +
        (
            (df_quality['Candidate Email 1'].fillna('').str.strip() != '') &
            (df_quality['Candidate Email 2'].fillna('').str.strip() != '')
        ).astype(int) * 2
    )

    # Normalized helpers
    ev_yes = df_quality['Employment Verified'].fillna('').str.lower().eq('yes')
    email1 = df_quality['Candidate Email 1'].fillna('').str.strip() != ''
    email2 = df_quality['Candidate Email 2'].fillna('').str.strip() != ''
    website = df_quality['Company Website'].fillna('').str.strip() != ''

    df_sorted = df_quality.sort_values('_quality_score', ascending=False)

    selected_idxs = []
    tiers = []

    def pick(mask, label):
        nonlocal selected_idxs, tiers
        rows = df_sorted[mask & ~df_sorted.index.isin(selected_idxs)]
        for idx in rows.index:
            if len(selected_idxs) >= TARGET_COUNT:
                break
            selected_idxs.append(idx)
            tiers.append((idx, label))

    # Tiered selection
    pick(ev_yes & email1 & email2 & website, 'strict')
    pick(ev_yes & (email1 | email2) & website, 'A')
    pick(ev_yes & (email1 | email2), 'B')
    pick(~ev_yes & email1 & email2 & website, 'C')
    pick(email1 | email2, 'fallback')

    df_selected = df_sorted.loc[selected_idxs].copy()

    if len(df_selected) < TARGET_COUNT:
        quality_issues.append(
            f"Only {len(df_selected)} executives available (target {TARGET_COUNT})"
        )

    # Attach tier
    tier_map = {i: t for i, t in tiers}
    df_selected['Quality Tier'] = df_selected.index.map(tier_map)

    # Tier ordering (for internal use)
    tier_order = {'strict': 0, 'A': 1, 'B': 2, 'C': 3, 'fallback': 4}
    df_selected['_tier_rank'] = df_selected['Quality Tier'].map(tier_order)

    # ------------------------------------------------------------------------
    # Email format validation
    # ------------------------------------------------------------------------
    email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    email_format_issues = 0

    for _, r in df_quality.iterrows():
        for col in ['Candidate Email 1', 'Candidate Email 2']:
            e = str(r.get(col, '')).strip()
            if e and not re.match(email_regex, e):
                email_format_issues += 1

    if email_format_issues:
        quality_issues.append(f"{email_format_issues} invalid email formats detected")

    # ------------------------------------------------------------------------
    # Domain consistency check (safe version)
    # ------------------------------------------------------------------------
    domain_issues = 0
    for _, r in df_quality.iterrows():
        site = str(r.get('Company Website', ''))
        email = str(r.get('Candidate Email 1', ''))

        if site and email and '@' in email:
            site_domain = (
                site.replace('https://', '')
                    .replace('http://', '')
                    .replace('www.', '')
                    .split('/')[0]
            )
            email_domain = email.split('@')[1]
            if site_domain and not email_domain.endswith(site_domain):
                domain_issues += 1

    if domain_issues:
        quality_issues.append(f"{domain_issues} potential domain mismatches")

    # ------------------------------------------------------------------------
    # Final output
    # ------------------------------------------------------------------------
    final_columns = [
        'Name', 'Title', 'Company', 'Company Website',
        'Verification Source', 'Employment Verified', 'Verified At',
        'LinkedIn Search URL',
        'Candidate Email 1', 'Candidate Email 2',
        'Quality Tier', 'Source', 'Domain Notes', 'Confidence'
    ]

    for c in final_columns:
        if c not in df_selected.columns:
            df_selected[c] = ''

    # Preserve original input order if available
    if 'Original Order' in df_selected.columns:
        df_final = (
            df_selected
            .sort_values('Original Order', kind='stable')
            .drop(columns=['_quality_score', '_tier_rank'], errors='ignore')
            .reset_index(drop=True)
        )
    else:
        # Fallback: keep the current selection order
        df_final = (
            df_selected
            .drop(columns=['_quality_score', '_tier_rank'], errors='ignore')
            .reset_index(drop=True)
        )

    OUTPUT_CSV = OUTPUT_DIR / "final_executives_list.csv"
    FINAL_ALIAS = OUTPUT_DIR / "final.csv"
    QUALITY_REPORT = OUTPUT_DIR / "quality_report.txt"

    df_final.to_csv(OUTPUT_CSV, index=False)
    df_final.to_csv(FINAL_ALIAS, index=False)

    with open(QUALITY_REPORT, "w") as f:
        f.write("QUALITY CHECK REPORT\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Final executives: {len(df_final)} / {TARGET_COUNT}\n\n")
        f.write("Tier breakdown:\n")
        for t in ['strict', 'A', 'B', 'C', 'fallback']:
            f.write(f"  • {t}: {(df_final['Quality Tier'] == t).sum()}\n")
        f.write("\nIssues:\n")
        if quality_issues:
            for i in quality_issues:
                f.write(f"  • {i}\n")
        else:
            f.write("  • None\n")

    log(f"Final list generated with {len(df_final)} rows")
    print("✔ Task 5 completed")
    print(f"Final list saved to: {OUTPUT_CSV}")
    print(f"Quality report saved to: {QUALITY_REPORT}")

    return df_final
