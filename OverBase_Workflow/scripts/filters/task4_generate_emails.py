#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import re
import tldextract
from urllib.parse import urlparse
from unidecode import unidecode

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
        f.write(f"[task4_generate_emails] {message}\n")


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def normalize_name(name: str):
    """Normalize name → first + last (ASCII, no suffixes)."""
    if pd.isna(name) or not name:
        return "", ""

    name_clean = unidecode(str(name)).strip()
    name_clean = re.sub(r'["\'].*?["\']', '', name_clean)
    name_clean = re.sub(r'\b(Jr\.|Sr\.|II|III|IV)\b', '', name_clean, flags=re.I)
    name_clean = re.sub(r'\([^)]*\)', '', name_clean).strip()

    parts = [p for p in name_clean.split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0].lower(), ""

    return parts[0].lower(), parts[-1].lower()


def extract_domain_from_url(url):
    """Extract clean company domain from website URL."""
    if pd.isna(url) or not url:
        return None

    try:
        ext = tldextract.extract(url)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
    except Exception:
        pass

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        return domain or None
    except Exception:
        return None


def generate_email_candidates(first, last, domain):
    """Generate ordered email candidates with pattern labels."""
    if not domain:
        return []

    first = re.sub(r"[^a-z0-9]", "", first or "")
    last = re.sub(r"[^a-z0-9]", "", last or "")

    # Always prioritize the two most common formats
    cands = []
    if first and last:
        cands.extend([
            (f"{first}.{last}@{domain}", "first.last@domain"),
            (f"{first[0]}{last}@{domain}", "flast@domain"),
        ])

    # Add other common variants
    if first and last:
        cands.extend([
            (f"{first}{last}@{domain}", "firstlast@domain"),
            (f"{first[0]}.{last}@{domain}", "f.last@domain"),
            (f"{first}_{last}@{domain}", "first_last@domain"),
        ])
    elif last and not first:
        cands.append((f"{last}@{domain}", "last@domain"))
    elif first and not last:
        cands.append((f"{first}@{domain}", "first@domain"))

    # Deduplicate while preserving order
    seen, unique = set(), []
    for e, p in cands:
        if e not in seen:
            unique.append((e, p))
            seen.add(e)
    return unique


DOMAIN_PREFERRED = {
    "salesforce.com": ["first.last@domain", "flast@domain"],
    "microsoft.com": ["first.last@domain", "flast@domain"],
    "oracle.com": ["first.last@domain", "flast@domain"],
    "ibm.com": ["first.last@domain", "flast@domain"],
    "adobe.com": ["first.last@domain", "flast@domain"],
}


def pick_top_two(candidates, domain):
    """Pick the two most likely emails."""
    if not candidates:
        return None, None, []

    label_map = {p: e for e, p in candidates}
    preferred = DOMAIN_PREFERRED.get(domain.lower(), [])

    chosen = [label_map.get(p) for p in preferred if label_map.get(p)]

    for e, _ in candidates:
        if e not in chosen:
            chosen.append(e)
        if len(chosen) == 2:
            break

    return (
        chosen[0] if len(chosen) > 0 else None,
        chosen[1] if len(chosen) > 1 else None,
        candidates,
    )


# ----------------------------------------------------------------------------
# TASK 4: Generate Email Addresses
# ----------------------------------------------------------------------------
def task4_generate_emails(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 70)
    print("▶ Task 4: Generate Email Addresses")
    print("=" * 70)

    results, pattern_log = [], []

    for idx, row in df.iterrows():
        name = row.get("Name", "")
        website = row.get("Company Website", "")
        domain = extract_domain_from_url(website)

        # Safe fallback only for short company names
        if not domain:
            company = str(row.get("Company", "")).lower()
            company_clean = re.sub(r"[^a-z0-9]", "", company)
            if 3 <= len(company_clean) <= 15:
                domain = f"{company_clean}.com"

        first, last = normalize_name(name)
        candidates = generate_email_candidates(first, last, domain)
        email1, email2, tried = pick_top_two(candidates, domain or "")

        row["Candidate Email 1"] = email1 or ""
        row["Candidate Email 2"] = email2 or ""

        # Confidence scoring
        if domain and email1 and "first.last" in email1:
            row["Email Confidence"] = "high"
        elif domain and email1:
            row["Email Confidence"] = "medium"
        else:
            row["Email Confidence"] = "low"

        if email1 or email2:
            pattern_log.append({
                "Name": name,
                "Domain": domain or "",
                "Email 1": email1 or "",
                "Email 2": email2 or "",
                "Patterns Tried": " | ".join(p for _, p in tried),
            })

        results.append(row)
        print(f"{idx+1}/{len(df)} → {name}: {email1}, {email2}")

    df_out = pd.DataFrame(results)

    OUTPUT_CSV = OUTPUT_DIR / "senior_execs_with_emails.csv"
    STEP_CSV = OUTPUT_DIR / "step4_emails.csv"
    PATTERN_LOG = OUTPUT_DIR / "email_patterns_used.csv"

    df_out.to_csv(OUTPUT_CSV, index=False)
    df_out.to_csv(STEP_CSV, index=False)

    if pattern_log:
        pd.DataFrame(pattern_log).to_csv(PATTERN_LOG, index=False)

    log(f"Generated emails for {len(df_out)} rows")
    print("✔ Task 4 completed")
    return df_out
