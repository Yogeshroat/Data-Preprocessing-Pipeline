#!/usr/bin/env python3

from pathlib import Path
import time
from datetime import datetime
import re
import urllib.parse
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

# Project paths
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent
PROJECT_ROOT = scripts_dir.parent.resolve()
OUTPUT_DIR = PROJECT_ROOT / "outputs"
LOGS_DIR = OUTPUT_DIR / "logs"
for p in [OUTPUT_DIR, LOGS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "workflow.log"

# Scrape mode toggle via env: OVERBASE_SCRAPE_MODE=accurate for deeper scan
SCRAPE_MODE = os.getenv("OVERBASE_SCRAPE_MODE", "normal").lower()
ACCURATE = SCRAPE_MODE in ("accurate", "max", "deep")

REQUEST_TIMEOUT = 10 if ACCURATE else 5
PER_ROW_MAX_SECONDS = 90 if ACCURATE else 30
MAX_MAIN = 12 if ACCURATE else 8
MAX_TOTAL = 20 if ACCURATE else 12
SUBLINKS_PER_PAGE = 10 if ACCURATE else 5
EXTRACT_LINKS_LIMIT = 15 if ACCURATE else 10
VERBOSE_PROGRESS = True


def log(message: str):
    with open(LOG_FILE, "a") as f:
        f.write(f"[task3c_verify_employment_webscrape] {message}\n")


def _clean_domain(url: str) -> str:
    if not isinstance(url, str) or not url:
        return ""
    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url


def _http_get(url: str, timeout=REQUEST_TIMEOUT):
    started = time.time()
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            },
            timeout=timeout,
            allow_redirects=True,
        )
        if resp.status_code == 200 and resp.content:
            if VERBOSE_PROGRESS:
                print(f"    GET {url} -> 200 in {time.time()-started:.1f}s", flush=True)
            return resp
    except Exception:
        pass
    if VERBOSE_PROGRESS:
        print(f"    GET {url} -> failed in {time.time()-started:.1f}s", flush=True)
    return None


def _extract_links(base_url: str, html: str) -> list:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return []

    links = []
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"].strip()
        if not href:
            continue
        # Absolute or relative
        target = urllib.parse.urljoin(base_url, href)
        # Only keep same-domain links
        if urllib.parse.urlparse(target).netloc != urllib.parse.urlparse(base_url).netloc:
            continue
        # Prefer likely team/leadership pages
        if re.search(r"about|team|leadership|people|management|company|executive|board", text):
            links.append(target)
    # De-dup while preserving order
    seen, uniq = set(), []
    for u in links:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
    return uniq[:EXTRACT_LINKS_LIMIT]


def _page_contains_name(html: str, first: str, last: str) -> bool:
    if not html:
        return False
    txt = unidecode(BeautifulSoup(html, "lxml").get_text(" ") or "")
    txt = re.sub(r"\s+", " ", txt).strip().lower()
    first = (first or "").lower()
    last = (last or "").lower()
    if not first and not last:
        return False
    # Full name
    if first and last and f"{first} {last}" in txt:
        return True
    # Last, First
    if first and last and f"{last}, {first}" in txt:
        return True
    # Try looser match if uncommon last name
    if last and len(last) > 4 and txt.count(last) <= 5:
        return True if last in txt else False
    return False


def _split_name(name: str):
    if not isinstance(name, str) or not name.strip():
        return "", ""
    n = unidecode(name)
    n = re.sub(r"[\"'].*?[\"']", "", n)
    n = re.sub(r"\b(Jr\.|Sr\.|II|III|IV)\b", "", n, flags=re.I)
    n = re.sub(r"\([^)]*\)", "", n)
    parts = [p for p in re.split(r"\s+", n.strip()) if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0].lower(), ""
    return parts[0].lower(), parts[-1].lower()


def task3c_verify_employment_webscrape(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 70)
    print("▶ Task 3c: Website Scrape Employment Verification")
    print("=" * 70)

    df_out = df.copy()
    total = len(df_out)

    # Ensure required columns
    for col in ["Employment Verified", "Verification Source", "Verified At", "Company Website"]:
        if col not in df_out.columns:
            df_out[col] = ""

    start = time.time()
    verified_count = 0

    for idx, row in df_out.iterrows():
        row_start = time.time()
        name = str(row.get("Name", "")).strip()
        first, last = _split_name(name)
        base = _clean_domain(str(row.get("Company Website", "")).strip())
        already = str(row.get("Employment Verified", "")).strip().lower() == "yes"

        if VERBOSE_PROGRESS:
            print(f"3c [{idx+1}/{total}] {name or '(no name)'} | base={base or '-'}", flush=True)

        if not (first or last):
            if VERBOSE_PROGRESS:
                print("  - skip: empty name", flush=True)
            continue

        if already:
            if VERBOSE_PROGRESS:
                print("  - skip: already verified", flush=True)
            continue

        if not base:
            if VERBOSE_PROGRESS:
                print("  - skip: no company website", flush=True)
            continue

        # Candidates: a small set of standard leadership/team pages
        candidates = [
            base,
            urllib.parse.urljoin(base, "/about"),
            urllib.parse.urljoin(base, "/about-us"),
            urllib.parse.urljoin(base, "/company"),
            urllib.parse.urljoin(base, "/team"),
            urllib.parse.urljoin(base, "/our-team"),
            urllib.parse.urljoin(base, "/leadership"),
            urllib.parse.urljoin(base, "/leadership-team"),
            urllib.parse.urljoin(base, "/executives"),
            urllib.parse.urljoin(base, "/management"),
            urllib.parse.urljoin(base, "/people"),
        ]
        if ACCURATE:
            candidates.extend([
                urllib.parse.urljoin(base, "/who-we-are"),
                urllib.parse.urljoin(base, "/about/company"),
                urllib.parse.urljoin(base, "/about/leadership"),
                urllib.parse.urljoin(base, "/company/leadership"),
                urllib.parse.urljoin(base, "/executive-team"),
                urllib.parse.urljoin(base, "/management-team"),
            ])
        # Deduplicate
        seen, uniq = set(), []
        for u in candidates:
            if u not in seen:
                uniq.append(u)
                seen.add(u)
        candidates = uniq

        found = False
        scanned = 0

        for url in candidates:
            if time.time() - row_start > PER_ROW_MAX_SECONDS:
                if VERBOSE_PROGRESS:
                    print(f"  - time budget reached ({PER_ROW_MAX_SECONDS}s), stopping", flush=True)
                break
            if scanned >= MAX_MAIN:
                break
            if VERBOSE_PROGRESS:
                print(f"  - scan {scanned+1}: {url}", flush=True)
            resp = _http_get(url)
            scanned += 1
            if not resp:
                continue
            if _page_contains_name(resp.text, first, last):
                df_out.at[idx, "Employment Verified"] = "yes"
                df_out.at[idx, "Verification Source"] = url
                df_out.at[idx, "Verified At"] = datetime.utcnow().date().isoformat()
                verified_count += 1
                found = True
                break
            # If not found, mine the page for likely links and scan a few
            for sub_url in _extract_links(url, resp.text)[:SUBLINKS_PER_PAGE]:
                if time.time() - row_start > PER_ROW_MAX_SECONDS:
                    if VERBOSE_PROGRESS:
                        print(f"  - time budget reached ({PER_ROW_MAX_SECONDS}s), stopping", flush=True)
                    break
                if scanned >= MAX_TOTAL:
                    break
                if VERBOSE_PROGRESS:
                    print(f"    - sub-scan {scanned+1}: {sub_url}", flush=True)
                sub_resp = _http_get(sub_url)
                scanned += 1
                if not sub_resp:
                    continue
                if _page_contains_name(sub_resp.text, first, last):
                    df_out.at[idx, "Employment Verified"] = "yes"
                    df_out.at[idx, "Verification Source"] = sub_url
                    df_out.at[idx, "Verified At"] = datetime.utcnow().date().isoformat()
                    verified_count += 1
                    found = True
                    break
            if found:
                break

        took = time.time() - row_start
        if VERBOSE_PROGRESS:
            print(f"  - done: verified={'yes' if found else 'no'}, scanned={scanned}, took={took:.1f}s", flush=True)

        # polite delay
        time.sleep(0.3)

    step_csv = OUTPUT_DIR / "step3c_verified_web.csv"
    df_out.to_csv(step_csv, index=False)
    log(f"Verified via website scraping: +{verified_count} rows; wrote {step_csv}")

    print(f"✓ Web verification completed. Newly verified: {verified_count}")
    return df_out
