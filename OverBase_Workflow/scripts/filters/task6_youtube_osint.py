from pathlib import Path
import pandas as pd
import re
import json
import time
from datetime import datetime
import requests

script_dir = Path(__file__).parent
scripts_dir = script_dir.parent
PROJECT_ROOT = scripts_dir.parent.resolve()
OUTPUT_DIR = PROJECT_ROOT / "outputs"
LOGS_DIR = OUTPUT_DIR / "logs"
for p in [OUTPUT_DIR, LOGS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

REQUEST_TIMEOUT = 5
VERBOSE = True
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

SENIOR_ALLOW = (
    "chief", "cmo", "cto", "cfo", "ceo", "vp", "svp", "evp", "founder", "co-founder", "cofounder"
)
SENIOR_DROP = ("manager", "director", "analyst", "consultant")


def _norm_tokens(s: str):
    s = str(s or "").lower()
    return re.findall(r"[a-z0-9]+", s)


def _is_senior_title(title: str) -> bool:
    t = str(title or "").lower()
    if any(k in t for k in SENIOR_DROP):
        return False
    return any(k in t for k in SENIOR_ALLOW)


def _http_get(url: str):
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200 and resp.text:
            return resp
    except Exception:
        return None
    return None


def _parse_youtube(html: str):
    title = None
    desc = None
    published = None
    m = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html, re.I)
    if m:
        title = m.group(1)
    m = re.search(r'<meta[^>]+name="description"[^>]+content="([^"]+?)"', html, re.I)
    if m:
        desc = m.group(1)
    m = re.search(r'<meta[^>]+itemprop="datePublished"[^>]+content="([^"]+)"', html, re.I)
    if m:
        published = m.group(1)
    for m in re.finditer(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.I | re.S):
        try:
            jtxt = m.group(1).strip()
            j = json.loads(jtxt)
            items = j if isinstance(j, list) else [j]
            for item in items:
                if isinstance(item, dict) and item.get("@type") == "VideoObject":
                    if not desc:
                        desc = item.get("description") or desc
                    if not title:
                        title = item.get("name") or title
                    if not published:
                        published = item.get("uploadDate") or published
        except Exception:
            continue
    return title, desc, published


def _score_osint(name: str, title: str, company: str, page_title: str, description: str):
    name_tokens = _norm_tokens(name)
    company_tokens = _norm_tokens(company)
    title_tokens = _norm_tokens(title)
    def has(tokens, text):
        text_tokens = set(_norm_tokens(text))
        return all(t in text_tokens for t in tokens)
    def any_has(tokens, text):
        text_tokens = set(_norm_tokens(text))
        return any(t in text_tokens for t in tokens)
    def note(s):
        return s
    score = 0
    ev = ""
    if description:
        if has(name_tokens, description) and has(company_tokens, description) and (any_has(("chief","cmo","cto","cfo","ceo","vp","svp","evp","founder"), description) or any_has(title_tokens, description)) and re.search(r"\bat\b", description.lower()):
            return 90, note("Description explicit: name + title + at + company")
        if has(name_tokens, description) and (has(company_tokens, description) or any_has(title_tokens, description)):
            score = max(score, 80); ev = "Description: name + (company/title)"
        elif has(company_tokens, description) and any_has(("webinar","event","summit","conference","keynote"), description):
            score = max(score, 70); ev = "Branded event: company mentioned"
        elif any_has(("panel","discussion"), description) and has(company_tokens, description):
            score = max(score, 60); ev = "Panel mention with company"
    if page_title and score < 80:
        if has(name_tokens, page_title) and has(company_tokens, page_title):
            score = max(score, 80); ev = ev or "Title: name + company"
        elif has(company_tokens, page_title) and any_has(("webinar","event","summit","conference"), page_title) and _is_senior_title(title):
            score = max(score, 70); ev = ev or "Title: company-branded event"
    if score == 0:
        score = 30; ev = "Insufficient evidence"
    return score, ev


def task6_youtube_osint(df: pd.DataFrame) -> pd.DataFrame:
    df_out = df.copy()
    if "Youtube URL" not in df_out.columns:
        src = PROJECT_ROOT / "data" / "cmo_videos_names.csv"
        if src.exists():
            base = pd.read_csv(src)
            cols = [c for c in ["Name","Company","Youtube URL"] if c in base.columns]
            base = base[cols]
            df_out = df_out.merge(base, on=[c for c in ["Name","Company"] if c in df_out.columns and c in base.columns], how="left")
    if VERBOSE:
        print("OSINT: starting YouTube confidence scoring")
    if "Employment Verified" not in df_out.columns:
        df_out["Employment Verified"] = ""
    df_out["OSINT Verification Source"] = ""
    df_out["OSINT Evidence"] = ""
    df_out["OSINT Confidence"] = 0
    df_out["OSINT Video Published"] = ""
    total = len(df_out)
    for idx, row in df_out.iterrows():
        name = str(row.get("Name",""))
        title = str(row.get("Title",""))
        company = str(row.get("Company",""))
        verified = str(row.get("Employment Verified",""))
        yt = str(row.get("Youtube URL",""))
        if VERBOSE:
            print(f"OSINT [{idx+1}/{total}] {name or '(no name)'} | verified={verified}")
        if verified.strip().lower() == "yes":
            df_out.at[idx, "OSINT Confidence"] = 100
            df_out.at[idx, "OSINT Verification Source"] = "Website"
            df_out.at[idx, "OSINT Evidence"] = "Already website-verified"
            continue
        if not _is_senior_title(title):
            continue
        if not yt or not yt.startswith("http"):
            continue
        resp = _http_get(yt)
        if not resp:
            continue
        page_title, description, published = _parse_youtube(resp.text)
        score, ev = _score_osint(name, title, company, page_title or "", description or "")
        df_out.at[idx, "OSINT Verification Source"] = "YouTube"
        df_out.at[idx, "OSINT Evidence"] = ev
        df_out.at[idx, "OSINT Confidence"] = score
        if published:
            try:
                dt = datetime.fromisoformat(published.replace("Z",""))
                df_out.at[idx, "OSINT Video Published"] = dt.date().isoformat()
            except Exception:
                df_out.at[idx, "OSINT Video Published"] = published
        time.sleep(0.2)
    scored_csv = OUTPUT_DIR / "step6_osint_scored.csv"
    df_out.to_csv(scored_csv, index=False)
    elig = df_out[(df_out["Employment Verified"].astype(str).str.lower() != "yes") & (df_out["OSINT Confidence"] >= 60) & (df_out["OSINT Verification Source"] == "YouTube")]
    elig = elig.copy()
    def _sort_key(row):
        conf = int(row["OSINT Confidence"]) if pd.notna(row["OSINT Confidence"]) else 0
        date = row.get("OSINT Video Published", "") or ""
        return (-conf, date)
    elig = elig.sort_values(by=["OSINT Confidence","OSINT Video Published"], ascending=[False, False])
    seen_comp = set()
    picks = []
    for _, r in elig.iterrows():
        comp = str(r.get("Company"," ")).strip().lower()
        if comp in seen_comp:
            continue
        seen_comp.add(comp)
        picks.append(r)
        if len(picks) >= 15:
            break
    top15 = pd.DataFrame(picks)
    top15_csv = OUTPUT_DIR / "step6_osint_top15.csv"
    top15.to_csv(top15_csv, index=False)
    if VERBOSE:
        print(f"OSINT: wrote {scored_csv} and {top15_csv}")
    return df_out


if __name__ == "__main__":
    path = OUTPUT_DIR / "senior_execs_with_emails.csv"
    if path.exists():
        df0 = pd.read_csv(path)
        task6_youtube_osint(df0)
        print("Done")
    else:
        print("Missing outputs/senior_execs_with_emails.csv")
