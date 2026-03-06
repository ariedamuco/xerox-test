"""
scrape_abtl.py
==============
Scrapes all biographical records from the ÁBTL Archontológia database.
URL pattern: https://www.abtl.hu/ords/archontologia/f?p=108:5:::::P5_PRS_ID:{id}

Strategy:
- The list page shows ~940 records with PRS_IDs as links
- Scrape the index pages to get all PRS_IDs
- Then fetch each individual record

Output (written to data/ sibling directory):
- abtl_raw.json       — all raw records
- abtl_postings.csv   — city x year postings (one row per posting)
- abtl_officers.csv   — officer-level summary
"""

import csv
import json
import re
import time
from collections import Counter
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE = "https://www.abtl.hu/ords/archontologia/f"

_HERE = Path(__file__).parent
DATA_DIR = _HERE.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hu,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


# ---------------------------------------------------------------------------
# HTTP session + APEX session ID
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_apex_session(http: requests.Session) -> str:
    """
    Fetch the index page and extract the APEX session ID from any link
    that uses the f?p=108:N:SESSION:: pattern.
    Raises RuntimeError if the page doesn't contain a session ID.
    """
    r = http.get(f"{BASE}?p=108:4::::4::", timeout=15)
    r.raise_for_status()
    # Session IDs are large integers embedded in every APEX link
    m = re.search(r"f\?p=108:\d+:(\d+)::", r.text)
    if not m:
        raise RuntimeError(
            "Could not extract APEX session ID from index page. "
            "The page may require a different entry point or have changed structure."
        )
    return m.group(1)


def get_region_id(html: str) -> str:
    """
    Extract the APEX interactive-report region ID used for pagination.
    Falls back to the previously observed value if not found.
    """
    m = re.search(r"pg_R_(\d+)", html)
    return m.group(1) if m else "40092188966690279"


# ---------------------------------------------------------------------------
# Index scraping — collect all PRS_IDs
# ---------------------------------------------------------------------------

def get_all_prs_ids(http: requests.Session, apex_session: str) -> list[int]:
    """Scrape all PRS_IDs from the paginated index (15 per page)."""
    prs_ids: list[int] = []
    seen: set[int] = set()
    page = 1
    min_row = 1
    region_id: str | None = None

    while True:
        if min_row == 1:
            url = f"{BASE}?p=108:4:{apex_session}::NO:RP,4:"
        else:
            url = (
                f"{BASE}?p=108:4:{apex_session}"
                f":pg_R_{region_id}:NO"
                f"&pg_min_row={min_row}&pg_max_rows=15&pg_rows_fetched=15"
            )

        try:
            r = http.get(url, timeout=15)
            r.raise_for_status()
        except requests.exceptions.RequestException as exc:
            print(f"  Warning: failed to fetch page {page}: {exc}")
            break

        # Extract region ID from first page response
        if region_id is None:
            region_id = get_region_id(r.text)

        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.find_all("a", href=re.compile(r"P5_PRS_ID:\d+"))

        new_ids = []
        for link in links:
            m = re.search(r"P5_PRS_ID:(\d+)", link["href"])
            if m:
                pid = int(m.group(1))
                if pid not in seen:
                    seen.add(pid)
                    new_ids.append(pid)
                    prs_ids.append(pid)

        print(f"  Page {page}: +{len(new_ids)} new IDs (total: {len(prs_ids)})")

        # Stop when no new IDs appear (real end of list)
        if not new_ids:
            break

        min_row += 15
        page += 1
        time.sleep(0.5)

    return prs_ids


# ---------------------------------------------------------------------------
# Record parsing
# ---------------------------------------------------------------------------

KNOWN_CITIES = [
    "Budapest", "Miskolc", "Debrecen", "Győr", "Pécs", "Szeged", "Kecskemét",
    "Székesfehérvár", "Szolnok", "Veszprém", "Kaposvár", "Eger", "Zalaegerszeg",
    "Szombathely", "Nyíregyháza", "Tatabánya", "Sopron", "Békéscsaba",
    "Salgótarján", "Dunaújváros", "Esztergom", "Hódmezővásárhely",
    "Taszár", "Pápa", "Nagykanizsa", "Szekszárd", "Érd",
]

BUDAPEST_KEYWORDS = [
    "BM III", "BM II", "Belügyminisztérium", "Legfőbb", "Minisztertanács", "MSZMP KB",
]


def extract_city(institution: str) -> str | None:
    if not institution:
        return None
    for city in KNOWN_CITIES:
        if city in institution:
            return city
    if any(kw in institution for kw in BUDAPEST_KEYWORDS):
        return "Budapest"
    m = re.search(r",\s*([A-ZÁÉÍÓÖŐÚÜŰ][a-záéíóöőúüű]+)\s*$", institution)
    if m:
        return m.group(1)
    return None


def parse_years(text: str) -> tuple[int | None, int | None]:
    # "1951-1952" or "1951–1952"
    m = re.search(r"(\d{4})(?:\.\d{2})?\s*[-–]\s*(\d{4})(?:\.\d{2})?", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    # "1930.04.08." → birth date style, single year
    m = re.search(r"(\d{4})\.\d{2}\.\d{2}", text)
    if m:
        return int(m.group(1)), int(m.group(1))
    # plain single year "1985"
    m = re.match(r"^\d{4}$", text.strip())
    if m:
        y = int(m.group(0))
        return y, y
    return None, None


def parse_record(html: str, prs_id: int) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    record: dict = {
        "prs_id": prs_id,
        "name": None,
        "mother": None,
        "birth_date": None,
        "birth_place": None,
        "death_date": None,
        "notes": None,
        "education": [],
        "ranks": [],
        "postings": [],
        "sources": [],
    }

    # Name from <title>: "ÁBTL Archontológia - Ábrahám Lajos életrajzi adatok"
    title = soup.find("title")
    if title:
        name = (
            title.text
            .replace("ÁBTL Archontológia -", "")
            .replace("életrajzi adatok", "")
            .strip(" -")
            .strip()
        )
        record["name"] = name or None

    # Walk every <tr> once, deduplicate by cell-text tuple (tables are nested
    # and the same rows appear in multiple ancestor tables).
    seen: set[tuple[str, ...]] = set()

    for row in soup.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        # Some rows use nested <td> — fall back to all td children
        if not cells:
            cells = row.find_all("td")
        texts = tuple(c.get_text(strip=True) for c in cells)
        if not any(texts) or texts in seen:
            continue
        seen.add(texts)

        n = len(texts)

        if n == 2:
            label, value = texts
            if not value:
                continue
            if "Anyja neve" in label:
                record["mother"] = value
            elif "Született" in label:
                parts = value.split(",", 1)
                record["birth_date"] = parts[0].strip()
                if len(parts) > 1:
                    record["birth_place"] = parts[1].strip()
            elif "Meghalt" in label:
                record["death_date"] = value
            elif "Megjegyzés" in label:
                record["notes"] = value
            elif "Forrás" in label:
                record["sources"].append(value)
            elif re.match(r"^\d{4}$", value) and label and not label.endswith(":"):
                # Education row: [course_name, year]
                record["education"].append({"name": label, "year": int(value)})

        elif n == 4:
            # Rank row: [rank_name, year, '', 'N/A']
            rank, year_str, _, _ = texts
            if rank and re.match(r"^\d{4}$", year_str):
                record["ranks"].append({"rank": rank, "year": int(year_str)})

        elif n == 3:
            # Posting row: [institution, years, role]
            institution, years, role = texts
            if not institution or "Forrás" in institution:
                continue
            y_start, y_end = parse_years(years)
            if y_start:
                record["postings"].append({
                    "institution": institution,
                    "city": extract_city(institution),
                    "year_start": y_start,
                    "year_end": y_end,
                    "role": role if role != "N/A" else None,
                    "raw_years": years,
                })

    return record


# ---------------------------------------------------------------------------
# Per-record fetch
# ---------------------------------------------------------------------------

def scrape_record(http: requests.Session, prs_id: int) -> dict | None:
    url = f"{BASE}?p=108:5:::::P5_PRS_ID:{prs_id}"
    try:
        r = http.get(url, timeout=15)
        r.raise_for_status()
        return parse_record(r.text, prs_id)
    except requests.exceptions.RequestException as exc:
        print(f"  Error fetching PRS_ID {prs_id}: {exc}")
        return None


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Saved {len(rows)} rows → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape ÁBTL Archontológia")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit number of records (for testing)",
    )
    parser.add_argument(
        "--output-dir", default=str(DATA_DIR),
        help=f"Output directory (default: {DATA_DIR})",
    )
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    http = make_session()

    # Step 1: get all PRS_IDs (cached after first run)
    ids_path = out / "abtl_prs_ids.json"
    if ids_path.exists():
        with open(ids_path, encoding="utf-8") as f:
            prs_ids = json.load(f)
        print(f"Loaded {len(prs_ids)} cached IDs from {ids_path}")
    else:
        print("Extracting APEX session ID...")
        apex_session = get_apex_session(http)
        print(f"Session: {apex_session}")
        print("Scraping index pages for PRS_IDs...")
        prs_ids = get_all_prs_ids(http, apex_session)
        with open(ids_path, "w", encoding="utf-8") as f:
            json.dump(prs_ids, f)
        print(f"Saved {len(prs_ids)} IDs to {ids_path}")

    if args.limit:
        prs_ids = prs_ids[: args.limit]

    # Step 2: scrape individual records (resume-aware)
    raw_path = out / "abtl_raw.json"
    existing: dict[int, dict] = {}
    if raw_path.exists():
        with open(raw_path, encoding="utf-8") as f:
            for rec in json.load(f):
                existing[rec["prs_id"]] = rec
        print(f"Resuming: {len(existing)} already scraped")

    records: list[dict] = []
    for i, pid in enumerate(prs_ids):
        if pid in existing:
            records.append(existing[pid])
            continue

        print(f"  [{i + 1}/{len(prs_ids)}] PRS_ID {pid}...")
        rec = scrape_record(http, pid)
        if rec:
            records.append(rec)
            existing[pid] = rec

        if (i + 1) % 50 == 0:
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  Checkpoint: {len(records)} records saved")

        time.sleep(0.3)

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(records)} raw records → {raw_path}")

    # Step 3: flatten to CSVs
    officers = []
    postings = []

    for rec in records:
        officers.append({
            "prs_id":      rec["prs_id"],
            "name":        rec.get("name"),
            "birth_date":  rec.get("birth_date"),
            "birth_place": rec.get("birth_place"),
            "death_date":  rec.get("death_date"),
            "mother":      rec.get("mother"),
            "notes":       rec.get("notes"),
            "n_postings":  len(rec.get("postings", [])),
            "n_ranks":     len(rec.get("ranks", [])),
        })
        for p in rec.get("postings", []):
            postings.append({
                "prs_id":      rec["prs_id"],
                "name":        rec.get("name"),
                "institution": p.get("institution"),
                "city":        p.get("city"),
                "year_start":  p.get("year_start"),
                "year_end":    p.get("year_end"),
                "role":        p.get("role"),
            })

    write_csv(officers, out / "abtl_officers.csv")
    write_csv(postings, out / "abtl_postings.csv")

    # Quick city summary for active 1985–1989
    cities_1985 = [
        p["city"] for p in postings
        if p.get("city") and p.get("year_start") and p.get("year_end")
        and p["year_start"] <= 1989 and p["year_end"] >= 1985
    ]
    if cities_1985:
        print(f"\nCities with officers active 1985–1989: {len(set(cities_1985))}")
        print("Top cities:")
        for city, n in Counter(cities_1985).most_common(20):
            print(f"  {n:4d}  {city}")
