"""
Scraper for jeltelenul.hu — "Jeltelenül elföldelve"
Hungarian historical archive of political repression victims (1945–1967)
operated by the Historical Archives of the Hungarian State Security Services (ÁBTL).

Scrapes the list of deceased persons and their detailed records, saving output
to a CSV file. Supports resuming interrupted runs via a checkpoint file.

Usage:
    python scrape_jeltelenul.py [--output OUTPUT] [--delay DELAY] [--max-pages N]
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
import unicodedata
import urllib.parse
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://jeltelenul.hu"
LIST_PATH = "/szemelyek"
DEFAULT_OUTPUT = "jeltelenul_data.csv"
CHECKPOINT_FILE = "jeltelenul_checkpoint.json"
DEFAULT_DELAY = 1.5  # seconds between requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": BASE_URL,
}

# CSV column order
COLUMNS = [
    "url",
    "name",
    "name_variants",
    "birth_date",
    "birth_place",
    "residences",
    "occupations",
    "biographical_note",
    "date_of_arrest",
    "charges",
    "location_of_act",
    "classification_of_act",
    "criminal_proceedings",
    "proceedings_link",
    "penal_measures",
    "date_of_death",
    "place_of_death",
    "cause_of_death",
    "burial_place",
    "burial_link",
    "death_registry_number",
    "sources",
    "raw_fields",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scrape_jeltelenul.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch(session: requests.Session, url: str, retries: int = 3) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup object. Returns None on failure."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except requests.exceptions.HTTPError as exc:
            log.warning("HTTP %s for %s (attempt %d/%d)", exc.response.status_code, url, attempt, retries)
            if exc.response.status_code in (403, 404):
                return None  # don't retry client errors
        except requests.exceptions.RequestException as exc:
            log.warning("Request error for %s: %s (attempt %d/%d)", url, exc, attempt, retries)
        if attempt < retries:
            time.sleep(2 ** attempt)
    log.error("Failed to fetch %s after %d attempts", url, retries)
    return None


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def slugify_hu(text: str) -> str:
    """
    Convert a Hungarian name/text to a URL-safe slug.
    e.g. "Ábrahám József" → "abraham-jozsef"
    """
    # NFD decomposition lets us strip combining diacritical marks (accents)
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    lowered = stripped.lower()
    slugged = re.sub(r"[^a-z0-9]+", "-", lowered)
    return slugged.strip("-")


def href_to_url(href: str) -> str:
    """
    Convert an href attribute value to a proper absolute URL.

    Handles three cases observed on jeltelenul.hu:
      1. Already absolute: "https://jeltelenul.hu/rubletzky-geza"
      2. Proper relative path: "/node/731" or "/abonyi-ferenc"
      3. Raw Hungarian name used as href: "Abonyi Ferenc" (no leading slash)
         → slugified to "https://jeltelenul.hu/abonyi-ferenc"
    """
    href = href.strip()
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        # Percent-encode any spaces/special chars in the path while keeping / intact
        return urljoin(BASE_URL, urllib.parse.quote(href, safe="/:?=&#"))
    # No leading slash → treat as a display name, slugify to build the path alias
    return f"{BASE_URL}/{slugify_hu(href)}"


# ---------------------------------------------------------------------------
# Listing page helpers
# ---------------------------------------------------------------------------

def build_list_url(page: int = 0) -> str:
    """Build paginated listing URL (Drupal Views style)."""
    params = urlencode({"order": "title", "sort": "asc", "page": page})
    return f"{BASE_URL}{LIST_PATH}?{params}"


def parse_list_page(soup: BeautifulSoup) -> list[dict]:
    """
    Extract person links from a listing page.

    Drupal Views typically renders rows as <tr> inside a <table> or as
    <div class="views-row"> elements. We handle both patterns.
    Returns a list of dicts: {name, url}.
    """
    persons = []

    # --- Table layout ---
    table = soup.find("table", class_=lambda c: c and "views" in c)
    if not table:
        table = soup.find("table")

    if table:
        for row in table.find_all("tr")[1:]:  # skip header
            cells = row.find_all("td")
            if not cells:
                continue

            first_cell = cells[0]

            # The title cell contains two <a> tags for published persons:
            #   1. <a href="Raw Name"></a>       ← empty text, raw name as href
            #   2. <a href="/slug" hreflang="hu">Name</a>  ← correct one
            # Stub persons (not yet published) have NO link — only plain text
            # like "Motil József [Budapest, 1935]".
            #
            # Always prefer the hreflang link; it has both the proper slug
            # and the display name.
            link_tag = first_cell.find("a", hreflang=True)
            if not link_tag:
                # Fallback for older/different page formats
                link_tag = first_cell.find("a", href=lambda h: h and h.startswith("/"))

            if link_tag:
                href = link_tag["href"]
                name = link_tag.get_text(strip=True)
                full_url = href_to_url(href)
                birth_info = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                persons.append({"name": name, "url": full_url, "birth_info": birth_info})
            else:
                # Stub: no published page exists.
                # Bracket formats seen in the wild:
                #   "Name [City, Year]"   ← most common (comma-separated)
                #   "Name [City Year]"    ← no comma
                #   "Name [nincs adat]"   ← "no data" placeholder
                cell_text = first_cell.get_text(" ", strip=True)
                m = re.match(r'^(.+?)\s*\[([^\]]+)\]\s*$', cell_text)
                if m:
                    name = m.group(1).strip()
                    bracket = m.group(2).strip()
                    year_m = re.search(r'\b(\d{4})\b', bracket)
                    if year_m:
                        birth_year = year_m.group(1)
                        birth_city = bracket[:year_m.start()].rstrip(", ").strip()
                    else:
                        birth_year = ""
                        birth_city = bracket  # e.g. "nincs adat"
                else:
                    name = cell_text
                    birth_city = birth_year = ""
                # Grab criminal proceedings text and link from column 3 (0-indexed)
                proceedings = ""
                proceedings_link = ""
                if len(cells) > 3:
                    proceedings = cells[3].get_text(strip=True)
                    proc_a = cells[3].find("a", href=True)
                    if proc_a:
                        proceedings_link = href_to_url(proc_a["href"])
                persons.append({
                    "name": name,
                    "url": None,
                    "birth_info": birth_year,
                    "birth_city": birth_city,
                    "criminal_proceedings": proceedings,
                    "proceedings_link": proceedings_link,
                    "is_stub": True,
                })

        if persons:
            return persons

    # --- Div-based layout ---
    for row in soup.find_all("div", class_=lambda c: c and "views-row" in c):
        link_tag = row.find("a", href=True)
        if link_tag:
            href = link_tag["href"]
            name = link_tag.get_text(strip=True)
            persons.append({"name": name, "url": href_to_url(href), "birth_info": ""})

    # --- Fallback: any link that points to /node/ or a single-segment path ---
    if not persons:
        for link_tag in soup.find_all("a", href=True):
            href = link_tag["href"]
            if "/node/" in href or (
                href.startswith("/") and href.count("/") == 1 and len(href) > 5
            ):
                name = link_tag.get_text(strip=True)
                if name:
                    persons.append({"name": name, "url": href_to_url(href), "birth_info": ""})

    return persons


def has_next_page(soup: BeautifulSoup) -> bool:
    """Detect whether a 'next page' link exists (Drupal pager)."""
    pager = soup.find(class_=lambda c: c and "pager" in c)
    if pager:
        next_link = pager.find("a", title=lambda t: t and (
            "következő" in t.lower() or "next" in t.lower()
        ))
        if next_link:
            return True
        # Drupal often uses rel="next" on <a> or <link>
        if pager.find("a", rel="next") or soup.find("link", rel="next"):
            return True
        # Check for a li.pager-next that contains an <a>
        next_li = pager.find("li", class_=lambda c: c and "next" in c)
        if next_li and next_li.find("a"):
            return True
    return bool(soup.find("link", rel="next") or soup.find("a", rel="next"))


# ---------------------------------------------------------------------------
# Detail page parser
# ---------------------------------------------------------------------------

def _text(tag) -> str:
    """Return stripped text content, or empty string."""
    return tag.get_text(separator=" ", strip=True) if tag else ""


def _clean_label(raw: str) -> str:
    """
    Normalise a field label extracted from HTML.
    Drupal emits labels as "Születési hely:\xa0" — the non-breaking space
    (\xa0) sits *after* the colon, so plain .strip() / .rstrip(":") leaves
    the colon intact and the FIELD_MAP lookup silently fails.
    """
    return raw.replace("\xa0", " ").strip().rstrip(":").strip()


# Maps every known Hungarian field label variant → CSV column name.
# Defined at module level so it is not rebuilt on every page parse.
FIELD_MAP: dict[str, str] = {
    # name / identity
    "Névváltozat": "name_variants",
    "Névváltozatok": "name_variants",
    "Névváltozat(ok)": "name_variants",
    "Más névváltozat": "name_variants",
    # birth
    "Születési idő": "birth_date",
    "Születési dátum": "birth_date",
    "Született": "birth_date",
    "Születési hely": "birth_place",
    # residence
    "Lakóhely": "residences",
    "Lakóhelyek": "residences",
    "Lakhely": "residences",
    # occupation
    "Foglalkozás": "occupations",
    "Foglalkozások": "occupations",
    "Foglalkozás(ok)": "occupations",
    # biography
    "Életrajzi megjegyzés": "biographical_note",
    "Életrajzi megjegyzések": "biographical_note",
    "Megjegyzés": "biographical_note",
    # arrest
    "Őrizetbevétel ideje": "date_of_arrest",
    "Elfogás ideje": "date_of_arrest",
    "Letartóztatás ideje": "date_of_arrest",
    # charges
    "Terhére rótt cselekmény": "charges",
    "Terhére rótt cselekmény(ek)": "charges",
    "Vád": "charges",
    # location of act
    "A cselekmény helyszíne": "location_of_act",
    "A cselekmény helyszíne(i)": "location_of_act",
    "Cselekmény helyszíne": "location_of_act",
    # classification
    "A cselekmény minősítése": "classification_of_act",
    "A cselekmény minősítése(i)": "classification_of_act",
    "Cselekmény minősítése": "classification_of_act",
    # criminal proceedings
    "Büntetőeljárás": "criminal_proceedings",
    "Büntetőeljárások": "criminal_proceedings",
    "Kapcsolódó büntetőeljárás": "criminal_proceedings",
    # penal measure
    "Büntetőintézkedés": "penal_measures",
    "Büntetőintézkedések": "penal_measures",
    "Büntetés": "penal_measures",
    "Ítélet": "penal_measures",
    # death
    "Elhalálozás ideje": "date_of_death",
    "Halál ideje": "date_of_death",
    "Kivégzés ideje": "date_of_death",
    "Elhunyt": "date_of_death",
    "Elhalálozás helye": "place_of_death",
    "Halál helye": "place_of_death",
    "Kivégzés helye": "place_of_death",
    # cause of death
    "Elhalálozás oka": "cause_of_death",
    "Halál oka": "cause_of_death",
    "Kivégzés módja": "cause_of_death",
    # burial
    "Temetés helye": "burial_place",
    "Temetési/elföldelési helyszín": "burial_place",
    "Temetési helyszín": "burial_place",
    "Elföldelés helye": "burial_place",
    "Temető": "burial_place",
    # criminal trial name
    "A büntetőper megnevezése": "criminal_proceedings",
    # death registry
    "Halotti anyakönyvi bejegyzés száma": "death_registry_number",
    # sources
    "Felhasznált forrás(ok)": "sources",
    "Felhasznált források": "sources",
}


def _collect_raw_fields(soup: BeautifulSoup, url: str) -> dict[str, str]:
    """
    Extract every label→value pair from a detail page.

    The site runs Drupal 9/10 whose field markup uses double-hyphen BEM
    notation (field--name-*, field__label, field__items, field__item),
    quite different from the Drupal 7 single-hyphen/underscore conventions.

    Two structural patterns appear on jeltelenul.hu:

    A) Standard labelled field
       <div class="field field--name-field-szuletesi-ido …">
         <div class="field__label">Születési idő</div>
         <div class="field__item"><time …>1931. 08. 15.</time></div>
       </div>

    B) combined_data wrapper (birth place, residence — label + city + county)
       <div class="combined_data">
         <div class="title">Születési hely:</div>
         <div class="field … field__item">Dorog</div>
         <div class="field … field__item">Komárom-Esztergom vármegye</div>
       </div>
    """
    raw: dict[str, str] = {}

    def add(label: str, value: str) -> None:
        label = _clean_label(label)
        if label and value and label not in raw:
            raw[label] = value

    # Pattern B — combined_data (must run first; its child field divs have
    # field--label-hidden so pattern A won't pick them up anyway, but
    # running B first keeps the combined value intact)
    for combined in soup.find_all("div", class_="combined_data"):
        title = combined.find("div", class_="title")
        if not title:
            continue
        # All direct child divs that carry the field__item class are the values
        child_values = [
            _text(d) for d in combined.find_all("div", recursive=False)
            if "field__item" in (d.get("class") or [])
        ]
        add(title.get_text(strip=True), ", ".join(v for v in child_values if v))

    # Pattern A — standard Drupal 9/10 field divs (field--name-*)
    for field_div in soup.find_all("div", class_=lambda c: c and "field--name-" in c):
        label_tag = field_div.find("div", class_="field__label", recursive=False)
        if not label_tag:
            continue
        items_wrapper = field_div.find("div", class_="field__items", recursive=False)
        if items_wrapper:
            # Multi-value: only direct field__item children to avoid duplicates
            # from nested reference fields (e.g. field--name-field-telepulesnev)
            vals = [_text(d) for d in items_wrapper.find_all("div", recursive=False)
                    if "field__item" in (d.get("class") or [])]
        else:
            # Single-value: one field__item child (may contain <time>, <p>, etc.)
            vals = [_text(d) for d in field_div.find_all("div", recursive=False)
                    if "field__item" in (d.get("class") or [])]
        value = " | ".join(v for v in vals if v)
        add(label_tag.get_text(strip=True), value)

    if not raw:
        log.warning("No labelled fields found on %s — page structure unknown", url)
    else:
        log.debug("Fields on %s: %s", url, list(raw.keys()))

    return raw


def parse_detail_page(soup: BeautifulSoup, url: str, name: str) -> dict:
    record = {col: "" for col in COLUMNS}
    record["url"] = url
    record["name"] = name

    raw = _collect_raw_fields(soup, url)

    for label, value in raw.items():
        col = FIELD_MAP.get(label)
        if col:
            if record[col]:
                record[col] += " | " + value
            else:
                record[col] = value
        else:
            log.debug("Unmapped label %r = %r", label, value[:80])

    unmapped = {k: v for k, v in raw.items() if k not in FIELD_MAP}
    record["raw_fields"] = json.dumps(unmapped, ensure_ascii=False) if unmapped else ""

    # Extract links to criminal proceedings and burial pages
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"]
        link_text = link_tag.get_text(strip=True)
        full_link = href if href.startswith("http") else urljoin(BASE_URL, href)
        if "bunteto" in href.lower() or "eljárás" in link_text.lower():
            record["proceedings_link"] += (full_link + " ")
        if "temeto" in href.lower() or "temetés" in link_text.lower() or "elföldelés" in link_text.lower():
            record["burial_link"] += (full_link + " ")

    # Clean up trailing spaces
    for col in ("proceedings_link", "burial_link"):
        record[col] = record[col].strip()

    # Try to pull name from <h1> if not populated
    if not record["name"]:
        h1 = soup.find("h1")
        record["name"] = _text(h1)

    return record


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint(path: str) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"done_urls": [], "current_page": 0}


def save_checkpoint(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Main scraping loop
# ---------------------------------------------------------------------------

def scrape(
    output: str = DEFAULT_OUTPUT,
    delay: float = DEFAULT_DELAY,
    max_pages: int | None = None,
    resume: bool = True,
) -> None:
    session = make_session()
    checkpoint = load_checkpoint(CHECKPOINT_FILE) if resume else {"done_urls": [], "current_page": 0}
    done_urls: set[str] = set(checkpoint["done_urls"])
    start_page: int = checkpoint["current_page"]

    # Open CSV in append mode if resuming, write mode otherwise
    file_mode = "a" if resume and os.path.exists(output) else "w"
    write_header = file_mode == "w"

    log.info("Output: %s | Mode: %s | Start page: %d | Already done: %d",
             output, file_mode, start_page, len(done_urls))

    with open(output, file_mode, newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=COLUMNS, extrasaction="ignore")
        if write_header:
            writer.writeheader()

        page = start_page
        total_scraped = 0

        while True:
            if max_pages is not None and page >= start_page + max_pages:
                log.info("Reached max_pages limit (%d). Stopping.", max_pages)
                break

            list_url = build_list_url(page)
            log.info("Fetching list page %d: %s", page, list_url)

            soup = fetch(session, list_url)
            if soup is None:
                log.error("Failed to fetch list page %d. Stopping.", page)
                break

            persons = parse_list_page(soup)
            if not persons:
                log.info("No persons found on page %d. End of listing.", page)
                break

            log.info("Found %d persons on page %d.", len(persons), page)

            for person in persons:
                person_url = person["url"]

                # Stub persons have no published page — write what we know
                # from the listing row and move on.
                if person.get("is_stub"):
                    stub_key = f"stub:{slugify_hu(person['name'])}"
                    if stub_key in done_urls:
                        continue
                    record = {col: "" for col in COLUMNS}
                    record["name"] = person["name"]
                    record["birth_date"] = person.get("birth_info", "")
                    record["birth_place"] = person.get("birth_city", "")
                    record["criminal_proceedings"] = person.get("criminal_proceedings", "")
                    record["proceedings_link"] = person.get("proceedings_link", "")
                    writer.writerow(record)
                    csvfile.flush()
                    done_urls.add(stub_key)
                    total_scraped += 1
                    log.info("Stub (no page): %s", person["name"])
                    continue

                if person_url in done_urls:
                    log.debug("Skipping already-scraped: %s", person_url)
                    continue

                time.sleep(delay)
                log.info("Scraping: %s", person_url)
                detail_soup = fetch(session, person_url)

                # Some jeltelenul.hu pages live under /index.php/ — try that
                # variant automatically when the primary slug URL returns 404.
                if detail_soup is None and "/index.php/" not in person_url:
                    slug = person_url.rstrip("/").rsplit("/", 1)[-1]
                    alt_url = f"{BASE_URL}/index.php/{slug}"
                    log.info("Primary 404 — retrying as %s", alt_url)
                    detail_soup = fetch(session, alt_url)
                    if detail_soup is not None:
                        person_url = alt_url

                if detail_soup is None:
                    log.warning("Detail page unavailable for %s — writing partial record", person_url)
                    record = {col: "" for col in COLUMNS}
                    record["url"] = person_url
                    record["name"] = person["name"]
                    record["birth_date"] = person.get("birth_info", "")
                    writer.writerow(record)
                    csvfile.flush()
                    done_urls.add(person_url)
                    total_scraped += 1
                    checkpoint["done_urls"] = list(done_urls)
                    checkpoint["current_page"] = page
                    save_checkpoint(CHECKPOINT_FILE, checkpoint)
                    continue

                record = parse_detail_page(detail_soup, person_url, person["name"])
                writer.writerow(record)
                csvfile.flush()

                done_urls.add(person_url)
                total_scraped += 1

                # Save checkpoint after each record
                checkpoint["done_urls"] = list(done_urls)
                checkpoint["current_page"] = page
                save_checkpoint(CHECKPOINT_FILE, checkpoint)

            # Advance page
            if not has_next_page(soup):
                log.info("No next page after page %d. Scraping complete.", page)
                break

            page += 1
            time.sleep(delay)

    log.info("Done. Total records scraped this run: %d", total_scraped)
    log.info("Output saved to: %s", output)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape person records from jeltelenul.hu"
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT,
        help=f"Output CSV file (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Delay in seconds between requests (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of list pages to process (default: all)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignoring any existing checkpoint file",
    )
    args = parser.parse_args()

    scrape(
        output=args.output,
        delay=args.delay,
        max_pages=args.max_pages,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    main()
