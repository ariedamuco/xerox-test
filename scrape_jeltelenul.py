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
import sys
import time
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
    "nev",                   # Name (from listing)
    "nevvaltozat",           # Name variants
    "szuletesi_ido",         # Date of birth
    "szuletesi_hely",        # Place of birth
    "lakohelyek",            # Place(s) of residence
    "foglalkozasok",         # Occupation(s)
    "eletrajzi_megjegyzes",  # Biographical note
    "orizetbevetel_ideje",   # Date of arrest/detention
    "terhelt_cselekmeny",    # Charges/acts attributed
    "cselekmeny_helyszine",  # Location(s) of the act
    "cselekmeny_minosites",  # Classification of the act
    "buntetoeljarasok",      # Related criminal proceedings
    "buntetoeljaras_link",   # Link to criminal proceedings
    "bunteto_intezkedesek",  # Penal measures/sentences
    "elhalalozes_ideje",     # Date of death
    "elhalalozes_helye",     # Place of death
    "temetes_helye",         # Burial/interment place
    "temetes_link",          # Link to burial place
    "raw_fields",            # All other fields as JSON fallback
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
            link_tag = row.find("a", href=True)
            if link_tag:
                href = link_tag["href"]
                full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                name = link_tag.get_text(strip=True)
                # Get birth year from second cell if available
                birth_info = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                persons.append({"name": name, "url": full_url, "birth_info": birth_info})
        return persons

    # --- Div-based layout ---
    for row in soup.find_all("div", class_=lambda c: c and "views-row" in c):
        link_tag = row.find("a", href=True)
        if link_tag:
            href = link_tag["href"]
            full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
            name = link_tag.get_text(strip=True)
            persons.append({"name": name, "url": full_url, "birth_info": ""})

    # --- Fallback: any link that points to /node/ or a person slug ---
    if not persons:
        for link_tag in soup.find_all("a", href=True):
            href = link_tag["href"]
            if "/node/" in href or (
                href.startswith("/") and href.count("/") == 1 and len(href) > 5
            ):
                full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                name = link_tag.get_text(strip=True)
                if name:
                    persons.append({"name": name, "url": full_url, "birth_info": ""})

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


def _multi_text(tags) -> str:
    """Join multiple tags' text with ' | ' separator."""
    return " | ".join(_text(t) for t in tags if _text(t))


def parse_detail_page(soup: BeautifulSoup, url: str, name: str) -> dict:
    """
    Extract all relevant fields from an individual person detail page.

    Drupal field markup uses patterns like:
      <div class="field field-name-field-szuletesi-hely ...">
        <div class="field-label">Születési hely:&nbsp;</div>
        <div class="field-items">
          <div class="field-item even">Budapest</div>
        </div>
      </div>
    """
    record = {col: "" for col in COLUMNS}
    record["url"] = url
    record["nev"] = name

    # Collect all labelled fields into a dict for flexible mapping
    raw = {}

    # --- Drupal field divs ---
    for field_div in soup.find_all("div", class_=lambda c: c and "field-name-" in c):
        label_tag = field_div.find(class_=lambda c: c and "field-label" in c)
        items_tag = field_div.find(class_=lambda c: c and "field-items" in c)
        if not label_tag:
            continue
        label = label_tag.get_text(strip=True).rstrip(":").strip()
        if items_tag:
            values = [_text(i) for i in items_tag.find_all(
                class_=lambda c: c and "field-item" in c
            )]
            value = " | ".join(v for v in values if v)
        else:
            value = _text(field_div)
        raw[label] = value

    # --- Table-based field layout (some Drupal themes) ---
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) == 2:
            label = _text(cells[0]).rstrip(":").strip()
            value = _text(cells[1])
            if label and value:
                raw[label] = value

    # --- Definition list layout ---
    for dl in soup.find_all("dl"):
        terms = dl.find_all("dt")
        descs = dl.find_all("dd")
        for dt, dd in zip(terms, descs):
            label = _text(dt).rstrip(":").strip()
            value = _text(dd)
            if label:
                raw[label] = value

    # Map Hungarian field labels to output columns
    FIELD_MAP = {
        # name / identity
        "Névváltozat": "nevvaltozat",
        "Névváltozatok": "nevvaltozat",
        "Névváltozat(ok)": "nevvaltozat",
        "Más névváltozat": "nevvaltozat",
        # birth
        "Születési idő": "szuletesi_ido",
        "Születési dátum": "szuletesi_ido",
        "Született": "szuletesi_ido",
        "Születési hely": "szuletesi_hely",
        # residence
        "Lakóhely": "lakohelyek",
        "Lakóhelyek": "lakohelyek",
        "Lakhely": "lakohelyek",
        # occupation
        "Foglalkozás": "foglalkozasok",
        "Foglalkozások": "foglalkozasok",
        "Foglalkozás(ok)": "foglalkozasok",
        # biography
        "Életrajzi megjegyzés": "eletrajzi_megjegyzes",
        "Életrajzi megjegyzések": "eletrajzi_megjegyzes",
        "Megjegyzés": "eletrajzi_megjegyzes",
        # arrest
        "Őrizetbevétel ideje": "orizetbevetel_ideje",
        "Elfogás ideje": "orizetbevetel_ideje",
        "Letartóztatás ideje": "orizetbevetel_ideje",
        # charges
        "Terhére rótt cselekmény": "terhelt_cselekmeny",
        "Terhére rótt cselekmény(ek)": "terhelt_cselekmeny",
        "Vád": "terhelt_cselekmeny",
        # location of act
        "A cselekmény helyszíne": "cselekmeny_helyszine",
        "A cselekmény helyszíne(i)": "cselekmeny_helyszine",
        "Cselekmény helyszíne": "cselekmeny_helyszine",
        # classification
        "A cselekmény minősítése": "cselekmeny_minosites",
        "A cselekmény minősítése(i)": "cselekmeny_minosites",
        "Cselekmény minősítése": "cselekmeny_minosites",
        # criminal proceedings
        "Büntetőeljárás": "buntetoeljarasok",
        "Büntetőeljárások": "buntetoeljarasok",
        "Kapcsolódó büntetőeljárás": "buntetoeljarasok",
        # penal measure
        "Büntetőintézkedés": "bunteto_intezkedesek",
        "Büntetőintézkedések": "bunteto_intezkedesek",
        "Büntetés": "bunteto_intezkedesek",
        "Ítélet": "bunteto_intezkedesek",
        # death
        "Elhalálozás ideje": "elhalalozes_ideje",
        "Halál ideje": "elhalalozes_ideje",
        "Kivégzés ideje": "elhalalozes_ideje",
        "Elhunyt": "elhalalozes_ideje",
        "Elhalálozás helye": "elhalalozes_helye",
        "Halál helye": "elhalalozes_helye",
        "Kivégzés helye": "elhalalozes_helye",
        # burial
        "Temetési/elföldelési helyszín": "temetes_helye",
        "Temetési helyszín": "temetes_helye",
        "Elföldelés helye": "temetes_helye",
        "Temető": "temetes_helye",
    }

    for label, value in raw.items():
        col = FIELD_MAP.get(label)
        if col:
            if record[col]:  # append if already populated
                record[col] += " | " + value
            else:
                record[col] = value

    # Store any unmapped fields in raw_fields for traceability
    unmapped = {k: v for k, v in raw.items() if k not in FIELD_MAP}
    record["raw_fields"] = json.dumps(unmapped, ensure_ascii=False) if unmapped else ""

    # Extract links to criminal proceedings and burial pages
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"]
        link_text = link_tag.get_text(strip=True)
        full_link = href if href.startswith("http") else urljoin(BASE_URL, href)
        if "bunteto" in href.lower() or "eljárás" in link_text.lower():
            record["buntetoeljaras_link"] += (full_link + " ")
        if "temeto" in href.lower() or "temetés" in link_text.lower() or "elföldelés" in link_text.lower():
            record["temetes_link"] += (full_link + " ")

    # Clean up trailing spaces
    for col in ("buntetoeljaras_link", "temetes_link"):
        record[col] = record[col].strip()

    # Try to pull name from <h1> if not populated
    if not record["nev"]:
        h1 = soup.find("h1")
        record["nev"] = _text(h1)

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
                if person_url in done_urls:
                    log.debug("Skipping already-scraped: %s", person_url)
                    continue

                time.sleep(delay)
                log.info("Scraping: %s", person_url)
                detail_soup = fetch(session, person_url)

                if detail_soup is None:
                    log.warning("Skipping %s (fetch failed)", person_url)
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
