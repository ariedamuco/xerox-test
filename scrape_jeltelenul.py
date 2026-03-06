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
    "elhalalozes_oka",       # Cause of death
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

            # Person name is always in the first column — look there exclusively
            # to avoid accidentally picking up criminal-proceedings links from
            # later columns (which have proper slugs but are not person pages).
            first_cell = cells[0]
            link_tag = first_cell.find("a", href=True)

            if link_tag:
                href = link_tag["href"]
                name = link_tag.get_text(strip=True)
                full_url = href_to_url(href)
            else:
                # No <a> in first cell — derive URL from the cell's plain text
                name = first_cell.get_text(strip=True)
                if not name:
                    continue
                full_url = f"{BASE_URL}/{slugify_hu(name)}"

            birth_info = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            persons.append({"name": name, "url": full_url, "birth_info": birth_info})

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
    # cause of death
    "Elhalálozás oka": "elhalalozes_oka",
    "Halál oka": "elhalalozes_oka",
    "Kivégzés módja": "elhalalozes_oka",
    # burial
    "Temetési/elföldelési helyszín": "temetes_helye",
    "Temetési helyszín": "temetes_helye",
    "Elföldelés helye": "temetes_helye",
    "Temető": "temetes_helye",
}


def _collect_raw_fields(soup: BeautifulSoup, url: str) -> dict[str, str]:
    """
    Extract every label→value pair from a detail page using four independent
    strategies so we are resilient to whatever HTML the theme produces.

    Priority: later strategies only fill in keys not already found.
    """
    raw: dict[str, str] = {}

    def add(label: str, value: str) -> None:
        label = _clean_label(label)
        if label and label not in raw:
            raw[label] = value

    # 1. Standard Drupal 7 field divs
    #    <div class="field field-name-field-szuletesi-hely …">
    #      <div class="field-label">Születési hely:&nbsp;</div>
    #      <div class="field-items"><div class="field-item even">Budapest</div></div>
    #    </div>
    for field_div in soup.find_all("div", class_=lambda c: c and "field-name-" in c):
        label_tag = field_div.find(class_=lambda c: c and "field-label" in c)
        if not label_tag:
            continue
        items_tag = field_div.find(class_=lambda c: c and "field-items" in c)
        if items_tag:
            values = [_text(i) for i in items_tag.find_all(
                class_=lambda c: c and "field-item" in c
            )]
            value = " | ".join(v for v in values if v)
        else:
            value = _text(field_div)
        add(label_tag.get_text(strip=True), value)

    # 2. Drupal Views field cells (list/table Views output)
    #    <td class="views-field views-field-field-szuletesi-hely">…</td>
    for cell in soup.find_all(class_=lambda c: c and "views-field-field-" in c):
        css = " ".join(cell.get("class", []))
        # derive a label from the CSS class name
        field_name = re.search(r"views-field-field-([\w-]+)", css)
        if not field_name:
            continue
        label_from_css = field_name.group(1).replace("-", " ").title()
        label_tag = cell.find(class_=lambda c: c and "views-label" in c)
        label = label_tag.get_text(strip=True) if label_tag else label_from_css
        value = _text(cell)
        # strip the label prefix from the value if it echoes it
        if value.startswith(label):
            value = value[len(label):].strip()
        add(label, value)

    # 3. Two-cell table rows  <tr><th>Label:</th><td>Value</td></tr>
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) == 2:
            add(cells[0].get_text(strip=True), _text(cells[1]))

    # 4. Definition lists  <dl><dt>Label:</dt><dd>Value</dd></dl>
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            add(dt.get_text(strip=True), _text(dd))

    # 5. <strong> or <b> inline labels followed by sibling text
    #    e.g.  <p><strong>Születési hely:</strong> Budapest</p>
    for bold in soup.find_all(["strong", "b"]):
        label_text = bold.get_text(strip=True)
        if not label_text.endswith((":", "\xa0")):
            continue
        # value is the text immediately after the bold tag
        nxt = bold.next_sibling
        if nxt and isinstance(nxt, str):
            value = nxt.strip()
        elif nxt:
            value = _text(nxt)
        else:
            value = ""
        if value:
            add(label_text, value)

    if not raw:
        log.warning("No labelled fields found on %s — page structure unknown", url)
    else:
        log.debug("Fields on %s: %s", url, list(raw.keys()))

    return raw


def parse_detail_page(soup: BeautifulSoup, url: str, name: str) -> dict:
    record = {col: "" for col in COLUMNS}
    record["url"] = url
    record["nev"] = name

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
