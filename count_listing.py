"""
Quickly counts all person rows across every listing page.
Prints a breakdown of: linked persons, stubs, and any rows skipped by the parser.
Run: python count_listing.py
"""
import re
import sys
import time
import urllib.parse
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://jeltelenul.hu"
LIST_PATH = "/szemelyek"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE_URL,
}


def fetch(session, url):
    for attempt in range(1, 4):
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP {e.response.status_code} — {url}")
            if e.response.status_code in (403, 404):
                return None
        except requests.exceptions.RequestException as e:
            print(f"  Error: {e}")
        if attempt < 3:
            time.sleep(2 ** attempt)
    return None


def has_next_page(soup):
    pager = soup.find(class_=lambda c: c and "pager" in c)
    if pager:
        if pager.find("a", title=lambda t: t and ("következő" in t.lower() or "next" in t.lower())):
            return True
        if pager.find("a", rel="next") or soup.find("link", rel="next"):
            return True
        next_li = pager.find("li", class_=lambda c: c and "next" in c)
        if next_li and next_li.find("a"):
            return True
    return bool(soup.find("link", rel="next") or soup.find("a", rel="next"))


def count_page(soup, page_num):
    table = soup.find("table", class_=lambda c: c and "views" in c) or soup.find("table")
    if not table:
        print(f"  Page {page_num}: no table found")
        return 0, 0, []

    linked = 0
    stubs = 0
    skipped = []

    for row in table.find_all("tr")[1:]:  # skip header
        cells = row.find_all("td")
        if not cells:
            continue

        first_cell = cells[0]
        raw_text = first_cell.get_text(" ", strip=True)

        # Check for hreflang link (published person)
        link_tag = first_cell.find("a", hreflang=True)
        if not link_tag:
            link_tag = first_cell.find("a", href=lambda h: h and h.startswith("/"))

        if link_tag:
            linked += 1
        else:
            # Check for stub pattern
            m = re.match(r'^(.+?)\s*\[([^,\]]+),\s*(\d{4})\]\s*$', raw_text)
            if m:
                stubs += 1
            else:
                # Neither linked nor recognisable stub
                # Check if there are ANY links at all in the row
                any_links = [(a.get("href", ""), a.get_text(strip=True)) for a in first_cell.find_all("a")]
                skipped.append({
                    "page": page_num,
                    "text": raw_text[:120],
                    "links": any_links,
                    "num_cells": len(cells),
                })
                stubs += 1  # still counted (scrapy writes these as stubs)

    return linked, stubs, skipped


def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    total_linked = 0
    total_stubs = 0
    all_skipped = []
    page = 0

    while True:
        params = urlencode({"order": "title", "sort": "asc", "page": page})
        url = f"{BASE_URL}{LIST_PATH}?{params}"
        print(f"Page {page}: {url}")

        soup = fetch(session, url)
        if soup is None:
            print(f"  Failed to fetch page {page}. Stopping.")
            break

        linked, stubs, skipped = count_page(soup, page)
        total_linked += linked
        total_stubs += stubs
        all_skipped.extend(skipped)

        print(f"  linked={linked}  stubs={stubs}  page_total={linked + stubs}")

        if not has_next_page(soup):
            print(f"  → No next page, stopping after page {page}.")
            break

        page += 1
        time.sleep(0.5)

    print()
    print("=" * 60)
    print(f"Total linked persons : {total_linked}")
    print(f"Total stub persons   : {total_stubs}")
    print(f"Grand total          : {total_linked + total_stubs}")
    print()

    if all_skipped:
        print(f"Rows that didn't match hreflang OR stub pattern ({len(all_skipped)}):")
        for s in all_skipped:
            print(f"  page={s['page']} cells={s['num_cells']} text={s['text']!r}")
            if s["links"]:
                for href, text in s["links"]:
                    print(f"    <a href={href!r}>{text!r}</a>")
    else:
        print("All rows matched either linked or stub pattern — no silent drops.")


if __name__ == "__main__":
    main()
