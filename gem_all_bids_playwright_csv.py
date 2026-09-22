# ================= MEMORY & STATE MODEL (DO NOT CHANGE) =================
#
# There are THREE strictly separated data structures. Mixing them causes
# data loss and false "new bid" counts.
#
# 1) PERSISTED STATE (loaded once at session start)
#    - known_bids            : set of Bid No already written to CSV
#    - bid_to_endhash        : last saved End Date hash per Bid No
#    - bid_to_firstseen      : immutable First Seen Date per Bid No
#    Source: CSV on disk
#    Rule: These represent historical truth. NEVER rebuild from memory.
#
# 2) SESSION STATE (reset every run)
#    - seen_this_run         : guards against duplicates caused by
#                              pagination glitches or DOM reuse
#    Source: current browser session only
#    Rule: Must NOT be used to decide "new vs old".
#
# 3) WRITE BUFFER (temporary, disposable)
#    - write_buffer          : rows that are fully classified and ready
#                              to be appended to CSV
#    Lifecycle:
#       append -> flush to CSV -> clear immediately
#    Rule: After flushing, buffer MUST be cleared.
#
# DISK (CSV) IS THE ONLY AUTHORITATIVE STATE BETWEEN SESSIONS.
# MEMORY MUST NEVER REPLACE OR RECONSTRUCT DISK STATE.
# ======================================================================

"""
STATEFUL GeM BID SCRAPER (COPILOT-SAFE) - MULTI-SESSION VERSION

MODE 1 ENABLED: GOODS-ONLY (/B/) SCRAPING

AUTHORITATIVE DESIGN:
- CSV is append-only and the ONLY source of truth
- In-memory state is NEVER used to rebuild CSV
- Memory mutations cannot reduce row count

NEW: Command-line argument support for parallel sessions:
    --session-id N        : Session identifier (1-4)
    --keyword TERM       : Search keyword
    --output-csv PATH    : Output CSV file path

Tracks:
1) Bid existence using plain Bid No (STRING)
2) End Date changes using SHA256(Bid No + "|" + End Date)
"""

import sys
import os
import time
import hashlib
import argparse
from pathlib import Path
from datetime import datetime
import json

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError

# =================================================
# Parse command-line arguments
# =================================================
parser = argparse.ArgumentParser(description="GeM Bid Scraper - Parallel Session Support")
parser.add_argument("--session-id", type=int, default=1, help="Session ID (1-4)")
parser.add_argument("--keyword", type=str, default="batter", help="Search keyword")
parser.add_argument("--output-csv", type=str, default=None, help="Output CSV file path")

args = parser.parse_args()

SESSION_ID = args.session_id
SEARCH_KEYWORD = args.keyword

# =================================================
# Paths & config
# =================================================
BASE_DIR = Path(sys._MEIPASS) if getattr(sys, "frozen", False) else Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

START_URL = "https://bidplus.gem.gov.in/all-bids"

# Use provided output CSV or default session file
if args.output_csv:
    CSV_FILE = Path(args.output_csv)
else:
    CSV_FILE = DATA_DIR / f"gem_all_bids_session{SESSION_ID}.csv"

RUN_DATE = datetime.now().strftime("%Y-%m-%d")

PAGE_DELAY_SEC = 1.5
MAX_NEXT_MISSES = 2

STATUS_FILE = DATA_DIR / "run_status.json"
LOG_FILE = DATA_DIR / f"scrape_session{SESSION_ID}_status.log"

CSV_COLUMNS = [
    "Bid No",
    "Items",
    "Quantity",
    "Department Name And Address",
    "Start Date",
    "End Date",
    "Bid EndDate Hash",
    "First Seen Date",
    "New Today",
    "End Date Changed",
]

# =================================================
# Hash helper
# =================================================
def enddate_hash(bid_no: str, end_date: str) -> str:
    return hashlib.sha256(f"{bid_no}|{end_date}".encode("utf-8")).hexdigest()

# =================================================
# Logging & status
# =================================================
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefixed_msg = f"[SESSION {SESSION_ID}] {msg}"
    
    # Write to session-specific log
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {prefixed_msg}\n")
    
    # Also write to main scrape_status.log for UI display
    main_log = DATA_DIR / "scrape_status.log"
    with open(main_log, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {prefixed_msg}\n")

def update_status(**kwargs):
    """
    Atomic read-modify-write of THIS session's sub-key in the status file.
    Lock file prevents simultaneous writes from 4 scrapers + the monitor thread
    from corrupting each other.
    """
    lock_path = str(STATUS_FILE) + ".lock"
    max_attempts = 10

    for attempt in range(max_attempts):
        try:
            # --- acquire lock ---
            lock_fd = open(lock_path, 'w')
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            # --- read current state ---
            data = {}
            if STATUS_FILE.exists():
                try:
                    with open(STATUS_FILE, 'rb') as f:
                        raw = f.read().decode('utf-8').strip()
                    if raw:
                        data = json.loads(raw)
                except Exception:
                    data = {}

            # --- merge into this session's sub-key only ---
            session_key = f"session{SESSION_ID}_status"
            if session_key not in data:
                data[session_key] = {}
            data[session_key].update(kwargs)

            # --- atomic write via temp file ---
            tmp_path = str(STATUS_FILE) + ".tmp"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, STATUS_FILE)   # atomic on Windows & POSIX

            # --- release lock ---
            if sys.platform == "win32":
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
            return  # success

        except (IOError, OSError):
            # Lock held by another process – wait and retry
            try:
                lock_fd.close()
            except Exception:
                pass
            time.sleep(0.05 * (attempt + 1))

    # Fallback without lock
    try:
        data = {}
        if STATUS_FILE.exists():
            try:
                with open(STATUS_FILE, 'rb') as f:
                    raw = f.read().decode('utf-8').strip()
                if raw:
                    data = json.loads(raw)
            except Exception:
                data = {}
        session_key = f"session{SESSION_ID}_status"
        if session_key not in data:
            data[session_key] = {}
        data[session_key].update(kwargs)
        STATUS_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        log(f"⚠️ update_status fallback failed: {e}")

# =================================================
# Load persisted state (READ-ONLY SNAPSHOT)
# =================================================
def load_state():
    """Load state from this session's CSV file"""
    if not CSV_FILE.exists():
        return set(), {}, {}

    df = pd.read_csv(CSV_FILE)

    return (
        set(df["Bid No"].astype(str)),
        dict(zip(df["Bid No"], df["Bid EndDate Hash"])),
        dict(zip(df["Bid No"], df["First Seen Date"])),
    )

# =================================================
# Safe CSV append
# =================================================
def append_rows(rows):
    if not rows:
        return
    df = pd.DataFrame(rows).reindex(columns=CSV_COLUMNS)
    write_header = not CSV_FILE.exists()
    df.to_csv(CSV_FILE, mode="a", index=False, header=write_header)

# =================================================
# Extraction
# =================================================
def extract_bids(page):
    bids = []
    cards = page.locator("div.card")

    for i in range(cards.count()):
        card = cards.nth(i)
        bid_links = card.locator(".bid_no_hover")
        if bid_links.count() == 0:
            continue

        bid_no = bid_links.first.inner_text().strip()

        items = ""
        items_block = card.locator("strong:has-text('Items:')").locator("..")
        if items_block.count():
            a = items_block.locator("a")
            if a.count():
                dc = a.first.get_attribute("data-content")
                items = dc.strip() if dc else a.first.text_content().strip()
            if not items:
                raw = items_block.evaluate("el => el.textContent")
                items = raw.replace("Items:", "").strip()

        quantity = (
            card.locator("strong:has-text('Quantity:')")
            .locator("..")
            .inner_text()
            .replace("Quantity:", "")
            .strip()
        )

        department = (
            card.locator("div.col-md-5 .row")
            .nth(1)
            .inner_text()
            .replace("\n", " | ")
            .strip()
        )

        bids.append({
            "Bid No": bid_no,
            "Items": items,
            "Quantity": quantity,
            "Department Name And Address": department,
            "Start Date": card.locator(".start_date").inner_text().strip(),
            "End Date": card.locator(".end_date").inner_text().strip(),
        })

    return bids

# =================================================
# Main
# =================================================
def main():
    log(f"Starting scraper - Keyword: '{SEARCH_KEYWORD}' | Output: {CSV_FILE.name}")
    
    update_status(
        keyword=SEARCH_KEYWORD,
        output_csv=str(CSV_FILE),
        started=str(datetime.now()),
        status="RUNNING"
    )

    known_bids, bid_to_endhash, bid_to_firstseen = load_state()

    seen_this_run = set()
    write_buffer = []
    new_found = 0
    next_misses = 0
    page_count = 0

    with sync_playwright() as p:
        launch_kwargs = dict(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        # Optional proxy (e.g. an Indian residential proxy), set via
        # Streamlit secrets / environment variables. Left unset = no proxy.
        _proxy_server = os.environ.get("GEM_PROXY_SERVER", "").strip()
        if _proxy_server:
            launch_kwargs["proxy"] = {"server": _proxy_server}
            if os.environ.get("GEM_PROXY_USER"):
                launch_kwargs["proxy"]["username"] = os.environ["GEM_PROXY_USER"]
                launch_kwargs["proxy"]["password"] = os.environ.get("GEM_PROXY_PASS", "")
            log("Launching with proxy from GEM_PROXY_SERVER")

        browser = p.chromium.launch(**launch_kwargs)
        page = browser.new_page(viewport={"width": 1366, "height": 900})

        # Images/fonts/media are not needed for scraping and cost bandwidth
        page.route(
            "**/*",
            lambda r: r.abort()
            if r.request.resource_type in ("image", "font", "media")
            else r.continue_(),
        )

        page.goto(START_URL, timeout=90_000)
        page.wait_for_selector("div.card", timeout=60_000)

        search = page.locator("#searchBid")
        search.fill("")
        search.type(SEARCH_KEYWORD, delay=100)
        search.press("Enter")
        page.wait_for_timeout(1500)

        while True:
            page_count += 1
            log(f"Processing page {page_count}")
            
            for bid in extract_bids(page):
                bid_no = bid["Bid No"]

                if "/B/" not in bid_no:
                    continue
                if bid_no in seen_this_run:
                    continue
                seen_this_run.add(bid_no)

                eh = enddate_hash(bid_no, bid["End Date"])

                if bid_no not in known_bids:
                    bid.update({
                        "Bid EndDate Hash": eh,
                        "First Seen Date": RUN_DATE,
                        "New Today": "YES",
                        "End Date Changed": "NO",
                    })
                    known_bids.add(bid_no)
                    bid_to_firstseen[bid_no] = RUN_DATE
                    bid_to_endhash[bid_no] = eh
                    new_found += 1
                else:
                    bid.update({
                        "Bid EndDate Hash": eh,
                        "First Seen Date": bid_to_firstseen[bid_no],
                        "New Today": "NO",
                        "End Date Changed": "YES" if eh != bid_to_endhash.get(bid_no) else "NO",
                    })
                    bid_to_endhash[bid_no] = eh

                write_buffer.append(bid)

            # Flush buffer after each page
            append_rows(write_buffer)
            log(f"Saved {len(write_buffer)} bids from page {page_count}")
            write_buffer.clear()
            
            # Update status
            update_status(
                pages_processed=page_count,
                new_bids_found=new_found,
                total_bids_seen=len(seen_this_run)
            )

            next_btn = page.locator("#light-pagination .next:not(.current)")
            if next_btn.count() == 0:
                next_misses += 1
                if next_misses >= MAX_NEXT_MISSES:
                    log("Pagination completed safely.")
                    break
                time.sleep(2)
                continue

            next_misses = 0
            first_bid = page.locator(".bid_no_hover", has_text="/B/").first.inner_text()
            next_btn.click()
            time.sleep(PAGE_DELAY_SEC)

            try:
                page.wait_for_function(
                    """
                    (prev) => {
                        const el = [...document.querySelectorAll('.bid_no_hover')]
                          .find(a => a.innerText.includes('/B/'));
                        return el && el.innerText !== prev;
                    }
                    """,
                    arg=first_bid,
                    timeout=60_000,
                )
            except TimeoutError:
                log("Pagination stalled. Retrying.")

        browser.close()

    log(f"Session completed: {new_found} new bids found, {page_count} pages processed")
    
    update_status(
        status="COMPLETED",
        completed=str(datetime.now()),
        total_new_bids=new_found,
        total_pages=page_count,
        final_bid_count=len(seen_this_run)
    )

# =================================================
if __name__ == "__main__":
    main()