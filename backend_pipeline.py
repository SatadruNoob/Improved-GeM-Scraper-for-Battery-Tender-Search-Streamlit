# file: backend_pipeline.py

import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import subprocess
import sys
import os
import signal
import threading
import traceback
import time

# -------------------------------------------------
# Resolve BASE directory (exe-aware)
# -------------------------------------------------
if getattr(sys, "frozen", False):
    INTERNAL_DIR = Path(sys._MEIPASS)
    BASE_DIR = INTERNAL_DIR
else:
    BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Master CSV (merged output)
INPUT_CSV = DATA_DIR / "gem_all_bids.csv"
OUTPUT_XLSX = DATA_DIR / "gem_bid_analysis.xlsx"
STATUS_FILE = DATA_DIR / "run_status.json"
CHECKED_BIDS_FILE = DATA_DIR / "checked_bids.json"  # NEW: Track checked bids

# Individual session CSV files
SESSION_CSV_FILES = [
    DATA_DIR / "gem_all_bids_session1.csv",
    DATA_DIR / "gem_all_bids_session2.csv",
    DATA_DIR / "gem_all_bids_session3.csv",
    DATA_DIR / "gem_all_bids_session4.csv",
]

# Search keywords for each session
SEARCH_KEYWORDS = [
    "batter",
    "batter",
    "batter",
    "batter",
]

KEYWORD_GROUPS = {
    # Order matters: Most specific categories first
    "Diesel Loco": ["diesel loco", "7624"],
    "Traction Batteries": ["traction", "5154"],
    "Plante Batteries": ["plante", "1652"],
    "Lead Acid": ["lead acid"],  # Most generic, check last
}  

# -------------------------------------------------
# Debug boot info
# -------------------------------------------------
with open(BASE_DIR / "backend_debug.log", "w") as f:
    f.write(f"sys.frozen: {getattr(sys, 'frozen', False)}\n")
    f.write(f"sys._MEIPASS: {getattr(sys, '_MEIPASS', 'NOT SET')}\n")
    f.write(f"BASE_DIR: {BASE_DIR}\n")
    f.write(f"Working directory: {os.getcwd()}\n")

# -------------------------------------------------
# Status helpers
# -------------------------------------------------
def write_status(**kwargs):
    """
    Atomic read-modify-write of the top-level status fields.
    Uses a lock file so concurrent scraper processes and the monitor thread
    can never corrupt each other's writes.
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

            # --- merge new values at top level only ---
            data.update(kwargs)

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

    # Fallback: try without lock to avoid silent failure
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
        data.update(kwargs)
        STATUS_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        log_backend(f"⚠️ write_status fallback failed: {e}")


def log_backend(msg):
    """Log backend messages to a dedicated file"""
    log_file = DATA_DIR / "backend_merge.log"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


# -------------------------------------------------
# CHECKED BIDS MANAGEMENT
# -------------------------------------------------
def load_checked_bids():
    """Load the set of checked bid numbers from JSON file"""
    if not CHECKED_BIDS_FILE.exists():
        return {}
    
    try:
        with open(CHECKED_BIDS_FILE, 'r') as f:
            data = json.load(f)
            # Return dict with bid_no -> {'checked_date': date, 'notes': optional_notes}
            return data
    except (json.JSONDecodeError, Exception) as e:
        log_backend(f"⚠️ Error loading checked bids: {e}")
        return {}


def save_checked_bids(checked_bids_dict):
    """Save the checked bids dictionary to JSON file"""
    try:
        with open(CHECKED_BIDS_FILE, 'w') as f:
            json.dump(checked_bids_dict, indent=2, fp=f)
        log_backend(f"✓ Saved {len(checked_bids_dict)} checked bids")
    except Exception as e:
        log_backend(f"❌ Error saving checked bids: {e}")


def mark_bid_checked(bid_no, notes=""):
    """Mark a bid as checked"""
    checked_bids = load_checked_bids()
    checked_bids[str(bid_no)] = {
        'checked_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'notes': notes
    }
    save_checked_bids(checked_bids)
    return True


def unmark_bid_checked(bid_no):
    """Remove a bid from checked list"""
    checked_bids = load_checked_bids()
    if str(bid_no) in checked_bids:
        del checked_bids[str(bid_no)]
        save_checked_bids(checked_bids)
        return True
    return False


def is_bid_checked(bid_no):
    """Check if a bid is marked as checked"""
    checked_bids = load_checked_bids()
    return str(bid_no) in checked_bids


def get_checked_info(bid_no):
    """Get checked info for a bid"""
    checked_bids = load_checked_bids()
    return checked_bids.get(str(bid_no), None)



# -------------------------------------------------
# MERGE & DEDUPLICATE - FIXED VERSION
# -------------------------------------------------
def merge_and_deduplicate_sessions(progress_cb=None):
    """
    Merge all 4 session CSV files into the master CSV,
    removing duplicates based on composite key logic.
    
    FIXED: Preserves End Date column properly during deduplication
    """
    log_backend("=" * 60)
    log_backend("STARTING MERGE & DEDUPLICATION")
    
    if progress_cb:
        progress_cb("🔄 Merging session CSV files...", 10)
    
    all_frames = []
    
    # Load all session CSVs with detailed logging
    for i, csv_file in enumerate(SESSION_CSV_FILES, 1):
        if not csv_file.exists():
            log_backend(f"⚠️  Session {i} CSV not found: {csv_file}")
            continue
        
        try:
            df = pd.read_csv(csv_file)
            log_backend(f"✓ Loaded Session {i}: {len(df)} rows from {csv_file.name}")
            
            # Debug: Check if End Date column exists and has data
            if 'End Date' in df.columns:
                non_null_count = df['End Date'].notna().sum()
                log_backend(f"  → End Date: {non_null_count}/{len(df)} non-null values")
                if len(df) > 0:
                    log_backend(f"  → Sample: {df['End Date'].iloc[0]}")
            else:
                log_backend(f"  ⚠️  WARNING: No 'End Date' column found!")
            
            all_frames.append(df)
        except Exception as e:
            log_backend(f"❌ Error loading Session {i}: {e}")
            log_backend(f"   Traceback: {traceback.format_exc()}")
            continue
    
    if not all_frames:
        log_backend("⚠️  No session data to merge!")
        if progress_cb:
            progress_cb("⚠️ No session data found", 100)
        return
    
    # Concatenate all dataframes
    merged_df = pd.concat(all_frames, ignore_index=True)
    log_backend(f"📊 Total rows before deduplication: {len(merged_df)}")
    
    # Debug: Check End Date after merge
    if 'End Date' in merged_df.columns:
        non_null_count = merged_df['End Date'].notna().sum()
        log_backend(f"📊 End Date after concat: {non_null_count}/{len(merged_df)} non-null values")
        if len(merged_df) > 0:
            log_backend(f"📊 Sample End Dates: {merged_df['End Date'].head(3).tolist()}")
    else:
        log_backend(f"⚠️  CRITICAL: No 'End Date' column after concat!")
    
    if progress_cb:
        progress_cb("🔍 Removing duplicates...", 40)
    
    # CRITICAL FIX: Preserve original End Date string
    merged_df['End Date Original'] = merged_df['End Date'].astype(str)
    
    # Parse End Date to datetime for sorting ONLY
    merged_df['End Date Parsed'] = pd.to_datetime(
        merged_df['End Date'], 
        format='%d-%b-%Y %I:%M %p', 
        errors='coerce'
    )
    
    # Log any parsing failures
    parse_failures = merged_df['End Date Parsed'].isna().sum()
    if parse_failures > 0:
        log_backend(f"⚠️  Warning: {parse_failures} End Dates failed to parse")
        failed_samples = merged_df[merged_df['End Date Parsed'].isna()]['End Date Original'].head(5).tolist()
        log_backend(f"   Failed samples: {failed_samples}")
    
    # Sort by Bid No and parsed End Date (descending) to keep latest
    merged_df = merged_df.sort_values(
        ['Bid No', 'End Date Parsed'], 
        ascending=[True, False]
    )
    
    # Group by Bid No and aggregate
    def aggregate_duplicates(group):
        # Keep the row with latest End Date (first row after sorting)
        latest = group.iloc[0].copy()
        
        # Preserve earliest First Seen Date
        earliest_first_seen = group['First Seen Date'].min()
        latest['First Seen Date'] = earliest_first_seen
        
        # FIXED: "New Today" should be YES only if First Seen Date is today
        # If earliest First Seen Date is today, it's genuinely new
        today = datetime.now().strftime("%Y-%m-%d")
        if earliest_first_seen == today:
            latest['New Today'] = 'YES'
        else:
            latest['New Today'] = 'NO'
        
        # Preserve any "YES" flags for End Date Changed
        if (group['End Date Changed'] == 'YES').any():
            latest['End Date Changed'] = 'YES'
        
        # CRITICAL FIX: Use original End Date string
        latest['End Date'] = latest['End Date Original']
        
        return latest
    
    # Apply deduplication using loop (safer than groupby().apply())
    log_backend("🔄 Applying deduplication logic...")
    deduplicated_list = []
    
    for bid_no, group in merged_df.groupby('Bid No', sort=False):
        deduplicated_list.append(aggregate_duplicates(group))
    
    # Create DataFrame from list (avoids nested structure)
    deduplicated_df = pd.DataFrame(deduplicated_list)
    
    # Drop helper columns
    deduplicated_df = deduplicated_df.drop(
        columns=['End Date Original', 'End Date Parsed'], 
        errors='ignore'
    )
    
    # Reset index
    deduplicated_df = deduplicated_df.reset_index(drop=True)
    
    log_backend(f"✓ Rows after deduplication: {len(deduplicated_df)}")
    log_backend(f"🗑️  Removed {len(merged_df) - len(deduplicated_df)} duplicate rows")
    
    if progress_cb:
        progress_cb("💾 Saving merged CSV...", 70)
    
    # Final verification before saving
    if 'End Date' in deduplicated_df.columns:
        non_null_count = deduplicated_df['End Date'].notna().sum()
        log_backend(f"✅ Final End Date check: {non_null_count}/{len(deduplicated_df)} non-null values")
        if len(deduplicated_df) > 0:
            log_backend(f"✅ Sample final End Dates: {deduplicated_df['End Date'].head(3).tolist()}")
    else:
        log_backend(f"❌ CRITICAL ERROR: No 'End Date' column in final DataFrame!")
    
    # Verify all required columns exist
    required_columns = [
        'Bid No', 'Items', 'Quantity', 'Department Name And Address', 
        'Start Date', 'End Date', 'Bid EndDate Hash', 'First Seen Date', 
        'New Today', 'End Date Changed'
    ]
    missing_columns = [col for col in required_columns if col not in deduplicated_df.columns]
    if missing_columns:
        log_backend(f"⚠️  Missing columns in final DataFrame: {missing_columns}")
    
    # Save to master CSV
    deduplicated_df.to_csv(INPUT_CSV, index=False)
    log_backend(f"✓ Saved master CSV: {INPUT_CSV}")
    log_backend(f"✓ Total unique bids saved: {len(deduplicated_df)}")
    
    # Update status
    write_status(
        merge_completed=str(datetime.now()),
        total_unique_bids=len(deduplicated_df),
        merge_status="COMPLETED"
    )
    
    if progress_cb:
        progress_cb("✅ Merge completed", 100)
    
    log_backend("MERGE & DEDUPLICATION COMPLETED")
    log_backend("=" * 60)



# -------------------------------------------------
# RAW EXTRACTION (4 PARALLEL SESSIONS)
# -------------------------------------------------
def run_raw_extraction(progress_cb=None):
    """
    Launch 4 parallel scraping sessions, each with a different keyword.
    Each session writes to its own CSV file.
    Clears stale session sub-keys from any previous crashed run first.
    """
    # Wipe top-level AND per-session keys so nothing from a previous crash bleeds in
    clean_start = {
        "extraction_started": str(datetime.now()),
        "extraction_status": "RUNNING",
        "sessions_running": 4,
        "sessions_completed": 0,
        "session_pids": [],
        # Explicitly clear all 4 session sub-keys
        "session1_status": {},
        "session2_status": {},
        "session3_status": {},
        "session4_status": {},
    }
    tmp_path = str(STATUS_FILE) + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(clean_start, f, indent=2)
    os.replace(tmp_path, STATUS_FILE)

    # Remove stale lock file from previous run if it exists
    lock_path = Path(str(STATUS_FILE) + ".lock")
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass

    if progress_cb:
        progress_cb("🚀 Starting 4 parallel scraping sessions...", 5)

    processes = []
    session_pids = []
    
    def run_scraper_session(session_id, keyword, csv_file):
        """Run a single scraper session"""
        try:
            debug_log = DATA_DIR / f"scraper_session{session_id}_debug.log"
            with open(debug_log, "a") as log:
                log.write(f"\n[START SESSION {session_id}] {datetime.now()}\n")
                log.write(f"Keyword: {keyword}\n")
                log.write(f"Output CSV: {csv_file}\n")

            script_path = BASE_DIR / "gem_all_bids_playwright_csv.py"
            if not script_path.exists():
                raise RuntimeError(f"Script not found: {script_path}")

            # Use the Windows venv ONLY on Windows. A committed
            # .venv/Scripts/python.exe cannot be executed on Streamlit
            # Community Cloud (Linux) — this was the original bug.
            python_exe = BASE_DIR / ".venv" / "Scripts" / "python.exe"
            if sys.platform != "win32" or not python_exe.exists():
                python_exe = sys.executable

            # Inherit the FULL environment (HOME, PLAYWRIGHT_BROWSERS_PATH,
            # any GEM_PROXY_* vars, etc). The old hard-coded Windows-only
            # env dict left Linux with no HOME, so Playwright couldn't find
            # its browser cache and the subprocess likely failed silently.
            env = os.environ.copy()

            # Send output to a per-session log file instead of unread pipes.
            # An unread PIPE can also fill its OS buffer and hang the child.
            stdout_log_path = DATA_DIR / f"scraper_session{session_id}_stdout.log"
            out_log = open(stdout_log_path, "a", encoding="utf-8")
            out_log.write(f"\n[LAUNCH {datetime.now()}] python_exe={python_exe}\n")
            out_log.flush()

            # Pass session parameters as command-line arguments
            process = subprocess.Popen(
                [
                    str(python_exe),
                    str(script_path),
                    "--session-id", str(session_id),
                    "--keyword", keyword,
                    "--output-csv", str(csv_file)
                ],
                cwd=str(BASE_DIR),
                stdout=out_log,
                stderr=subprocess.STDOUT,
                text=True,
                env=env
            )

            session_pids.append(process.pid)
            processes.append((session_id, process))

            with open(debug_log, "a") as log:
                log.write(f"[OK] Subprocess PID: {process.pid}\n")
                log.write(f"[INFO] Session {session_id} running asynchronously\n")

            # Block here (this runs on its own thread) until the scraper
            # exits, then reap it. Without this, a finished/crashed child
            # becomes a zombie on Linux and os.kill(pid, 0) in the monitor
            # thread keeps reporting it as "running" forever.
            return_code = process.wait()
            out_log.write(f"[EXIT {datetime.now()}] return code {return_code}\n")
            out_log.close()
            with open(debug_log, "a") as log:
                log.write(f"[EXIT] Session {session_id} exited with code {return_code}\n")

        except Exception as e:
            error_msg = f"Session {session_id} failed: {str(e)}"
            log_backend(f"❌ {error_msg}")
            with open(DATA_DIR / f"scraper_session{session_id}_debug.log", "a") as log:
                log.write("[ERROR]\n")
                log.write(traceback.format_exc())

    # Launch all 4 sessions
    for i, (keyword, csv_file) in enumerate(zip(SEARCH_KEYWORDS, SESSION_CSV_FILES), 1):
        log_backend(f"🚀 Launching Session {i} - Keyword: '{keyword}'")
        thread = threading.Thread(
            target=run_scraper_session,
            args=(i, keyword, csv_file),
            daemon=False
        )
        thread.start()
        time.sleep(3)  # Stagger launches to avoid resource conflicts

    # Update status with all PIDs
    time.sleep(1)  # Give threads time to populate session_pids
    write_status(session_pids=session_pids)

    # Start monitoring thread to track completion of all sessions
    def monitor_all_sessions():
        """Monitor all 4 sessions and trigger merge when all complete"""
        completed_sessions = set()
        
        while len(completed_sessions) < 4:
            time.sleep(3)  # Check every 3 seconds
            
            if not STATUS_FILE.exists():
                continue
                
            try:
                with open(STATUS_FILE, 'rb') as f:
                    raw = f.read().decode('utf-8').strip()
                status = json.loads(raw) if raw else {}
                pids = status.get("session_pids", [])
                
                if not pids or len(pids) != 4:
                    continue
                
                # Check each session
                for session_id, pid in enumerate(pids, 1):
                    if session_id in completed_sessions:
                        continue  # Already counted
                    
                    # Check if process is still running
                    try:
                        if sys.platform == "win32":
                            result = subprocess.run(
                                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                                capture_output=True, text=True
                            )
                            process_running = str(pid) in result.stdout
                        else:
                            os.kill(pid, 0)
                            process_running = True
                    except (OSError, subprocess.SubprocessError):
                        process_running = False
                    
                    if not process_running:
                        completed_sessions.add(session_id)
                        log_backend(f"✓ Session {session_id} completed ({len(completed_sessions)}/4)")
                        
                        # Update status
                        write_status(sessions_completed=len(completed_sessions))
                
            except (json.JSONDecodeError, Exception) as e:
                log_backend(f"⚠️  Monitor error: {e}")
                continue
        
        # All sessions completed - trigger merge
        log_backend("✅ All 4 sessions completed! Starting merge...")
        write_status(
            extraction_status="MERGING",
            extraction_completed=str(datetime.now())
        )
        
        try:
            merge_and_deduplicate_sessions(progress_cb=None)
            
            write_status(
                extraction_status="COMPLETED",
                final_status="All sessions completed and merged successfully"
            )
            log_backend("🎉 EXTRACTION COMPLETE - All sessions merged and deduplicated")
            
        except Exception as e:
            error_msg = f"Merge failed: {str(e)}"
            log_backend(f"❌ {error_msg}")
            write_status(
                extraction_status="MERGE_FAILED",
                error=error_msg
            )

    threading.Thread(target=monitor_all_sessions, daemon=True).start()

    if progress_cb:
        progress_cb("⏳ 4 sessions running in parallel...", 50)


# -------------------------------------------------
# CANCEL EXTRACTION
# -------------------------------------------------
def cancel_extraction():
    """Cancel all running scraper sessions and reset status cleanly."""
    if not STATUS_FILE.exists():
        return False

    # Safe read via lock
    try:
        with open(STATUS_FILE, 'rb') as f:
            raw = f.read().decode('utf-8').strip()
        status = json.loads(raw) if raw else {}
    except Exception:
        status = {}

    pids = status.get("session_pids", [])
    cancelled_count = 0

    for i, pid in enumerate(pids, 1):
        try:
            if sys.platform == "win32":
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            else:
                os.kill(pid, signal.SIGTERM)
            log_backend(f"🛑 Cancelled Session {i} (PID: {pid})")
            cancelled_count += 1
        except Exception as e:
            log_backend(f"⚠️  Failed to cancel Session {i} (PID: {pid}): {e}")

    # Mark every individual session that was RUNNING as CANCELLED
    for i in range(1, 5):
        sk = f"session{i}_status"
        if status.get(sk, {}).get("status") == "RUNNING":
            status[sk]["status"] = "CANCELLED"

    # Write the cleaned-up status
    status["extraction_status"] = "CANCELLED"
    status["extraction_completed"] = str(datetime.now())
    status["sessions_cancelled"] = cancelled_count

    tmp_path = str(STATUS_FILE) + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(status, f, indent=2)
    os.replace(tmp_path, STATUS_FILE)

    # Remove the lock file so nothing is stuck
    lock_path = Path(str(STATUS_FILE) + ".lock")
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass

    return True   # Always return True so UI shows the cancellation message


# -------------------------------------------------
# ANALYSIS
# -------------------------------------------------
def prepare_analysis_excel(progress_cb=None):
    write_status(
        analysis_started=str(datetime.now()),
        analysis_status="RUNNING"
    )

    # CRITICAL FIX: Check if session CSVs exist and merge them first
    session_csvs_exist = any(csv.exists() for csv in SESSION_CSV_FILES)
    
    if session_csvs_exist:
        if progress_cb:
            progress_cb("🔄 Merging session CSVs first...", 10)
        
        log_backend("📋 Session CSVs detected - running merge before analysis")
        
        try:
            merge_and_deduplicate_sessions(progress_cb=progress_cb)
            log_backend("✅ Merge completed successfully")
        except Exception as e:
            error_msg = f"Merge failed during analysis prep: {str(e)}"
            log_backend(f"❌ {error_msg}")
            write_status(
                analysis_status="FAILED",
                error=error_msg
            )
            if progress_cb:
                progress_cb(f"❌ {error_msg}", 100)
            return

    if progress_cb:
        progress_cb("📊 Loading extracted CSV…", 30)

    if not INPUT_CSV.exists():
        log_backend("❌ Master CSV not found. Run extraction first.")
        write_status(
            analysis_status="FAILED",
            error="Master CSV not found. Please run scraping sessions first."
        )
        if progress_cb:
            progress_cb("❌ No data found. Run scraping first.", 100)
        return

    df = pd.read_csv(INPUT_CSV)
    df["Items"] = df["Items"].fillna("").astype(str)

    def classify(items):
        """
        Classify items using category specificity matching.
        
        Logic:
        1. Check if BOTH a specific category (Traction/Plante/Diesel Loco) 
           AND "Lead Acid" are present
        2. If yes, prioritize the specific category (since Lead Acid is the generic type)
        3. If only one category matches, use that
        
        Examples:
        - "Lead Acid Traction Battery" → Traction Batteries (not Lead Acid)
        - "Lead Acid Plante Battery" → Plante Batteries (not Lead Acid)
        - "Standard Lead Acid Battery" → Lead Acid (no specific subtype)
        """
        text = items.lower()
        
        # Check for all matches
        matched_groups = []
        for group, keys in KEYWORD_GROUPS.items():
            if any(k in text for k in keys):
                matched_groups.append(group)
        
        if not matched_groups:
            return None
        
        # If only one match, return it
        if len(matched_groups) == 1:
            return matched_groups[0]
        
        # Multiple matches: prioritize specific categories over "Lead Acid"
        # Remove "Lead Acid" if a more specific category is also present
        if "Lead Acid" in matched_groups and len(matched_groups) > 1:
            specific_categories = [g for g in matched_groups if g != "Lead Acid"]
            if specific_categories:
                # Return the first specific category found (based on dict order)
                for group in KEYWORD_GROUPS.keys():
                    if group in specific_categories:
                        return group
        
        # Default: return first match based on dictionary order
        for group in KEYWORD_GROUPS.keys():
            if group in matched_groups:
                return group
        
        return None

    df["Keyword Group"] = df["Items"].apply(classify)
    df_filtered = df[df["Keyword Group"].notna()]

    if progress_cb:
        progress_cb("📝 Writing analysis Excel…", 80)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All_Bids", index=False)
        df_filtered.to_excel(
            writer,
            sheet_name="Target_Keyword_Bids",
            index=False
        )

    write_status(
        analysis_completed=str(datetime.now()),
        analysis_status="COMPLETED",
        total_bids_analyzed=len(df),
        filtered_bids=len(df_filtered)
    )

    if progress_cb:
        progress_cb("✅ Analysis completed.", 100)

    log_backend(f"📊 Analysis complete: {len(df)} total, {len(df_filtered)} filtered")
    
    return df_filtered