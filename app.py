# file: app.py

import streamlit as st
import pandas as pd
import json
from pathlib import Path
import time
import sys
import os

# -------------------------------------------------
# Resolve BASE directory (exe-aware)
# -------------------------------------------------
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# DEBUG: Write debug info
app_debug_path = DATA_DIR / "app_debug.log"
with open(app_debug_path, "w") as f:
    f.write(f"sys.frozen: {getattr(sys, 'frozen', False)}\n")
    f.write(f"sys._MEIPASS: {getattr(sys, '_MEIPASS', 'NOT SET')}\n")
    f.write(f"__file__: {__file__}\n")
    f.write(f"BASE_DIR: {BASE_DIR}\n")
    f.write(f"DATA_DIR: {DATA_DIR}\n")
    f.write(f"STATUS_FILE: {DATA_DIR / 'run_status.json'}\n")
    f.write(f"Working directory: {os.getcwd()}\n")

from backend_pipeline import (
    run_raw_extraction,
    prepare_analysis_excel,
    cancel_extraction,
    load_checked_bids,
    save_checked_bids,
    mark_bid_checked,
    unmark_bid_checked,
    is_bid_checked,
    get_checked_info
)

CSV_FILE = DATA_DIR / "gem_all_bids.csv"
EXCEL_FILE = DATA_DIR / "gem_bid_analysis.xlsx"
LOG_FILE = DATA_DIR / "scrape_status.log"
STATUS_FILE = DATA_DIR / "run_status.json"
CHECKED_BIDS_FILE = DATA_DIR / "checked_bids.json"
SHEET_NAME = "Target_Keyword_Bids"

st.set_page_config(
    page_title="GeM Bid Intelligence Dashboard",
    layout="wide"
)

# ────────────────────────────────────────
# Install Playwright's Chromium once per server start (Linux / Streamlit Cloud).
# On Windows (local dev) this is skipped — assumes Chromium is already installed.
# ────────────────────────────────────────
import subprocess as _subprocess


@st.cache_resource(show_spinner=False)
def ensure_chromium():
    r = _subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        capture_output=True, text=True,
    )
    return r.returncode, (r.stdout + r.stderr)[-1200:]


if sys.platform != "win32":
    _chromium_code, _chromium_msg = ensure_chromium()
    if _chromium_code != 0:
        st.error(
            "⚠️ Chromium install failed — scraping will not work until this is fixed. "
            "Make sure packages.txt lists the required system libraries."
        )
        with st.expander("Chromium install output"):
            st.code(_chromium_msg)

# ────────────────────────────────────────
# Helpers
# ────────────────────────────────────────
def read_status_file():
    """
    Read the status JSON file.
    Waits briefly if a write lock is held so it never reads a half-written file.
    """
    if not Path(STATUS_FILE).exists():
        return {}

    lock_path = str(STATUS_FILE) + ".lock"

    for attempt in range(5):
        try:
            # Try shared read lock (non-blocking)
            lock_fd = open(lock_path, 'w')
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)

            # Lock acquired — safe to read
            with open(STATUS_FILE, 'rb') as f:
                raw = f.read().decode('utf-8').strip()

            # Release lock
            if sys.platform == "win32":
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()

            if not raw:
                return {}
            return json.loads(raw)

        except (IOError, OSError):
            # Write in progress — wait and retry
            try:
                lock_fd.close()
            except Exception:
                pass
            time.sleep(0.05)
        except json.JSONDecodeError:
            st.error("⚠️ Status file contains invalid JSON. Showing raw content:")
            try:
                with open(STATUS_FILE, 'rb') as f:
                    st.code(f.read().decode('utf-8', errors='replace')[:500], language="text")
            except Exception:
                pass
            return {}
        except Exception as e:
            st.error(f"Error reading status file: {e}")
            return {}

    # Fallback: plain read if we couldn't acquire lock after retries
    try:
        with open(STATUS_FILE, 'rb') as f:
            raw = f.read().decode('utf-8').strip()
        return json.loads(raw) if raw else {}
    except Exception:
        return {}

def is_extraction_running():
    """Check if extraction is currently running by checking status file and session states"""
    status = read_status_file()
    if not status:
        return False
    
    # Check extraction_status field
    extraction_status = status.get("extraction_status", "")
    if extraction_status in ["RUNNING", "MERGING"]:
        return True
    
    # Also check if any individual session is still running
    for i in range(1, 5):
        session_key = f"session{i}_status"
        if session_key in status:
            session_status = status[session_key].get("status", "")
            if session_status == "RUNNING":
                return True
    
    return False

def read_last_lines(path, n=50):
    if not Path(path).exists():
        return ["[waiting for logs…]"]
    with open(path, "r", encoding="utf-8") as f:
        return f.readlines()[-n:]

# ────────────────────────────────────────
# Custom CSS for highlighting checked rows
# ────────────────────────────────────────
st.markdown("""
<style>
    /* Make dataframe cells more readable */
    .stDataFrame {
        font-size: 14px;
    }
    
    /* Highlight for checked status badge */
    .checked-badge {
        background-color: #ffd700;
        color: #000;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: bold;
        font-size: 12px;
    }
    
    .unchecked-badge {
        background-color: #e8e8e8;
        color: #666;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 12px;
    }
    
    /* Status indicator styles */
    .status-running {
        color: #1f77b4;
        font-weight: bold;
    }
    
    .status-completed {
        color: #2ca02c;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ────────────────────────────────────────
# Session state initialization
# ────────────────────────────────────────
if "data_ready" not in st.session_state:
    st.session_state.data_ready = False

if "current_filter" not in st.session_state:
    st.session_state.current_filter = "All"

if "show_only_unchecked" not in st.session_state:
    st.session_state.show_only_unchecked = False

if "last_update_time" not in st.session_state:
    st.session_state.last_update_time = time.time()

# ────────────────────────────────────────
# UI
# ────────────────────────────────────────
st.title("📊 GeM Bid Intelligence Dashboard")

# CRITICAL FIX: Force reload status from disk on every rerun
# Read ONCE per rerun cycle and reuse the same object to avoid read-time mismatches
status = read_status_file()
extraction_status = status.get("extraction_status", "UNKNOWN")
has_active_sessions = False
has_session_data = False

# Check if any session is running OR has data to display
for i in range(1, 5):
    session_key = f"session{i}_status"
    if session_key in status:
        has_session_data = True  # We have session data to display
        session_status = status[session_key].get("status", "")
        if session_status == "RUNNING":
            has_active_sessions = True
            # Don't break - continue checking all sessions

# ── PID-validated running check ──────────────────────────────────────────────
# Trust the JSON status ONLY if the actual OS processes are still alive.
# This prevents stale "RUNNING" state after a crash from locking the UI.
def pid_is_alive(pid):
    """Return True only if the OS process with this PID is still running."""
    try:
        if sys.platform == "win32":
            import subprocess as _sp
            r = _sp.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                        capture_output=True, text=True)
            return str(pid) in r.stdout
        else:
            import os as _os
            _os.kill(pid, 0)   # signal 0 = existence check only
            return True
    except (OSError, Exception):
        return False

# Check whether ANY stored PID is actually alive right now
stored_pids = status.get("session_pids", [])
any_pid_alive = any(pid_is_alive(p) for p in stored_pids) if stored_pids else False

# is_running is True ONLY when real OS processes back up the RUNNING status
is_running = (
    (extraction_status in ["RUNNING", "MERGING"] or has_active_sessions)
    and any_pid_alive
)

# Auto-heal: if JSON says RUNNING but no process is alive → mark CANCELLED
if (extraction_status in ["RUNNING", "MERGING"] or has_active_sessions) and not any_pid_alive and has_session_data:
    # Stale state from a crash — quietly rewrite status so UI unlocks
    try:
        from backend_pipeline import write_status as _write_status
        _write_status(extraction_status="CANCELLED",
                      extraction_completed="(auto-cleared after crash)",
                      sessions_cancelled="unknown")
        # Also clear individual session RUNNING flags
        stale_status = read_status_file()
        for _i in range(1, 5):
            _sk = f"session{_i}_status"
            if stale_status.get(_sk, {}).get("status") == "RUNNING":
                stale_status[_sk]["status"] = "CANCELLED"
        import json as _json
        _lock = str(STATUS_FILE) + ".lock"
        _tmp  = str(STATUS_FILE) + ".tmp"
        with open(_tmp, 'w', encoding='utf-8') as _f:
            _json.dump(stale_status, _f, indent=2)
        import os as _os_heal
        _os_heal.replace(_tmp, STATUS_FILE)
        # Re-read so the rest of this render cycle sees the healed state
        status = read_status_file()
        extraction_status = status.get("extraction_status", "CANCELLED")
        has_active_sessions = False
        is_running = False
    except Exception:
        pass  # Best-effort; UI will unlock on next rerun anyway

# Last Run Status
st.subheader("🕐 Last Run Status")

# Add timestamp of last update
current_time = time.time()
if has_active_sessions or extraction_status in ["RUNNING", "MERGING"]:
    st.caption(f"🔄 Auto-refreshing... Last update: {time.strftime('%H:%M:%S', time.localtime(current_time))}")

# Display status based on extraction status or active sessions
if extraction_status in ["RUNNING", "MERGING", "COMPLETED"] or has_active_sessions or has_session_data:
    # Show appropriate status message
    if extraction_status == "COMPLETED" and not has_active_sessions:
        st.success("✅ **Extraction Completed Successfully**")
        if "total_unique_bids" in status:
            st.metric("Total Unique Bids", status["total_unique_bids"])
    elif extraction_status in ["RUNNING", "MERGING"] or has_active_sessions:
        st.info(f"🔄 **Extraction Status: {extraction_status if extraction_status != 'UNKNOWN' else 'RUNNING'}**")
    
    # Show progress for each session (only while running)
    sessions_running = status.get("sessions_running", 0)
    sessions_completed = status.get("sessions_completed", 0)
    
    if sessions_running > 0 and has_active_sessions:
        progress = sessions_completed / sessions_running
        st.progress(progress)
        st.write(f"**Sessions Progress:** {sessions_completed}/{sessions_running} completed")
    
    # Individual session details - SHOW IF ANY SESSION DATA EXISTS (running or completed)
    st.markdown("### 📋 Session Details")
    cols = st.columns(4)
    
    sessions_found = False
    for i in range(1, 5):
        session_key = f"session{i}_status"
        if session_key in status:
            sessions_found = True
            session_data = status[session_key]
            with cols[i-1]:
                session_status = session_data.get("status", "UNKNOWN")
                keyword = session_data.get("keyword", "N/A")
                
                if session_status == "COMPLETED":
                    st.success(f"✅ Session {i}")
                elif session_status == "RUNNING":
                    st.info(f"⏳ Session {i}")
                else:
                    st.warning(f"⚪ Session {i}")
                
                st.write(f"**Keyword:** {keyword}")
                
                # Show metrics - use final counts for completed, current for running
                if session_status == "COMPLETED":
                    pages = session_data.get("total_pages", session_data.get("pages_processed", 0))
                    new_bids = session_data.get("total_new_bids", session_data.get("new_bids_found", 0))
                    total_bids = session_data.get("final_bid_count", session_data.get("total_bids_seen", 0))
                else:
                    pages = session_data.get("pages_processed", 0)
                    new_bids = session_data.get("new_bids_found", 0)
                    total_bids = session_data.get("total_bids_seen", 0)
                
                st.metric("Pages", pages)
                st.metric("New Bids", new_bids)
                st.metric("Total Seen", total_bids)
                
                # Show last update time if available
                if "started" in session_data:
                    st.caption(f"Started: {session_data['started'][:19]}")
    
    if not sessions_found:
        st.info("⏳ Waiting for session data...")
    
    # 🔍 Full Status JSON Debug Viewer (for troubleshooting)
    if status:
        with st.expander("🔍 View Full Status JSON (Debug)"):
            st.json(status)

    # Raw scraper output — this is where a launch failure (wrong
    # interpreter, missing Chromium, proxy error, crash traceback) actually
    # shows up, since the scraper's own stdout/stderr now go to these files.
    with st.expander("🛠️ Scraper debug logs"):
        _log_files = sorted(DATA_DIR.glob("scraper_session*_stdout.log")) + \
                     sorted(DATA_DIR.glob("scraper_session*_debug.log"))
        if not _log_files:
            st.caption("No scraper logs yet.")
        for _lf in _log_files:
            st.caption(_lf.name)
            try:
                st.code(_lf.read_text(encoding="utf-8", errors="replace")[-3000:] or "(empty)")
            except Exception as _e:
                st.caption(f"Could not read {_lf.name}: {_e}")

    st.divider()

elif extraction_status == "CANCELLED":
    st.warning("⚠️ **Extraction was cancelled**")
    if "sessions_cancelled" in status:
        st.write(f"Sessions cancelled: {status['sessions_cancelled']}")

elif extraction_status == "FAILED":
    st.error("❌ **Extraction Failed**")
    if "error" in status:
        st.error(status["error"])

else:
    st.info("No extraction has been run yet.")

st.divider()

# Control buttons
col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)

with col_btn1:
    if st.button(
        "🚀 Run 4 Parallel Scraping Sessions",
        disabled=is_running,
        type="primary"
    ):
        with st.spinner("Starting 4 parallel browser sessions..."):
            run_raw_extraction()
            st.success("✅ 4 sessions launched successfully!")
            time.sleep(1)
            st.rerun()

with col_btn2:
    if st.button(
        "🛑 Cancel Extraction",
        disabled=not is_running,
        type="secondary"
    ):
        if cancel_extraction():
            st.warning("⚠️ Extraction cancelled")
            time.sleep(1)
            st.rerun()
        else:
            st.error("No running sessions to cancel")

with col_btn3:
    if st.button(
        "📊 Run Analysis",
        disabled=is_running or not CSV_FILE.exists(),
        type="secondary"
    ):
        with st.spinner("Analyzing extracted data..."):
            prepare_analysis_excel()
            st.session_state.data_ready = True
            st.success("✅ Analysis complete!")
            time.sleep(1)
            st.rerun()

with col_btn4:
    # Always enabled — lets user escape from any stuck/stale state after a crash
    if st.button(
        "🔄 Reset Status",
        type="secondary",
        help="Use this if the scraper crashed and buttons are stuck. "
             "Clears the stale session state so you can start fresh."
    ):
        try:
            from backend_pipeline import write_status as _ws
            # Wipe entire status so every field starts clean
            import json as _j, os as _o
            _tmp = str(STATUS_FILE) + ".tmp"
            with open(_tmp, 'w', encoding='utf-8') as _f:
                _j.dump({"extraction_status": "CANCELLED",
                          "extraction_completed": f"(manually reset at {time.strftime('%Y-%m-%d %H:%M:%S')})"}, _f, indent=2)
            _o.replace(_tmp, STATUS_FILE)
            # Delete the lock file if it exists (safe — no process holds it now)
            _lock = Path(str(STATUS_FILE) + ".lock")
            if _lock.exists():
                _lock.unlink(missing_ok=True)
            st.success("✅ Status reset. You can now start a new scraping session.")
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Reset failed: {e}")

st.divider()

# 🟢 Live Scraper Status (Real-time)
# Show this section if:
# 1. Sessions are currently running (has_active_sessions)
# 2. OR sessions just completed and we have fresh data (has_session_data and extraction_status == "COMPLETED")
# 3. OR we're in MERGING state
show_live_status = (
    has_active_sessions or 
    (has_session_data and extraction_status in ["RUNNING", "MERGING", "COMPLETED"])
)

if show_live_status:
    # Use different title based on status
    if has_active_sessions:
        expander_title = "🟢 Live Scraper Status (Real-time)"
        is_expanded = True
    else:
        expander_title = "📊 Completed Session Summary"
        is_expanded = False
    
    with st.expander(expander_title, expanded=is_expanded):
        st.markdown("### 📊 Session Progress")
        
        # Create a table showing real-time progress
        progress_data = []
        for i in range(1, 5):
            session_key = f"session{i}_status"
            if session_key in status:
                session_data = status[session_key]
                session_status = session_data.get("status", "UNKNOWN")
                
                # Use appropriate data fields based on completion status
                if session_status == "COMPLETED":
                    pages = session_data.get("total_pages", session_data.get("pages_processed", 0))
                    new_bids = session_data.get("total_new_bids", session_data.get("new_bids_found", 0))
                    total_bids = session_data.get("final_bid_count", session_data.get("total_bids_seen", 0))
                else:
                    pages = session_data.get("pages_processed", 0)
                    new_bids = session_data.get("new_bids_found", 0)
                    total_bids = session_data.get("total_bids_seen", 0)
                
                progress_data.append({
                    "Session": i,
                    "Status": "🟢 Running" if session_status == "RUNNING" else "✅ Done",
                    "Keyword": session_data.get("keyword", "N/A"),
                    "Pages": pages,
                    "New Bids": new_bids,
                    "Total Bids": total_bids,
                })
        
        if progress_data:
            progress_df = pd.DataFrame(progress_data)
            st.dataframe(progress_df, use_container_width=True, hide_index=True)
        
        st.markdown("### 📜 Recent Log Entries")
        log_lines = read_last_lines(LOG_FILE, n=20)
        st.code("".join(log_lines), language="log")
        
        if has_active_sessions:
            st.caption(f"⏱️ Auto-refreshing every 2 seconds... Current time: {time.strftime('%H:%M:%S')}")
        else:
            st.caption(f"✅ All sessions completed at: {status.get('extraction_completed', 'N/A')}")

# 📋 Final Session Summary (Only after completion)
if extraction_status == "COMPLETED" and has_session_data and not has_active_sessions:
    with st.expander("📋 Final Session Summary", expanded=False):
        st.markdown("### Session Performance Breakdown")
        
        cols = st.columns(4)
        total_pages = 0
        total_new = 0
        total_seen = 0
        
        for i in range(1, 5):
            session_key = f"session{i}_status"
            if session_key in status:
                session_data = status[session_key]
                
                # Use final counts
                pages = session_data.get("total_pages", session_data.get("pages_processed", 0))
                new_bids = session_data.get("total_new_bids", session_data.get("new_bids_found", 0))
                total_bids = session_data.get("final_bid_count", session_data.get("total_bids_seen", 0))
                
                total_pages += pages
                total_new += new_bids
                total_seen += total_bids
                
                with cols[i-1]:
                    st.success(f"✅ Session {i}")
                    st.write(f"**Keyword:** {session_data.get('keyword', 'N/A')}")
                    st.metric("Pages Processed", pages)
                    st.metric("New Bids Found", new_bids)
                    st.metric("Total Bids Seen", total_bids)
                    
                    if "started" in session_data and "completed" in session_data:
                        st.caption(f"Duration: {session_data.get('started', '')[:19]} - {session_data.get('completed', '')[:19]}")
        
        st.divider()
        
        # Overall totals
        st.markdown("### 📊 Combined Totals")
        col_t1, col_t2, col_t3, col_t4 = st.columns(4)
        
        with col_t1:
            st.metric("Total Pages", total_pages)
        with col_t2:
            st.metric("Total New Bids", total_new)
        with col_t3:
            st.metric("Total Bids Seen", total_seen)
        with col_t4:
            unique_bids = status.get("total_unique_bids", "N/A")
            st.metric("Unique After Deduplication", unique_bids)

st.divider()

# Data Analysis Dashboard
st.header("📊 Data Analysis Dashboard")

if EXCEL_FILE.exists():
    st.session_state.data_ready = True

if st.session_state.data_ready and EXCEL_FILE.exists():
    # Load checked bids
    checked_bids = load_checked_bids()
    
    # Load Excel data
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)
    
    # Add checked status columns
    df['Checked'] = df['Bid No'].astype(str).apply(
        lambda x: '✅ Checked' if x in checked_bids else '⬜ Not Checked'
    )
    df['Checked On'] = df['Bid No'].astype(str).apply(
        lambda x: checked_bids.get(x, {}).get('checked_date', '') if x in checked_bids else ''
    )
    
    # ────────────────────────────────────────
    # Summary Metrics Section (Before Filters)
    # ────────────────────────────────────────
    st.subheader("📌 Overall Summary")
    
    total_bids = len(df)
    total_checked = len([bid for bid in df['Bid No'].astype(str) if str(bid) in checked_bids])
    total_unchecked = total_bids - total_checked
    overall_completion = (total_checked / total_bids * 100) if total_bids > 0 else 0
    
    col_sum1, col_sum2, col_sum3, col_sum4 = st.columns(4)
    
    with col_sum1:
        st.metric("📊 Total Bids", total_bids)
    
    with col_sum2:
        st.metric("✅ Checked", total_checked)
    
    with col_sum3:
        st.metric("⬜ Unchecked", total_unchecked)
    
    with col_sum4:
        st.metric("🎯 Overall Completion", f"{overall_completion:.1f}%")
    
    st.divider()
    
    # Filters
    col_filter1, col_filter2, col_filter3 = st.columns([2, 2, 2])
    
    with col_filter1:
        unique_groups = ["All"] + sorted(df["Keyword Group"].dropna().unique().tolist())
        selected_group = st.selectbox(
            "Filter by Keyword Group",
            unique_groups,
            index=unique_groups.index(st.session_state.current_filter)
        )
        st.session_state.current_filter = selected_group
    
    with col_filter2:
        check_filter = st.selectbox(
            "Filter by Check Status",
            options=["All", "Checked Only", "Unchecked Only"]
        )
    
    with col_filter3:
        st.metric("Total Bids", len(df))
        checked_count = len([bid for bid in df['Bid No'].astype(str) if str(bid) in checked_bids])
        completion_pct = (checked_count / len(df) * 100) if len(df) > 0 else 0
        
        col_metric1, col_metric2 = st.columns(2)
        with col_metric1:
            st.metric("Checked", f"{checked_count}/{len(df)}")
        with col_metric2:
            st.metric("Completion %", f"{completion_pct:.1f}%")
    
    # Apply filters
    if selected_group != "All":
        df_view = df[df["Keyword Group"] == selected_group].copy()
    else:
        df_view = df.copy()
    
    # Apply check status filter
    if check_filter == "Checked Only":
        df_view = df_view[df_view['Checked'] == '✅ Checked']
    elif check_filter == "Unchecked Only":
        df_view = df_view[df_view['Checked'] == '⬜ Not Checked']
    
    st.info(f"Showing {len(df_view)} bids")
    
    # Display dataframe with styled columns
    display_columns = [
        "Bid No",
        "Checked",
        "Checked On",
        "Items",
        "Quantity",
        "Keyword Group",
        "Department Name And Address",
        "Start Date",
        "End Date",
        "New Today",
        "End Date Changed"
    ]
    
    # Create a styled dataframe
    def highlight_checked_rows(row):
        if row['Checked'] == '✅ Checked':
            return ['background-color: #fffacd'] * len(row)  # Light yellow
        return [''] * len(row)
    
    styled_df = df_view[display_columns].style.apply(highlight_checked_rows, axis=1)
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        height=400
    )
    
    st.divider()
    
    # ────────────────────────────────────────
    # Interactive Bid Checking Section
    # ────────────────────────────────────────
    st.subheader("✅ Mark Bids as Checked")
    
    col_check1, col_check2 = st.columns([3, 1])
    
    with col_check1:
        st.markdown("""
        **How to use:**
        - Select bid(s) from the dropdown below
        - Click "Mark as Checked" to tag them (they'll turn yellow)
        - Click "Unmark Selected" to remove the check tag
        - Checked bids persist across sessions
        """)
    
    with col_check2:
        # Bulk operations
        if st.button("✅ Mark All Visible as Checked", help="Mark all currently filtered bids as checked"):
            for bid_no in df_view['Bid No'].astype(str):
                mark_bid_checked(bid_no)
            st.success(f"Marked {len(df_view)} bids as checked!")
            time.sleep(1)
            st.rerun()
    
    # Multi-select for bids
    bid_options = []
    for idx, row in df_view.iterrows():
        bid_no = row['Bid No']
        items = str(row['Items'])[:60]
        bid_options.append(f"{bid_no} - {items}...")
    
    bid_to_items_map = dict(zip(
        df_view['Bid No'].astype(str),
        df_view['Items']
    ))
    
    selected_bids_display = st.multiselect(
        "Select Bid(s) to Mark/Unmark",
        options=bid_options,
        help="You can select multiple bids"
    )
    
    # Extract actual bid numbers from selections
    selected_bid_nos = [item.split(' - ')[0] for item in selected_bids_display]
    
    if selected_bid_nos:
        col_btn1, col_btn2, col_btn3 = st.columns(3)
        
        with col_btn1:
            if st.button("✅ Mark as Checked", type="primary"):
                for bid_no in selected_bid_nos:
                    mark_bid_checked(bid_no)
                st.success(f"Marked {len(selected_bid_nos)} bid(s) as checked!")
                time.sleep(1)
                st.rerun()
        
        with col_btn2:
            if st.button("❌ Unmark Selected"):
                for bid_no in selected_bid_nos:
                    unmark_bid_checked(bid_no)
                st.info(f"Unmarked {len(selected_bid_nos)} bid(s)")
                time.sleep(1)
                st.rerun()
        
        with col_btn3:
            # Show details of selected bids
            st.write(f"**Selected:** {len(selected_bid_nos)} bid(s)")
    
    st.divider()
    
    # ────────────────────────────────────────
    # Quick Stats by Keyword Group
    # ────────────────────────────────────────
    st.subheader("📊 Check Status by Keyword Group")
    
    group_stats = []
    for group in sorted(df["Keyword Group"].unique()):
        group_df = df[df["Keyword Group"] == group]
        total = len(group_df)
        checked = len([bid for bid in group_df['Bid No'].astype(str) if str(bid) in checked_bids])
        unchecked = total - checked
        pct = (checked / total * 100) if total > 0 else 0
        
        group_stats.append({
            'Keyword Group': group,
            'Total': total,
            'Checked': checked,
            'Unchecked': unchecked,
            'Completion %': f"{pct:.1f}%"
        })
    
    stats_df = pd.DataFrame(group_stats)
    st.dataframe(stats_df, use_container_width=True)
    
    st.divider()
    
    # Download buttons
    col_dl1, col_dl2, col_dl3 = st.columns(3)
    
    with col_dl1:
        # Download filtered data
        csv_data = df_view.to_csv(index=False)
        st.download_button(
            label="📥 Download Filtered Data (CSV)",
            data=csv_data,
            file_name=f"gem_bids_{selected_group.lower().replace(' ', '_')}.csv",
            mime="text/csv"
        )
    
    with col_dl2:
        # Download checked bids only
        checked_df = df[df['Checked'] == '✅ Checked']
        if len(checked_df) > 0:
            checked_csv = checked_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Checked Bids (CSV)",
                data=checked_csv,
                file_name="gem_bids_checked.csv",
                mime="text/csv"
            )
    
    with col_dl3:
        # Download unchecked bids only
        unchecked_df = df[df['Checked'] == '⬜ Not Checked']
        if len(unchecked_df) > 0:
            unchecked_csv = unchecked_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Unchecked Bids (CSV)",
                data=unchecked_csv,
                file_name="gem_bids_unchecked.csv",
                mime="text/csv"
            )

else:
    st.info("Run extraction and analysis to populate the dashboard.")
    
    # Show info about the 4 session approach
    with st.expander("ℹ️ How does parallel scraping work?"):
        st.markdown("""
        ### 4-Session Parallel Scraping
        
        When you click **"Run 4 Parallel Scraping Sessions"**, the system:
        
        1. **Launches 4 independent browser sessions** simultaneously
        2. Each session searches with a different keyword:
           - Session 1: "battery"
           - Session 2: "batter"
           - Session 3: "lead acid"
           - Session 4: "traction"
        3. Each session writes to its own CSV file (`session1.csv`, `session2.csv`, etc.)
        4. **Data integrity is maintained** - each session tracks its own state
        5. **Process isolation** - sessions run independently and can't interfere with each other
        6. Once all 4 sessions complete, the system automatically:
           - **Merges** all 4 CSV files into one master file
           - **Removes duplicates** based on Bid No (keeping latest End Date)
           - **Preserves First Seen Date** from earliest occurrence
        7. The merged, deduplicated data is ready for analysis
        
        **Benefits:**
        - 🚀 **4x faster** data collection
        - 🔍 **Broader coverage** with multiple search terms
        - 🛡️ **No data loss** - deduplication preserves all unique bids
        - 🔄 **Reliable** - each session is independent
        """)
    
    with st.expander("✅ How does bid checking work?"):
        st.markdown("""
        ### Interactive Bid Checking System
        
        **Purpose:** Track which bids you've already reviewed to avoid re-checking the same bids daily.
        
        **How it works:**
        1. After running analysis, bids appear in the dashboard
        2. Select bid(s) you want to mark as "checked"
        3. Click "Mark as Checked" - they turn **yellow** and show a ✅
        4. The check status is **saved permanently** in `checked_bids.json`
        5. Next day when you run the scraper again:
           - Previously checked bids remain marked (yellow)
           - New bids appear unmarked (white)
           - You can instantly see which bids need review
        
        **Features:**
        - ✅ **Persistent tracking** - checks survive across sessions
        - 🎨 **Visual highlighting** - yellow background for checked bids
        - 📊 **Progress tracking** - see completion % for each category
        - 🔍 **Smart filtering** - show only checked/unchecked bids
        - 📥 **Separate exports** - download checked/unchecked bids separately
        
        **The system does NOT modify your scraping data** - it maintains a separate tracking file.
        """)

# ────────────────────────────────────────
# AUTO-REFRESH LOGIC (must be at the very end)
# ────────────────────────────────────────
# CRITICAL FIX: Use pre-computed is_running to avoid re-reading status file
# This ensures consistency throughout the entire rerun cycle
if is_running or has_active_sessions:
    # Update timestamp
    st.session_state.last_update_time = time.time()
    
    # Sleep for 2 seconds then rerun to get fresh data
    time.sleep(2)
    st.rerun()