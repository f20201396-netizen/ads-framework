#!/usr/bin/env python3
"""
Wake-sync daemon — runs the full sheet refresh whenever the Mac wakes from sleep.

How it works:
  - Polls `sysctl kern.waketime` every 60 s to detect a new wake event.
  - On wake, waits 60 s for network to come up, then runs sync + WoW report.
  - Skips if a sync already ran within the last 10 minutes (double-wake guard).
  - Logs to ~/Library/Logs/univest-wake-sync.log

Setup (run once):
    python3 scripts/wake_sync.py --install
    # then to start immediately:
    launchctl load ~/Library/LaunchAgents/in.univest.ads.wake-sync.plist

Uninstall:
    launchctl unload ~/Library/LaunchAgents/in.univest.ads.wake-sync.plist
    rm ~/Library/LaunchAgents/in.univest.ads.wake-sync.plist
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_DIR   = Path(__file__).parent.parent.resolve()
PYTHON        = str(PROJECT_DIR / ".venv" / "bin" / "python3")
SYNC_SCRIPT   = str(PROJECT_DIR / "scripts" / "sync.py")
WOW_SCRIPT    = str(PROJECT_DIR / "scripts" / "wow_report.py")
LOG_FILE      = Path.home() / "Library" / "Logs" / "univest-wake-sync.log"
PLIST_PATH    = Path.home() / "Library" / "LaunchAgents" / "in.univest.ads.wake-sync.plist"
PLIST_LABEL   = "in.univest.ads.wake-sync"

POLL_INTERVAL    = 60         # seconds between wake checks
NETWORK_WAIT     = 60         # seconds to wait after wake for network
MIN_SYNC_GAP     = 600        # seconds — don't re-sync if last sync was < 10 min ago
MAX_SYNC_GAP     = 6 * 3600   # seconds — force a sync if it's been ≥ 6h since the last one

PROSP_SHEET_ID = "1RI_C29egX2LITxPYU9GIkESNcDmFcMr32EPMoyE6RrI"
ANDROID_SHEET_ID = "1EBu7vZWGdLUVdL4I6a0J22soLIoXKWWIRRWTGk3BZ7s"

PLIST_CONTENT = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{PLIST_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{PYTHON}</string>
        <string>{PROJECT_DIR}/scripts/wake_sync.py</string>
        <string>--run</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{PROJECT_DIR}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{LOG_FILE}</string>
    <key>StandardErrorPath</key>
    <string>{LOG_FILE}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)


def get_wake_time() -> str | None:
    """Return kern.waketime as a string, or None on error."""
    try:
        r = subprocess.run(
            ["sysctl", "-n", "kern.waketime"],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def run_sync():
    log("Running sync...")
    result = subprocess.run(
        [PYTHON, SYNC_SCRIPT,
         "--prosp-sheet-id", PROSP_SHEET_ID,
         "--skip-change-log"],
        cwd=PROJECT_DIR,
        capture_output=False,
        timeout=900,
    )
    if result.returncode != 0:
        log(f"sync.py exited with code {result.returncode}")
    else:
        log("sync.py done.")

    log("Running WoW report...")
    result = subprocess.run(
        [PYTHON, WOW_SCRIPT, "--sheet-id", ANDROID_SHEET_ID],
        cwd=PROJECT_DIR,
        capture_output=False,
        timeout=120,
    )
    if result.returncode != 0:
        log(f"wow_report.py exited with code {result.returncode}")
    else:
        log("wow_report.py done.")


# ── Daemon loop ───────────────────────────────────────────────────────────────

def run_daemon():
    log(f"Wake-sync daemon started (PID {os.getpid()}). "
        f"Polling every {POLL_INTERVAL}s; auto-sync every {MAX_SYNC_GAP // 3600}h.")
    last_wake = get_wake_time()
    # Pretend we just synced at startup so the first timer-triggered sync fires
    # MAX_SYNC_GAP from now, not immediately on daemon boot.
    last_sync_at = time.time()

    while True:
        time.sleep(POLL_INTERVAL)
        current_wake = get_wake_time()
        now = time.time()
        elapsed = now - last_sync_at

        woke_up   = current_wake and current_wake != last_wake
        timer_due = elapsed >= MAX_SYNC_GAP

        if not (woke_up or timer_due):
            continue

        if woke_up:
            last_wake = current_wake

        if elapsed < MIN_SYNC_GAP:
            log(f"Trigger ({'wake' if woke_up else 'timer'}) but last sync was "
                f"{int(elapsed)}s ago — skipping.")
            continue

        if woke_up:
            log(f"Wake detected (kern.waketime={current_wake}). "
                f"Waiting {NETWORK_WAIT}s for network...")
            time.sleep(NETWORK_WAIT)
        else:
            log(f"6-hour timer fired ({int(elapsed)}s since last sync).")

        last_sync_at = time.time()
        try:
            run_sync()
        except Exception as e:
            log(f"ERROR during sync: {e}")


# ── Install ───────────────────────────────────────────────────────────────────

def install():
    PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    PLIST_PATH.write_text(PLIST_CONTENT)
    print(f"Wrote plist → {PLIST_PATH}")
    print()
    print("To activate now, run:")
    print(f"  launchctl load {PLIST_PATH}")
    print()
    print("To stop:")
    print(f"  launchctl unload {PLIST_PATH}")
    print()
    print(f"Logs: tail -f {LOG_FILE}")


# ── Entry ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--install", action="store_true",
                        help="Write the launchd plist and print activation instructions")
    parser.add_argument("--run", action="store_true",
                        help="Start the daemon (called by launchd)")
    args = parser.parse_args()

    if args.install:
        install()
    elif args.run:
        run_daemon()
    else:
        parser.print_help()
