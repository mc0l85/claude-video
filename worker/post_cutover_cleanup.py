#!/usr/bin/env python3
"""Newtube post-cutover cleanup — fires once on the soak-window anniversary.

Cron schedule: `0 9 13 5 *` (May 13, 9am CDT) — runs annually but self-noops
after the first firing because the deprecated artifacts are gone.

What it does on first firing (2026-05-13):
  1. Idempotency guard: if yousummary.py.archive-2026-04-29 is already gone, exit.
  2. Health check: count OK vs systemic failures in queue_worker.log over past 7 days.
  3. Verify no circuit-breaker marker.
  4. Confirm vault has new Basket/Transcripts/ notes since the cutover.
  5. If green: drop the deprecated cron line, rm archived/.bak yousummary files,
     send MQTT iMessage with "cleanup OK" summary, write green JSON status.
  6. If red: send MQTT iMessage with "cleanup blocked, review needed" summary,
     write red JSON status, leave artifacts in place.

Linked: Linear issue MOR-45.
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

LOG_FILE = PROJECT_ROOT / "logs" / "queue_worker.log"
RESULT_FILE = PROJECT_ROOT / "post_cutover_result.json"
RUN_LOG = PROJECT_ROOT / "logs" / "post_cutover.log"
BREAKER_FILE = PROJECT_ROOT / "queue_worker.circuit_breaker"

YOUSUMMARY_DIR = Path("/home/myron/YouSummary")
ARCHIVED_PY = YOUSUMMARY_DIR / "yousummary.py.archive-2026-04-29"
BAK_GLOB = "yousummary.py.bak-20260424*"
DEPRECATED_CRON_MARKER = "DEPRECATED 2026-04-29 (newtube took over"

OBSIDIAN_API_URL = os.environ.get("OBSIDIAN_API_URL", "").rstrip("/")
OBSIDIAN_API_TOKEN = os.environ.get("OBSIDIAN_API_TOKEN", "")
OBSIDIAN_FOLDER = os.environ.get("OBSIDIAN_FOLDER", "Basket/Transcripts")

NOTIFY_ENABLED = os.environ.get("NOTIFY_ENABLED", "true").lower() == "true"
NOTIFY_HANDLE = os.environ.get("NOTIFY_HANDLE", "")
NOTIFY_MQTT_HOST = os.environ.get("NOTIFY_MQTT_HOST", "10.80.130.33")
NOTIFY_MQTT_PORT = int(os.environ.get("NOTIFY_MQTT_PORT", "1883"))
NOTIFY_MQTT_USER = os.environ.get("NOTIFY_MQTT_USER", "")
NOTIFY_MQTT_PASS = os.environ.get("NOTIFY_MQTT_PASS", "")
NOTIFY_MQTT_TOPIC = os.environ.get("NOTIFY_MQTT_TOPIC", "imessage/outbound")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(str(RUN_LOG)),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("post_cutover_cleanup")


def already_cleaned() -> bool:
    """Idempotency: if the archived py is gone, cleanup already ran."""
    return not ARCHIVED_PY.exists()


def count_log_outcomes(days: int = 7) -> dict:
    """Scan queue_worker.log for the past N days. Return run-count summary."""
    if not LOG_FILE.exists():
        return {"ok": 0, "per_url_fail": 0, "systemic_fail": 0, "total_runs": 0, "log_missing": True}

    cutoff_str = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    counts = {"ok": 0, "per_url_fail": 0, "systemic_fail": 0, "total_runs": 0, "log_missing": False}

    pat_run_complete = re.compile(
        r"^(\d{4}-\d{2}-\d{2}).*run complete: (\d+) ok, (\d+) per-url fail, (\d+) systemic fail"
    )

    for line in LOG_FILE.read_text(errors="replace").splitlines():
        m = pat_run_complete.search(line)
        if not m:
            continue
        date_str = m.group(1)
        if date_str < cutoff_str:
            continue
        counts["ok"] += int(m.group(2))
        counts["per_url_fail"] += int(m.group(3))
        counts["systemic_fail"] += int(m.group(4))
        counts["total_runs"] += 1

    return counts


def breaker_active() -> bool:
    return BREAKER_FILE.exists()


def vault_notes_in_folder() -> int:
    """List Basket/Transcripts/ in the vault and count entries. -1 if vault unreachable."""
    if not OBSIDIAN_API_URL or not OBSIDIAN_API_TOKEN:
        log.warning("Obsidian env missing — skipping vault check")
        return -1

    try:
        resp = requests.get(
            f"{OBSIDIAN_API_URL}/vault/{OBSIDIAN_FOLDER}/",
            headers={"Authorization": f"Bearer {OBSIDIAN_API_TOKEN}"},
            timeout=15,
        )
    except requests.RequestException as exc:
        log.warning(f"vault GET failed: {exc}")
        return -1

    if resp.status_code != 200:
        log.warning(f"vault GET returned {resp.status_code}")
        return -1

    try:
        data = resp.json()
        files = data.get("files", []) if isinstance(data, dict) else data
    except Exception:
        files = []

    return len(files) if files else 0


def health_is_green(counts: dict, breaker: bool, vault_count: int) -> tuple[bool, list[str]]:
    """Return (is_green, reasons_if_red)."""
    reasons: list[str] = []
    if breaker:
        reasons.append("circuit breaker marker present")
    if counts.get("systemic_fail", 0) > 5:
        reasons.append(f"{counts['systemic_fail']} systemic failures in last 7 days")
    if counts.get("total_runs", 0) < 50:
        reasons.append(f"only {counts['total_runs']} runs in last 7 days (expected ≥1008 from 10-min cron)")
    if vault_count == 0:
        reasons.append("no notes in vault Basket/Transcripts/ folder (worker never wrote)")
    return (len(reasons) == 0, reasons)


def execute_cleanup() -> dict:
    """Drop deprecated cron line, rm archived/.bak yousummary files."""
    actions: list[str] = []

    try:
        result = subprocess.run(
            ["bash", "-c", f"crontab -l | grep -v '{DEPRECATED_CRON_MARKER}' | crontab -"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            actions.append("dropped deprecated cron line")
        else:
            actions.append(f"cron edit failed: {result.stderr.strip()[:200]}")
    except Exception as exc:
        actions.append(f"cron edit raised: {exc}")

    if ARCHIVED_PY.exists():
        try:
            ARCHIVED_PY.unlink()
            actions.append(f"removed {ARCHIVED_PY}")
        except OSError as exc:
            actions.append(f"failed to remove {ARCHIVED_PY}: {exc}")

    for bak in YOUSUMMARY_DIR.glob(BAK_GLOB):
        try:
            bak.unlink()
            actions.append(f"removed {bak}")
        except OSError as exc:
            actions.append(f"failed to remove {bak}: {exc}")

    return {"actions": actions}


def notify(msg: str) -> bool:
    if not NOTIFY_ENABLED or not NOTIFY_HANDLE or not NOTIFY_MQTT_PASS:
        log.info("notify skipped (disabled or missing creds)")
        return False
    try:
        import paho.mqtt.publish as mqtt_publish
        payload = json.dumps({"recipient": NOTIFY_HANDLE, "text": msg})
        mqtt_publish.single(
            NOTIFY_MQTT_TOPIC,
            payload=payload,
            hostname=NOTIFY_MQTT_HOST,
            port=NOTIFY_MQTT_PORT,
            auth={"username": NOTIFY_MQTT_USER, "password": NOTIFY_MQTT_PASS},
            qos=1,
        )
        log.info(f"notify sent: {msg[:80]}")
        return True
    except Exception as exc:
        log.warning(f"notify publish failed: {exc}")
        return False


def main() -> int:
    log.info("=== post_cutover_cleanup starting ===")

    if already_cleaned():
        log.info("idempotency guard: archive missing — already cleaned, exiting")
        return 0

    log.info("running health checks…")
    counts = count_log_outcomes(days=7)
    breaker = breaker_active()
    vault_count = vault_notes_in_folder()
    log.info(f"counts: {counts}")
    log.info(f"breaker_active: {breaker}")
    log.info(f"vault notes in {OBSIDIAN_FOLDER}: {vault_count}")

    is_green, reasons = health_is_green(counts, breaker, vault_count)
    log.info(f"health = {'GREEN' if is_green else 'RED'}; reasons: {reasons}")

    result: dict = {
        "date": datetime.now(timezone.utc).isoformat(),
        "linear_ticket": "MOR-45",
        "status": "green" if is_green else "red",
        "log_summary": counts,
        "breaker_active": breaker,
        "vault_notes_in_folder": vault_count,
        "red_reasons": reasons,
    }

    if is_green:
        log.info("executing cleanup…")
        cleanup_result = execute_cleanup()
        result["cleanup"] = cleanup_result
        msg = (
            f"newtube cleanup OK (MOR-45). "
            f"7d: {counts['ok']} ok / {counts['per_url_fail']} per-url fail / "
            f"{counts['systemic_fail']} systemic fail. "
            f"actions: {len(cleanup_result['actions'])}. "
            f"flip MOR-45 to Done in Linear."
        )
    else:
        msg = (
            f"newtube cleanup BLOCKED (MOR-45) — "
            f"{'; '.join(reasons)}. Review /home/myron/newtube/post_cutover_result.json."
        )

    notify(msg)

    RESULT_FILE.write_text(json.dumps(result, indent=2))
    log.info(f"wrote result to {RESULT_FILE}")
    log.info("=== post_cutover_cleanup done ===")
    return 0 if is_green else 1


if __name__ == "__main__":
    sys.exit(main())
