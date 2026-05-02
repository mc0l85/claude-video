#!/usr/bin/env python3
"""Newtube queue worker — replaces yousummary.py.

Polls the xscribe.txt queue every cron tick, runs each URL through watch.py
(frames + transcript), summarizes via Claude (Sonnet 4.6), and writes an
Obsidian note. Inherits yousummary's lock, circuit-breaker, and MQTT-alert
patterns; adds frame extraction (the upstream claude-video differentiator).

Per-URL flags supported in xscribe.txt (space-separated after URL):
  :audio                       transcript only — frames extracted but not handed to Claude
  :zoom=MM:SS-MM:SS            extract frames only from this window
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
WORKER_DIR = PROJECT_ROOT / "worker"

sys.path.insert(0, str(WORKER_DIR))
load_dotenv(PROJECT_ROOT / ".env")

import obsidian_writer  # noqa: E402

# --- Config ---
TEXT_FILE_URL = os.environ.get("TEXT_FILE_URL", "http://www.ng0m.com/myt/xscribe.txt")
CLEANUP_URL = os.environ.get("CLEANUP_URL", "http://www.ng0m.com/myt/remove.php")
CLAUDE_CLI_PATH = os.environ.get("CLAUDE_CLI_PATH", "claude")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

CLAUDE_MAX_BUDGET_USD = float(os.environ.get("CLAUDE_MAX_BUDGET_USD", "2.0"))
CLAUDE_TIMEOUT = int(os.environ.get("CLAUDE_TIMEOUT", str(15 * 60)))  # 15 min

# Long-video chunked extraction (Change 2 — yousummary parity).
# When transcript exceeds threshold, pre-process via Haiku into structured
# Topics/Key Points/Examples/Quotes per chunk, swap the watch_report transcript
# for that intermediate, then run final synthesis. Set NEWTUBE_LONG_THRESHOLD=0
# to disable.
LONG_VIDEO_CHAR_THRESHOLD = int(os.environ.get("NEWTUBE_LONG_THRESHOLD", "12000"))
CHUNK_TARGET_CHARS = int(os.environ.get("NEWTUBE_CHUNK_CHARS", "5000"))
CHUNK_MODEL = os.environ.get("NEWTUBE_CHUNK_MODEL", "claude-haiku-4-5")
CHUNK_TIMEOUT = int(os.environ.get("NEWTUBE_CHUNK_TIMEOUT", "180"))

# Fabric extract_wisdom enrichment (Change 3 — production-side richer notes).
# Splices a SUMMARY/IDEAS/INSIGHTS/QUOTES/HABITS/FACTS/REFERENCES block between
# the model's body and the appended raw transcript. Cadence-fixed prompt lives
# alongside this file. Set NEWTUBE_WISDOM=0 to disable.
WISDOM_ENABLED = os.environ.get("NEWTUBE_WISDOM", "1") == "1"
WISDOM_TIMEOUT = int(os.environ.get("NEWTUBE_WISDOM_TIMEOUT", "300"))
WISDOM_MODEL = os.environ.get("NEWTUBE_WISDOM_MODEL", "claude-haiku-4-5")
WISDOM_SYSTEM_PROMPT_PATH = WORKER_DIR / "extract_wisdom_system.md"

DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", str(20 * 60)))
HTTP_REQUEST_TIMEOUT = int(os.environ.get("HTTP_REQUEST_TIMEOUT", "30"))
LOCK_FILE_AGE_LIMIT = int(os.environ.get("LOCK_FILE_AGE_LIMIT", str(6 * 3600)))

CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", "5"))
CIRCUIT_BREAKER_PAUSE_SECONDS = int(os.environ.get("CIRCUIT_BREAKER_PAUSE_SECONDS", str(30 * 60)))

NOTIFY_ENABLED = os.environ.get("NOTIFY_ENABLED", "true").lower() == "true"
NOTIFY_HANDLE = os.environ.get("NOTIFY_HANDLE", "")
NOTIFY_MQTT_HOST = os.environ.get("NOTIFY_MQTT_HOST", "10.80.130.33")
NOTIFY_MQTT_PORT = int(os.environ.get("NOTIFY_MQTT_PORT", "1883"))
NOTIFY_MQTT_USER = os.environ.get("NOTIFY_MQTT_USER", "")
NOTIFY_MQTT_PASS = os.environ.get("NOTIFY_MQTT_PASS", "")
NOTIFY_MQTT_TOPIC = os.environ.get("NOTIFY_MQTT_TOPIC", "imessage/outbound")
NOTIFY_RATELIMIT_SECONDS = int(os.environ.get("NOTIFY_RATELIMIT_SECONDS", str(15 * 60)))

LOCK_FILE_PATH = PROJECT_ROOT / "queue_worker.lock"
CIRCUIT_BREAKER_MARKER_PATH = PROJECT_ROOT / "queue_worker.circuit_breaker"
NOTIFY_RATELIMIT_PATH = PROJECT_ROOT / "queue_worker.notify_ratelimit"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# --- Logging ---
logger = logging.getLogger("newtube.worker")
logger.setLevel(logging.INFO)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
logger.addHandler(_console)
_file = RotatingFileHandler(
    str(LOG_DIR / "queue_worker.log"),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_file.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
logger.addHandler(_file)

logging.getLogger("newtube.obsidian").setLevel(logging.INFO)

http_session = requests.Session()


# --- Lock file ---
def check_stale_lock() -> bool:
    if not LOCK_FILE_PATH.exists():
        return True

    try:
        content = LOCK_FILE_PATH.read_text()
        pid_match = re.search(r"PID:\s*(\d+)", content)
        lock_pid = int(pid_match.group(1)) if pid_match else None
        age = time.time() - LOCK_FILE_PATH.stat().st_mtime
        logger.info(f"existing lock (age={age / 3600:.2f}h, pid={lock_pid})")

        if age > LOCK_FILE_AGE_LIMIT:
            if lock_pid:
                try:
                    os.kill(lock_pid, 0)
                    logger.warning(f"pid {lock_pid} still running despite stale lock — exiting")
                    return False
                except OSError:
                    logger.warning(f"pid {lock_pid} not running — removing stale lock")
                    LOCK_FILE_PATH.unlink()
                    return True
            LOCK_FILE_PATH.unlink()
            return True

        if lock_pid:
            try:
                os.kill(lock_pid, 0)
                logger.info(f"another instance (pid {lock_pid}) running — exiting")
                return False
            except OSError:
                logger.warning(f"pid {lock_pid} dead but lock recent — removing orphan")
                LOCK_FILE_PATH.unlink()
                return True
        return False

    except Exception as exc:
        logger.error(f"lock check failed: {exc}")
        return False


def create_lock_file() -> bool:
    try:
        LOCK_FILE_PATH.write_text(
            f"PID: {os.getpid()}\nStarted: {time.ctime()}\nScript: {Path(__file__).absolute()}\n"
        )
        return True
    except Exception as exc:
        logger.error(f"create lock failed: {exc}")
        return False


def remove_lock_file() -> None:
    try:
        if LOCK_FILE_PATH.exists():
            LOCK_FILE_PATH.unlink()
    except Exception as exc:
        logger.error(f"remove lock failed: {exc}")


# --- Circuit breaker ---
def write_circuit_breaker_marker() -> None:
    try:
        CIRCUIT_BREAKER_MARKER_PATH.write_text(str(time.time()))
        logger.warning(
            f"circuit breaker tripped — pausing for {CIRCUIT_BREAKER_PAUSE_SECONDS // 60} min"
        )
    except Exception as exc:
        logger.error(f"breaker write failed: {exc}")


def clear_circuit_breaker_marker() -> None:
    try:
        if CIRCUIT_BREAKER_MARKER_PATH.exists():
            CIRCUIT_BREAKER_MARKER_PATH.unlink()
    except Exception as exc:
        logger.error(f"breaker clear failed: {exc}")


def should_pause_circuit_breaker() -> tuple[bool, int]:
    if not CIRCUIT_BREAKER_MARKER_PATH.exists():
        return False, 0
    try:
        tripped_at = float(CIRCUIT_BREAKER_MARKER_PATH.read_text().strip())
        remaining = CIRCUIT_BREAKER_PAUSE_SECONDS - (time.time() - tripped_at)
        if remaining > 0:
            return True, int(remaining)
        clear_circuit_breaker_marker()
        return False, 0
    except (ValueError, IOError) as exc:
        logger.warning(f"breaker read failed: {exc}; clearing")
        clear_circuit_breaker_marker()
        return False, 0


# --- MQTT alerts ---
def notify(msg: str, kind: str = "generic") -> bool:
    if not NOTIFY_ENABLED:
        return False
    if not NOTIFY_HANDLE or not NOTIFY_MQTT_PASS:
        logger.warning("notify: NOTIFY_HANDLE or NOTIFY_MQTT_PASS missing — skipping")
        return False

    last: dict = {}
    try:
        if NOTIFY_RATELIMIT_PATH.exists():
            last = json.loads(NOTIFY_RATELIMIT_PATH.read_text())
            if time.time() - last.get(kind, 0) < NOTIFY_RATELIMIT_SECONDS:
                logger.info(f"notify suppressed (rate-limit kind={kind})")
                return False
    except Exception:
        last = {}

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
        last[kind] = time.time()
        NOTIFY_RATELIMIT_PATH.write_text(json.dumps(last))
        logger.info(f"notify sent (kind={kind})")
        return True
    except Exception as exc:
        logger.warning(f"notify publish failed: {exc}")
        return False


# --- Signal handlers ---
def _cleanup_handler(signum: int, frame: Any) -> None:
    logger.info(f"signal {signum} — cleanup + exit")
    remove_lock_file()
    sys.exit(0)


signal.signal(signal.SIGINT, _cleanup_handler)
signal.signal(signal.SIGTERM, _cleanup_handler)


# --- Queue parsing ---
YOUTUBE_ID_RE = re.compile(
    r"(?:youtube\.com/(?:watch\?v=|embed/|v/|shorts/)|youtu\.be/)([a-zA-Z0-9_-]{11})"
)
ZOOM_RE = re.compile(r":zoom=([0-9:]+)-([0-9:]+)")


def fetch_queue() -> list[dict]:
    """Fetch xscribe.txt and parse each line into a queue item."""
    cache_buster = f"?t={int(time.time())}"
    try:
        resp = http_session.get(
            f"{TEXT_FILE_URL}{cache_buster}",
            headers={"Cache-Control": "no-cache"},
            timeout=HTTP_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"queue fetch failed: {exc}")
        raise

    items: list[dict] = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        url_part = line.split()[0]
        match = YOUTUBE_ID_RE.search(url_part)
        if not match:
            logger.warning(f"no YouTube ID in line: {line!r}")
            continue

        item = {
            "url": url_part,
            "video_id": match.group(1),
            "audio_only": ":audio" in line,
            "zoom_start": None,
            "zoom_end": None,
            "raw": line,
        }
        zoom = ZOOM_RE.search(line)
        if zoom:
            item["zoom_start"] = zoom.group(1)
            item["zoom_end"] = zoom.group(2)
        items.append(item)

    logger.info(f"queue: {len(items)} item(s)")
    return items


# --- Per-URL pipeline ---
PROMPT_TEMPLATE = """Create a complete Obsidian Markdown note from this video.

You will receive a watch.py report containing video metadata, frame paths, and a timestamped transcript. **Use the Read tool to view each frame at the listed path** so visual context (slides, demos, what's on-screen) informs the note. Then write the note grounded in BOTH frames and transcript.

CRITICAL OUTPUT RULES:
- First three characters MUST be: ---
- NO preamble, NO commentary — output the raw markdown only.
- Frontmatter (in this exact order; tags and aliases as MULTI-LINE YAML, not inline lists):
    title: "human-readable title with normal word spacing"
    created: ISO 8601 with time (YYYY-MM-DDTHH:MM:SSZ) from upload_date if available, else today at T00:00:00Z
    tags:
      - lowercase-with-hyphens
      - (4-6 total)
    aliases:
      - lowercase-with-hyphens
      - (2-3 total)
    description: "one complete sentence with normal word spacing"
    channel: "from metadata"
    source: "original URL"
- Use double quotes around title, description, channel, source for valid YAML.
- HYPHENATION IS ONLY FOR `tags` AND `aliases` ENTRIES. NEVER replace spaces with hyphens inside `title` or `description`. Wrong: `title: "Five-iPhone-AI-Habits"`. Right: `title: "Five iPhone AI Habits"`.

After frontmatter and a blank line, extract the YouTube video ID from the source URL (the `v=` parameter, or the last path segment of a `youtu.be/...` short URL), then output the thumbnail line:

![](https://i.ytimg.com/vi/VIDEO_ID/maxresdefault.jpg)

## TITLE

###### Channel: [CHANNEL](channel @handle URL if available, e.g. https://www.youtube.com/@handle)
###### Source: SOURCE_URL
###### Duration: DURATION

---

## Key Takeaways:
- 3-5 specific, concrete findings drawn from BOTH the frames and the transcript.
- Each takeaway must state a specific fact, decision, or measurable claim — NOT generic advice.
- Avoid abstractions ("choose carefully", "be selective", "stay focused") unless followed immediately by a concrete instance.
- Make each takeaway distinct and self-contained.

EXCLUDE the following from the entire note (takeaways AND body):
- Channel self-promotion: subscribe/like reminders, "follow me on X", merch shoutouts, Patreon/Discord asks.
- Sponsorship reads, UNLESS the sponsor's product is the actual subject of that segment.
- Interaction prompts: "comment below", "let me know what you think", "ring the bell".
- Generic intro/outro filler: "In today's video…", "Thanks for watching", "See you next time".

Then 3-6 main content sections with ## headers, varying formats between adjacent sections (callouts `> [!tip]`, tables, block quotes, lists). Available callouts: !tip !info !warning !example !quote !summary !important !abstract !question !danger !success !failure !bug. Tables need a blank line before.

FRAME INTEGRATION RULE: Where a frame visually supports a claim, weave the observation INTO the body prose as a natural visual claim — e.g., "the side-by-side benchmark chart shows model A losing 30% throughput vs B" or "the rotary fixture keeps the print head at a constant distance across the curved surface." DO NOT use stage-direction phrasings like "the frames show…", "the early frames also show…", or "the maintenance frames demonstrate…" as paragraph leads or section subjects. Frames serve the claim; they are not the subject.

Avoid generic [[wikilinks]] inside body prose. End with a Related Links line: 5-10 specific concept phrases (Title Case, 1-4 words each) on a single line, separated by " - ":

---

[[Concept One]] - [[Concept Two]] - [[Person Name]] - [[Channel Name]]

End with EXACTLY:

---
## Transcript:

The worker appends the raw transcript after that line — do not add anything below "## Transcript:".

==== watch.py report begins ====
{watch_report}
==== watch.py report ends ===="""


CLAUDE_SYSTEM_PROMPT = (
    "You are a video-to-Obsidian-note synthesizer. Use the Read tool to view "
    "frame images at the paths provided in the input. Output ONLY the raw "
    "markdown note as instructed. Never ask clarifying questions. Never use "
    "Bash, Edit, Write, or any tool other than Read."
)


def run_watch(url: str, work_dir: Path, zoom_start: str | None, zoom_end: str | None) -> str:
    """Run watch.py and capture its stdout (the markdown report)."""
    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "watch.py"),
        url,
        "--out-dir", str(work_dir),
    ]
    if zoom_start:
        cmd += ["--start", zoom_start]
    if zoom_end:
        cmd += ["--end", zoom_end]

    logger.info(f"running watch.py: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=DOWNLOAD_TIMEOUT,
    )
    if result.returncode != 0:
        raise RuntimeError(f"watch.py exit {result.returncode}: {result.stderr.strip()[:500]}")
    return result.stdout


FRAMES_SECTION_RE = re.compile(r"\n## Frames\n.*?(?=\n## Transcript\n)", re.DOTALL)


def strip_frames_section(report: str) -> str:
    """Drop the '## Frames' block — used for :audio mode."""
    return FRAMES_SECTION_RE.sub("\n", report)


def call_claude(prompt: str) -> str:
    """Run claude --print --output-format json and return the result text.

    Parsing the JSON envelope (instead of relying on stderr) surfaces the real
    error subtype (error_max_turns, error_during_execution, rate_limit, etc.)
    when claude exits non-zero. Without this, claude often exits silently with
    empty stderr and the worker has nothing to log.
    """
    cmd = [
        CLAUDE_CLI_PATH,
        "--print",
        "--model", CLAUDE_MODEL,
        "--tools", "Read",
        "--append-system-prompt", CLAUDE_SYSTEM_PROMPT,
        "--max-budget-usd", str(CLAUDE_MAX_BUDGET_USD),
        "--no-session-persistence",
        "--output-format", "json",
        prompt,
    ]
    logger.info(
        f"calling claude (model={CLAUDE_MODEL}, budget=${CLAUDE_MAX_BUDGET_USD}, "
        f"prompt_chars={len(prompt)})"
    )
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=CLAUDE_TIMEOUT,
    )

    try:
        payload = json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError:
        raise RuntimeError(
            f"claude returned non-JSON (exit {result.returncode}): "
            f"stdout[:300]={result.stdout[:300]!r} stderr[:300]={result.stderr.strip()[:300]!r}"
        )

    is_error = payload.get("is_error") or payload.get("subtype") in (
        "error_max_turns",
        "error_during_execution",
    )
    if is_error or result.returncode != 0:
        raise RuntimeError(
            f"claude error (exit={result.returncode}, "
            f"subtype={payload.get('subtype')!r}, "
            f"duration_ms={payload.get('duration_ms')}, "
            f"num_turns={payload.get('num_turns')}, "
            f"cost_usd={payload.get('total_cost_usd')}): "
            f"result={(payload.get('result') or payload.get('error') or result.stderr.strip())[:500]!r}"
        )

    logger.info(
        f"claude ok (turns={payload.get('num_turns')}, "
        f"cost=${payload.get('total_cost_usd', 0):.3f}, "
        f"duration_ms={payload.get('duration_ms')})"
    )
    return payload.get("result", "")


FRONTMATTER_RE = re.compile(r"^---\n(.+?)\n---\n", re.DOTALL)
FM_TITLE_RE = re.compile(r'^title:\s*"?([^"\n]+)"?\s*$', re.MULTILINE)
TRANSCRIPT_SOURCE_REPORT_RE = re.compile(r"_Source:\s*([^.]+)\.")

TRANSCRIPT_SOURCE_TAG_MAP = {
    "captions": "Caption",
    "whisper (groq)": "Groq",
    "whisper (local)": "Local",
}


def map_transcript_source(raw: str) -> str:
    return TRANSCRIPT_SOURCE_TAG_MAP.get((raw or "").strip().lower(), "Unknown")


def inject_transcript_source_into_frontmatter(note: str, source_value: str) -> str:
    """Add `transcript_source: <value>` to the markdown note's YAML frontmatter."""
    fm_match = re.match(r"^(---\n)(.*?)(\n---\n)", note, re.DOTALL)
    if not fm_match:
        return note
    fm_body = fm_match.group(2)
    if re.search(r"^transcript_source:", fm_body, flags=re.MULTILINE):
        new_fm_body = re.sub(
            r"^transcript_source:.*$",
            f"transcript_source: {source_value}",
            fm_body,
            flags=re.MULTILINE,
        )
    else:
        new_fm_body = fm_body.rstrip() + f"\ntranscript_source: {source_value}"
    return fm_match.group(1) + new_fm_body + fm_match.group(3) + note[fm_match.end():]


def extract_title(claude_output: str, fallback: str) -> str:
    fm_match = FRONTMATTER_RE.match(claude_output)
    if not fm_match:
        return fallback
    title_match = FM_TITLE_RE.search(fm_match.group(1))
    return title_match.group(1).strip() if title_match else fallback


def append_transcript_block(claude_output: str, transcript_text: str) -> str:
    """The prompt instructs Claude to end at '## Transcript:' — append the code block here."""
    if not claude_output.rstrip().endswith("## Transcript:"):
        return claude_output.rstrip() + "\n\n## Transcript:\n\n```\n" + transcript_text + "\n```\n"
    return claude_output.rstrip() + "\n\n```\n" + transcript_text + "\n```\n"


TRANSCRIPT_SECTION_RE = re.compile(r"\n## Transcript\n.+?\n```\n(.+?)\n```", re.DOTALL)


def extract_transcript_from_report(report: str) -> str:
    match = TRANSCRIPT_SECTION_RE.search(report)
    return match.group(1).strip() if match else ""


# --- Long-video two-pass extraction (Change 2) ---

CHUNK_EXTRACT_PROMPT = """Extract structured information from this section of a YouTube transcript.

Transcript section {section_idx}/{total_sections}:
{section_text}

Extract and output in this format. Skip any section that doesn't apply.

## Topics Covered:
- (Main topics or sub-sections discussed in this part.)

## Key Points:
- (Important information, technical details, decisions, claims, numbers.)

## Examples/Demos:
- (Concrete examples, demonstrations, walkthroughs, or applications shown.)

## Important Quotes:
- (Notable direct quotes worth preserving — use the exact transcript wording.)"""

CHUNK_SYSTEM_PROMPT = (
    "You are a transcript-extraction specialist. Output ONLY the requested "
    "structured extraction. Never use Bash, Edit, Write, or any tool. No "
    "preamble, no commentary."
)


def split_transcript(text: str, target: int = CHUNK_TARGET_CHARS) -> list[str]:
    """Split a transcript into ~`target`-char chunks at newline boundaries."""
    if len(text) <= target:
        return [text]
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for line in text.split("\n"):
        line_len = len(line) + 1
        if cur_len + line_len > target and cur:
            chunks.append("\n".join(cur))
            cur = [line]
            cur_len = line_len
        else:
            cur.append(line)
            cur_len += line_len
    if cur:
        chunks.append("\n".join(cur))
    return chunks


def _call_claude_text(prompt: str, system_prompt: str, model: str, timeout: int) -> str:
    """Plain-text Claude call (no tools, no JSON envelope) for helper passes."""
    cmd = [
        CLAUDE_CLI_PATH,
        "--print",
        "--model", model,
        "--tools", "",
        "--append-system-prompt", system_prompt,
        "--no-session-persistence",
        "--output-format", "text",
    ]
    result = subprocess.run(
        cmd,
        input=prompt,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"helper claude exit {result.returncode}: {result.stderr.strip()[:300]}"
        )
    return result.stdout.strip()


def chunk_extract_transcript(transcript_text: str) -> str:
    """Run yousummary-style structured extraction per chunk; concatenate sections."""
    chunks = split_transcript(transcript_text)
    sections: list[str] = []
    for i, chunk in enumerate(chunks, 1):
        prompt = CHUNK_EXTRACT_PROMPT.format(
            section_idx=i, total_sections=len(chunks), section_text=chunk,
        )
        try:
            extracted = _call_claude_text(prompt, CHUNK_SYSTEM_PROMPT, CHUNK_MODEL, CHUNK_TIMEOUT)
        except (subprocess.TimeoutExpired, RuntimeError) as exc:
            logger.warning(f"chunk {i}/{len(chunks)} extract failed: {exc}")
            extracted = f"_[chunk {i} extraction failed; raw transcript follows]_\n\n```\n{chunk[:2000]}\n```"
        sections.append(f"### Section {i} of {len(chunks)}\n\n{extracted}")
    return "\n\n".join(sections)


def replace_transcript_in_report(report: str, structured_block: str) -> str:
    """Swap the raw '## Transcript' code block for a structured chunk-extract intermediate."""
    replacement = (
        "\n## Transcript (structured chunk-extract for synthesis)\n\n"
        "_The full raw transcript is appended at the bottom of the final note. "
        "This is a per-section structured intermediate the worker pre-extracts "
        "for long videos to preserve detail across sections._\n\n"
        f"{structured_block}"
    )
    return TRANSCRIPT_SECTION_RE.sub(lambda _m: replacement, report, count=1)


# --- Fabric extract_wisdom enrichment (Change 3) ---


def call_haiku_for_wisdom(transcript_text: str) -> str:
    """Run cadence-fixed Fabric extract_wisdom on the raw transcript. Returns the
    SUMMARY/IDEAS/... markdown block, or '' on any failure (best-effort)."""
    if not WISDOM_ENABLED:
        return ""
    if not WISDOM_SYSTEM_PROMPT_PATH.exists():
        logger.warning(f"wisdom system prompt missing at {WISDOM_SYSTEM_PROMPT_PATH}; skipping")
        return ""
    try:
        system = WISDOM_SYSTEM_PROMPT_PATH.read_text()
    except OSError as exc:
        logger.warning(f"wisdom system prompt unreadable: {exc}")
        return ""
    try:
        return _call_claude_text(transcript_text, system, WISDOM_MODEL, WISDOM_TIMEOUT)
    except subprocess.TimeoutExpired:
        logger.warning(f"wisdom extraction timed out after {WISDOM_TIMEOUT}s")
        return ""
    except RuntimeError as exc:
        logger.warning(f"wisdom extraction failed: {exc}")
        return ""


def splice_wisdom_block(claude_output: str, wisdom: str) -> str:
    """Insert wisdom between the model's body and the '## Transcript:' anchor."""
    if not wisdom:
        return claude_output
    anchor = "## Transcript:"
    if anchor in claude_output:
        head, _, _ = claude_output.rpartition(anchor)
        return f"{head.rstrip()}\n\n---\n\n{wisdom}\n\n---\n\n{anchor}"
    return claude_output.rstrip() + f"\n\n---\n\n{wisdom}\n\n---\n\n{anchor}"


def call_cleanup_endpoint(video_id: str) -> bool:
    try:
        resp = http_session.post(
            CLEANUP_URL,
            data={"videoID": video_id},
            timeout=HTTP_REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            logger.info(f"dequeued video {video_id}")
            return True
        logger.warning(f"cleanup returned {resp.status_code}: {resp.text[:200]}")
        return False
    except requests.RequestException as exc:
        logger.error(f"cleanup endpoint failed: {exc}")
        raise


def process_video(item: dict, work_root: Path) -> dict:
    """Run one item end-to-end. Returns {status, kind, message}."""
    video_id = item["video_id"]
    url = item["url"]
    work_dir = Path(tempfile.mkdtemp(prefix=f"newtube-{video_id}-", dir=work_root))

    try:
        try:
            report = run_watch(url, work_dir, item["zoom_start"], item["zoom_end"])
        except subprocess.TimeoutExpired:
            return {"status": "fail", "kind": "per-url", "message": "watch.py timeout"}
        except RuntimeError as exc:
            return {"status": "fail", "kind": "per-url", "message": str(exc)}

        transcript_text = extract_transcript_from_report(report)
        if not transcript_text:
            return {"status": "fail", "kind": "per-url", "message": "no transcript extracted"}

        prompt_input = strip_frames_section(report) if item["audio_only"] else report

        # Change 2: long-video chunked extraction. Pre-process the transcript
        # into structured per-section Topics/Key Points/Examples/Quotes so the
        # synthesis pass operates on a richer intermediate. The raw transcript
        # still lands in the final note via append_transcript_block below.
        if (
            LONG_VIDEO_CHAR_THRESHOLD > 0
            and len(transcript_text) > LONG_VIDEO_CHAR_THRESHOLD
        ):
            logger.info(
                f"long video ({len(transcript_text)} chars > {LONG_VIDEO_CHAR_THRESHOLD}); "
                f"running chunked structured extraction"
            )
            try:
                structured = chunk_extract_transcript(transcript_text)
            except subprocess.TimeoutExpired:
                logger.warning("chunked extraction timed out wholesale; falling back to raw")
                structured = ""
            if structured:
                prompt_input = replace_transcript_in_report(prompt_input, structured)

        try:
            claude_output = call_claude(PROMPT_TEMPLATE.replace("{watch_report}", prompt_input))
        except subprocess.TimeoutExpired:
            return {"status": "fail", "kind": "systemic", "message": "claude timeout"}
        except RuntimeError as exc:
            return {"status": "fail", "kind": "systemic", "message": f"claude crashed: {exc}"}

        if not claude_output.lstrip().startswith("---"):
            return {
                "status": "fail",
                "kind": "per-url",
                "message": f"claude output missing frontmatter (head: {claude_output[:200]!r})",
            }

        # Change 3: Fabric extract_wisdom enrichment. Best-effort — failures log
        # and skip; the note still ships without the wisdom block.
        wisdom = call_haiku_for_wisdom(transcript_text)
        if wisdom:
            claude_output = splice_wisdom_block(claude_output, wisdom)

        final_note = append_transcript_block(claude_output, transcript_text)

        source_match = TRANSCRIPT_SOURCE_REPORT_RE.search(report)
        if source_match:
            final_note = inject_transcript_source_into_frontmatter(
                final_note, map_transcript_source(source_match.group(1).strip())
            )

        title = extract_title(claude_output, fallback=video_id)

        try:
            ok, vault_path = obsidian_writer.put_note(
                title=title,
                content=final_note,
                video_id=video_id,
            )
        except requests.RequestException as exc:
            return {"status": "fail", "kind": "systemic", "message": f"vault unreachable: {exc}"}
        if not ok:
            return {"status": "fail", "kind": "per-url", "message": "vault put returned non-2xx"}

        try:
            call_cleanup_endpoint(video_id)
        except requests.RequestException as exc:
            logger.warning(f"dequeue raised but note already saved: {exc}")

        mode = "audio" if item["audio_only"] else (
            f"zoom={item['zoom_start']}-{item['zoom_end']}" if item["zoom_start"] else "full"
        )
        log_line = (
            f"| {time.strftime('%Y-%m-%d %H:%M')} | OK | {title} |  |  | {mode} | "
            f"[link]({url}) |"
        )
        try:
            obsidian_writer.append_to_log(log_line)
        except Exception as exc:
            logger.warning(f"log append failed (non-fatal): {exc}")

        return {"status": "ok", "kind": None, "message": vault_path}

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# --- Main ---
def main() -> int:
    if not check_stale_lock():
        return 0
    if not create_lock_file():
        return 1

    try:
        paused, remaining = should_pause_circuit_breaker()
        if paused:
            logger.warning(f"circuit breaker active — {remaining // 60} min remaining; exiting")
            return 0

        try:
            items = fetch_queue()
        except requests.RequestException:
            notify("newtube: queue fetch failed (xscribe.txt unreachable)", kind="systemic")
            return 1

        if not items:
            logger.info("queue empty")
            return 0

        work_root = PROJECT_ROOT / "tmp"
        work_root.mkdir(exist_ok=True)

        per_url_fails = 0
        systemic_fails = 0
        successes = 0

        for item in items:
            logger.info(f"processing {item['video_id']} ({item['raw']})")
            try:
                result = process_video(item, work_root)
            except Exception as exc:
                logger.exception(f"unhandled exception on {item['video_id']}")
                result = {"status": "fail", "kind": "systemic", "message": str(exc)}

            if result["status"] == "ok":
                successes += 1
                logger.info(f"OK: {result['message']}")
            else:
                kind = result["kind"]
                logger.error(f"FAIL ({kind}): {result['message']}")
                if kind == "systemic":
                    systemic_fails += 1
                    if systemic_fails >= CIRCUIT_BREAKER_THRESHOLD:
                        write_circuit_breaker_marker()
                        notify(
                            f"newtube: circuit breaker tripped "
                            f"({systemic_fails} systemic fails). Pausing "
                            f"{CIRCUIT_BREAKER_PAUSE_SECONDS // 60} min.",
                            kind="circuit_breaker",
                        )
                        break
                else:
                    per_url_fails += 1

        logger.info(
            f"run complete: {successes} ok, {per_url_fails} per-url fail, "
            f"{systemic_fails} systemic fail"
        )
        if successes > 0:
            clear_circuit_breaker_marker()

        return 0

    finally:
        remove_lock_file()


if __name__ == "__main__":
    raise SystemExit(main())
