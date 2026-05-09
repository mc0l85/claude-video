#!/usr/bin/env python3
"""Format Claude's note output for Obsidian and write it to the vault filesystem.

Post-MOR-61 (2026-05-02): vault access is filesystem-based (sshfs mount), not REST.
Set OBSIDIAN_VAULT_ROOT to the local path of the McKay vault root.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path

logger = logging.getLogger("newtube.obsidian")

OBSIDIAN_API_ENABLED = os.environ.get("OBSIDIAN_API_ENABLED", "true").lower() == "true"
OBSIDIAN_VAULT_ROOT = os.environ.get("OBSIDIAN_VAULT_ROOT", "").rstrip("/")
OBSIDIAN_FOLDER = os.environ.get("OBSIDIAN_FOLDER", "")


def sanitize_filename(text: str, max_length: int = 245) -> str:
    """Strip path-illegal chars and collapse whitespace. Cap at max_length, word-aligned."""
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rsplit(" ", 1)[0]
    return cleaned or "untitled"


def put_note(
    title: str,
    content: str,
    video_id: str | None = None,
    folder: str | None = None,
) -> tuple[bool, str]:
    """Write a markdown note to the vault filesystem. Returns (success, vault_path_used)."""
    if not OBSIDIAN_API_ENABLED:
        logger.info("OBSIDIAN_API_ENABLED=false — skipping vault write")
        return False, ""

    if not OBSIDIAN_VAULT_ROOT:
        logger.error("OBSIDIAN_VAULT_ROOT not set")
        return False, ""

    base = sanitize_filename(title)
    fname = f"{base} [{video_id}].md" if video_id else f"{base}.md"
    parent = folder if folder is not None else OBSIDIAN_FOLDER
    rel_path = f"{parent}/{fname}" if parent else fname
    full_path = Path(OBSIDIAN_VAULT_ROOT) / rel_path

    logger.info(f"writing to vault: {rel_path}")
    try:
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content, encoding="utf-8")
        return True, rel_path
    except OSError as exc:
        logger.error(f"vault write failed: {exc}")
        return False, rel_path


def append_to_log(line: str, log_filename: str = "_Newtube_log.md") -> bool:
    """Append a single line to a vault log file. Creates it with a header on first write."""
    if not OBSIDIAN_API_ENABLED or not OBSIDIAN_VAULT_ROOT:
        return False

    log_rel = f"{OBSIDIAN_FOLDER}/{log_filename}" if OBSIDIAN_FOLDER else log_filename
    log_path = Path(OBSIDIAN_VAULT_ROOT) / log_rel

    try:
        if log_path.exists():
            existing = log_path.read_text(encoding="utf-8")
        else:
            existing = (
                "# Newtube processing log\n\n"
                "| Date | Status | Title | Channel | Duration | Mode | Source |\n"
                "|------|--------|-------|---------|----------|------|--------|\n"
            )
            log_path.parent.mkdir(parents=True, exist_ok=True)

        log_path.write_text(existing.rstrip() + "\n" + line + "\n", encoding="utf-8")
        return True
    except OSError as exc:
        logger.warning(f"log append failed: {exc}")
        return False
