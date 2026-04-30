#!/usr/bin/env python3
"""Format Claude's note output for Obsidian and PUT it to the vault via REST.

The vault REST endpoint (Obsidian Local REST plugin) accepts:
  PUT  /vault/<path>            create/overwrite a note
  GET  /vault/<path>            read a note
  Auth: Authorization: Bearer <OBSIDIAN_API_TOKEN>

Note filename is sanitized from the video title; optionally appends a video ID
in brackets to avoid collisions when two videos share a title.
"""
from __future__ import annotations

import logging
import os
import re

import requests

logger = logging.getLogger("newtube.obsidian")

OBSIDIAN_API_ENABLED = os.environ.get("OBSIDIAN_API_ENABLED", "true").lower() == "true"
OBSIDIAN_API_URL = os.environ.get("OBSIDIAN_API_URL", "").rstrip("/")
OBSIDIAN_API_TOKEN = os.environ.get("OBSIDIAN_API_TOKEN", "")
OBSIDIAN_FOLDER = os.environ.get("OBSIDIAN_FOLDER", "")


def sanitize_filename(text: str, max_length: int = 245) -> str:
    """Strip path-illegal chars and collapse whitespace. Cap at max_length, word-aligned."""
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rsplit(" ", 1)[0]
    return cleaned or "untitled"


def _vault_url(file_path: str) -> str:
    return f"{OBSIDIAN_API_URL}/vault/{file_path}"


def _headers(content_type: str | None = None) -> dict[str, str]:
    h = {"Authorization": f"Bearer {OBSIDIAN_API_TOKEN}"}
    if content_type:
        h["Content-Type"] = content_type
    return h


def put_note(
    title: str,
    content: str,
    video_id: str | None = None,
    folder: str | None = None,
) -> tuple[bool, str]:
    """PUT a markdown note. Returns (success, vault_path_used)."""
    if not OBSIDIAN_API_ENABLED:
        logger.info("OBSIDIAN_API_ENABLED=false — skipping upload")
        return False, ""

    if not OBSIDIAN_API_URL or not OBSIDIAN_API_TOKEN:
        logger.error("OBSIDIAN_API_URL or OBSIDIAN_API_TOKEN missing")
        return False, ""

    base = sanitize_filename(title)
    fname = f"{base} [{video_id}].md" if video_id else f"{base}.md"
    parent = folder if folder is not None else OBSIDIAN_FOLDER
    file_path = f"{parent}/{fname}" if parent else fname
    endpoint = _vault_url(file_path)

    logger.info(f"uploading to Obsidian: {file_path}")
    try:
        resp = requests.put(
            endpoint,
            headers=_headers("text/markdown"),
            data=content.encode("utf-8"),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.error(f"Obsidian PUT raised: {exc}")
        raise

    if resp.status_code in (200, 201, 204):
        logger.info(f"uploaded: {file_path}")
        return True, file_path

    logger.error(f"Obsidian PUT returned {resp.status_code}: {resp.text[:200]}")
    return False, file_path


def append_to_log(line: str, log_filename: str = "_Newtube_log.md") -> bool:
    """Append a single line to a vault log file. Creates it with a header on first write."""
    if not OBSIDIAN_API_ENABLED:
        return False

    log_path = f"{OBSIDIAN_FOLDER}/{log_filename}" if OBSIDIAN_FOLDER else log_filename
    endpoint = _vault_url(log_path)

    try:
        get = requests.get(endpoint, headers=_headers(), timeout=15)
        if get.status_code == 200:
            existing = get.text
        elif get.status_code == 404:
            existing = (
                "# Newtube processing log\n\n"
                "| Date | Status | Title | Channel | Duration | Mode | Source |\n"
                "|------|--------|-------|---------|----------|------|--------|\n"
            )
        else:
            logger.warning(f"log GET returned {get.status_code}; skipping append")
            return False

        new = existing.rstrip() + "\n" + line + "\n"
        put = requests.put(
            endpoint,
            headers=_headers("text/markdown"),
            data=new.encode("utf-8"),
            timeout=15,
        )
        if put.status_code in (200, 201, 204):
            return True
        logger.warning(f"log PUT returned {put.status_code}")
        return False
    except requests.RequestException as exc:
        logger.warning(f"log append raised: {exc}")
        return False
