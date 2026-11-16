"""Checkpoint and log helpers for resumable daily-price crawls."""

from __future__ import annotations

import csv
import re
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping


def normalize_id(value: object) -> str:
    text = str(value or "").strip().lstrip("\ufeff")
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def load_completed_keys(path: Path, run_date: str) -> set[tuple[str, str]]:
    """Read successful product/variant keys already persisted for this date."""
    completed: set[tuple[str, str]] = set()
    if not path.is_file() or path.stat().st_size == 0:
        return completed
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            if (row.get("crawl_date") or "").strip()[:10] != run_date:
                continue
            pid = normalize_id(row.get("product_id"))
            spid = normalize_id(row.get("seller_product_id"))
            if pid and spid:
                completed.add((pid, spid))
    return completed


def load_failed_targets(
    path: Path, run_date: str
) -> tuple[set[tuple[str, str]], set[str]]:
    """Read today's structured retry log, retaining support for legacy logs."""
    failed_keys: set[tuple[str, str]] = set()
    failed_product_ids: set[str] = set()
    if not path.is_file() or path.stat().st_size == 0:
        return failed_keys, failed_product_ids

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            logged_date = (row.get("crawl_date") or row.get("log_time") or "").strip()
            if logged_date[:10] != run_date:
                continue
            pid = normalize_id(row.get("product_id"))
            spid = normalize_id(row.get("seller_product_id"))
            if pid:
                failed_product_ids.add(pid)
                if spid:
                    failed_keys.add((pid, spid))
            # Legacy logs only stored "<product_id>: <error>" in error.
            match = re.match(r"^\s*(\d+)\s*:", row.get("error") or "")
            if match:
                failed_product_ids.add(match.group(1))
    return failed_keys, failed_product_ids


def pending_rows(
    rows: Iterable[Mapping[str, object]],
    completed_keys: set[tuple[str, str]],
    failed_keys: set[tuple[str, str]],
    failed_product_ids: set[str],
) -> list[Mapping[str, object]]:
    """Return every unfinished input, prioritizing IDs present in today's log.

    The output file is the durable success checkpoint. Reconciliation against
    it also recovers items that were in-flight when a prior process was killed
    before it could append their failures to the log.
    """
    pending: list[tuple[bool, Mapping[str, object]]] = []
    for row in rows:
        pid = normalize_id(row.get("product_id"))
        spid = normalize_id(row.get("seller_product_id"))
        if not pid or not spid or (pid, spid) in completed_keys:
            continue
        was_logged_failed = (pid, spid) in failed_keys or pid in failed_product_ids
        pending.append((was_logged_failed, row))
    pending.sort(key=lambda entry: not entry[0])
    return [row for _, row in pending]


def validate_run_date(value: str | None, fallback_date: str) -> str:
    """Validate an Airflow logical date or use the local date for manual runs."""
    candidate = value or fallback_date
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError as exc:
        raise ValueError(f"TIKI_RUN_DATE must use YYYY-MM-DD, got {candidate!r}") from exc
