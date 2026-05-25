"""Download NYC Taxi monthly Parquet files with retries and idempotency."""

from __future__ import annotations

import os
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from shared.config import get_settings
from shared.logging_config import get_logger

log = get_logger(__name__)


class ExtractError(RuntimeError):
    """Raised when the source file cannot be retrieved."""


@dataclass(frozen=True)
class ExtractResult:
    year: int
    month: int
    source_url: str
    local_path: str
    bytes_written: int


def _filename(dataset: str, year: int, month: int) -> str:
    return f"{dataset}_{year:04d}-{month:02d}.parquet"


def build_source_url(year: int, month: int) -> str:
    settings = get_settings()
    return f"{settings.taxi_data_base_url}/{_filename(settings.taxi_dataset, year, month)}"


def local_target_path(year: int, month: int) -> Path:
    settings = get_settings()
    base = Path(settings.data_dir) / "raw"
    base.mkdir(parents=True, exist_ok=True)
    return base / _filename(settings.taxi_dataset, year, month)


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.RequestException, ExtractError)),
)
def download_month(year: int, month: int, *, force: bool = False) -> ExtractResult:
    """Download a single month of trip data, retrying transient failures."""
    url = build_source_url(year, month)
    target = local_target_path(year, month)

    if target.exists() and target.stat().st_size > 0 and not force:
        log.info("Skipping download, already present: %s", target)
        return ExtractResult(year, month, url, str(target), target.stat().st_size)

    log.info("Downloading %s -> %s", url, target)
    tmp_path = target.with_suffix(target.suffix + ".part")
    bytes_written = 0
    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            if resp.status_code == 404:
                raise ExtractError(f"Source not found (HTTP 404): {url}")
            resp.raise_for_status()
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
                        bytes_written += len(chunk)
        os.replace(tmp_path, target)
    finally:
        if tmp_path.exists():
            with suppress(OSError):
                tmp_path.unlink()

    if bytes_written == 0:
        raise ExtractError(f"Downloaded zero bytes from {url}")

    log.info("Downloaded %d bytes to %s", bytes_written, target)
    return ExtractResult(year, month, url, str(target), bytes_written)
