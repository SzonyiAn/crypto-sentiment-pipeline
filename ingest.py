"""Step 1 of the pipeline: ingestion (sources → raw layer).

Two sources are ingested:
  1. Kaggle crypto prices: one CSV file per coin, copied unchanged.
  2. Fear & Greed Index: JSON response of a REST API, saved unchanged.

The raw layer PRESERVES the original data. We do not clean or change anything
here; that is the job of the staged layer. Keeping the original means we can
always re-process it later, e.g. if we find a bug in the transformations.

Raw path convention (same idea as in Lab 03):
    <domain>/<YYYY>/<MM>/<DD>/source=<file|api>/<filename>
e.g. prices/2026/10/26/source=file/coin_Bitcoin.csv
     fear_greed/2026/10/26/source=api/fear_greed.json

Usage:
    python ingest.py                          # local only
    python ingest.py --target azure           # local + Azure Blob Storage
    python ingest.py --fng-file saved.json    # use a saved API response (offline demo)
"""

import argparse
import csv
import io
import json
import logging
import time
from pathlib import Path

import requests

import config
from storage import Storage

log = logging.getLogger("ingest")


def raw_folder(domain: str, source: str) -> str:
    """The raw-layer folder of today's (or INGESTION_DATE's) ingestion."""
    d = config.ingestion_date()
    return f"{domain}/{d:%Y}/{d:%m}/{d:%d}/source={source}"


def raw_path(domain: str, source: str, filename: str) -> str:
    return f"{raw_folder(domain, source)}/{filename}"


# --- Source 1: Kaggle CSV files ---------------------------------------------

def find_coin_files(source_dir: Path) -> list[Path]:
    """Find the real coin CSV files and skip junk.

    The Kaggle zip, unpacked on a Mac, also contains a __MACOSX folder with
    hidden '._coin_*.csv' files. They are not data, so we skip anything
    starting with '._' or lying in __MACOSX.
    """
    if not source_dir.is_dir():
        raise FileNotFoundError(
            f"Kaggle folder not found: {source_dir}. Unzip the dataset there first."
        )
    files = sorted(
        p for p in source_dir.rglob("coin_*.csv")
        if not p.name.startswith("._") and "__MACOSX" not in p.parts
    )
    if not files:
        raise FileNotFoundError(f"No coin_*.csv files found in {source_dir}")
    return files


def check_csv_header(path: Path) -> None:
    """Fail early if a file does not look like a coin price file.

    This is only a quick sanity check on the structure. The real data quality
    checks on the values come later, in validate.py.
    """
    with path.open(newline="", encoding="utf-8") as f:
        header = next(csv.reader(f), [])
    if header != config.PRICE_COLUMNS:
        raise ValueError(f"Unexpected columns in {path.name}: {header}")


def ingest_prices(storage: Storage, source_dir: Path) -> int:
    files = find_coin_files(source_dir)
    log.info("Found %d coin files in %s", len(files), source_dir)
    # Check ALL files before changing anything in the raw layer, so a bad file
    # cannot leave a half-written ingestion behind.
    for path in files:
        check_csv_header(path)

    # Today's raw folder must be an exact snapshot of the source: remove what an
    # earlier run of the same day wrote (e.g. a file deleted from the source since).
    storage.clear_folder("raw", raw_folder("prices", "file"))
    for path in files:
        # read_bytes + save_bytes = an exact copy, nothing is changed.
        storage.save_bytes("raw", raw_path("prices", "file", path.name), path.read_bytes())
    return len(files)


# --- Source 2: Fear & Greed REST API ----------------------------------------

def fetch_fear_greed() -> bytes:
    """Call the API with retries.

    Network calls can fail temporarily (timeout, server busy). Instead of
    failing the whole pipeline at the first error, we try again a few times,
    waiting a bit longer each time (2s, 4s, ...). This is called "retry with
    backoff" and is a basic reliability technique.
    """
    for attempt in range(1, config.API_MAX_ATTEMPTS + 1):
        try:
            log.info("Calling Fear & Greed API (attempt %d)", attempt)
            response = requests.get(config.FNG_API_URL, timeout=config.API_TIMEOUT_SECONDS)
            response.raise_for_status()  # turns HTTP errors (404, 500...) into exceptions
            return response.content
        except requests.RequestException as exc:
            if attempt == config.API_MAX_ATTEMPTS:
                raise RuntimeError(f"Fear & Greed API failed {attempt} times") from exc
            wait = 2 ** attempt
            log.warning("API call failed (%s), retrying in %ds", exc, wait)
            time.sleep(wait)


def check_fear_greed_response(content: bytes) -> int:
    """Make sure we got a usable answer before saving it.

    An API can answer with HTTP 200 (OK) but still report an error inside the
    JSON, or return an empty list. We check both, and return the record count.
    """
    payload = json.loads(content)
    error = payload.get("metadata", {}).get("error")
    if error:
        raise ValueError(f"API reported an error: {error}")
    records = payload.get("data")
    if not isinstance(records, list) or not records:
        raise ValueError("API response contains no data records")
    return len(records)


def ingest_fear_greed(storage: Storage, saved_file: Path | None) -> int:
    if saved_file:
        # Demo / offline mode: use a previously downloaded API response.
        log.info("Using saved Fear & Greed response: %s", saved_file)
        content = saved_file.read_bytes()
    else:
        content = fetch_fear_greed()

    count = check_fear_greed_response(content)
    storage.save_bytes("raw", raw_path("fear_greed", "api", "fear_greed.json"), content)
    log.info("Fear & Greed: %d daily records", count)
    return count


# --- Entry point -------------------------------------------------------------

def run(target: str = config.DEFAULT_TARGET,
        kaggle_dir: Path = config.KAGGLE_SOURCE_DIR,
        fng_file: Path | None = None) -> None:
    """Run the whole ingestion. run_pipeline.py will call this function."""
    storage = Storage(target)
    log.info("=== INGEST started (target=%s, date=%s) ===", target, config.ingestion_date())
    n_files = ingest_prices(storage, kaggle_dir)
    n_days = ingest_fear_greed(storage, fng_file)
    log.info("=== INGEST finished: %d price files, %d fear & greed records ===", n_files, n_days)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest both sources into the raw layer")
    parser.add_argument("--target", choices=["local", "azure"], default=config.DEFAULT_TARGET)
    parser.add_argument("--kaggle-dir", type=Path, default=config.KAGGLE_SOURCE_DIR)
    parser.add_argument("--fng-file", type=Path, help="saved API response to use instead of calling the API")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    run(args.target, args.kaggle_dir, args.fng_file)


if __name__ == "__main__":
    main()
