"""Runs the whole pipeline in the correct order, with one command:

    1. INGEST     sources → raw
    2. TRANSFORM  raw → staged → curated (curated only written locally)
    3. VALIDATE   data quality checks
    4. PUBLISH    curated → Azure, ONLY if validation passed

If a step fails, the following steps do not run. In particular, data that
failed validation is never published as the official data product.

Usage:
    python run_pipeline.py                         # everything, locally
    python run_pipeline.py --target azure          # everything, results in Azure Blob Storage
    python run_pipeline.py --fng-file data/sample/fear_greed.json
                                                   # use a saved API response (offline demo)
    python run_pipeline.py --skip-ingest           # re-process the raw data already present

Exit codes (so a scheduler, or you, can see the outcome):
    0 = success, 1 = data quality failed, 2 = technical error
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import config

log = logging.getLogger("pipeline")


def setup_logging() -> Path:
    """Log to the terminal AND to a file per run (logs/pipeline_<timestamp>.log),
    so the outcome of every earlier run can be looked up later."""
    log_dir = config.PROJECT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")],
    )
    return log_file


def ensure_java() -> None:
    """PySpark needs Java 17 or 21. If JAVA_HOME is not set, try to find a
    Homebrew-installed Java 17 (macOS), so you don't have to export it by hand
    in every new terminal. On the Azure VM, Java is installed system-wide."""
    if os.getenv("JAVA_HOME") or not shutil.which("brew"):
        return
    try:
        prefix = subprocess.run(["brew", "--prefix", "openjdk@17"],
                                capture_output=True, text=True, check=True).stdout.strip()
    except subprocess.CalledProcessError:
        return
    if Path(prefix, "bin", "java").exists():
        os.environ["JAVA_HOME"] = prefix
        log.info("Using Java from %s", prefix)


class Step:
    """Small helper that logs the start, end and duration of each step."""

    def __init__(self, number: int, name: str):
        self.label = f"STEP {number}/4 {name}"

    def __enter__(self):
        self.start = time.time()
        log.info("────── %s ──────", self.label)
        return self

    def __exit__(self, exc_type, exc, tb):
        seconds = time.time() - self.start
        if exc_type is None:
            log.info("%s OK (%.1fs)", self.label, seconds)
        else:
            log.error("%s FAILED after %.1fs", self.label, seconds)
        return False  # never swallow the exception, let it stop the pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full crypto sentiment pipeline")
    parser.add_argument("--target", choices=["local", "azure"], default=config.DEFAULT_TARGET)
    parser.add_argument("--kaggle-dir", type=Path, default=config.KAGGLE_SOURCE_DIR)
    parser.add_argument("--fng-file", type=Path, help="saved API response instead of calling the API")
    parser.add_argument("--skip-ingest", action="store_true", help="use the raw data already present")
    args = parser.parse_args()

    log_file = setup_logging()
    ensure_java()

    # Imported only now: transform/validate load PySpark, which needs Java ready.
    import ingest
    import transform
    import validate
    from storage import Storage

    started = time.time()
    log.info("PIPELINE started: target=%s, log file=%s", args.target, log_file.name)
    spark = None
    try:
        with Step(1, "INGEST"):
            if args.skip_ingest:
                log.info("Skipped on request (--skip-ingest), using existing raw data")
            else:
                ingest.run(args.target, args.kaggle_dir, args.fng_file)

        # One Spark session for transform and validate: starting Spark is slow.
        spark = transform.create_spark()

        with Step(2, "TRANSFORM"):
            transform.run(args.target, spark)

        with Step(3, "VALIDATE"):
            validate.run(spark)

        with Step(4, "PUBLISH"):
            storage = Storage(args.target)
            transform.publish_curated(storage)
            report = config.DATA_DIR / "curated" / "_quality_report.json"
            storage.save_bytes("curated", report.name, report.read_bytes())
            if args.target == "local":
                log.info("Target is local: curated tables are in data/curated/")

    except validate.DataQualityError as exc:
        log.error("PIPELINE STOPPED: data quality failed, nothing was published. %s", exc)
        log.error("See data/curated/_quality_report.json for all check results.")
        return 1
    except Exception:
        # log.exception also prints the full error (traceback) into the log file.
        log.exception("PIPELINE STOPPED: technical error")
        return 2
    finally:
        if spark is not None:
            spark.stop()

    log.info("PIPELINE finished successfully in %.1fs", time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
