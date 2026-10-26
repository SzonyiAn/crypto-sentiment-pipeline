"""Central settings for the pipeline.

Every other script imports its paths and constants from here, so a change
(for example a new folder or a new stablecoin) only has to be made in one place.
"""

import os
from datetime import date
from pathlib import Path

# --- Folders -----------------------------------------------------------------
# PROJECT_DIR is the folder this file is in, wherever the project is cloned.
PROJECT_DIR = Path(__file__).resolve().parent

# The local working copy of the three data layers. When running against Azure,
# these folders are also uploaded to Blob containers with the same names.
DATA_DIR = PROJECT_DIR / "data"
LAYERS = ("raw", "staged", "curated")

# Where you put the unzipped Kaggle files (the coin_*.csv files).
KAGGLE_SOURCE_DIR = DATA_DIR / "source" / "kaggle_crypto"

# --- Source 1: Kaggle crypto prices -----------------------------------------
# The columns every coin file must have. Used as a quick sanity check on ingest.
PRICE_COLUMNS = [
    "SNo", "Name", "Symbol", "Date", "High", "Low",
    "Open", "Close", "Volume", "Marketcap",
]

# Coins whose price is pegged to the US dollar (1 coin ≈ $1). They should NOT
# react to market sentiment, so we keep them in the data but flag them, as a
# "control group". (Wrapped Bitcoin, WBTC, is NOT here: it follows Bitcoin's
# price, so it is just as volatile as BTC.)
STABLECOINS = {"USDT", "USDC"}

# --- Source 2: Fear & Greed Index API ---------------------------------------
# limit=0 means "give me the full history", format=json is the response type.
FNG_API_URL = "https://api.alternative.me/fng/?limit=0&format=json"
API_TIMEOUT_SECONDS = 30
API_MAX_ATTEMPTS = 3

# --- Where to store results --------------------------------------------------
# "local" = only the data/ folder. "azure" = data/ folder + Azure Blob Storage.
# Can be overridden on the command line (see run_pipeline.py / ingest.py).
DEFAULT_TARGET = os.getenv("PIPELINE_TARGET", "local")

# The connection string is a secret: it is read from the environment and is
# never written in the code or committed to GitHub.
AZURE_CONNECTION_STRING_ENV = "AZURE_STORAGE_CONNECTION_STRING"


def ingestion_date() -> date:
    """The date used in raw-layer paths (YYYY/MM/DD).

    Normally today. It can be fixed with the INGESTION_DATE environment
    variable (format YYYY-MM-DD) to re-run an older day on purpose.
    """
    override = os.getenv("INGESTION_DATE")
    return date.fromisoformat(override) if override else date.today()
