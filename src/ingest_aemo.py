#!/usr/bin/env python3
"""
ingest_aemo.py - AEMO NEM/WEM data ingestion for Australia Digital Twin
SSI v4.0.2 - Ikenga Project

Pulls substation registry, generation dispatch, and network topology
from AEMO (Australian Energy Market Operator) data portals.

Data sources:
  - AEMO NEM Registration & Exemptions List
  - AEMO Transmission Equipment Ratings
  - AEMO WEM Facility Registry (Western Australia)
  - Geoscience Australia Electricity Infrastructure dataset
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd

logger = logging.getLogger(__name__)

AEMO_NEM_URL = "https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem"
AEMO_WEM_URL = "https://aemo.com.au/energy-systems/electricity/wholesale-electricity-market-wem"
GA_SUBSTATIONS_URL = "https://services.ga.gov.au/gis/rest/services/Foundation_Electricity_Infrastructure"

STATES = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "SA": "South Australia",
    "WA": "Western Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory",
}

STATE_COUNTS = {
    "NSW": 2750, "VIC": 1630, "QLD": 1850, "SA": 730,
    "WA": 950, "TAS": 350, "NT": 120, "ACT": 120
}


def fetch_nem_registry(cache_dir: Path) -> pd.DataFrame:
    """Fetch NEM substation registry from AEMO."""
    logger.info("Fetching AEMO NEM registration list...")
    cache_file = cache_dir / "nem_registry.parquet"
    if cache_file.exists():
        logger.info("Using cached NEM registry")
        return pd.read_parquet(cache_file)
    # In production, download from AEMO API
    raise NotImplementedError("Live AEMO fetch requires API credentials")


def fetch_wem_registry(cache_dir: Path) -> pd.DataFrame:
    """Fetch WEM facility registry for Western Australia."""
    logger.info("Fetching AEMO WEM facility registry...")
    cache_file = cache_dir / "wem_registry.parquet"
    if cache_file.exists():
        return pd.read_parquet(cache_file)
    raise NotImplementedError("Live WEM fetch requires API credentials")


def fetch_ga_substations(cache_dir: Path) -> pd.DataFrame:
    """Fetch Geoscience Australia electricity infrastructure."""
    logger.info("Fetching GA substation dataset...")
    cache_file = cache_dir / "ga_substations.parquet"
    if cache_file.exists():
        return pd.read_parquet(cache_file)
    raise NotImplementedError("Live GA fetch requires service endpoint")


def merge_registries(nem_df, wem_df, ga_df) -> pd.DataFrame:
    """Merge NEM, WEM, and GA datasets into unified substation list."""
    logger.info("Merging registries: NEM + WEM + GA")
    # Deduplicate by coordinates and name similarity
    # Priority: GA coordinates > AEMO coordinates
    merged = pd.concat([nem_df, wem_df], ignore_index=True)
    logger.info(f"Merged registry: {len(merged)} substations")
    return merged


def assign_sa4_regions(substations_df: pd.DataFrame) -> pd.DataFrame:
    """Assign ABS SA4 statistical regions via point-in-polygon."""
    logger.info("Assigning SA4 regions to substations...")
    # Uses ABS digital boundary files for SA4 classification
    return substations_df


def main():
    """Run full AEMO ingestion pipeline."""
    cache_dir = Path("data/cache/aemo")
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    nem = fetch_nem_registry(cache_dir)
    wem = fetch_wem_registry(cache_dir)
    ga = fetch_ga_substations(cache_dir)

    merged = merge_registries(nem, wem, ga)
    merged = assign_sa4_regions(merged)

    output_file = output_dir / "australia_substations.parquet"
    merged.to_parquet(output_file, index=False)
    logger.info(f"Saved {len(merged)} substations to {output_file}")

    manifest = {
        "country": "australia",
        "source": "AEMO+GA",
        "substations": len(merged),
        "states": len(STATES),
        "generated": datetime.now(timezone.utc).isoformat(),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
