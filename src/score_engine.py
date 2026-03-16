#!/usr/bin/env python3
"""
score_engine.py - SSI Scoring Engine for Australia
SSI v4.0.2 - Ikenga Project

Computes the 6-component SSI R-score for each Australian substation:
  C = Condition (asset age, maintenance history, failure rates)
  V = Vulnerability (seismic, cyclone, flood, bushfire exposure)
  I = Interconnection (graph topology, N-1 redundancy, transfer capacity)
  E = Economic (demand growth, investment pipeline, market price signals)
  S = Social (population density, critical facilities, equity weighting)
  T = Transition (renewable penetration, battery storage, DER adoption)

Master equation:
  R = w_C*C + w_V*V + w_I*I + w_E*E + w_S*S + w_T*T + modifier_sum
  where modifier_sum = R3 + R4 + R6r + R6s + R7

Weights (RAW format, sum > 1.0):
  w_C=0.22, w_V=0.20, w_I=0.18, w_E=0.15, w_S=0.12, w_T=0.13

Band classification:
  Low:      R < 0.25
  Medium:   0.25 <= R < 0.50
  High:     0.50 <= R < 0.75
  Critical: R >= 0.75
"""

import json
import logging
import math
import random
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

WEIGHTS = {"C": 0.22, "V": 0.20, "I": 0.18, "E": 0.15, "S": 0.12, "T": 0.13}
MODIFIER_KEYS = ["R3", "R4", "R6r", "R6s", "R7"]

STATE_BIAS = {
    "New South Wales": 0.0, "Victoria": -0.01, "Queensland": 0.02,
    "South Australia": 0.04, "Western Australia": 0.01, "Tasmania": -0.02,
    "Northern Territory": 0.06, "Australian Capital Territory": -0.03,
}


def compute_r_score(components: dict, modifiers: dict) -> float:
    """Compute weighted R-score from components and modifiers."""
    base = sum(WEIGHTS[k] * components.get(k, 0) for k in WEIGHTS)
    mod = sum(modifiers.get(k, 0) for k in MODIFIER_KEYS)
    return min(max(base + mod, 0.0), 1.0)


def classify_band(r: float) -> str:
    """Classify R-score into risk band."""
    if r >= 0.75: return "Critical"
    if r >= 0.50: return "High"
    if r >= 0.25: return "Medium"
    return "Low"


def score_fleet(substations: list) -> dict:
    """Score all substations and compute fleet summary."""
    scores = []
    bands = {"Low": 0, "Medium": 0, "High": 0, "Critical": 0}
    
    for sub in substations:
        r = compute_r_score(sub["components"], sub.get("modifiers", {}))
        sub["R_median"] = round(r, 4)
        sub["band"] = classify_band(r)
        scores.append(r)
        bands[sub["band"]] += 1
    
    scores.sort()
    n = len(scores)
    summary = {
        "total": n,
        "median_R": round(scores[n // 2], 4) if n else 0,
        "mean_R": round(sum(scores) / n, 4) if n else 0,
        "P5": round(scores[int(n * 0.05)], 4) if n else 0,
        "P95": round(scores[int(n * 0.95)], 4) if n else 0,
        "bands": bands,
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }
    return summary


def generate_ssi_data_json(substations: list, summary: dict, output_path: Path):
    """Write ssi-data.json in dashboard-compatible format."""
    regions = {}
    for sub in substations:
        rid = sub.get("region_id", 0)
        if rid not in regions:
            regions[rid] = {"region_id": rid, "name": sub.get("province", ""), "substations": []}
        regions[rid]["substations"].append(sub["substation_id"])
    
    data = {
        "meta": {
            "country": "australia",
            "version": "4.0.2",
            "substations": len(substations),
            "generated": datetime.now(timezone.utc).isoformat(),
        },
        "fleet_summary": summary,
        "regions": regions,
        "substations": substations,
    }
    
    output_path.write_text(json.dumps(data, ensure_ascii=False))
    logger.info(f"Wrote ssi-data.json: {len(substations)} substations, {output_path.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Score engine ready - import and call score_fleet()")
