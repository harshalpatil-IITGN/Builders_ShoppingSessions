#!/usr/bin/env python3
"""
Shopping Sessions: End-to-end Pipeline
Reads $DATA_DIR, processes according to rules.md, and writes
/output/user_daily/ and /output/campaign_daily/ as Parquet.
"""
import json
import os
import sys

# Ensure repository root is on sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import polars as pl
from pipeline.attribution import load_campaigns, process_clicks_and_attribute
from pipeline.events import load_products, process_events
from pipeline.identity import build_identity_mapping
from pipeline.timestamp_parser import parse_iso_datetime

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")

USER_DAILY_SCHEMA = {
    "user_id": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "events": pl.Int64,
    "revenue_cents": pl.Int64,
    "top_category": pl.String,
}

CAMPAIGN_DAILY_SCHEMA = {
    "campaign_id": pl.String,
    "channel": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "purchasing_sessions": pl.Int64,
    "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}

def main():
    # 1. Read manifest
    with open(os.path.join(DATA, "manifest.json")) as f:
        manifest = json.load(f)
    window_start_ms = parse_iso_datetime(manifest["window_start"])
    window_end_ms = parse_iso_datetime(manifest["window_end"])
    
    # 2. Build Identity Resolution Graph (Track A)
    identity_map = build_identity_mapping(DATA)
    products = load_products(DATA)
    
    # 3. Process Events & Sessionize (Track A)
    sessions, user_daily_rows = process_events(
        DATA, window_start_ms, window_end_ms, identity_map, products
    )
    
    # 4. Process Clicks & Attribute (Track B)
    campaigns = load_campaigns(DATA)
    campaign_daily_rows = process_clicks_and_attribute(
        DATA, window_start_ms, window_end_ms, identity_map, campaigns, sessions
    )
    
    # 5. Output Parquet files
    user_out_dir = os.path.join(OUT, "user_daily")
    campaign_out_dir = os.path.join(OUT, "campaign_daily")
    os.makedirs(user_out_dir, exist_ok=True)
    os.makedirs(campaign_out_dir, exist_ok=True)
    
    if user_daily_rows:
        df_user = pl.DataFrame(user_daily_rows, schema=USER_DAILY_SCHEMA)
    else:
        df_user = pl.DataFrame(schema=USER_DAILY_SCHEMA)
    df_user.write_parquet(os.path.join(user_out_dir, "part-0.parquet"))
    
    if campaign_daily_rows:
        df_camp = pl.DataFrame(campaign_daily_rows, schema=CAMPAIGN_DAILY_SCHEMA)
    else:
        df_camp = pl.DataFrame(schema=CAMPAIGN_DAILY_SCHEMA)
    df_camp.write_parquet(os.path.join(campaign_out_dir, "part-0.parquet"))

if __name__ == "__main__":
    main()
