"""
Clicks parsing and attribution engine (Track B).
Follows rules.md:
1. Parse clicks: drop malformed lines and lines where click_id is empty.
2. Dedup on click_id: keep any one copy.
3. Validate: user_id non-empty, ts valid and window_start - 7 days <= ts < window_end,
   and campaign_id in campaigns.csv.
4. Credit each session to one click:
   - Among the same person's valid clicks (any of their IDs).
   - click.ts <= session_start AND session_start - click.ts <= 7 days (604,800,000 ms).
   - Take the one with the latest ts; on a tie in ts, smallest click_id compared byte-by-byte.
   - If no qualifying click -> credit to campaign 'organic' with channel 'organic'.
5. Assists:
   - A session is assisted by every campaign with at least one qualifying click in the 7-day window.
   - Each campaign assists the session once.
   - 'organic' never assists.
6. campaign_daily output table:
   - One row per (campaign_id, day) with at least one session credited to or assisted by the campaign.
   - If a campaign only assisted that day, it has a row with 0 in credited columns.
"""
from datetime import datetime
import glob
import os
from typing import Dict, List, Optional, Set, Tuple
from pipeline.csv_dialect import parse_csv_line
from pipeline.events import Session
from pipeline.identity import UnionFind
from pipeline.timestamp_parser import parse_timestamp


def load_campaigns(data_dir: str) -> Dict[str, str]:
    path = os.path.join(data_dir, "campaigns.csv")
    campaigns = {}
    if not os.path.exists(path):
        return campaigns
    with open(path, "rb") as f:
        _ = f.readline()
        for line in f:
            row = parse_csv_line(line, 2)
            if row and row[0] is not None:
                campaigns[row[0]] = row[1] if row[1] is not None else ""
    return campaigns


class ValidClick:
    __slots__ = ("click_id", "person_id", "campaign_id", "ts")
    def __init__(self, click_id: str, person_id: str, campaign_id: str, ts: int):
        self.click_id = click_id
        self.person_id = person_id
        self.campaign_id = campaign_id
        self.ts = ts


def process_clicks_and_attribute(
    data_dir: str,
    window_start_ms: int,
    window_end_ms: int,
    identity_map: UnionFind,
    campaigns: Dict[str, str],
    sessions: List[Session]
) -> List[dict]:
    """
    Returns list of rows for campaign_daily table.
    """
    click_files = sorted(glob.glob(os.path.join(data_dir, "clicks", "*.csv")))
    
    # 7-day lookback window start for clicks
    click_window_start = window_start_ms - 604800000 # 7 * 86400 * 1000
    
    # Dedup on click_id
    seen_click_ids: Set[str] = set()
    person_clicks: Dict[str, List[ValidClick]] = {}
    
    for fpath in click_files:
        with open(fpath, "rb") as f:
            _ = f.readline() # Header: click_id,user_id,campaign_id,ts
            for line in f:
                row = parse_csv_line(line, 4)
                if not row:
                    continue
                click_id, user_id, campaign_id, ts_str = row
                if not click_id:
                    continue
                if click_id in seen_click_ids:
                    continue
                seen_click_ids.add(click_id)
                
                # Validate
                if not user_id:
                    continue
                if not campaign_id or campaign_id not in campaigns:
                    continue
                    
                ts_ms = parse_timestamp(ts_str)
                if ts_ms is None or ts_ms < click_window_start or ts_ms >= window_end_ms:
                    continue
                    
                person = identity_map.get_canonical(user_id)
                if person not in person_clicks:
                    person_clicks[person] = []
                person_clicks[person].append(ValidClick(click_id, person, campaign_id, ts_ms))

    # Campaign aggregation tracker: (campaign_id, day_str) -> metrics
    # day_str -> format YYYY-MM-DD
    camp_daily_map: Dict[Tuple[str, str], dict] = {}
    
    def get_camp_entry(c_id: str, day_str: str) -> dict:
        key = (c_id, day_str)
        if key not in camp_daily_map:
            channel = "organic" if c_id == "organic" else campaigns.get(c_id, "")
            camp_daily_map[key] = {
                "campaign_id": c_id,
                "channel": channel,
                "day": day_str,
                "sessions": 0,
                "purchasing_sessions": 0,
                "revenue_cents": 0,
                "assisted_sessions": 0,
            }
        return camp_daily_map[key]

    SEVEN_DAYS_MS = 604800000

    # Attribute each session
    for s in sessions:
        clicks = person_clicks.get(s.person_id, [])
        # Find all qualifying clicks:
        # click.ts <= session_start AND (session_start - click.ts) <= 7 days
        qualifying = [
            c for c in clicks 
            if c.ts <= s.start_ts and (s.start_ts - c.ts) <= SEVEN_DAYS_MS
        ]
        
        if not qualifying:
            # Credit to organic
            entry = get_camp_entry("organic", s.day_str)
            entry["sessions"] += 1
            if s.is_purchasing:
                entry["purchasing_sessions"] += 1
            entry["revenue_cents"] += s.revenue_cents
        else:
            # Pick winning click: latest ts, smallest click_id on tie
            # We sort descending by ts, ascending by click_id
            qualifying.sort(key=lambda c: (-c.ts, c.click_id))
            winning_click = qualifying[0]
            
            entry = get_camp_entry(winning_click.campaign_id, s.day_str)
            entry["sessions"] += 1
            if s.is_purchasing:
                entry["purchasing_sessions"] += 1
            entry["revenue_cents"] += s.revenue_cents
            
            # Assists: each distinct non-organic campaign with a qualifying click assists once
            assisting_campaigns = {c.campaign_id for c in qualifying}
            for c_id in assisting_campaigns:
                c_entry = get_camp_entry(c_id, s.day_str)
                c_entry["assisted_sessions"] += 1

    # Format into list of dicts for parquet
    campaign_daily_rows = []
    for (c_id, day_str), data in camp_daily_map.items():
        campaign_daily_rows.append({
            "campaign_id": data["campaign_id"],
            "channel": data["channel"],
            "day": datetime.strptime(day_str, "%Y-%m-%d").date(),
            "sessions": data["sessions"],
            "purchasing_sessions": data["purchasing_sessions"],
            "revenue_cents": data["revenue_cents"],
            "assisted_sessions": data["assisted_sessions"],
        })

    return campaign_daily_rows
