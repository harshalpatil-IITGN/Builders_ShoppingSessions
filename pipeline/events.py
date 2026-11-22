"""
Event processing and sessionization pipeline for Track A.
Follows rules.md:
1. Parse: drop malformed lines and lines where event_id is empty.
2. Dedup on event_id: keep ONLY the copy with the smallest ingest_ts.
   Dedup comes before validation!
3. Validate kept copy:
   - user_id non-empty
   - ts is valid and window_start <= ts < window_end
   - event_type in ('view', 'cart', 'purchase')
   - product_id non-empty
   - If purchase: price_cents must be ASCII digits 1 to 10,000,000.
4. Drop late events: ingest_ts - ts > 24 hours (86,400,000 ms).
5. Map user_id to person_id (using identity.py UnionFind).
6. Sessionize surviving events per person:
   - Sorted by ts.
   - Gap > 30 minutes (1,800,000 ms) starts a new session.
   - Equal timestamps share a session.
7. One day per session: UTC date of its start.
8. Categories from products.csv. Unknown category if not present.
"""
from datetime import datetime, timezone
import glob
import os
from typing import Dict, List, Optional, Tuple
from pipeline.csv_dialect import parse_csv_line
from pipeline.identity import UnionFind
from pipeline.timestamp_parser import parse_timestamp, parse_iso_datetime


def is_valid_price(p: Optional[str]) -> Optional[int]:
    if not p:
        return None
    for c in p:
        if ord(c) < 48 or ord(c) > 57:
            return None
    try:
        val = int(p)
        if 1 <= val <= 10000000:
            return val
    except (ValueError, OverflowError):
        return None
    return None


def load_products(data_dir: str) -> Dict[str, str]:
    products_csv = os.path.join(data_dir, "products.csv")
    products = {}
    if not os.path.exists(products_csv):
        return products
    with open(products_csv, "rb") as f:
        _ = f.readline()
        for line in f:
            row = parse_csv_line(line, 2)
            if row and row[0] is not None:
                products[row[0]] = row[1] if row[1] is not None else ""
    return products


class RawEventCopy:
    __slots__ = ("ingest_ts_ms", "user_id", "ts_str", "event_type", "product_id", "price_str")
    def __init__(self, ingest_ts_ms: int, user_id: Optional[str], ts_str: Optional[str],
                 event_type: Optional[str], product_id: Optional[str], price_str: Optional[str]):
        self.ingest_ts_ms = ingest_ts_ms
        self.user_id = user_id
        self.ts_str = ts_str
        self.event_type = event_type
        self.product_id = product_id
        self.price_str = price_str


class CleanEvent:
    __slots__ = ("ts", "event_type", "product_id", "price_cents")
    def __init__(self, ts: int, event_type: str, product_id: str, price_cents: int):
        self.ts = ts
        self.event_type = event_type
        self.product_id = product_id
        self.price_cents = price_cents


class Session:
    __slots__ = ("session_id", "person_id", "start_ts", "day_str", "events_count",
                 "revenue_cents", "top_category", "is_purchasing")
    def __init__(self, session_id: int, person_id: str, start_ts: int, day_str: str):
        self.session_id = session_id
        self.person_id = person_id
        self.start_ts = start_ts
        self.day_str = day_str
        self.events_count = 0
        self.revenue_cents = 0
        self.top_category: Optional[str] = None
        self.is_purchasing = False


def process_events(
    data_dir: str,
    window_start_ms: int,
    window_end_ms: int,
    identity_map: UnionFind,
    products: Dict[str, str]
) -> Tuple[List[Session], List[dict]]:
    """
    Returns:
    - List of Sessions (for attribution in Track B)
    - List of user_daily rows (for output table 1)
    """
    event_files = sorted(glob.glob(os.path.join(data_dir, "events", "*.csv")))
    
    # Step 1 & 2: Parse and Dedup on event_id keeping smallest ingest_ts
    kept_events: Dict[str, RawEventCopy] = {}
    
    for fpath in event_files:
        with open(fpath, "rb") as f:
            _ = f.readline() # Header: event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents
            for line in f:
                row = parse_csv_line(line, 7)
                if not row:
                    continue
                event_id, user_id, ts_str, ingest_ts_str, event_type, product_id, price_str = row
                if not event_id:
                    continue
                # ingest_ts is always valid F1 according to rules.md
                ingest_ms = parse_iso_datetime(ingest_ts_str) if ingest_ts_str else None
                if ingest_ms is None:
                    continue
                    
                prev = kept_events.get(event_id)
                if prev is None or ingest_ms < prev.ingest_ts_ms:
                    kept_events[event_id] = RawEventCopy(
                        ingest_ms, user_id, ts_str, event_type, product_id, price_str
                    )

    # Step 3 & 4: Validate kept copy and drop late events
    person_events: Dict[str, List[CleanEvent]] = {}
    
    for event_id, raw in kept_events.items():
        if not raw.user_id:
            continue
        if raw.event_type not in ("view", "cart", "purchase"):
            continue
        if not raw.product_id:
            continue
            
        ts_ms = parse_timestamp(raw.ts_str)
        if ts_ms is None or ts_ms < window_start_ms or ts_ms >= window_end_ms:
            continue
            
        # Purchase price validation
        price_cents = 0
        if raw.event_type == "purchase":
            p = is_valid_price(raw.price_str)
            if p is None:
                continue
            price_cents = p
            
        # Late event check: drop if ingest_ts - ts > 24 hours (86,400,000 ms)
        if (raw.ingest_ts_ms - ts_ms) > 86400000:
            continue
            
        person = identity_map.get_canonical(raw.user_id)
        if person not in person_events:
            person_events[person] = []
        person_events[person].append(CleanEvent(ts_ms, raw.event_type, raw.product_id, price_cents))

    # Step 5 & 6: Sessionize each person's events
    all_sessions: List[Session] = []
    session_counter = 0
    
    # Store user_daily metrics: (person, day) -> {sessions, events, revenue_cents, category_rev}
    user_daily_map: Dict[Tuple[str, str], dict] = {}
    
    for person, ev_list in person_events.items():
        # Sort surviving events by ts
        ev_list.sort(key=lambda e: e.ts)
        
        # Group into sessions: gap between DISTINCT timestamps > 30 min (1,800,000 ms)
        current_session: Optional[Session] = None
        last_distinct_ts: Optional[int] = None
        
        # For category tracking in the current session
        session_cat_rev: Dict[str, int] = {}
        
        def finalize_session(s: Session, cat_rev: Dict[str, int]):
            if s is None:
                return
            if cat_rev:
                # Top category: highest revenue, ties broken by byte-wise smallest name
                best_cat = None
                best_rev = -1
                for cat, rev in cat_rev.items():
                    if rev > best_rev:
                        best_rev = rev
                        best_cat = cat
                    elif rev == best_rev:
                        if best_cat is None or cat < best_cat:
                            best_cat = cat
                s.top_category = best_cat
            
            key = (s.person_id, s.day_str)
            if key not in user_daily_map:
                user_daily_map[key] = {
                    "user_id": s.person_id,
                    "day": s.day_str,
                    "sessions": 0,
                    "events": 0,
                    "revenue_cents": 0,
                    "cat_rev": {}
                }
            ud = user_daily_map[key]
            ud["sessions"] += 1
            ud["events"] += s.events_count
            ud["revenue_cents"] += s.revenue_cents
            for c, r in cat_rev.items():
                ud["cat_rev"][c] = ud["cat_rev"].get(c, 0) + r

        for ev in ev_list:
            if current_session is None:
                session_counter += 1
                # Day of session start in UTC
                dt = datetime.fromtimestamp(ev.ts / 1000, tz=timezone.utc)
                day_str = dt.strftime("%Y-%m-%d")
                current_session = Session(session_counter, person, ev.ts, day_str)
                last_distinct_ts = ev.ts
                session_cat_rev = {}
                all_sessions.append(current_session)
            else:
                if ev.ts != last_distinct_ts:
                    if (ev.ts - last_distinct_ts) > 1800000:
                        finalize_session(current_session, session_cat_rev)
                        session_counter += 1
                        dt = datetime.fromtimestamp(ev.ts / 1000, tz=timezone.utc)
                        day_str = dt.strftime("%Y-%m-%d")
                        current_session = Session(session_counter, person, ev.ts, day_str)
                        session_cat_rev = {}
                        all_sessions.append(current_session)
                    last_distinct_ts = ev.ts

            current_session.events_count += 1
            if ev.event_type == "purchase":
                current_session.is_purchasing = True
                current_session.revenue_cents += ev.price_cents
                cat = products.get(ev.product_id, "unknown")
                session_cat_rev[cat] = session_cat_rev.get(cat, 0) + ev.price_cents
                
        if current_session is not None:
            finalize_session(current_session, session_cat_rev)

    # Compute final user_daily rows
    user_daily_rows = []
    for (person, day_str), ud in user_daily_map.items():
        top_cat = None
        if ud["cat_rev"]:
            best_cat = None
            best_rev = -1
            for cat, rev in ud["cat_rev"].items():
                if rev > best_rev:
                    best_rev = rev
                    best_cat = cat
                elif rev == best_rev:
                    if best_cat is None or cat < best_cat:
                        best_cat = cat
            top_cat = best_cat
        
        user_daily_rows.append({
            "user_id": person,
            "day": datetime.strptime(day_str, "%Y-%m-%d").date(),
            "sessions": ud["sessions"],
            "events": ud["events"],
            "revenue_cents": ud["revenue_cents"],
            "top_category": top_cat
        })

    return all_sessions, user_daily_rows
