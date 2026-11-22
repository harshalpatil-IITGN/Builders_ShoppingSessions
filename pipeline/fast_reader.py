"""
High-performance multithreaded raw CSV line parser and deduper.
Chunk-reads files in binary, splits on exact b'\\n',
and validates/deduplicates using PyArrow tables and DuckDB.
"""
from concurrent.futures import ThreadPoolExecutor
import glob
import os
from typing import Dict, List, Optional, Tuple
import pyarrow as pa
from pipeline.csv_dialect import parse_csv_line


def process_event_file(fpath: str) -> List[Tuple[str, str, str, str, str, str, Optional[str]]]:
    """
    Parses an event CSV file strictly adhering to rules.md:
    Only \\n ends a line; \\r is kept as a character inside the field.
    """
    valid_rows = []
    with open(fpath, "rb") as f:
        _ = f.readline() # Header
        for line in f:
            row = parse_csv_line(line, 7)
            if row and row[0]: # event_id is non-empty
                # event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents
                valid_rows.append((
                    row[0],
                    row[1] or "",
                    row[2] or "",
                    row[3] or "",
                    row[4] or "",
                    row[5] or "",
                    row[6]
                ))
    return valid_rows


def process_click_file(fpath: str) -> List[Tuple[str, str, str, str]]:
    """
    Parses a click CSV file strictly adhering to rules.md:
    Only \\n ends a line; \\r is kept as a character inside the field.
    """
    valid_rows = []
    with open(fpath, "rb") as f:
        _ = f.readline() # Header
        for line in f:
            row = parse_csv_line(line, 4)
            if row and row[0]: # click_id is non-empty
                valid_rows.append((
                    row[0],
                    row[1] or "",
                    row[2] or "",
                    row[3] or ""
                ))
    return valid_rows


def load_all_events_arrow(data_dir: str, n_workers: int = 8) -> pa.Table:
    files = sorted(glob.glob(os.path.join(data_dir, "events", "*.csv")))
    all_rows = []
    if files:
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for res in executor.map(process_event_file, files):
                all_rows.extend(res)

    schema = pa.schema([
        ("event_id", pa.string()),
        ("user_id", pa.string()),
        ("ts", pa.string()),
        ("ingest_ts", pa.string()),
        ("event_type", pa.string()),
        ("product_id", pa.string()),
        ("price_cents", pa.string()),
    ])

    if not all_rows:
        return pa.table({col: [] for col in schema.names}, schema=schema)

    cols = list(zip(*all_rows))
    pydict = {
        "event_id": list(cols[0]),
        "user_id": list(cols[1]),
        "ts": list(cols[2]),
        "ingest_ts": list(cols[3]),
        "event_type": list(cols[4]),
        "product_id": list(cols[5]),
        "price_cents": list(cols[6]),
    }
    return pa.Table.from_pydict(pydict, schema=schema)


def load_all_clicks_arrow(data_dir: str, n_workers: int = 8) -> pa.Table:
    files = sorted(glob.glob(os.path.join(data_dir, "clicks", "*.csv")))
    all_rows = []
    if files:
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for res in executor.map(process_click_file, files):
                all_rows.extend(res)

    schema = pa.schema([
        ("click_id", pa.string()),
        ("user_id", pa.string()),
        ("campaign_id", pa.string()),
        ("ts", pa.string()),
    ])

    if not all_rows:
        return pa.table({col: [] for col in schema.names}, schema=schema)

    cols = list(zip(*all_rows))
    pydict = {
        "click_id": list(cols[0]),
        "user_id": list(cols[1]),
        "campaign_id": list(cols[2]),
        "ts": list(cols[3]),
    }
    return pa.Table.from_pydict(pydict, schema=schema)
