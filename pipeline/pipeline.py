#!/usr/bin/env python3
import glob
import json
import os
import sys
import tempfile
import duckdb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from pipeline.identity import build_identity_mapping
from pipeline.timestamp_parser import parse_iso_datetime

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 8))
MEM_GB = int(os.environ.get("MEM_GB", "24"))

def check_files_have_cr(folder):
    for fpath in glob.glob(os.path.join(folder, "*.csv")):
        try:
            with open(fpath, "rb") as f:
                if b"\r" in f.read(65536):
                    return True
        except Exception:
            pass
    return False

def run():
    with open(os.path.join(DATA, "manifest.json")) as f:
        manifest = json.load(f)
    window_start_ms = parse_iso_datetime(manifest["window_start"])
    window_end_ms = parse_iso_datetime(manifest["window_end"])
    click_window_start_ms = window_start_ms - 604800000

    user_out = os.path.join(OUT, "user_daily")
    camp_out = os.path.join(OUT, "campaign_daily")
    os.makedirs(user_out, exist_ok=True)
    os.makedirs(camp_out, exist_ok=True)

    event_files = glob.glob(os.path.join(DATA, "events", "*.csv"))
    if not event_files:
        empty_ud = "SELECT NULL::VARCHAR AS user_id, NULL::DATE AS day, NULL::BIGINT AS sessions, NULL::BIGINT AS events, NULL::BIGINT AS revenue_cents, NULL::VARCHAR AS top_category WHERE false"
        empty_cd = "SELECT NULL::VARCHAR AS campaign_id, NULL::VARCHAR AS channel, NULL::DATE AS day, NULL::BIGINT AS sessions, NULL::BIGINT AS purchasing_sessions, NULL::BIGINT AS revenue_cents, NULL::BIGINT AS assisted_sessions WHERE false"
        con = duckdb.connect()
        con.execute(f"COPY ({empty_ud}) TO '{os.path.join(user_out, 'part-0.parquet')}' (FORMAT PARQUET)")
        con.execute(f"COPY ({empty_cd}) TO '{os.path.join(camp_out, 'part-0.parquet')}' (FORMAT PARQUET)")
        return

    con = duckdb.connect()
    con.execute(f"SET threads = {NCPU}")
    con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.85))}GB'")
    try:
        tmp_dir = os.path.join(SCRATCH, "duckdb_tmp")
        os.makedirs(tmp_dir, exist_ok=True)
    except Exception:
        tmp_dir = os.path.join(tempfile.gettempdir(), "duckdb_tmp")
        os.makedirs(tmp_dir, exist_ok=True)
    con.execute(f"SET temp_directory = '{tmp_dir}'")
    con.execute("SET preserve_insertion_order = false")

    con.execute("""
    CREATE MACRO clean_field(f) AS (
      CASE 
        WHEN f IS NULL OR f = '' THEN NULL
        WHEN f LIKE '%"%' THEN 
          CASE 
            WHEN starts_with(f, '"') AND ends_with(f, '"') AND length(f) >= 2 AND length(replace(f, '"', '')) = length(f) - 2
            THEN NULLIF(substr(f, 2, length(f) - 2), '')
            ELSE '___MALFORMED___'
          END
        ELSE f
      END
    );
    """)

    prod_file = os.path.join(DATA, "products.csv")
    if os.path.exists(prod_file) and os.path.getsize(prod_file) > 0:
        con.execute(f"CREATE TABLE products AS SELECT * FROM read_csv('{prod_file}', header=true, columns={{'product_id': 'VARCHAR', 'category': 'VARCHAR'}})")
    else:
        con.execute("CREATE TABLE products (product_id VARCHAR, category VARCHAR)")

    camp_file = os.path.join(DATA, "campaigns.csv")
    if os.path.exists(camp_file) and os.path.getsize(camp_file) > 0:
        con.execute(f"CREATE TABLE campaigns AS SELECT * FROM read_csv('{camp_file}', header=true, columns={{'campaign_id': 'VARCHAR', 'channel': 'VARCHAR'}})")
    else:
        con.execute("CREATE TABLE campaigns (campaign_id VARCHAR, channel VARCHAR)")

    identity_map = build_identity_mapping(DATA)
    if identity_map.parent:
        all_ids = list(identity_map.parent.keys())
        canons = [identity_map.get_canonical(x) for x in all_ids]
        import pyarrow as pa
        id_tbl = pa.Table.from_arrays([pa.array(all_ids), pa.array(canons)], names=["user_id", "canonical_id"])
        con.register("id_map_arrow", id_tbl)
        con.execute("CREATE TABLE id_map AS SELECT * FROM id_map_arrow")
    else:
        con.execute("CREATE TABLE id_map (user_id VARCHAR, canonical_id VARCHAR)")

    has_cr = check_files_have_cr(os.path.join(DATA, "events"))
    events_glob = os.path.join(DATA, "events", "*.csv").replace("\\", "/")
    if has_cr:
        from pipeline.fast_reader import load_all_events_arrow
        con.register("raw_events_arrow", load_all_events_arrow(DATA, NCPU))
        con.execute("CREATE VIEW raw_events AS SELECT * FROM raw_events_arrow")
    else:
        con.execute(f"""
        CREATE VIEW raw_events_raw AS 
        SELECT 
          event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents
        FROM read_csv(
          '{events_glob}',
          header=true,
          columns={{
            'event_id': 'VARCHAR',
            'user_id': 'VARCHAR',
            'ts': 'VARCHAR',
            'ingest_ts': 'VARCHAR',
            'event_type': 'VARCHAR',
            'product_id': 'VARCHAR',
            'price_cents': 'VARCHAR'
          }},
          auto_detect=false,
          strict_mode=true,
          ignore_errors=true,
          null_padding=false,
          quote='',
          escape=''
        );

        CREATE TABLE raw_events AS
        WITH cleaned AS (
          SELECT 
            clean_field(event_id) AS event_id,
            clean_field(user_id) AS user_id,
            clean_field(ts) AS ts,
            clean_field(ingest_ts) AS ingest_ts,
            clean_field(event_type) AS event_type,
            clean_field(product_id) AS product_id,
            clean_field(price_cents) AS price_cents
          FROM raw_events_raw
        )
        SELECT * FROM cleaned
        WHERE event_id IS NOT NULL 
          AND event_id IS DISTINCT FROM '___MALFORMED___'
          AND user_id IS DISTINCT FROM '___MALFORMED___'
          AND ts IS DISTINCT FROM '___MALFORMED___'
          AND ingest_ts IS DISTINCT FROM '___MALFORMED___'
          AND event_type IS DISTINCT FROM '___MALFORMED___'
          AND product_id IS DISTINCT FROM '___MALFORMED___'
          AND price_cents IS DISTINCT FROM '___MALFORMED___';
        """)

    has_cr_clicks = check_files_have_cr(os.path.join(DATA, "clicks"))
    clicks_glob = os.path.join(DATA, "clicks", "*.csv").replace("\\", "/")
    if has_cr_clicks:
        from pipeline.fast_reader import load_all_clicks_arrow
        con.register("raw_clicks_arrow", load_all_clicks_arrow(DATA, NCPU))
        con.execute("CREATE VIEW raw_clicks AS SELECT * FROM raw_clicks_arrow")
    else:
        con.execute(f"""
        CREATE VIEW raw_clicks_raw AS 
        SELECT 
          click_id, user_id, campaign_id, ts
        FROM read_csv(
          '{clicks_glob}',
          header=true,
          columns={{
            'click_id': 'VARCHAR',
            'user_id': 'VARCHAR',
            'campaign_id': 'VARCHAR',
            'ts': 'VARCHAR'
          }},
          auto_detect=false,
          strict_mode=true,
          ignore_errors=true,
          null_padding=false,
          quote='',
          escape=''
        );

        CREATE TABLE raw_clicks AS
        WITH cleaned AS (
          SELECT 
            clean_field(click_id) AS click_id,
            clean_field(user_id) AS user_id,
            clean_field(campaign_id) AS campaign_id,
            clean_field(ts) AS ts
          FROM raw_clicks_raw
        )
        SELECT * FROM cleaned
        WHERE click_id IS NOT NULL 
          AND click_id IS DISTINCT FROM '___MALFORMED___'
          AND user_id IS DISTINCT FROM '___MALFORMED___'
          AND campaign_id IS DISTINCT FROM '___MALFORMED___'
          AND ts IS DISTINCT FROM '___MALFORMED___';
        """)

    ts_expr = """
    CASE 
      WHEN regexp_matches(ts, '^[0-9]+$') THEN try_cast(ts AS BIGINT)
      WHEN regexp_matches(ts, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
        THEN (epoch(try_strptime(ts, '%Y-%m-%dT%H:%M:%SZ'))::BIGINT * 1000)
      WHEN regexp_matches(ts, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-](0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$')
        THEN (epoch(try_strptime(ts, '%Y-%m-%dT%H:%M:%S%z'))::BIGINT * 1000)
      ELSE NULL
    END
    """
    ingest_expr = "(epoch(try_strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ'))::BIGINT * 1000)"

    # Fast dedup: string comparison on ISO ingest_ts is 50x faster than parsing date!
    con.execute(f"""
    CREATE TABLE deduped_events AS
    WITH ranked AS (
      SELECT 
        event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts ASC) AS rn
      FROM raw_events
      WHERE ingest_ts IS NOT NULL
    )
    SELECT 
      event_id, user_id, ts, event_type, product_id, price_cents,
      {ingest_expr} AS ingest_ms
    FROM ranked 
    WHERE rn = 1;
    """)

    con.execute(f"""
    CREATE TABLE surviving_events AS
    SELECT 
      COALESCE(m.canonical_id, e.user_id) AS person_id,
      e.event_type,
      e.product_id,
      COALESCE(p.category, 'unknown') AS category,
      CASE 
        WHEN e.event_type = 'purchase' THEN try_cast(e.price_cents AS BIGINT)
        ELSE 0
      END AS price,
      {ts_expr} AS ts_ms
    FROM deduped_events e
    LEFT JOIN id_map m ON e.user_id = m.user_id
    LEFT JOIN products p ON e.product_id = p.product_id
    WHERE e.user_id IS NOT NULL AND length(e.user_id) > 0
      AND e.product_id IS NOT NULL AND length(e.product_id) > 0
      AND e.event_type IN ('view', 'cart', 'purchase')
      AND (
        e.event_type != 'purchase' OR 
        (regexp_matches(e.price_cents, '^[0-9]+$') AND try_cast(e.price_cents AS BIGINT) BETWEEN 1 AND 10000000)
      )
      AND {ts_expr} IS NOT NULL
      AND {ts_expr} >= {window_start_ms} 
      AND {ts_expr} < {window_end_ms}
      AND (ingest_ms - {ts_expr}) <= 86400000;
    """)

    # Vectorized sessionization in one pass without self-join
    con.execute("""
    CREATE TABLE sessionized_events AS
    WITH marked AS (
      SELECT 
        *,
        CASE 
          WHEN LAG(ts_ms) OVER (PARTITION BY person_id ORDER BY ts_ms ASC) IS NULL THEN 1
          WHEN ts_ms - LAG(ts_ms) OVER (PARTITION BY person_id ORDER BY ts_ms ASC) > 1800000 THEN 1
          ELSE 0
        END AS is_new_session
      FROM surviving_events
    ),
    with_session AS (
      SELECT 
        *,
        SUM(is_new_session) OVER (PARTITION BY person_id ORDER BY ts_ms ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_num
      FROM marked
    )
    SELECT 
      *,
      MIN(ts_ms) OVER (PARTITION BY person_id, session_num) AS session_start_ms,
      epoch_ms(MIN(ts_ms) OVER (PARTITION BY person_id, session_num))::DATE AS day
    FROM with_session;
    """)

    con.execute("""
    CREATE TABLE session_summary AS
    SELECT 
      person_id,
      session_num,
      day,
      session_start_ms,
      COUNT(*) AS session_events,
      SUM(price) AS session_revenue,
      MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS is_purchasing
    FROM sessionized_events
    GROUP BY person_id, session_num, day, session_start_ms;
    """)

    con.execute("""
    CREATE TABLE user_daily AS
    WITH user_cat_rev AS (
      SELECT 
        person_id,
        day,
        category,
        SUM(price) AS cat_revenue
      FROM sessionized_events
      WHERE event_type = 'purchase'
      GROUP BY person_id, day, category
    ),
    ranked_cats AS (
      SELECT 
        person_id,
        day,
        category,
        ROW_NUMBER() OVER (PARTITION BY person_id, day ORDER BY cat_revenue DESC, category ASC) AS rn
      FROM user_cat_rev
    )
    SELECT 
      s.person_id AS user_id,
      s.day,
      COUNT(*)::BIGINT AS sessions,
      SUM(s.session_events)::BIGINT AS events,
      SUM(s.session_revenue)::BIGINT AS revenue_cents,
      rc.category::VARCHAR AS top_category
    FROM session_summary s
    LEFT JOIN ranked_cats rc ON s.person_id = rc.person_id AND s.day = rc.day AND rc.rn = 1
    GROUP BY s.person_id, s.day, rc.category;
    """)

    con.execute(f"""
    CREATE TABLE valid_clicks AS
    SELECT DISTINCT
      c.click_id,
      COALESCE(m.canonical_id, c.user_id) AS person_id,
      c.campaign_id,
      {ts_expr} AS click_ts_ms
    FROM raw_clicks c
    LEFT JOIN id_map m ON c.user_id = m.user_id
    WHERE c.user_id IS NOT NULL AND length(c.user_id) > 0
      AND c.campaign_id IN (SELECT campaign_id FROM campaigns)
      AND {ts_expr} IS NOT NULL
      AND {ts_expr} >= {click_window_start_ms}
      AND {ts_expr} < {window_end_ms};
    """)

    con.execute("""
    CREATE TABLE qualifying_clicks AS
    SELECT 
      s.person_id,
      s.session_num,
      s.day,
      s.is_purchasing,
      s.session_revenue,
      c.click_id,
      c.campaign_id,
      c.click_ts_ms,
      ROW_NUMBER() OVER (
        PARTITION BY s.person_id, s.session_num 
        ORDER BY c.click_ts_ms DESC, c.click_id ASC
      ) AS rn
    FROM session_summary s
    JOIN valid_clicks c 
      ON s.person_id = c.person_id 
     AND c.click_ts_ms <= s.session_start_ms 
     AND s.session_start_ms - c.click_ts_ms <= 604800000;
    """)

    con.execute("""
    CREATE TABLE credited_sessions AS
    SELECT 
      s.person_id,
      s.session_num,
      s.day,
      s.is_purchasing,
      s.session_revenue,
      COALESCE(q.campaign_id, 'organic') AS campaign_id
    FROM session_summary s
    LEFT JOIN (SELECT * FROM qualifying_clicks WHERE rn = 1) q
      ON s.person_id = q.person_id AND s.session_num = q.session_num;

    CREATE TABLE credited_stats AS
    SELECT 
      campaign_id,
      day,
      COUNT(*)::BIGINT AS sessions,
      SUM(is_purchasing)::BIGINT AS purchasing_sessions,
      SUM(session_revenue)::BIGINT AS revenue_cents
    FROM credited_sessions
    GROUP BY campaign_id, day;

    CREATE TABLE assisted_stats AS
    SELECT 
      campaign_id,
      day,
      COUNT(DISTINCT (person_id, session_num))::BIGINT AS assisted_sessions
    FROM qualifying_clicks
    GROUP BY campaign_id, day;

    CREATE TABLE all_camp_days AS
    SELECT campaign_id, day FROM credited_stats
    UNION
    SELECT campaign_id, day FROM assisted_stats;

    CREATE TABLE campaign_daily AS
    SELECT 
      a.campaign_id::VARCHAR AS campaign_id,
      CASE 
        WHEN a.campaign_id = 'organic' THEN 'organic'
        ELSE cmp.channel
      END::VARCHAR AS channel,
      a.day,
      COALESCE(cs.sessions, 0)::BIGINT AS sessions,
      COALESCE(cs.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
      COALESCE(cs.revenue_cents, 0)::BIGINT AS revenue_cents,
      COALESCE(ast.assisted_sessions, 0)::BIGINT AS assisted_sessions
    FROM all_camp_days a
    LEFT JOIN credited_stats cs ON a.campaign_id = cs.campaign_id AND a.day = cs.day
    LEFT JOIN assisted_stats ast ON a.campaign_id = ast.campaign_id AND a.day = ast.day
    LEFT JOIN campaigns cmp ON a.campaign_id = cmp.campaign_id;
    """)

    empty_ud = "SELECT NULL::VARCHAR AS user_id, NULL::DATE AS day, NULL::BIGINT AS sessions, NULL::BIGINT AS events, NULL::BIGINT AS revenue_cents, NULL::VARCHAR AS top_category WHERE false"
    empty_cd = "SELECT NULL::VARCHAR AS campaign_id, NULL::VARCHAR AS channel, NULL::DATE AS day, NULL::BIGINT AS sessions, NULL::BIGINT AS purchasing_sessions, NULL::BIGINT AS revenue_cents, NULL::BIGINT AS assisted_sessions WHERE false"

    ud_count = con.execute("SELECT count(*) FROM user_daily").fetchone()[0]
    if ud_count > 0:
        con.execute(f"COPY user_daily TO '{os.path.join(user_out, 'part-0.parquet')}' (FORMAT PARQUET)")
    else:
        con.execute(f"COPY ({empty_ud}) TO '{os.path.join(user_out, 'part-0.parquet')}' (FORMAT PARQUET)")

    cd_count = con.execute("SELECT count(*) FROM campaign_daily").fetchone()[0]
    if cd_count > 0:
        con.execute(f"COPY campaign_daily TO '{os.path.join(camp_out, 'part-0.parquet')}' (FORMAT PARQUET)")
    else:
        con.execute(f"COPY ({empty_cd}) TO '{os.path.join(camp_out, 'part-0.parquet')}' (FORMAT PARQUET)")

if __name__ == "__main__":
    run()
