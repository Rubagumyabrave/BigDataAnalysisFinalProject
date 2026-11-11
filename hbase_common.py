import os
from datetime import datetime, timezone
import happybase

HOST = os.environ.get("HBASE_HOST", "hbase")
SESSIONS = "user_sessions"
PRODUCT_DAILY = "product_daily"
# happybase leaves bloom filter and block cache OFF unless asked, so we turn them on explicitly
OPTS = dict(max_versions=1, bloom_filter_type="ROW", block_cache_enabled=True)
MAX_TS = 9_999_999_999_999          # used to build a reverse timestamp (newest row sorts first)

def connect():
    return happybase.Connection(HOST, port=9090, timeout=120000)

def to_ms(iso):
    """ISO string (naive, treated as UTC) -> epoch milliseconds."""
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp() * 1000)

def rev_ts(iso):
    return f"{MAX_TS - to_ms(iso):013d}"

def session_key(user_id, start_time, session_id):
    """Row key: user_id # reverse_timestamp # session_id
    - same user's sessions are contiguous (prefix scan = one user)
    - reverse timestamp puts the newest session first
    - session_id keeps the key unique if two sessions start in the same millisecond."""
    return f"{user_id}#{rev_ts(start_time)}#{session_id}".encode()

def product_day_key(product_id, day):
    """Row key: product_id # yyyymmdd  (day like '2026-08-14')."""
    return f"{product_id}#{day.replace('-', '')}".encode()

def recreate_tables(h):
    existing = set(h.tables())
    for name, families in [
        (SESSIONS, {"meta": dict(OPTS),    # session info, device, geo, conversion
                    "pv":   dict(OPTS),    # one column per page view (sparse, variable)
                    "cart": dict(OPTS)}),  # one column per cart product (sparse)
        (PRODUCT_DAILY, {"stats": dict(OPTS)}),
    ]:
        if name.encode() in existing:
            h.delete_table(name, disable=True)
        h.create_table(name, families)
    return h.tables()
