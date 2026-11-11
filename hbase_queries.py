import sys, time
sys.path.insert(0, "/home/jovyan/work")
import hbase_common as hc

h = hc.connect()
sess, daily = h.table(hc.SESSIONS), h.table(hc.PRODUCT_DAILY)
USER = "user_000042"

def timed(label, fn):
    t = time.time(); out = fn(); ms = (time.time() - t) * 1000
    print(f"[{ms:8.1f} ms] {label}"); return out

# Q1 - all sessions of one user, newest first (only the small 'meta' family is read)
rows = timed("Q1 sessions of one user (prefix scan, meta only)",
             lambda: list(sess.scan(row_prefix=f"{USER}#".encode(), columns=[b"meta"])))
print(f"   {len(rows)} sessions; newest 3:")
for key, d in rows[:3]:
    print("  ", d[b"meta:start_time"].decode(), d[b"meta:status"].decode(), d[b"meta:device"].decode(), d[b"meta:referrer"].decode())

# Q2 - the most recent session with all its page views (all families)
key, d = timed("Q2 latest session, all families",
               lambda: next(iter(sess.scan(row_prefix=f"{USER}#".encode(), limit=1))))
print("   session", d[b"meta:session_id"].decode(), "| status", d[b"meta:status"].decode())
for q in sorted(k for k in d if k.startswith(b"pv:")):
    ts, page, prod, cat, dur = d[q].decode().split("|")
    print(f"   {q.decode()}  {ts[11:]}  {page:16s} {prod:11s} {dur}s")
print("   cart:", {k.decode(): v.decode() for k, v in d.items() if k.startswith(b"cart:")})

# Q3 - time-range scan for one user: sessions in the last 30 days of the dataset
lo = hc.session_key(USER, "2026-11-05T00:00:00", ""); hi = hc.session_key(USER, "2026-10-05T00:00:00", "")
r = timed("Q3 user sessions 2026-10-05..2026-11-05 (row range)",
          lambda: list(sess.scan(row_start=lo, row_stop=hi, columns=[b"meta:start_time"])))
print(f"   {len(r)} sessions")

# Q4 - product performance over time: range scan on product_id#date, then conversion rates
PID = "prod_00123"
r = timed("Q4 product_daily range scan for one product, September",
          lambda: list(daily.scan(row_start=hc.product_day_key(PID, "2026-09-01"),
                                  row_stop=hc.product_day_key(PID, "2026-10-01"))))
v = sum(int(d[b"stats:views"]) for _, d in r); c = sum(int(d[b"stats:cart_adds"]) for _, d in r)
u = sum(int(d[b"stats:units_sold"]) for _, d in r)
ul = sum(int(d[b"stats:units_sold_linked"]) for _, d in r)
print(f"   {PID}: {len(r)} days | views {v} | cart adds {c} | units sold {u} (from sessions: {ul}) | view->cart {100*c/max(v,1):.1f}%")

# Q5 - why the row key matters: same question answered by scanning the WHOLE table with a filter
r = timed("Q5 same user via full-table RowFilter (no key design)",
          lambda: list(sess.scan(filter=f"RowFilter(=, 'substring:{USER}#')", columns=[b"meta:session_id"])))
print(f"   {len(r)} sessions found (same answer, scanned all rows)")

# Table sizes
n = timed("Q6 count user_sessions rows (keys only)",
          lambda: sum(1 for _ in sess.scan(filter="KeyOnlyFilter() AND FirstKeyOnlyFilter()")))
print(f"   user_sessions rows: {n:,}")
