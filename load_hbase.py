import json, os, sys, time, resource
from collections import defaultdict
sys.path.insert(0, "/home/jovyan/work")
import hbase_common as hc

D = sys.argv[1] if len(sys.argv) > 1 else "/home/jovyan/work/data"
BATCH = 1000

def load(name):
    with open(os.path.join(D, name)) as f:
        return json.load(f)

def s(v):
    return b"" if v is None else str(v).encode()

h = hc.connect()
print("tables:", hc.recreate_tables(h))
sessions_t = h.table(hc.SESSIONS)

# product_daily counters, filled while the sessions stream through
views = defaultdict(int)       # (product, day) -> product_detail page views
cart_adds = defaultdict(int)   # (product, day) -> sessions with the product in the cart

t0 = time.time()
n_rows = n_cells = 0
session_files = sorted(f for f in os.listdir(D) if f.startswith("sessions_") and f.endswith(".json"))
for fname in session_files:
    sessions = load(fname)                      # one file at a time keeps memory bounded
    with sessions_t.batch(batch_size=BATCH) as b:
        for x in sessions:
            geo, dev = x["geo_data"], x["device_profile"]
            day = x["start_time"][:10]
            cells = {
                b"meta:session_id": s(x["session_id"]), b"meta:user_id": s(x["user_id"]),
                b"meta:start_time": s(x["start_time"]), b"meta:end_time": s(x["end_time"]),
                b"meta:duration_s": s(x["duration_seconds"]), b"meta:status": s(x["conversion_status"]),
                b"meta:referrer": s(x["referrer"]), b"meta:device": s(dev["type"]),
                b"meta:os": s(dev["os"]), b"meta:browser": s(dev["browser"]),
                b"meta:city": s(geo["city"]), b"meta:state": s(geo["state"]),
                b"meta:country": s(geo["country"]), b"meta:ip": s(geo["ip_address"]),
            }
            for i, pv in enumerate(x["page_views"]):
                cells[f"pv:{i:02d}".encode()] = "|".join(
                    [pv["timestamp"], pv["page_type"], pv["product_id"] or "",
                     pv["category_id"] or "", str(pv["view_duration"])]).encode()
                if pv["page_type"] == "product_detail" and pv["product_id"]:
                    views[(pv["product_id"], day)] += 1
            for pid, c in x["cart_contents"].items():
                cells[f"cart:{pid}".encode()] = f"{c['quantity']}|{c['price']}".encode()
                cart_adds[(pid, day)] += 1
            b.put(hc.session_key(x["user_id"], x["start_time"], x["session_id"]), cells)
            n_rows += 1
            n_cells += len(cells)
            if n_rows % 20000 == 0:
                print(f"  {n_rows:,} sessions loaded ({time.time() - t0:.0f}s)")
    del sessions
print(f"user_sessions: {n_rows:,} rows, {n_cells:,} cells in {time.time() - t0:.0f}s")

# purchases per product per day come from the transactions file
units = defaultdict(int); units_linked = defaultdict(int); revenue = defaultdict(float)
for t in load("transactions.json"):
    day = t["timestamp"][:10]
    for it in t["items"]:
        k = (it["product_id"], day)
        units[k] += it["quantity"]
        revenue[k] += it["subtotal"]
        if t["session_id"]:
            units_linked[k] += it["quantity"]

keys = set(views) | set(cart_adds) | set(units)
pd_t = h.table(hc.PRODUCT_DAILY)
with pd_t.batch(batch_size=BATCH) as b:
    for (pid, day) in keys:
        b.put(hc.product_day_key(pid, day), {
            b"stats:views": str(views.get((pid, day), 0)).encode(),
            b"stats:cart_adds": str(cart_adds.get((pid, day), 0)).encode(),
            b"stats:units_sold": str(units.get((pid, day), 0)).encode(),
            b"stats:units_sold_linked": str(units_linked.get((pid, day), 0)).encode(),
            b"stats:revenue": f"{revenue.get((pid, day), 0.0):.2f}".encode()})
print(f"product_daily: {len(keys):,} rows")
print(f"total time {time.time() - t0:.0f}s | peak memory {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MB")
