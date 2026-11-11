import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import pymongo
from pyspark.sql import types as T
import hbase_common as hc
from spark_common import get_spark

RESULTS = os.environ.get("RESULTS_DIR", "/home/jovyan/work/results")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")

HB_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("device", T.StringType()), T.StructField("referrer", T.StringType()),
    T.StructField("country", T.StringType()), T.StructField("status", T.StringType()),
    T.StructField("viewed_product", T.BooleanType()), T.StructField("cart_items", T.IntegerType()),
    T.StructField("checkout_page", T.BooleanType())])
MG_SCHEMA = T.StructType([
    T.StructField("txn_id", T.StringType()), T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()), T.StructField("revenue", T.DoubleType()),
    T.StructField("units", T.LongType())])

def read_hbase_sessions():
    """HBase: one row per session. We read only the columns the funnel needs and reduce the
    page-view columns to yes/no flags while streaming, so memory stays small."""
    table = hc.connect().table(hc.SESSIONS)
    cols = [b"meta:session_id", b"meta:user_id", b"meta:device", b"meta:referrer", b"meta:country",
            b"meta:status", b"pv", b"cart"]
    rows = []
    for _, d in table.scan(columns=cols, batch_size=2000):
        page_types = {v.split(b"|", 2)[1] for k, v in d.items() if k.startswith(b"pv:")}
        rows.append((d[b"meta:session_id"].decode(), d[b"meta:user_id"].decode(),
                     d[b"meta:device"].decode(), d[b"meta:referrer"].decode(),
                     d[b"meta:country"].decode(), d[b"meta:status"].decode(),
                     b"product_detail" in page_types,
                     sum(1 for k in d if k.startswith(b"cart:")),
                     b"checkout" in page_types))
    return rows

def read_mongo_txns():
    """MongoDB: only transactions that belong to a session (session_id not null)."""
    coll = pymongo.MongoClient(MONGO_URI)["ecommerce"].transactions
    cur = coll.find({"session_id": {"$ne": None}},
                    {"session_id": 1, "user_id": 1, "total": 1, "items.quantity": 1})
    return [(d["_id"], d["session_id"], d["user_id"], float(d["total"]),
             sum(i["quantity"] for i in d["items"])) for d in cur]

FUNNEL_SQL = """
WITH j AS (
    SELECT h.*, m.txn_id IS NOT NULL AS purchased, COALESCE(m.revenue, 0.0) AS revenue
    FROM hb_sessions h LEFT JOIN mg_txns m ON h.session_id = m.session_id)
SELECT {dim} AS segment,
       COUNT(*)                                                    AS sessions,
       SUM(CAST(viewed_product AS INT))                            AS viewed_product,
       SUM(CAST(cart_items > 0 AS INT))                            AS added_to_cart,
       SUM(CAST(checkout_page AND cart_items > 0 AS INT))          AS reached_checkout,
       SUM(CAST(purchased AS INT))                                 AS purchased,
       ROUND(100.0 * SUM(CAST(cart_items > 0 AS INT)) / SUM(CAST(viewed_product AS INT)), 1)                              AS view_to_cart_pct,
       ROUND(100.0 * SUM(CAST(checkout_page AND cart_items > 0 AS INT)) / SUM(CAST(cart_items > 0 AS INT)), 1)            AS cart_to_checkout_pct,
       ROUND(100.0 * SUM(CAST(purchased AS INT)) / SUM(CAST(checkout_page AND cart_items > 0 AS INT)), 1)                 AS checkout_to_purchase_pct,
       ROUND(100.0 * SUM(CAST(purchased AS INT)) / COUNT(*), 2)                                                           AS overall_pct,
       ROUND(1000 * SUM(revenue) / COUNT(*), 0)                                                                           AS revenue_per_1000_sessions
FROM j GROUP BY 1 ORDER BY sessions DESC
"""

def main():
    os.makedirs(RESULTS, exist_ok=True)
    t0 = time.time()
    hb_rows = read_hbase_sessions()
    print(f"HBase  : {len(hb_rows):,} sessions read in {time.time() - t0:.0f}s")
    t1 = time.time()
    mg_rows = read_mongo_txns()
    print(f"MongoDB: {len(mg_rows):,} linked transactions read in {time.time() - t1:.0f}s")

    spark = get_spark("04_funnel")
    spark.createDataFrame(hb_rows, HB_SCHEMA).createOrReplaceTempView("hb_sessions")
    spark.createDataFrame(mg_rows, MG_SCHEMA).createOrReplaceTempView("mg_txns")

    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 20)
    print("\n=== integrity check: HBase behaviour vs MongoDB outcome ===")
    chk = spark.sql("""
        WITH j AS (SELECT h.status, m.txn_id IS NOT NULL AS purchased
                   FROM hb_sessions h LEFT JOIN mg_txns m ON h.session_id = m.session_id)
        SELECT SUM(CAST(purchased AS INT)) AS purchased_in_mongodb,
               SUM(CAST(status = 'converted' AS INT)) AS converted_in_hbase,
               SUM(CAST(purchased <> (status = 'converted') AS INT)) AS mismatches FROM j""").toPandas()
    print(chk.to_string(index=False))
    orphans = spark.sql("SELECT COUNT(*) c FROM mg_txns m LEFT ANTI JOIN hb_sessions h ON m.session_id = h.session_id").collect()[0].c
    print("MongoDB transactions with no HBase session:", orphans)

    for name, dim in [("overall", "'ALL SESSIONS'"), ("by_device", "device"), ("by_referrer", "referrer")]:
        df = spark.sql(FUNNEL_SQL.format(dim=dim)).toPandas()
        df.to_csv(f"{RESULTS}/funnel_{name}.csv", index=False)
        print(f"\n=== funnel {name.replace('_', ' ')} ===")
        print(df.to_string(index=False))
    print(f"\ntotal {time.time() - t0:.0f}s | saved funnel_*.csv in {RESULTS}")

if __name__ == "__main__":
    main()
