import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F
from spark_common import get_spark, DATA, PARQUET, SESSIONS_SCHEMA

spark = get_spark("01_ingest")
t0 = time.time()

# ---- sessions: JSON array files -> typed columns -> Parquet ----
raw = spark.read.schema(SESSIONS_SCHEMA).option("multiLine", True).json(f"{DATA}/sessions_*.json")
sessions = (raw.withColumn("start_ts", F.col("start_time").cast("timestamp"))
               .withColumn("end_ts", F.col("end_time").cast("timestamp"))
               .withColumn("n_pages", F.size("page_views"))
               .withColumn("cart_items", F.size("cart_contents")))
sessions.write.mode("overwrite").parquet(f"{PARQUET}/sessions_raw")
print(f"sessions JSON -> Parquet: {time.time() - t0:.0f}s")

# ---- the small files ----
for name in ["users", "products", "categories", "transactions"]:
    spark.read.option("multiLine", True).json(f"{DATA}/{name}.json").write.mode("overwrite").parquet(f"{PARQUET}/{name}_raw")

s = spark.read.parquet(f"{PARQUET}/sessions_raw")
u, p, c, t = (spark.read.parquet(f"{PARQUET}/{n}_raw") for n in ["users", "products", "categories", "transactions"])
print("\n=== row counts ===")
print(f"sessions {s.count():,} | users {u.count():,} | products {p.count():,} | categories {c.count():,} | transactions {t.count():,}")
print("distinct session_id:", f"{s.select('session_id').distinct().count():,}")
print("\n=== data-quality profile ===")
print("null start/end timestamps:", s.filter("start_ts is null or end_ts is null").count())
print("end_time != start_time + duration:", s.filter(F.col("end_ts").cast("long") - F.col("start_ts").cast("long") != F.col("duration_seconds")).count())
print("status:", {r[0]: r[1] for r in s.groupBy("conversion_status").count().collect()})
pv = s.select("session_id", "cart_items", F.explode("page_views").alias("p"))
n_pv = pv.count()
print(f"page views: {n_pv:,} | zero-duration: {pv.filter('p.view_duration = 0').count():,}")
empty_cart = pv.filter("p.page_type in ('cart','checkout','confirmation') and cart_items = 0").select("session_id").distinct().count()
print(f"sessions with cart/checkout/confirmation page but EMPTY cart: {empty_cart:,}")
print("\n=== page-flow funnel (sessions reaching each page type) ===")
reach = {r[0]: r[1] for r in pv.select("session_id", "p.page_type").distinct().groupBy("page_type").count().collect()}
for k in ["home", "search", "category_listing", "product_detail", "cart", "checkout", "confirmation"]:
    print(f"  {k:17s} {reach.get(k, 0):>8,}")
print("\n=== referential integrity ===")
lt = t.filter("session_id is not null")
print("converted sessions without a transaction:", s.filter("conversion_status='converted'").join(lt, "session_id", "left_anti").count())
print("linked transactions whose session is missing:", lt.join(s, "session_id", "left_anti").count())
items = t.select(F.explode("items").alias("i")).select("i.product_id")
print("transaction items on inactive products:", items.join(p.filter("is_active = false").select("product_id"), "product_id").count())
print(f"\ntotal {time.time() - t0:.0f}s")
