import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
from pyspark.sql import functions as F
from spark_common import get_spark, PARQUET

RESULTS = os.environ.get("RESULTS_DIR", "/home/jovyan/work/results")
os.makedirs(RESULTS, exist_ok=True)
spark = get_spark("02_clean")
t0 = time.time()
report = []                                   # (dataset, check, rows affected, total rows, action)
def rep(ds, check, n, total, action):
    report.append((ds, check, int(n), int(total), action))

# ============================== SESSIONS ==============================
s = spark.read.parquet(f"{PARQUET}/sessions_raw")
n_in = s.count()
s = s.dropDuplicates(["session_id"])                                   # rule 1: no duplicate sessions
rep("sessions", "duplicate session_id removed", n_in - s.count(), n_in, "dropped")

pt = lambda name: F.exists("page_views", lambda x: x.page_type == name)
sc = (s.withColumn("user_id", F.trim("user_id"))                      # rule 2: standardise text formats
        .withColumn("country", F.upper(F.trim("geo_data.country")))
        .withColumn("state", F.upper(F.trim("geo_data.state")))
        .withColumn("city", F.trim("geo_data.city"))
        .withColumn("device", F.lower(F.trim("device_profile.type")))
        .withColumn("os", F.trim("device_profile.os"))
        .withColumn("browser", F.trim("device_profile.browser"))
        .withColumn("referrer", F.lower(F.trim("referrer")))
        .withColumn("session_date", F.to_date("start_ts"))
        .withColumn("start_hour", F.hour("start_ts"))
        .withColumn("day_of_week", F.date_format("start_ts", "E"))
        # rule 3: page-flow facts used by the funnel
        .withColumn("has_product_detail", pt("product_detail"))
        .withColumn("has_cart_page", pt("cart"))
        .withColumn("has_checkout_page", pt("checkout"))
        .withColumn("has_confirmation_page", pt("confirmation"))
        .withColumn("n_zero_dur_views", F.size(F.filter("page_views", lambda x: x.view_duration == 0)))
        # rule 4: flag (do not delete) generator inconsistencies
        .withColumn("flag_empty_cart_flow", (F.col("has_cart_page") | F.col("has_checkout_page") | F.col("has_confirmation_page")) & (F.col("cart_items") == 0))
        .withColumn("flag_confirm_no_purchase", F.col("has_confirmation_page") & (F.col("conversion_status") != "converted"))
        # clean funnel stages: a stage only counts if the previous one is real
        .withColumn("f_viewed_product", F.col("has_product_detail"))
        .withColumn("f_added_to_cart", F.col("cart_items") > 0)
        .withColumn("f_reached_checkout", F.col("has_checkout_page") & (F.col("cart_items") > 0))
        .withColumn("f_converted", F.col("conversion_status") == "converted")
        .drop("geo_data", "device_profile", "start_time", "end_time"))
a = sc.agg(F.count("*").alias("n"),
           F.sum((F.col("n_zero_dur_views") > 0).cast("int")).alias("zero_sessions"),
           F.sum("n_zero_dur_views").alias("zero_views"),
           F.sum(F.col("flag_empty_cart_flow").cast("int")).alias("empty_cart"),
           F.sum(F.col("flag_confirm_no_purchase").cast("int")).alias("confirm_no_buy"),
           F.sum(F.col("start_ts").isNull().cast("int")).alias("null_ts"),
           F.sum(F.col("user_id").isNull().cast("int")).alias("null_user"),
           F.sum(F.col("referrer").isNull().cast("int")).alias("null_ref")).collect()[0]
rep("sessions", "null timestamps / user_id / referrer", a.null_ts + a.null_user + a.null_ref, a.n, "none found")
rep("sessions", "sessions containing zero-duration page views", a.zero_sessions, a.n, f"flagged ({a.zero_views:,} views counted in n_zero_dur_views)")
rep("sessions", "cart/checkout/confirmation page with an empty cart", a.empty_cart, a.n, "flagged flag_empty_cart_flow")
rep("sessions", "confirmation page without a purchase", a.confirm_no_buy, a.n, "flagged flag_confirm_no_purchase")
sc.write.mode("overwrite").parquet(f"{PARQUET}/sessions_clean")

# ============================== TRANSACTIONS ==============================
t = spark.read.parquet(f"{PARQUET}/transactions_raw")
n_in = t.count()
t = t.dropDuplicates(["transaction_id"])
rep("transactions", "duplicate transaction_id removed", n_in - t.count(), n_in, "dropped")
tc = (t.withColumn("txn_ts", F.col("timestamp").cast("timestamp"))
        .withColumn("txn_date", F.to_date("txn_ts"))
        .withColumn("txn_month", F.date_format("txn_ts", "yyyy-MM"))
        .withColumn("n_items", F.size("items"))
        .withColumn("units", F.aggregate("items", F.lit(0).cast("long"), lambda acc, x: acc + x.quantity))
        .withColumn("items_sum", F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x.subtotal))
        .withColumn("is_linked", F.col("session_id").isNotNull())
        .withColumn("flag_subtotal_mismatch", F.abs(F.col("items_sum") - F.col("subtotal")) > 0.011)
        .withColumn("flag_total_mismatch", F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) > 0.011)
        .drop("timestamp"))
b = tc.agg(F.count("*").alias("n"), F.sum((~F.col("is_linked")).cast("int")).alias("unlinked"),
           F.sum(F.col("flag_subtotal_mismatch").cast("int")).alias("sub_bad"),
           F.sum(F.col("flag_total_mismatch").cast("int")).alias("tot_bad"),
           F.sum((F.col("n_items") == 0).cast("int")).alias("empty"),
           F.sum((F.col("total") <= 0).cast("int")).alias("nonpos")).collect()[0]
rep("transactions", "subtotal differs from sum of items", b.sub_bad, b.n, "none found" if b.sub_bad == 0 else "flagged")
rep("transactions", "total != subtotal - discount", b.tot_bad, b.n, "none found" if b.tot_bad == 0 else "flagged")
rep("transactions", "no items / non-positive total", b.empty + b.nonpos, b.n, "none found")
rep("transactions", "not linked to a session (session_id null)", b.unlinked, b.n, "kept, is_linked=false (excluded from funnel)")
tc.write.mode("overwrite").parquet(f"{PARQUET}/transactions_clean")

# ============================== USERS & PRODUCTS ==============================
u = spark.read.parquet(f"{PARQUET}/users_raw")
uc = (u.withColumn("registered", F.col("registration_date").cast("timestamp"))
       .withColumn("last_active_ts", F.col("last_active").cast("timestamp"))
       .withColumn("registration_month", F.date_format("registered", "yyyy-MM"))
       .withColumn("country", F.upper(F.trim("geo_data.country"))).withColumn("state", F.upper(F.trim("geo_data.state")))
       .withColumn("city", F.trim("geo_data.city")).drop("geo_data", "registration_date", "last_active"))
bad_u = uc.filter(F.col("last_active_ts") < F.col("registered")).count()
rep("users", "last_active earlier than registration", bad_u, uc.count(), "none found" if bad_u == 0 else "flagged")
uc.write.mode("overwrite").parquet(f"{PARQUET}/users_clean")

p = spark.read.parquet(f"{PARQUET}/products_raw")
pc = (p.withColumn("created", F.col("creation_date").cast("timestamp"))
       .withColumn("is_sellable", F.col("is_active") & (F.col("current_stock") > 0)).drop("creation_date"))
rep("products", "inactive products", pc.filter(~F.col("is_active")).count(), pc.count(), "kept, is_sellable=false")
rep("products", "zero stock", pc.filter(F.col("current_stock") == 0).count(), pc.count(), "kept, is_sellable=false")
pc.write.mode("overwrite").parquet(f"{PARQUET}/products_clean")

# ============================== REPORT ==============================
df = pd.DataFrame(report, columns=["dataset", "check", "rows", "of_total", "action"])
df["pct"] = (100 * df["rows"] / df["of_total"]).round(2)
pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 70)
print(df[["dataset", "check", "rows", "of_total", "pct", "action"]].to_string(index=False))
df.to_csv(f"{RESULTS}/cleaning_report.csv", index=False)
print(f"\ntotal {time.time() - t0:.0f}s | clean Parquet written to {PARQUET}")
