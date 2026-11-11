import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
from spark_common import get_spark, PARQUET

RESULTS = os.environ.get("RESULTS_DIR", "/home/jovyan/work/results")
os.makedirs(RESULTS, exist_ok=True)
spark = get_spark("03_cohort")
t0 = time.time()

spark.read.parquet(f"{PARQUET}/users_clean").createOrReplaceTempView("users")
spark.read.parquet(f"{PARQUET}/transactions_clean").createOrReplaceTempView("txns")

# Cohort = month the user registered. For every cohort and purchase month we measure how many users bought,
# how many orders, and how much revenue. Months at the edge of the data are partial (Aug starts on the 6th,
# Nov stops on the 4th), so revenue is also normalised per observed day.
cohort_sql = """
WITH cohort AS (                                   -- size of each registration cohort
    SELECT registration_month AS cohort, COUNT(*) AS cohort_size
    FROM users GROUP BY registration_month),
days AS (                                          -- observed days per purchase month
    SELECT txn_month AS month, COUNT(DISTINCT txn_date) AS days
    FROM txns GROUP BY txn_month),
agg AS (                                           -- cohort x month activity
    SELECT u.registration_month AS cohort, t.txn_month AS month,
           COUNT(DISTINCT t.user_id) AS active_buyers, COUNT(*) AS orders, SUM(t.total) AS revenue
    FROM txns t JOIN users u ON t.user_id = u.user_id
    GROUP BY u.registration_month, t.txn_month)
SELECT a.cohort, a.month, c.cohort_size, a.active_buyers,
       ROUND(100.0 * a.active_buyers / c.cohort_size, 1)                       AS buyer_pct,
       a.orders, ROUND(a.revenue, 2)                                           AS revenue,
       ROUND(a.revenue / c.cohort_size, 2)                                     AS revenue_per_user,
       ROUND(a.revenue / c.cohort_size / d.days, 2)                            AS revenue_per_user_per_day,
       ROUND(SUM(a.revenue) OVER (PARTITION BY a.cohort ORDER BY a.month)
             / c.cohort_size, 2)                                               AS cumulative_revenue_per_user
FROM agg a JOIN cohort c ON a.cohort = c.cohort JOIN days d ON a.month = d.month
ORDER BY a.cohort, a.month
"""
df = spark.sql(cohort_sql).toPandas()
df.to_csv(f"{RESULTS}/cohort_analysis.csv", index=False)

pd.set_option("display.width", 200)
sizes = df.drop_duplicates("cohort").set_index("cohort")["cohort_size"].rename("users")
print("=== cohort sizes (users by registration month) ===")
print(sizes.to_string())
print("\n=== % of cohort that bought, by purchase month ===")
print(df.pivot(index="cohort", columns="month", values="buyer_pct").to_string())
print("\n=== revenue per user per day, by purchase month ===")
print(df.pivot(index="cohort", columns="month", values="revenue_per_user_per_day").to_string())
print("\n=== cumulative revenue per user at the end of the data ===")
print(df.groupby("cohort")["cumulative_revenue_per_user"].max().to_string())
spread = df.groupby("cohort")["revenue_per_user_per_day"].mean()
print(f"\nmean daily revenue per user: min {spread.min():.2f} | max {spread.max():.2f} | spread {100*(spread.max()/spread.min()-1):.1f}%")
print(f"total {time.time() - t0:.0f}s | saved {RESULTS}/cohort_analysis.csv")
