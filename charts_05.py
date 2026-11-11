import os, sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mongo_pipelines import db, P1_REVENUE_BY_CATEGORY, P2_USER_SEGMENTS   # MongoDB aggregation pipelines

RESULTS = os.environ.get("RESULTS_DIR", "/home/jovyan/work/results")      # CSVs written by the Spark jobs
FIGS = os.environ.get("FIG_DIR", "/home/jovyan/work/figures")
os.makedirs(FIGS, exist_ok=True)
sns.set_theme(style="whitegrid", font_scale=0.95)
BLUE, ORANGE, GREY = "#2b6cb0", "#dd8452", "#9aa5b1"

def save(fig, name):
    path = f"{FIGS}/{name}"
    fig.tight_layout(); fig.savefig(path, dpi=160); plt.close(fig); print("saved", path)

# ---------- Figure 1: purchase funnel (HBase sessions + MongoDB transactions, joined in Spark) ----------
f = pd.read_csv(f"{RESULTS}/funnel_overall.csv").iloc[0]
stages = ["All sessions", "Viewed a product", "Added to cart", "Reached checkout", "Purchased"]
vals = [f.sessions, f.viewed_product, f.added_to_cart, f.reached_checkout, f.purchased]
fig, ax = plt.subplots(figsize=(8, 4.2))
ax.barh(stages[::-1], vals[::-1], color=[ORANGE, BLUE, BLUE, BLUE, GREY][::-1])
for i, (name, v) in enumerate(zip(stages, vals)):
    y = len(stages) - 1 - i
    txt = f"{int(v):,}" if i == 0 else f"{int(v):,}  ({100 * v / vals[i - 1]:.1f}% of previous stage)"
    ax.text(v + 2500, y, txt, va="center", fontsize=9)
ax.set_xlim(0, 200000 * 1.75); ax.set_xlabel("Sessions"); ax.grid(axis="y", visible=False)
ax.set_title(f"Purchase funnel: {100 * f.purchased / f.sessions:.1f}% of sessions end in a purchase")
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{int(x / 1000)}k"))
save(fig, "fig1_funnel.png")

# ---------- Figure 2: weekly revenue (MongoDB) ----------
week_pipe = [{"$group": {"_id": {"$dateTrunc": {"date": "$timestamp", "unit": "week", "startOfWeek": "monday"}},
                         "revenue": {"$sum": "$total"}, "orders": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
w = pd.DataFrame(list(db.transactions.aggregate(week_pipe))).rename(columns={"_id": "week"})
colors = [GREY if i in (0, len(w) - 1) else BLUE for i in range(len(w))]     # first and last weeks are partial
fig, ax = plt.subplots(figsize=(8, 4))
ax.bar(range(len(w)), w["revenue"] / 1e6, color=colors)
ax.set_xticks(range(len(w))); ax.set_xticklabels([d.strftime("%d %b") for d in w["week"]], rotation=45, ha="right")
ax.set_ylabel("Revenue (millions)"); ax.set_xlabel("Week starting (Monday)"); ax.set_ylim(0, w["revenue"].max() / 1e6 * 1.25); ax.grid(axis="x", visible=False)
ax.set_title("Weekly revenue: flat after the partial first and last weeks (grey)")
full = w["revenue"].iloc[1:-1] / 1e6
ax.axhline(full.mean(), color=ORANGE, ls="--", lw=1.2); ax.text(len(w) - 0.6, full.mean() * 1.04, f"mean {full.mean():.1f}M", color=ORANGE, ha="right", fontsize=9)
save(fig, "fig2_weekly_revenue.png")

# ---------- Figure 3: revenue vs estimated profit, top 10 categories (MongoDB pipeline 1) ----------
c = pd.DataFrame(list(db.transactions.aggregate(P1_REVENUE_BY_CATEGORY))).drop(columns="monthly").iloc[::-1]
y = np.arange(len(c)); h = 0.38
fig, ax = plt.subplots(figsize=(8, 4.8))
ax.barh(y + h / 2, c["total_revenue"] / 1e6, h, label="Gross revenue", color=BLUE)
ax.barh(y - h / 2, c["est_profit"] / 1e6, h, label="Estimated profit (avg. subcategory margin)", color=ORANGE)
ax.set_yticks(y); ax.set_yticklabels([n if len(n) < 27 else n[:25] + "…" for n in c["category"]])
for yy, rev, prof in zip(y, c["total_revenue"], c["est_profit"]):
    ax.text(prof / 1e6 + 0.05, yy - h / 2, f"{100 * prof / rev:.0f}% margin", va="center", fontsize=8, color="#7a4a1e")
ax.set_xlabel("Millions"); ax.grid(axis="y", visible=False)
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=2, fontsize=8, frameon=False)
ax.set_title("Top 10 categories: similar revenue, but profit differs more")
save(fig, "fig3_category_revenue_profit.png")

# ---------- Figure 4: user spend segments (MongoDB pipeline 2) ----------
seg = pd.DataFrame([{"segment": f"S{i + 1}", "avg_spend": r["avg_spend"], "avg_orders": r["avg_orders"],
                     "total": r["total_spend"], "users": r["users"]} for i, r in enumerate(db.transactions.aggregate(P2_USER_SEGMENTS))])
seg["share"] = 100 * seg["total"] / seg["total"].sum()
fig, ax = plt.subplots(figsize=(7.5, 4))
ax.bar(seg["segment"], seg["avg_spend"] / 1000, color=[GREY, "#6b9ac4", BLUE, "#1a4a7a"])
for i, r in seg.iterrows():
    ax.text(i, r["avg_spend"] / 1000 + 0.3, f"{r['avg_orders']:.1f} orders\n{r['share']:.1f}% of revenue", ha="center", fontsize=8.5)
ax.set_ylim(0, seg["avg_spend"].max() / 1000 * 1.25); ax.grid(axis="x", visible=False)
ax.set_ylabel("Average spend per user (thousands)"); ax.set_xlabel("Spend segment (equal-sized quarters of users, S1 lowest)")
ax.set_title(f"User segments: the top quarter of users brings {seg['share'].iloc[-1]:.0f}% of revenue")
save(fig, "fig4_user_segments.png")

# ---------- Figure 5: cohort heatmap (Spark SQL) ----------
co = pd.read_csv(f"{RESULTS}/cohort_analysis.csv")
piv = co.pivot(index="cohort", columns="month", values="revenue_per_user_per_day")
piv.columns = [m + ("*" if m in (piv.columns[0], piv.columns[-1]) else "") for m in piv.columns]
fig, ax = plt.subplots(figsize=(7.5, 4.2))
sns.heatmap(piv, annot=True, fmt=".0f", cmap="Blues", vmin=0, cbar_kws={"label": "Revenue per user per day"}, ax=ax, linewidths=0.5)
ax.set_xlabel("Purchase month (* = partial month)"); ax.set_ylabel("Registration cohort")
ax.set_title("Revenue per user per day by cohort: no visible cohort effect")
save(fig, "fig5_cohort_heatmap.png")
