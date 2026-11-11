import sys, time
import pymongo
import pandas as pd

URI = sys.argv[1] if len(sys.argv) > 1 else "mongodb://mongo:27017"
db = pymongo.MongoClient(URI)["ecommerce"]

# Pipeline 1 - Revenue analytics: gross revenue, units and estimated profit per category, with monthly trend.
# items are embedded and carry category_id, so no $lookup is needed to reach the category.
P1_REVENUE_BY_CATEGORY = [
    {"$unwind": "$items"},
    {"$group": {"_id": {"cat": "$items.category_id",
                        "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
                "revenue": {"$sum": "$items.subtotal"},
                "units": {"$sum": "$items.quantity"}}},
    {"$sort": {"_id.month": 1}},
    {"$group": {"_id": "$_id.cat",
                "total_revenue": {"$sum": "$revenue"},
                "units": {"$sum": "$units"},
                "monthly": {"$push": {"month": "$_id.month", "revenue": {"$round": ["$revenue", 2]}}}}},
    {"$lookup": {"from": "categories", "localField": "_id", "foreignField": "_id", "as": "cat"}},
    {"$unwind": "$cat"},
    {"$project": {"_id": 0, "category_id": "$_id", "category": "$cat.name",
                  "total_revenue": {"$round": ["$total_revenue", 2]}, "units": 1,
                  "est_profit": {"$round": [{"$multiply": ["$total_revenue", "$cat.avg_profit_margin"]}, 2]},
                  "monthly": 1}},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 10},
]

# Pipeline 2 - Basic user segmentation: per-user orders and spend, then 4 equal-sized spend segments.
P2_USER_SEGMENTS = [
    {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "spend": {"$sum": "$total"}}},
    {"$bucketAuto": {"groupBy": "$spend", "buckets": 4,
                     "output": {"users": {"$sum": 1}, "avg_orders": {"$avg": "$orders"},
                                "avg_spend": {"$avg": "$spend"}, "total_spend": {"$sum": "$spend"}}}},
]

if __name__ == "__main__":
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 20)

    t = time.time()
    r1 = list(db.transactions.aggregate(P1_REVENUE_BY_CATEGORY))
    t1 = time.time() - t
    base = pd.DataFrame(r1).drop(columns="monthly")
    months = pd.DataFrame([{m["month"]: m["revenue"] for m in r["monthly"]} for r in r1])
    print("=== Pipeline 1: top 10 categories by gross revenue ===")
    print(pd.concat([base, months], axis=1).to_string(index=False))
    print(f"(ran in {t1:.2f}s)\n")

    t = time.time()
    r2 = list(db.transactions.aggregate(P2_USER_SEGMENTS))
    t2 = time.time() - t
    seg = pd.DataFrame([{"segment": f"S{i+1}", "spend_from": r["_id"]["min"], "spend_to": r["_id"]["max"],
                         "users": r["users"], "avg_orders": round(r["avg_orders"], 2),
                         "avg_spend": round(r["avg_spend"], 2), "total_spend": round(r["total_spend"], 2)}
                        for i, r in enumerate(r2)])
    seg["share_of_revenue_%"] = (100 * seg["total_spend"] / seg["total_spend"].sum()).round(1)
    print("=== Pipeline 2: user segments by total spend (S1 lowest, S4 highest) ===")
    print(seg.to_string(index=False))
    print(f"(ran in {t2:.2f}s)")
