import json, os, sys
from datetime import datetime
import pymongo

D = sys.argv[1] if len(sys.argv) > 1 else "/home/jovyan/work/data"
URI = sys.argv[2] if len(sys.argv) > 2 else "mongodb://mongo:27017"
db = pymongo.MongoClient(URI)["ecommerce"]

def load(name):
    with open(os.path.join(D, name)) as f:
        return json.load(f)

def dt(s):
    return datetime.fromisoformat(s) if s else None

def insert_batches(coll, docs, size=10000):
    coll.drop()
    for i in range(0, len(docs), size):
        coll.insert_many(docs[i:i + size], ordered=False)

# ---------- categories (one document per category, subcategories embedded) ----------
cats = load("categories.json")
cat_docs, cat_info = [], {}
for c in cats:
    margins = [s["profit_margin"] for s in c["subcategories"]]
    avg = round(sum(margins) / len(margins), 4)
    cat_docs.append({"_id": c["category_id"], "name": c["name"],
                     "subcategories": c["subcategories"], "avg_profit_margin": avg})
    cat_info[c["category_id"]] = {"category_id": c["category_id"], "name": c["name"],
                                  "avg_profit_margin": avg}
insert_batches(db.categories, cat_docs)

# ---------- products (category summary and price history embedded) ----------
prods = load("products.json")
prod_docs, prod_cat = [], {}
for p in prods:
    prod_docs.append({
        "_id": p["product_id"], "name": p["name"],
        "category": cat_info[p["category_id"]],
        "price": p["base_price"], "stock": p["current_stock"],
        "is_active": p["is_active"], "created": dt(p["creation_date"]),
        "price_history": [{"price": h["price"], "date": dt(h["date"])} for h in p["price_history"]],
    })
    prod_cat[p["product_id"]] = p["category_id"]
insert_batches(db.products, prod_docs)

# ---------- users ----------
users = load("users.json")
user_docs = [{"_id": u["user_id"], "geo": u["geo_data"],
              "registered": dt(u["registration_date"]),
              "registration_month": u["registration_date"][:7],
              "last_active": dt(u["last_active"])} for u in users]
insert_batches(db.users, user_docs)

# ---------- transactions (items embedded, category_id copied into each item) ----------
txns = load("transactions.json")
txn_docs = []
for t in txns:
    items = [{"product_id": i["product_id"], "category_id": prod_cat[i["product_id"]],
              "quantity": i["quantity"], "unit_price": i["unit_price"],
              "subtotal": i["subtotal"]} for i in t["items"]]
    txn_docs.append({"_id": t["transaction_id"], "user_id": t["user_id"],
                     "session_id": t["session_id"], "timestamp": dt(t["timestamp"]),
                     "items": items, "subtotal": t["subtotal"], "discount": t["discount"],
                     "total": t["total"], "payment_method": t["payment_method"],
                     "status": t["status"]})
insert_batches(db.transactions, txn_docs)

# ---------- indexes ----------
db.transactions.create_index([("user_id", 1), ("timestamp", -1)])
db.transactions.create_index("timestamp")
db.transactions.create_index("items.product_id")
db.transactions.create_index("items.category_id")
db.products.create_index("category.category_id")
db.users.create_index("registration_month")
db.users.create_index("geo.country")

for name in ["categories", "products", "users", "transactions"]:
    print(f"{name:13s} {db[name].count_documents({}):>8,}")
