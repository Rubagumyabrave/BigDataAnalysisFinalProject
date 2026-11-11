# fix_generator.py - converts the Colab-notebook "dataset_generator.py" into a runnable script
import json, re, shutil
SRC = "dataset_generator.py"
shutil.copy(SRC, "dataset_generator_notebook_backup.json")
nb = json.load(open(SRC, encoding="utf-8"))
src = "".join(nb["cells"][0]["source"])

a = src.index("# --- Configuration ---")
b = src.index("# --- Initialization ---")
cfg = '''# --- Configuration (sizes can be set from the command line) ---
import argparse, os
_ap = argparse.ArgumentParser()
_ap.add_argument("--users", type=int, default=10000)
_ap.add_argument("--products", type=int, default=5000)
_ap.add_argument("--categories", type=int, default=25)
_ap.add_argument("--transactions", type=int, default=500000)
_ap.add_argument("--sessions", type=int, default=2000000)
_ap.add_argument("--days", type=int, default=90)
_ap.add_argument("--out", default=".")
_args = _ap.parse_args()
NUM_USERS = _args.users
NUM_PRODUCTS = _args.products
NUM_CATEGORIES = _args.categories
NUM_TRANSACTIONS = _args.transactions
NUM_SESSIONS = _args.sessions
TIMESPAN_DAYS = _args.days
OUT_DIR = _args.out
os.makedirs(OUT_DIR, exist_ok=True)
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2  # Fail-safe

'''
src = src[:a] + cfg + src[b:]
src = re.sub(r'open\("((?:users|products|categories|transactions)\.json)", "w"\)',
             r'open(os.path.join(OUT_DIR, "\1"), "w")', src)
src = src.replace('open(f"sessions_{i//CHUNK_SIZE}.json", "w")',
                  'open(os.path.join(OUT_DIR, f"sessions_{i//CHUNK_SIZE}.json"), "w")')
open(SRC, "w", encoding="utf-8").write(src)
print("Fixed. dataset_generator.py is now a normal Python script.")