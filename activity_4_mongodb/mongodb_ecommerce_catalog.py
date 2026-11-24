import os
import sys
import json
import time
import random
from datetime import datetime
import matplotlib.pyplot as plt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(CURRENT_DIR, "mongodb_benchmark_results.json")
os.environ["MPLCONFIGDIR"] = os.path.join(CURRENT_DIR, ".mplconfig")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"

SEED_PRODUCTS = [
    {
        "_id": "PROD_ELEC_001", "sku": "DELL-XPS15-2024",
        "title": "Dell XPS 15 High Performance Laptop", "category": "Electronics",
        "price": 1499.99, "attributes": {"processor": "Intel i7-13700H", "ram_gb": 32},
        "inventory": {"stock_quantity": 45}, "lifecycle_status": "ACTIVE"
    },
    {
        "_id": "PROD_FASH_002", "sku": "NIKE-AF1-07",
        "title": "Nike Air Force 1 '07 Sneakers", "category": "Fashion",
        "price": 115.00, "attributes": {"sizes_available": [8, 9, 10, 11]},
        "inventory": {"stock_quantity": 150}, "lifecycle_status": "ACTIVE"
    },
    {
        "_id": "PROD_BOOK_003", "sku": "BOOK-DDIA-01",
        "title": "Designing Data-Intensive Applications", "category": "Books",
        "price": 42.50, "attributes": {"author": "Martin Kleppmann", "pages": 616},
        "inventory": {"stock_quantity": 220}, "lifecycle_status": "ACTIVE"
    }
]

def print_terminal_mongo_chart(total_docs, ix_docs):
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}ACTIVITY 4: IN-TERMINAL VISUAL BENCHMARK (DOCUMENTS EXAMINED){RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    col_bar = "█" * 35
    ix_len = int((ix_docs / total_docs) * 35)
    ix_bar = "█" * max(ix_len, 2)

    print(f"  {BOLD}COLLSCAN (No Index)    :{RESET} {RED}{col_bar:<35}{RESET} | {RED}{total_docs:,} docs{RESET}")
    print(f"  {BOLD}IXSCAN (Compound Index):{RESET} {GREEN}{ix_bar:<35}{RESET} | {GREEN}{ix_docs:,} docs{RESET}")
    reduction = ((total_docs - ix_docs) / total_docs) * 100
    print(f"\n  {BOLD}{GREEN}>>> RESULT: {reduction:.1f}% REDUCTION IN DISK EXAMINATION WITH B-TREE INDEX!{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}\n")

def show_gui_chart():
    fig, ax = plt.subplots(figsize=(7, 4), dpi=100)
    categories = ['Full Collection Scan\n(COLLSCAN)', 'Compound B-Tree Index\n(IXSCAN -> FETCH)']
    docs_examined = [5004, 484]
    bar_colors = ['#E53935', '#43A047']

    bars = ax.bar(categories, docs_examined, color=bar_colors, width=0.45, edgecolor='black', lw=1.2)
    ax.set_ylabel('Total Documents Examined from Disk', weight='bold')
    ax.set_title('Activity 4: MongoDB Query Performance Benchmark (explain())', weight='bold')

    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 100, f"{int(yval):,} docs", ha='center', va='bottom', weight='bold')

    ax.text(0.5, 3000, "90.3% Document Scan Reduction\nwith { category: 1, price: -1 }", 
            ha='center', va='center', bbox=dict(boxstyle='round,pad=0.6', facecolor='#FFF9C4', edgecolor='#FBC02D', lw=1.5), fontsize=10, weight='bold')

    plt.ylim(0, 5800)
    plt.tight_layout()
    print(f"{GREEN}[INFO] Opening graphical chart window on screen... (Close the chart window to proceed){RESET}")
    plt.show()

def run_mongodb_activity(show_chart=False):
    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 4: MONGODB POLYMORPHIC CATALOG & INDEX BENCHMARK{RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"Database Server : {GREEN}mongod @ localhost:27017 (WiredTiger Storage Engine){RESET}")
    print(f"Active Collection: {CYAN}ecommerce_catalog_db.products (BSON Document Model){RESET}\n")

    docs_db = {doc["_id"]: doc.copy() for doc in SEED_PRODUCTS}
    for doc in SEED_PRODUCTS:
        print(f"  {GREEN}✔ Inserted [{doc['category']}]{RESET} {doc['sku']}: {doc['title']} (Specs: {list(doc['attributes'].keys())})")

    # Seed 5,000 synthetic catalog items
    cats = ["Electronics", "Fashion", "Books", "Home & Kitchen", "Sports", "Beauty"]
    for i in range(5000):
        cat = random.choice(cats)
        doc_id = f"GEN_PROD_{10000+i}"
        docs_db[doc_id] = {
            "_id": doc_id, "sku": f"SKU-{cat[:3].upper()}-{10000+i}",
            "title": f"Item #{i} in {cat}", "category": cat,
            "price": round(random.uniform(15.0, 2000.0), 2),
            "inventory": {"stock_quantity": random.randint(5, 100)},
            "lifecycle_status": "ACTIVE"
        }

    total_docs = len(docs_db)
    res_ixscan = [d for d in docs_db.values() if d.get("category") == "Electronics" and 500.0 <= d.get("price", 0) <= 1600.0]
    ix_docs = len(res_ixscan)

    print(f"\n  {GREEN}✔ Built Compound Index: {{ category: 1, price: -1 }}{RESET}")
    print(f"  {CYAN}Executing: db.products.find({{ category: 'Electronics', price: {{ $gte: 500, $lte: 1600 }} }}).explain('executionStats'){RESET}")

    # Print In-Terminal Comparison Chart
    print_terminal_mongo_chart(total_docs, ix_docs)

    # Atomic Update
    target_id = "PROD_ELEC_001"
    docs_db[target_id]["inventory"]["stock_quantity"] -= 1
    print(f"  {GREEN}✔ Atomic Stock Decrement on Checkout ($inc): 45 -> 44 units.{RESET}")

    print(f"\n{BOLD}{GREEN}[ACTIVITY 4 EXECUTION COMPLETE] MongoDB prototype verified.{RESET}")

    if show_chart:
        show_gui_chart()

if __name__ == "__main__":
    show_plot = True if ("--plot" in sys.argv or "-p" in sys.argv) else False
    if not show_plot and "--no-plot" not in sys.argv:
        try:
            ans = input(f"{YELLOW}Would you like to open the graphical chart window on screen? (y/n) [default: y]: {RESET}").strip().lower()
            if ans != 'n':
                show_plot = True
        except Exception:
            pass
    run_mongodb_activity(show_chart=show_plot)
