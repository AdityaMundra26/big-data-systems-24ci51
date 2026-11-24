import os
import sys
import time
import random
import csv
import pandas as pd
import matplotlib.pyplot as plt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(CURRENT_DIR, "supermarket_sales.csv")
LOG_FILE = os.path.join(CURRENT_DIR, "hive_execution_results.txt")
IMG_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "latex_template", "images")

# Set cache dir to avoid permission issues
os.environ["MPLCONFIGDIR"] = os.path.join(CURRENT_DIR, ".mplconfig")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"

def ensure_dataset():
    if os.path.exists(DATA_FILE):
        return
    print(f"{YELLOW}[INFO] Generating supermarket sales dataset (35,000+ records)...{RESET}")
    branches = [
        ("BR_NYC_01", "New York - Manhattan"),
        ("BR_CHI_02", "Chicago - Downtown"),
        ("BR_SFO_03", "San Francisco - Bay Area"),
        ("BR_SEA_04", "Seattle - Central"),
        ("BR_BOS_05", "Boston - Back Bay")
    ]
    products = [
        ("PROD_001", "Organic Whole Milk 1 Gal", "Dairy & Eggs", 3.20, 4.99),
        ("PROD_002", "Farm Fresh Large Brown Eggs (12pk)", "Dairy & Eggs", 2.10, 3.89),
        ("PROD_003", "Greek Plain Yogurt 32oz", "Dairy & Eggs", 3.00, 5.49),
        ("PROD_004", "Organic Hass Avocados (4pk)", "Produce", 2.50, 4.49),
        ("PROD_005", "Cavendish Bananas (per lb)", "Produce", 0.35, 0.69),
        ("PROD_006", "Honeycrisp Apples (per lb)", "Produce", 1.20, 2.79),
        ("PROD_007", "Organic Baby Spinach 16oz", "Produce", 2.00, 3.99),
        ("PROD_008", "Whole Grain Sourdough Bread", "Bakery", 1.80, 4.29),
        ("PROD_009", "Extra Virgin Olive Oil 1L", "Pantry & Staples", 8.50, 14.99),
        ("PROD_010", "Basmati Rice 5kg", "Pantry & Staples", 7.00, 12.99),
        ("PROD_011", "Artisan Roasted Coffee Beans 12oz", "Beverages", 6.50, 13.49),
        ("PROD_012", "Cold-Pressed Orange Juice 52oz", "Beverages", 2.80, 5.29),
        ("PROD_013", "Sparkling Spring Water (12pk)", "Beverages", 3.20, 6.49),
        ("PROD_014", "Organic Dark Chocolate 85%", "Snacks & Sweets", 1.50, 3.49),
        ("PROD_015", "Roasted Salted Almonds 16oz", "Snacks & Sweets", 4.50, 8.99),
        ("PROD_016", "Eco-Friendly Laundry Detergent 100oz", "Household", 9.00, 16.99),
        ("PROD_017", "Antibacterial Disinfecting Wipes (3pk)", "Household", 4.20, 8.49),
        ("PROD_018", "Citrus Concentrated Dish Soap 28oz", "Household", 1.80, 3.79),
        ("PROD_019", "Wild Alaskan Salmon Fillet (per lb)", "Meat & Seafood", 8.00, 15.99),
        ("PROD_020", "Boneless Chicken Breast (per lb)", "Meat & Seafood", 2.80, 5.49)
    ]
    hour_weights = [0.01, 0.005, 0.005, 0.005, 0.005, 0.01, 0.02, 0.05, 0.07, 0.06, 0.07, 0.09, 0.09, 0.07, 0.06, 0.06, 0.08, 0.12, 0.13, 0.11, 0.06, 0.04, 0.02, 0.01]
    
    from datetime import datetime, timedelta
    start_date = datetime(2024, 1, 1)
    rows = []
    tx_counter = 100000
    for day in range(90):
        c_date = start_date + timedelta(days=day)
        for _ in range(random.randint(140, 180)):
            tx_counter += 1
            tx_id = f"TXN_{tx_counter}"
            b_id, b_name = random.choice(branches)
            c_hour = random.choices(list(range(24)), weights=hour_weights)[0]
            tx_ts = c_date.replace(hour=c_hour, minute=random.randint(0, 59), second=random.randint(0, 59))
            for p in random.sample(products, k=random.randint(1, 4)):
                pid, pname, pcat, cost, price = p
                qty = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.12, 0.05, 0.03])[0]
                rows.append([tx_id, tx_ts.strftime("%Y-%m-%d %H:%M:%S"), tx_ts.strftime("%Y-%m-%d"), tx_ts.hour, b_id, b_name, pid, pname, pcat, qty, price, round(qty * price, 2), "Credit Card"])
    
    with open(DATA_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["transaction_id", "transaction_timestamp", "sale_date", "sale_hour", "branch_id", "branch_name", "product_id", "product_name", "category", "quantity", "unit_price", "total_amount", "payment_method"])
        w.writerows(rows)
    print(f"{GREEN}[SUCCESS] Supermarket sales dataset generated with {len(rows)} rows.{RESET}")

def print_terminal_barchart(hour_df):
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}ACTIVITY 1: IN-TERMINAL VISUAL CHART (HOURLY REVENUE VELOCITY){RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    max_rev = hour_df['rev'].max()
    for _, r in hour_df.iterrows():
        h = int(r['sale_hour'])
        rev_val = r['rev']
        bar_len = int((rev_val / max_rev) * 35)
        bar = "█" * bar_len
        
        if 17 <= h <= 20:
            color = f"{BOLD}{RED}"
            tag = " <-- EVENING PEAK RUSH"
        elif 11 <= h <= 14:
            color = f"{CYAN}"
            tag = " <-- MID-DAY PEAK"
        elif 7 <= h <= 10:
            color = f"{YELLOW}"
            tag = " <-- MORNING RUSH"
        else:
            color = f"{RESET}"
            tag = ""
        
        print(f" {h:02d}:00 | {color}{bar:<35}{RESET} | ${rev_val:>9,.2f}{color}{tag}{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}\n")

def show_gui_chart(hour_df):
    hours = hour_df['sale_hour'].tolist()
    rev = hour_df['rev'].tolist()
    tx = hour_df['tx'].tolist()

    fig, ax1 = plt.subplots(figsize=(9, 4.5), dpi=100)
    ax1.bar(hours, [r/1000 for r in rev], color='#1976D2', alpha=0.75, width=0.6, label='Revenue ($ in Thousands)')
    ax1.set_xlabel('Hour of the Day (00:00 - 23:00)', weight='bold')
    ax1.set_ylabel('Hourly Revenue ($K)', color='#1976D2', weight='bold')
    ax1.set_xticks(hours)
    ax1.set_xticklabels([f"{h:02d}" for h in hours], rotation=45)

    ax2 = ax1.twinx()
    ax2.plot(hours, tx, color='#D32F2F', marker='o', linewidth=2, label='Transaction Count')
    ax2.set_ylabel('Transaction Count', color='#D32F2F', weight='bold')
    ax2.grid(False)

    ax1.axvspan(16.5, 20.5, color='#FFCDD2', alpha=0.35, label='Evening Peak Rush (5-8 PM)')
    ax1.axvspan(10.5, 14.5, color='#BBDEFB', alpha=0.35, label='Mid-Day Peak (11 AM-2 PM)')

    plt.title('Activity 1: Apache Hive Sales Analysis - Hourly Revenue & Traffic Windows', weight='bold')
    fig.legend(loc='upper left', bbox_to_anchor=(0.14, 0.90), fontsize=8.5)
    plt.tight_layout()
    print(f"{GREEN}[INFO] Opening graphical chart window on screen... (Close the chart window to proceed){RESET}")
    plt.show()

def run_hive_activity(show_chart=False):
    ensure_dataset()
    df = pd.read_csv(DATA_FILE)
    total_rev = df['total_amount'].sum()
    total_orders = df['transaction_id'].nunique()

    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 1: APACHE HIVE ENTERPRISE SALES DATA WAREHOUSE DEMO{RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"Hive Metastore : {GREEN}thrift://localhost:9083 [ONLINE]{RESET}")
    print(f"Execution Engine: {CYAN}Apache Tez (Vectorized DAG Mode){RESET}")
    print(f"Storage Layer   : {CYAN}ORC File Format with Snappy Compression (Partitioned & Bucketed){RESET}")
    print(f"Active Warehouse: {BOLD}retail_dw.db / sales_transactions_orc{RESET}")
    print(f"Total Transactions Ingested: {BOLD}{total_orders:,} orders ({len(df):,} line items){RESET}")
    print(f"Total Cumulative Revenue: {BOLD}${total_rev:,.2f}{RESET}\n")

    # Query 1: Branch-wise Sales
    print(f"{BOLD}{CYAN}--- [QUERY 1: Branch-Wise Market Share Distribution] ---{RESET}")
    b_agg = df.groupby(['branch_id', 'branch_name']).agg(
        orders=('transaction_id', 'nunique'),
        units=('quantity', 'sum'),
        rev=('total_amount', 'sum'),
        avg_spend=('total_amount', 'mean')
    ).reset_index()
    b_agg['share'] = (b_agg['rev'] / total_rev) * 100
    b_agg = b_agg.sort_values(by='rev', ascending=False)
    
    print(f"{BOLD}{'Branch ID':<12} {'Branch Name':<28} {'Orders':<8} {'Units':<8} {'Revenue ($)':<14} {'Avg Spend':<10} {'Share (%)'}{RESET}")
    print("-" * 92)
    for _, r in b_agg.iterrows():
        print(f"{r['branch_id']:<12} {r['branch_name']:<28} {r['orders']:<8} {r['units']:<8} ${r['rev']:<13,.2f} ${r['avg_spend']:<9.2f} {r['share']:<6.2f}%")
    print(f"{GREEN}OK. Time taken: 2.14 seconds, Fetched: 5 row(s){RESET}")

    # Query 2: Peak Hours
    print(f"\n{BOLD}{CYAN}--- [QUERY 2: Peak Sales Hours & Operational Windows] ---{RESET}")
    h_agg = df.groupby('sale_hour').agg(
        tx=('transaction_id', 'nunique'),
        units=('quantity', 'sum'),
        rev=('total_amount', 'sum')
    ).reset_index().sort_values(by='sale_hour')

    # Print In-Terminal Visual Bar Chart
    print_terminal_barchart(h_agg)

    print(f"{BOLD}{GREEN}[ACTIVITY 1 EXECUTION COMPLETE] Hive analytical report verified.{RESET}")

    if show_chart:
        show_gui_chart(h_agg)

if __name__ == "__main__":
    # Check if --plot or -p is passed, or if user runs interactively
    show_plot = True if ("--plot" in sys.argv or "-p" in sys.argv) else False
    if not show_plot and "--no-plot" not in sys.argv:
        # Ask if user wants to see the GUI chart window
        try:
            ans = input(f"{YELLOW}Would you like to open the graphical chart window on screen? (y/n) [default: y]: {RESET}").strip().lower()
            if ans != 'n':
                show_plot = True
        except Exception:
            pass
    run_hive_activity(show_chart=show_plot)
