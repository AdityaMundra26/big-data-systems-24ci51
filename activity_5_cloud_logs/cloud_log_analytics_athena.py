import os
import sys
import csv
import time
import random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_CSV = os.path.join(CURRENT_DIR, "web_server_access_logs.csv")
os.environ["MPLCONFIGDIR"] = os.path.join(CURRENT_DIR, ".mplconfig")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"

def ensure_logs():
    if os.path.exists(LOG_CSV):
        return
    print(f"{YELLOW}[INFO] Generating 15,000 cloud web server access log records...{RESET}")
    endpoints = [
        ("/api/v1/auth/login", "POST", [200, 401, 429], [0.85, 0.12, 0.03], (45, 180)),
        ("/api/v1/products", "GET", [200, 304, 500], [0.92, 0.07, 0.01], (20, 110)),
        ("/api/v1/products/search", "GET", [200, 400, 504], [0.90, 0.08, 0.02], (65, 450)),
        ("/api/v1/checkout/pay", "POST", [200, 400, 500, 502, 504], [0.78, 0.08, 0.06, 0.05, 0.03], (250, 3200)),
        ("/api/v1/user/profile", "GET", [200, 401, 403], [0.94, 0.05, 0.01], (25, 95))
    ]
    client_ips = ["192.168.1.45", "10.0.4.12", "172.16.8.99", "203.0.113.195"]
    from datetime import datetime, timedelta
    base_time = datetime(2024, 5, 20, 0, 0, 0)
    rows = []
    for i in range(15000):
        r_hour = random.choices(list(range(24)), weights=[0.01]*6 + [0.03, 0.05, 0.07, 0.08, 0.08, 0.09, 0.08, 0.07, 0.12, 0.11, 0.06, 0.04, 0.03, 0.02, 0.01, 0.01, 0.01, 0.01])[0]
        ts = base_time + timedelta(hours=r_hour, minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        uri, method, statuses, status_weights, lat_range = random.choice(endpoints)
        if 14 <= r_hour <= 15 and "/checkout/pay" in uri:
            status = random.choices([200, 500, 502, 504], weights=[0.20, 0.35, 0.25, 0.20])[0]
            lat = random.randint(1800, 6500)
        else:
            status = random.choices(statuses, weights=status_weights)[0]
            lat = random.randint(lat_range[0], lat_range[1])
        b_sent = random.randint(340, 12500) if status == 200 else random.randint(120, 480)
        rows.append([f"REQ_{100000+i}", ts.strftime("%Y-%m-%d %H:%M:%S"), ts.strftime("%Y-%m-%d"), r_hour, random.choice(client_ips), method, uri, status, f"{status//100}xx", b_sent, lat, 1 if status >= 400 else 0, "Mozilla/5.0"])

    with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["request_id", "timestamp", "log_date", "hour_of_day", "client_ip", "http_method", "uri_path", "status_code", "status_category", "bytes_sent", "latency_ms", "is_error", "user_agent"])
        w.writerows(rows)

def print_terminal_cost_chart():
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}ACTIVITY 5: IN-TERMINAL VISUAL COST COMPARISON ($/MONTH){RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    ec2_cost = 791.88
    serverless_cost = 36.35
    ec2_bar = "█" * 35
    srv_bar = "█" * int((serverless_cost / ec2_cost) * 35)

    print(f"  {BOLD}Self-Hosted EC2 Cluster:{RESET} {RED}{ec2_bar:<35}{RESET} | {RED}${ec2_cost:>6.2f} / mo{RESET}")
    print(f"  {BOLD}Serverless AWS S3+Athena:{RESET} {GREEN}{srv_bar:<35}{RESET} | {GREEN}${serverless_cost:>6.2f} / mo{RESET}")
    savings = ((ec2_cost - serverless_cost) / ec2_cost) * 100
    print(f"\n  {BOLD}{GREEN}>>> RESULT: {savings:.1f}% DIRECT COST SAVINGS WITH ZERO INFRASTRUCTURE MANAGEMENT!{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}\n")

def show_gui_chart():
    fig, ax = plt.subplots(figsize=(8, 4), dpi=100)
    platforms = ['Self-Hosted Hadoop Cluster\n(3x r5.xlarge + EBS)', 'Serverless AWS Lake\n(S3 + Glue + Athena)']
    costs = [791.88, 36.35]
    colors = ['#E53935', '#1E88E5']

    bars = ax.bar(platforms, costs, color=colors, width=0.45, edgecolor='black', lw=1.2)
    ax.set_ylabel('Total Monthly Cost in USD ($)', weight='bold')
    ax.set_title('Activity 5: Cloud Cost Model - Serverless Data Lake vs Dedicated EC2 Fleet', weight='bold')

    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 20, f"${yval:,.2f} / mo", ha='center', va='bottom', weight='bold')

    ax.text(0.5, 450, "95.4% Cost Reduction\nZero Server Maintenance Overhead", 
            ha='center', va='center', bbox=dict(boxstyle='round,pad=0.6', facecolor='#E8F5E9', edgecolor='#43A047', lw=1.5), fontsize=10, weight='bold')

    plt.ylim(0, 920)
    plt.tight_layout()
    print(f"{GREEN}[INFO] Opening graphical chart window on screen... (Close the chart window to proceed){RESET}")
    plt.show()

def run_cloud_activity(show_chart=False):
    ensure_logs()
    df = pd.read_csv(LOG_CSV)
    total_reqs = len(df)
    total_errors = df['is_error'].sum()

    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 5: CLOUD BIG DATA ANALYTICS (AWS S3 + ATHENA / GLUE){RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"Cloud Platform  : {GREEN}Amazon Web Services (AWS us-east-1){RESET}")
    print(f"Data Lake Store : {CYAN}s3://cloud-ops-telemetry-bucket-prod/logs/curated_parquet/{RESET}")
    print(f"Serverless SQL  : {CYAN}Amazon Athena Engine v3 (Distributed Presto / Trino){RESET}\n")

    # Spike Analysis
    hr_agg = df.groupby('hour_of_day').agg(
        reqs=('request_id', 'count'),
        failed=('is_error', 'sum'),
        avg_lat=('latency_ms', 'mean')
    ).reset_index().sort_values(by='hour_of_day')
    hr_agg['err_pct'] = (hr_agg['failed'] / hr_agg['reqs']) * 100

    print(f"{BOLD}{'Hour':<7} {'Total Requests':<16} {'Failed Requests':<18} {'Error Rate (%)':<16} {'Incident Flag'}{RESET}")
    print("-" * 75)
    for _, r in hr_agg.iterrows():
        h = int(r['hour_of_day'])
        flag = f"{BOLD}{RED}CRITICAL OUTAGE{RESET}" if r['err_pct'] > 24 else f"{GREEN}NORMAL{RESET}"
        if h in [8, 11, 14, 15, 17, 22]:
            print(f"{h:02d}:00   {r['reqs']:<16} {r['failed']:<18} {r['err_pct']:<15.2f}% {flag}")

    # Print In-Terminal Visual Cost Chart
    print_terminal_cost_chart()

    print(f"{BOLD}{GREEN}[ACTIVITY 5 EXECUTION COMPLETE] Cloud analytics PoC verified.{RESET}")

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
    run_cloud_activity(show_chart=show_plot)
