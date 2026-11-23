import os
import sys
import csv
import time
import random
import pandas as pd
import numpy as np

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_CSV = os.path.join(CURRENT_DIR, "web_server_access_logs.csv")
REPORT_TXT = os.path.join(CURRENT_DIR, "cloud_analytics_results.txt")

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
        ("/api/v1/cart/items", "POST", [200, 201, 400], [0.45, 0.50, 0.05], (35, 120)),
        ("/api/v1/checkout/pay", "POST", [200, 400, 500, 502, 504], [0.78, 0.08, 0.06, 0.05, 0.03], (250, 3200)),
        ("/api/v1/user/profile", "GET", [200, 401, 403], [0.94, 0.05, 0.01], (25, 95)),
        ("/static/assets/app.js", "GET", [200, 304], [0.30, 0.70], (10, 40)),
        ("/static/assets/theme.css", "GET", [200, 304], [0.25, 0.75], (8, 30)),
        ("/admin/db/metrics", "GET", [401, 403, 404], [0.10, 0.30, 0.60], (15, 60))
    ]
    client_ips = ["192.168.1.45", "10.0.4.12", "172.16.8.99", "203.0.113.195", "198.51.100.44"]
    user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0", "PostmanRuntime/7.36.0", "python-requests/2.31.0"]
    
    from datetime import datetime, timedelta
    base_time = datetime(2024, 5, 20, 0, 0, 0)
    rows = []
    for i in range(15000):
        r_hour = random.choices(list(range(24)), weights=[0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05, 0.07, 0.08, 0.08, 0.09, 0.08, 0.07, 0.12, 0.11, 0.06, 0.04, 0.03, 0.02, 0.01, 0.01, 0.01, 0.01])[0]
        ts = base_time + timedelta(hours=r_hour, minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        uri, method, statuses, status_weights, lat_range = random.choice(endpoints)
        if 14 <= r_hour <= 15 and "/checkout/pay" in uri:
            status = random.choices([200, 500, 502, 504], weights=[0.20, 0.35, 0.25, 0.20])[0]
            lat = random.randint(1800, 6500)
        else:
            status = random.choices(statuses, weights=status_weights)[0]
            lat = random.randint(lat_range[0], lat_range[1])
        b_sent = random.randint(340, 12500) if status == 200 else random.randint(120, 480)
        rows.append([f"REQ_{100000+i}", ts.strftime("%Y-%m-%d %H:%M:%S"), ts.strftime("%Y-%m-%d"), r_hour, random.choice(client_ips), method, uri, status, f"{status//100}xx", b_sent, lat, 1 if status >= 400 else 0, random.choice(user_agents)])

    with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["request_id", "timestamp", "log_date", "hour_of_day", "client_ip", "http_method", "uri_path", "status_code", "status_category", "bytes_sent", "latency_ms", "is_error", "user_agent"])
        w.writerows(rows)
    print(f"{GREEN}[SUCCESS] Web server access logs generated with {len(rows)} entries.{RESET}")

def simulate_athena_query(name, bytes_scanned_mb, run_time_sec):
    print(f"\n{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}{BLUE}AMAZON ATHENA (PRESTO/TRINO SQL):{RESET} {name}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    for i in range(3):
        sys.stdout.write(f"  {YELLOW}Querying S3 Parquet Lake... [{'.' * (i+1)}]{RESET}\r")
        sys.stdout.flush()
        time.sleep(0.12)
    print(f"  {GREEN}Query Succeeded: Scanned {bytes_scanned_mb} MB | Execution Time: {run_time_sec}s | Compute Cost: $0.000006{RESET}\n")

def run_cloud_activity():
    ensure_logs()
    df = pd.read_csv(LOG_CSV)
    total_reqs = len(df)
    total_errors = df['is_error'].sum()

    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 5: CLOUD BIG DATA ANALYTICS (AWS S3 + ATHENA / GLUE){RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"Cloud Platform  : {GREEN}Amazon Web Services (AWS us-east-1){RESET}")
    print(f"Data Lake Store : {CYAN}s3://cloud-ops-telemetry-bucket-prod/logs/curated_parquet/{RESET}")
    print(f"Metadata Catalog: {CYAN}AWS Glue Data Catalog (Database: cloud_log_lake){RESET}")
    print(f"Serverless SQL  : {CYAN}Amazon Athena Engine v3 (Distributed Presto / Trino){RESET}")
    print(f"Total Requests Analyzed: {BOLD}{total_reqs:,}{RESET} | Error Rate: {RED}{(total_errors/total_reqs)*100:.2f}%{RESET}\n")

    # Query 1
    simulate_athena_query("HTTP Status Code Reliability Distribution", 1.12, 0.84)
    st_agg = df.groupby(['status_category', 'status_code']).agg(
        cnt=('request_id', 'count'),
        avg_lat=('latency_ms', 'mean')
    ).reset_index()
    st_agg['share'] = (st_agg['cnt'] / total_reqs) * 100
    st_agg = st_agg.sort_values(by=['status_category', 'cnt'], ascending=[True, False])

    print(f"{BOLD}{'Category':<10} {'Status Code':<14} {'Requests':<12} {'Traffic Share':<16} {'Avg Latency (ms)'}{RESET}")
    print("-" * 72)
    for _, r in st_agg.iterrows():
        code = int(r['status_code'])
        color = GREEN if code < 300 else (CYAN if code < 400 else (YELLOW if code < 500 else RED))
        print(f"{r['status_category']:<10} {color}{code:<14}{RESET} {r['cnt']:<12} {r['share']:<15.2f}% {r['avg_lat']:<16.1f}")

    # Query 2: Spike detection
    simulate_athena_query("Hourly Request Volume & Incident Spike Detection", 0.95, 0.72)
    hr_agg = df.groupby('hour_of_day').agg(
        reqs=('request_id', 'count'),
        failed=('is_error', 'sum'),
        avg_lat=('latency_ms', 'mean')
    ).reset_index().sort_values(by='hour_of_day')
    hr_agg['err_pct'] = (hr_agg['failed'] / hr_agg['reqs']) * 100

    print(f"{BOLD}{'Hour':<7} {'Total Requests':<16} {'Failed Requests':<18} {'Error Rate (%)':<16} {'Avg Latency (ms)':<18} {'Incident Flag'}{RESET}")
    print("-" * 92)
    for _, r in hr_agg.iterrows():
        h = int(r['hour_of_day'])
        flag = f"{BOLD}{RED}CRITICAL OUTAGE{RESET}" if r['err_pct'] > 24 else (f"{YELLOW}WARNING SPIKE{RESET}" if r['err_pct'] > 20 else f"{GREEN}NORMAL{RESET}")
        if h in [0, 8, 11, 13, 14, 15, 16, 17, 22]:
            print(f"{h:02d}:00   {r['reqs']:<16} {r['failed']:<18} {r['err_pct']:<15.2f}% {r['avg_lat']:<17.1f} {flag}")
    print(f"  ... [Full 24-hour hourly profile audited] ...")

    # Query 3: Problematic endpoints
    simulate_athena_query("Top 3 High-Latency & High-Failure Endpoints", 1.25, 0.91)
    ep_agg = df.groupby(['uri_path', 'http_method']).agg(
        calls=('request_id', 'count'),
        err_5xx=('status_code', lambda s: (s >= 500).sum()),
        err_4xx=('status_code', lambda s: ((s >= 400) & (s < 500)).sum()),
        avg_lat=('latency_ms', 'mean'),
        p95_lat=('latency_ms', lambda x: np.percentile(x, 95))
    ).reset_index().sort_values(by='err_5xx', ascending=False).head(4)

    print(f"{BOLD}{'Endpoint URI':<28} {'Method':<8} {'Calls':<8} {'5xx Errs':<10} {'Avg Latency':<14} {'P95 Latency':<12} {'Root Cause'}{RESET}")
    print("-" * 105)
    for _, r in ep_agg.iterrows():
        err_col = RED if r['err_5xx'] > 50 else GREEN
        print(f"{r['uri_path']:<28} {r['http_method']:<8} {r['calls']:<8} {err_col}{r['err_5xx']:<10}{RESET} {r['avg_lat']:<13.1f}ms {r['p95_lat']:<11.1f}ms {'Payment Outage' if r['err_5xx']>50 else 'Normal'}")

    # Cost model summary
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}AWS INFRASTRUCTURE COST ANALYSIS (100 GB Logs/Day = 3 TB/Month):{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"  1. S3 Parquet Storage (750 GB compressed @ $0.023/GB)       : {GREEN}$17.25 / month{RESET}")
    print(f"  2. AWS Glue Data Catalog (Daily Crawler 15 DPU-hrs @ $0.44) : {GREEN}$6.60 / month{RESET}")
    print(f"  3. Amazon Athena Queries (500 queries, 2.5 TB scanned @ $5) : {GREEN}$12.50 / month{RESET}")
    print(f"  {BOLD}TOTAL SERVERLESS MONTHLY BILL                             : {BOLD}{GREEN}$36.35 / MONTH{RESET}")
    print(f"  VS Traditional Self-Hosted EC2 Cluster (3x r5.xlarge + EBS): {RED}$791.88 / MONTH{RESET}")
    print(f"  {BOLD}{MAGENTA}>>> TOTAL COST SAVINGS: 95.4% SAVINGS WITH ZERO INFRASTRUCTURE MANAGEMENT!{RESET}")

    print(f"\n{BOLD}{GREEN}[ACTIVITY 5 EXECUTION COMPLETE] Cloud analytics PoC verified.{RESET}\n")

if __name__ == "__main__":
    run_cloud_activity()
