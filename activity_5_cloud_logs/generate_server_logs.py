import csv
import random
from datetime import datetime, timedelta

random.seed(101)

# API Endpoints
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

client_ips = [
    "192.168.1.45", "10.0.4.12", "172.16.8.99", "203.0.113.195",
    "198.51.100.44", "198.51.100.78", "203.0.113.88", "192.0.2.14"
]

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "PostmanRuntime/7.36.0",
    "python-requests/2.31.0"
]

base_time = datetime(2024, 5, 20, 0, 0, 0)
total_hours = 24
total_logs = 15000

log_file = "C:/Users/Aditya Mundra/.gemini/antigravity/scratch/big_data_systems_24ci51/activity_5_cloud_logs/web_server_access_logs.csv"

rows = []
for i in range(total_logs):
    # Skewed hour distribution: normal baseline, but outage surge between 14:00 - 16:00
    r_hour = random.choices(
        list(range(24)),
        weights=[0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05, 0.07, 0.08, 0.08, 0.09,
                 0.08, 0.07, 0.12, 0.11, 0.06, 0.04, 0.03, 0.02, 0.01, 0.01, 0.01, 0.01]
    )[0]
    
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    timestamp = base_time + timedelta(hours=r_hour, minutes=minute, seconds=second)

    ep_info = random.choice(endpoints)
    uri, method, statuses, status_weights, lat_range = ep_info

    # Simulate an incident spike between 14:00 and 15:59 where payment gateway degraded
    if 14 <= r_hour <= 15 and "/checkout/pay" in uri:
        status = random.choices([200, 500, 502, 504], weights=[0.20, 0.35, 0.25, 0.20])[0]
        latency = random.randint(1800, 6500)
    else:
        status = random.choices(statuses, weights=status_weights)[0]
        latency = random.randint(lat_range[0], lat_range[1])

    bytes_sent = random.randint(340, 12500) if status == 200 else random.randint(120, 480)
    client_ip = random.choice(client_ips)
    user_agent = random.choice(user_agents)

    status_category = f"{status // 100}xx"
    is_error = 1 if status >= 400 else 0

    rows.append([
        f"REQ_{100000 + i}",
        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        timestamp.strftime("%Y-%m-%d"),
        r_hour,
        client_ip,
        method,
        uri,
        status,
        status_category,
        bytes_sent,
        latency,
        is_error,
        user_agent
    ])

with open(log_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "request_id", "timestamp", "log_date", "hour_of_day",
        "client_ip", "http_method", "uri_path", "status_code",
        "status_category", "bytes_sent", "latency_ms", "is_error", "user_agent"
    ])
    writer.writerows(rows)

print(f"Generated {len(rows)} server log records at: {log_file}")
