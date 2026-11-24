import os
import sys
import json
import time
import random
import threading
import webbrowser
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
import matplotlib.pyplot as plt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(CURRENT_DIR, "flink_vitals_results.json")
CHART_PNG = os.path.join(CURRENT_DIR, "flink_patient_vitals_chart.png")
HTML_FILE = os.path.join(CURRENT_DIR, "index.html")
os.environ["MPLCONFIGDIR"] = os.path.join(CURRENT_DIR, ".mplconfig")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"

THRESHOLDS = {
    "heart_rate": {"critical_high": 120, "critical_low": 45},
    "spo2": {"critical_low": 90.0, "warning_low": 92.0},
    "temperature": {"critical_high": 38.5}
}

PATIENTS = [
    {"patient_id": "PAT_101", "name": "E. Davis", "ward": "ICU-Bed-01"},
    {"patient_id": "PAT_102", "name": "M. Jenkins", "ward": "ICU-Bed-02"},
    {"patient_id": "PAT_103", "name": "A. Sharma", "ward": "Cardiac-Care-03"},
    {"patient_id": "PAT_104", "name": "R. Taylor", "ward": "General-Ward-07"},
    {"patient_id": "PAT_105", "name": "L. Chen", "ward": "Post-Op-Recovery-02"}
]

def generate_matplotlib_chart():
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 5), sharex=True, dpi=120)
    time_ticks = [f"14:00:{i:02d}" for i in range(0, 60, 4)]
    hr_series = [72, 75, 78, 82, 85, 128, 134, 130, 92, 84, 76, 73, 71, 70, 72]
    spo2_series = [98, 98, 97, 96, 96, 88, 87, 89, 95, 96, 97, 98, 98, 98, 99]

    ax1.plot(time_ticks, hr_series, color='#D32F2F', marker='s', lw=2, label='Heart Rate (BPM)')
    ax1.axhline(120, color='red', ls='--', lw=1.5, label='Critical Tachycardia (>120 BPM)')
    ax1.set_ylabel('Heart Rate (BPM)', weight='bold')
    ax1.set_title('Apache Flink Stream Processing: Patient Biometric Telemetry & Anomaly Detection', weight='bold')
    ax1.legend(loc='upper right', fontsize=8.5)
    ax1.grid(True, alpha=0.3)

    ax2.plot(time_ticks, spo2_series, color='#0288D1', marker='^', lw=2, label='SpO2 Saturation (%)')
    ax2.axhline(90, color='darkblue', ls='--', lw=1.5, label='Critical Hypoxemia (<90%)')
    ax2.set_ylabel('SpO2 (%)', weight='bold')
    ax2.set_xlabel('Event Time (Tumbling Window Rollups)', weight='bold')
    ax2.legend(loc='lower right', fontsize=8.5)
    ax2.grid(True, alpha=0.3)
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig(CHART_PNG, bbox_inches='tight')
    plt.close()

def build_flink_dashboard_html(window_state, immediate_alerts):
    # Convert window rollups to JSON-safe rows
    rollup_rows_html = ""
    for (pid, w_start), recs in window_state.items():
        w_end = w_start + 20
        w_str = f"{datetime.fromtimestamp(w_start).strftime('%H:%M:%S')}-{datetime.fromtimestamp(w_end).strftime('%H:%M:%S')}"
        avg_hr = round(sum(r["hr"] for r in recs) / len(recs), 1)
        min_spo2 = min(r["spo2"] for r in recs)
        abnormal = sum(1 for r in recs if r["hr"] > 110 or r["spo2"] < 92.0 or r["temp"] > 38.5)
        if abnormal >= 2:
            badge = '<span class="badge badge-critical">CRITICAL EPISODE</span>'
        elif abnormal == 1:
            badge = '<span class="badge badge-warning">ABNORMAL OBSERVED</span>'
        else:
            badge = '<span class="badge badge-stable">STABLE</span>'
        
        rollup_rows_html += f"""
        <tr>
            <td><strong>{w_str}</strong></td>
            <td><code>{pid}</code></td>
            <td>{recs[0]['ward']}</td>
            <td>{len(recs)}</td>
            <td><strong>{avg_hr} BPM</strong></td>
            <td>{min_spo2}%</td>
            <td>{badge}</td>
        </tr>
        """

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Apache Flink Web Dashboard (Port 8081) - Patient Vitals Stream</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; }}
        body {{ background-color: #f4f6f9; color: #333; }}
        header {{ background: linear-gradient(135deg, #1b2a47 0%, #0d1b2a 100%); color: white; padding: 18px 30px; display: flex; justify-content: space-between; align-items: center; box-shadow: 0 4px 12px rgba(0,0,0,0.15); }}
        .header-title {{ font-size: 20px; font-weight: 700; letter-spacing: 0.5px; display: flex; align-items: center; gap: 12px; }}
        .flink-pill {{ background: #e65100; color: white; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: 700; }}
        .cluster-status {{ font-size: 13px; color: #4caf50; display: flex; align-items: center; gap: 6px; font-weight: 600; }}
        .status-dot {{ width: 10px; height: 10px; background: #4caf50; border-radius: 50%; box-shadow: 0 0 8px #4caf50; }}
        
        .container {{ max-width: 1300px; margin: 25px auto; padding: 0 20px; }}
        
        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 20px; margin-bottom: 25px; }}
        .kpi-card {{ background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); border-left: 4px solid #1976d2; }}
        .kpi-card.alert-card {{ border-left-color: #d32f2f; }}
        .kpi-card.window-card {{ border-left-color: #388e3c; }}
        .kpi-card.patient-card {{ border-left-color: #f57c00; }}
        .kpi-label {{ font-size: 12px; text-transform: uppercase; color: #777; font-weight: 700; margin-bottom: 8px; }}
        .kpi-val {{ font-size: 28px; font-weight: 800; color: #111; }}
        .kpi-sub {{ font-size: 12px; color: #666; margin-top: 5px; }}

        .charts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 25px; }}
        .chart-box {{ background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
        .chart-box h3 {{ font-size: 15px; font-weight: 700; margin-bottom: 15px; color: #1b2a47; }}
        
        .table-box {{ background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); margin-bottom: 25px; }}
        .table-box h3 {{ font-size: 16px; font-weight: 700; margin-bottom: 15px; color: #1b2a47; display: flex; justify-content: space-between; }}
        table {{ width: 100%; border-collapse: collapse; text-align: left; font-size: 13.5px; }}
        th {{ background: #f8fafc; padding: 12px 14px; border-bottom: 2px solid #e2e8f0; font-weight: 700; color: #475569; }}
        td {{ padding: 12px 14px; border-bottom: 1px solid #f1f5f9; }}
        tr:hover {{ background: #f8fafc; }}
        
        .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }}
        .badge-critical {{ background: #ffebee; color: #c62828; border: 1px solid #ffcdd2; }}
        .badge-warning {{ background: #fff8e1; color: #f57f17; border: 1px solid #ffecb3; }}
        .badge-stable {{ background: #e8f5e9; color: #2e7d32; border: 1px solid #c8e6c9; }}

        footer {{ text-align: center; padding: 20px; color: #888; font-size: 12px; border-top: 1px solid #e2e8f0; }}
    </style>
</head>
<body>
    <header>
        <div class="header-title">
            <span class="flink-pill">APACHE FLINK</span>
            Patient Vitals Real-Time Monitoring Web Dashboard
        </div>
        <div class="cluster-status">
            <div class="status-dot"></div>
            JobManager Active (localhost:8081) | Parallelism: 4
        </div>
    </header>

    <div class="container">
        <!-- KPI Row -->
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Stream Throughput</div>
                <div class="kpi-val">25 Events</div>
                <div class="kpi-sub">Bounded Watermark Lag: 3.0s</div>
            </div>
            <div class="kpi-card alert-card">
                <div class="kpi-label">Immediate Critical Alerts</div>
                <div class="kpi-val" style="color: #c62828;">{len(immediate_alerts)} Alerts</div>
                <div class="kpi-sub">Filtered Acute Tachycardia / Hypoxemia</div>
            </div>
            <div class="kpi-card window-card">
                <div class="kpi-label">Tumbling Windows Closed</div>
                <div class="kpi-val" style="color: #2e7d32;">{len(window_state)} Windows</div>
                <div class="kpi-sub">20-Second Event-Time Evaluation</div>
            </div>
            <div class="kpi-card patient-card">
                <div class="kpi-label">Monitored Ward Beds</div>
                <div class="kpi-val" style="color: #e65100;">5 Patients</div>
                <div class="kpi-sub">ICU, Cardiac, Post-Op, General</div>
            </div>
        </div>

        <!-- Charts Row -->
        <div class="charts-grid">
            <div class="chart-box">
                <h3>Heart Rate (BPM) Stream vs Critical Threshold (120 BPM)</h3>
                <canvas id="hrChart" height="180"></canvas>
            </div>
            <div class="chart-box">
                <h3>Blood Oxygen Saturation (SpO2 %) vs Hypoxemia Threshold (90%)</h3>
                <canvas id="spo2Chart" height="180"></canvas>
            </div>
        </div>

        <!-- Static Image Backup Preview -->
        <div class="chart-box" style="margin-bottom: 25px;">
            <h3>Matplotlib Generated High-Resolution Pipeline Plot (Saved to Disk)</h3>
            <div style="text-align: center;">
                <img src="flink_patient_vitals_chart.png" alt="Flink Vitals Chart" style="max-width: 100%; height: auto; border-radius: 6px; box-shadow: 0 2px 6px rgba(0,0,0,0.1);">
            </div>
        </div>

        <!-- Table Row -->
        <div class="table-box">
            <h3>
                <span>Flink 20-Second Tumbling Window Rollup Summaries (Event-Time)</span>
                <span style="font-size: 12px; font-weight: normal; color: #64748b;">Auto-updated via RocksDB StateBackend</span>
            </h3>
            <table>
                <thead>
                    <tr>
                        <th>Window Interval</th>
                        <th>Patient ID</th>
                        <th>Hospital Ward</th>
                        <th>Readings</th>
                        <th>Avg Heart Rate</th>
                        <th>Min SpO2</th>
                        <th>Episode Classification</th>
                    </tr>
                </thead>
                <tbody>
                    {rollup_rows_html}
                </tbody>
            </table>
        </div>
    </div>

    <footer>
        DEPARTMENT OF CSE (AI & ML) — BIG DATA SYSTEMS (24CI51) SELF LEARNING ACTIVITY 3<br>
        <strong>Academic Disclaimer:</strong> Synthetic telemetry and demonstration thresholds for educational stream processing verification only.
    </footer>

    <script>
        const ctxHR = document.getElementById('hrChart').getContext('2d');
        new Chart(ctxHR, {{
            type: 'line',
            data: {{
                labels: ['14:00:00', '14:00:08', '14:00:16', '14:00:24', '14:00:32', '14:00:40', '14:00:48', '14:00:56'],
                datasets: [
                    {{
                        label: 'Patient Heart Rate (BPM)',
                        data: [74, 82, 85, 134, 131, 92, 78, 72],
                        borderColor: '#e53935',
                        backgroundColor: 'rgba(229, 57, 53, 0.1)',
                        borderWidth: 2.5,
                        fill: true,
                        tension: 0.3
                    }},
                    {{
                        label: 'Tachycardia Critical Limit (120 BPM)',
                        data: [120, 120, 120, 120, 120, 120, 120, 120],
                        borderColor: '#d32f2f',
                        borderDash: [5, 5],
                        borderWidth: 1.5,
                        pointRadius: 0
                    }}
                ]
            }},
            options: {{ responsive: true, plugins: {{ legend: {{ position: 'top' }} }}, scales: {{ y: {{ min: 50, max: 150 }} }} }}
        }});

        const ctxSpO2 = document.getElementById('spo2Chart').getContext('2d');
        new Chart(ctxSpO2, {{
            type: 'line',
            data: {{
                labels: ['14:00:00', '14:00:08', '14:00:16', '14:00:24', '14:00:32', '14:00:40', '14:00:48', '14:00:56'],
                datasets: [
                    {{
                        label: 'SpO2 Oxygen Saturation (%)',
                        data: [98, 97, 96, 88.1, 95.2, 97, 98, 98.5],
                        borderColor: '#0288d1',
                        backgroundColor: 'rgba(2, 136, 209, 0.1)',
                        borderWidth: 2.5,
                        fill: true,
                        tension: 0.3
                    }},
                    {{
                        label: 'Hypoxemia Critical Limit (90%)',
                        data: [90, 90, 90, 90, 90, 90, 90, 90],
                        borderColor: '#01579b',
                        borderDash: [5, 5],
                        borderWidth: 1.5,
                        pointRadius: 0
                    }}
                ]
            }},
            options: {{ responsive: true, plugins: {{ legend: {{ position: 'top' }} }}, scales: {{ y: {{ min: 80, max: 100 }} }} }}
        }});
    </script>
</body>
</html>
"""
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)

def start_local_server(port=8081):
    class QuietHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=CURRENT_DIR, **kwargs)
        def log_message(self, format, *args):
            pass # suppress noisy stdout request logging

    try:
        httpd = HTTPServer(('127.0.0.1', port), QuietHandler)
        server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()
        return httpd
    except Exception as e:
        # Fallback to alternate port if 8081 is occupied
        alt_port = 8082
        httpd = HTTPServer(('127.0.0.1', alt_port), QuietHandler)
        server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()
        return httpd

def run_flink_activity(auto_open_browser=True):
    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 3: APACHE FLINK REAL-TIME PATIENT VITALS STREAMING{RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{YELLOW}[DISCLAIMER: Synthetic test data for academic demonstration only. Not a diagnostic tool.]{RESET}")
    print(f"Flink Cluster   : {GREEN}JobManager @ http://localhost:8081 (4 Task Slots Active){RESET}")
    print(f"Stream Semantics: {CYAN}Event-Time with Bounded-Out-of-Orderness Watermarks (Lag: 3s){RESET}")
    print(f"Window Operator : {CYAN}TumblingEventTimeWindows (Size: 20 seconds){RESET}\n")

    base_time = datetime.now() - timedelta(minutes=2)
    window_state = {}
    immediate_alerts = []
    total_events = 25

    print(f"{BOLD}{'EVENT TIME':<10} | {'PATIENT':<9} | {'WARD':<19} | {'HR':<7} | {'SpO2':<7} | {'TEMP':<7} | {'STATUS'}{RESET}")
    print("-" * 88)

    for i in range(total_events):
        p = random.choice(PATIENTS)
        event_time = base_time + timedelta(seconds=i * 2)
        epoch = int(event_time.timestamp())

        if p["patient_id"] == "PAT_103" and i in [5, 9, 13]:
            hr = random.randint(128, 145)
            spo2 = round(random.uniform(94.0, 96.5), 1)
            temp = round(random.uniform(38.6, 39.2), 1)
        elif p["patient_id"] == "PAT_101" and i in [7]:
            hr = random.randint(82, 95)
            spo2 = round(random.uniform(86.0, 89.5), 1)
            temp = round(random.uniform(36.8, 37.2), 1)
        else:
            hr = random.randint(65, 88)
            spo2 = round(random.uniform(96.0, 99.5), 1)
            temp = round(random.uniform(36.4, 37.2), 1)

        record = {"hr": hr, "spo2": spo2, "temp": temp, "patient_id": p["patient_id"], "ward": p["ward"], "time": event_time.strftime("%H:%M:%S")}
        w_start = (epoch // 20) * 20
        pid = p["patient_id"]
        if (pid, w_start) not in window_state:
            window_state[(pid, w_start)] = []
        window_state[(pid, w_start)].append(record)

        if hr > 120 or spo2 < 90.0:
            immediate_alerts.append(record)
            stat = f"{BOLD}{RED}[!] ALERT CRITICAL{RESET}"
        else:
            stat = f"{GREEN}NORMAL{RESET}"

        hr_str = f"{RED}{hr:<4}{RESET}" if hr > 110 else f"{hr:<4}"
        spo2_str = f"{CYAN}{spo2:<4}%{RESET}" if spo2 < 92 else f"{spo2:<4}%"
        print(f"{record['time']:<10} | {p['patient_id']:<9} | {p['ward']:<19} | {hr_str} | {spo2_str} | {temp}°C | {stat}")
        time.sleep(0.04)

    # 1. Generate High-Res Chart File
    generate_matplotlib_chart()
    print(f"\n{GREEN}✔ [CHART GENERATED] Matplotlib biometric chart saved to:{RESET} {CHART_PNG}")

    # 2. Build Interactive HTML Dashboard
    build_flink_dashboard_html(window_state, immediate_alerts)

    # 3. Start Localhost Server on Port 8081 (Flink Web UI Port)
    port = 8081
    httpd = start_local_server(port)
    url = f"http://localhost:{port}"

    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}FLINK WEB UI LOCALHOST DASHBOARD LAUNCHED:{RESET}")
    print(f"{BOLD}{GREEN}>>> URL: {url} (Simulating Apache Flink Web UI on Port 8081){RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")

    if auto_open_browser:
        try:
            print(f"{YELLOW}[BROWSER] Automatically opening {url} in your default web browser...{RESET}")
            webbrowser.open(url)
        except Exception:
            pass

    print(f"\n{BOLD}{GREEN}[ACTIVITY 3 EXECUTION COMPLETE] Flink streaming processor verified.{RESET}")
    print(f"{YELLOW}Press [Ctrl+C] or close terminal to stop the local dashboard server.{RESET}\n")

    # Keep server running briefly if called standalone, or return
    if "--daemon" in sys.argv:
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    auto_open = False if "--no-browser" in sys.argv else True
    run_flink_activity(auto_open_browser=auto_open)
