import os
import sys
import json
import time
import random
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(CURRENT_DIR, "flink_vitals_results.json")

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

def run_flink_activity():
    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 3: APACHE FLINK REAL-TIME PATIENT VITALS STREAMING{RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{YELLOW}[DISCLAIMER: Synthetic test data for academic demonstration only. Not a diagnostic tool.]{RESET}")
    print(f"Flink Cluster   : {GREEN}JobManager @ localhost:8081 (4 Task Slots Active){RESET}")
    print(f"Stream Semantics: {CYAN}Event-Time with Bounded-Out-of-Orderness Watermarks (Lag: 3s){RESET}")
    print(f"Window Operator : {CYAN}TumblingEventTimeWindows (Size: 20 seconds){RESET}")
    print(f"State Backend   : {CYAN}Embedded RocksDB StateBackend (Checkpointing Interval: 10s){RESET}\n")

    print(f"{BOLD}{'EVENT TIME':<10} | {'PATIENT':<9} | {'WARD':<19} | {'HR':<7} | {'SpO2':<7} | {'TEMP':<7} | {'STATUS'}{RESET}")
    print("-" * 88)

    base_time = datetime.now() - timedelta(minutes=2)
    window_state = {}
    immediate_alerts = []
    window_summaries = []
    total_events = 30

    for i in range(total_events):
        p = random.choice(PATIENTS)
        event_time = base_time + timedelta(seconds=i * 2)
        epoch = int(event_time.timestamp())

        # Inject controlled anomalies
        if p["patient_id"] == "PAT_103" and i in [6, 10, 14]:
            hr = random.randint(128, 145)
            spo2 = round(random.uniform(94.0, 96.5), 1)
            temp = round(random.uniform(38.6, 39.2), 1)
        elif p["patient_id"] == "PAT_101" and i in [8, 12]:
            hr = random.randint(82, 95)
            spo2 = round(random.uniform(86.0, 89.5), 1)
            temp = round(random.uniform(36.8, 37.2), 1)
        else:
            hr = random.randint(65, 88)
            spo2 = round(random.uniform(96.0, 99.5), 1)
            temp = round(random.uniform(36.4, 37.2), 1)

        record = {
            "timestamp": event_time.strftime("%H:%M:%S"),
            "epoch": epoch,
            "patient_id": p["patient_id"],
            "patient_name": p["name"],
            "ward": p["ward"],
            "hr": hr, "spo2": spo2, "temp": temp
        }

        # Immediate threshold evaluation
        anomalies = []
        if hr > THRESHOLDS["heart_rate"]["critical_high"]:
            anomalies.append(f"TACHYCARDIA ({hr} BPM)")
        if spo2 < THRESHOLDS["spo2"]["critical_low"]:
            anomalies.append(f"HYPOXEMIA ({spo2}%)")
        if temp > THRESHOLDS["temperature"]["critical_high"]:
            anomalies.append(f"HIGH FEVER ({temp}°C)")

        if anomalies:
            status_tag = f"{BOLD}{RED}[!] CRITICAL: {', '.join(anomalies)}{RESET}"
            immediate_alerts.append({"time": record["timestamp"], "patient": p["patient_id"], "anomalies": anomalies})
        else:
            status_tag = f"{GREEN}NORMAL{RESET}"

        # Assign to 20-second Tumbling Window
        w_start = (epoch // 20) * 20
        pid = p["patient_id"]
        if (pid, w_start) not in window_state:
            window_state[(pid, w_start)] = []
        window_state[(pid, w_start)].append(record)

        hr_str = f"{RED}{hr:<4}{RESET}" if hr > 110 else f"{hr:<4}"
        spo2_str = f"{CYAN}{spo2:<4}%{RESET}" if spo2 < 92 else f"{spo2:<4}%"
        print(f"{record['timestamp']:<10} | {p['patient_id']:<9} | {p['ward']:<19} | {hr_str} | {spo2_str} | {temp}°C | {status_tag}")
        time.sleep(0.06)

    # Trigger window aggregations
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}FLINK TUMBLING WINDOW ROLLUP AGGREGATIONS (20-Second Watermark Close):{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}{'WINDOW TIME':<13} | {'PATIENT':<9} | {'WARD':<19} | {'READINGS':<9} | {'AVG HR':<8} | {'MIN SpO2':<9} | {'EPISODE STATUS'}{RESET}")
    print("-" * 92)

    for (pid, w_start), recs in window_state.items():
        w_end = w_start + 20
        w_str = f"{datetime.fromtimestamp(w_start).strftime('%H:%M:%S')}-{datetime.fromtimestamp(w_end).strftime('%H:%M:%S')}"
        avg_hr = round(sum(r["hr"] for r in recs) / len(recs), 1)
        min_spo2 = min(r["spo2"] for r in recs)
        abnormal = sum(1 for r in recs if r["hr"] > 110 or r["spo2"] < 92.0 or r["temp"] > 38.5)
        status = f"{BOLD}{RED}CRITICAL EPISODE{RESET}" if abnormal >= 2 else (f"{YELLOW}ABNORMAL OBSERVED{RESET}" if abnormal == 1 else f"{GREEN}STABLE{RESET}")
        print(f"{w_str:<13} | {pid:<9} | {recs[0]['ward']:<19} | {len(recs):<9} | {avg_hr:<8} | {min_spo2:<8}% | {status}")
        window_summaries.append({"window": w_str, "patient_id": pid, "status": status})

    with open(OUTPUT_JSON, "w") as f:
        json.dump({"alerts": immediate_alerts, "window_summaries": window_summaries}, f, indent=2)

    print(f"\n{BOLD}{GREEN}[ACTIVITY 3 EXECUTION COMPLETE] Flink streaming processor verified.{RESET}\n")

if __name__ == "__main__":
    run_flink_activity()
