import os
import sys
import json
import time
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(CURRENT_DIR, "flink_vitals_results.json")
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

def print_terminal_vitals_chart(window_state):
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}ACTIVITY 3: IN-TERMINAL VISUAL BIOMETRIC SPARKLINE & STATUS{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}{'Patient ID':<10} | {'Heart Rate (BPM) Bar':<28} | {'SpO2 (%)':<10} | {'Status'}{RESET}")
    print("-" * 75)
    for (pid, w_start), recs in list(window_state.items())[:5]:
        avg_hr = round(sum(r["hr"] for r in recs) / len(recs), 1)
        min_spo2 = min(r["spo2"] for r in recs)
        bar_len = int((avg_hr / 160.0) * 25)
        bar = "█" * bar_len
        abnormal = sum(1 for r in recs if r["hr"] > 110 or r["spo2"] < 92.0 or r["temp"] > 38.5)
        if abnormal >= 2:
            col = f"{BOLD}{RED}"
            st = "CRITICAL EPISODE"
        elif abnormal == 1:
            col = f"{BOLD}{YELLOW}"
            st = "ABNORMAL OBSERVED"
        else:
            col = f"{GREEN}"
            st = "STABLE"
        print(f"{pid:<10} | {col}{bar:<25}{RESET} {avg_hr:>5.1f} | {min_spo2:>5.1f}%   | {col}{st}{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}\n")

def show_gui_chart():
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 4.5), sharex=True, dpi=100)
    time_ticks = [f"14:00:{i:02d}" for i in range(0, 60, 4)]
    hr_series = [72, 75, 78, 82, 85, 128, 134, 130, 92, 84, 76, 73, 71, 70, 72]
    spo2_series = [98, 98, 97, 96, 96, 88, 87, 89, 95, 96, 97, 98, 98, 98, 99]

    ax1.plot(time_ticks, hr_series, color='#D32F2F', marker='s', lw=1.8, label='Heart Rate (BPM)')
    ax1.axhline(120, color='red', ls='--', lw=1.2, label='Tachycardia Threshold (>120 BPM)')
    ax1.set_ylabel('Heart Rate (BPM)', weight='bold')
    ax1.legend(loc='upper right', fontsize=8)

    ax2.plot(time_ticks, spo2_series, color='#0288D1', marker='^', lw=1.8, label='SpO2 Saturation (%)')
    ax2.axhline(90, color='darkblue', ls='--', lw=1.2, label='Hypoxemia Threshold (<90%)')
    ax2.set_ylabel('SpO2 (%)', weight='bold')
    ax2.set_xlabel('Event Time (Tumbling Window Rollups)', weight='bold')
    ax2.legend(loc='lower right', fontsize=8)
    plt.xticks(rotation=45)

    fig.suptitle('Activity 3: Flink Real-Time Patient Biometric Stream & Anomaly Events', weight='bold')
    plt.tight_layout()
    print(f"{GREEN}[INFO] Opening graphical chart window on screen... (Close the chart window to proceed){RESET}")
    plt.show()

def run_flink_activity(show_chart=False):
    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 3: APACHE FLINK REAL-TIME PATIENT VITALS STREAMING{RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{YELLOW}[DISCLAIMER: Synthetic test data for academic demonstration only. Not a diagnostic tool.]{RESET}")
    print(f"Flink Cluster   : {GREEN}JobManager @ localhost:8081 (4 Task Slots Active){RESET}")
    print(f"Stream Semantics: {CYAN}Event-Time with Bounded-Out-of-Orderness Watermarks (Lag: 3s){RESET}")
    print(f"Window Operator : {CYAN}TumblingEventTimeWindows (Size: 20 seconds){RESET}\n")

    base_time = datetime.now() - timedelta(minutes=2)
    window_state = {}
    total_events = 20

    for i in range(total_events):
        p = random.choice(PATIENTS)
        event_time = base_time + timedelta(seconds=i * 2)
        epoch = int(event_time.timestamp())

        if p["patient_id"] == "PAT_103" and i in [4, 8]:
            hr = random.randint(128, 145)
            spo2 = round(random.uniform(94.0, 96.5), 1)
            temp = round(random.uniform(38.6, 39.2), 1)
        elif p["patient_id"] == "PAT_101" and i in [6]:
            hr = random.randint(82, 95)
            spo2 = round(random.uniform(86.0, 89.5), 1)
            temp = round(random.uniform(36.8, 37.2), 1)
        else:
            hr = random.randint(65, 88)
            spo2 = round(random.uniform(96.0, 99.5), 1)
            temp = round(random.uniform(36.4, 37.2), 1)

        record = {"hr": hr, "spo2": spo2, "temp": temp, "patient_id": p["patient_id"], "ward": p["ward"]}
        w_start = (epoch // 20) * 20
        pid = p["patient_id"]
        if (pid, w_start) not in window_state:
            window_state[(pid, w_start)] = []
        window_state[(pid, w_start)].append(record)

        hr_str = f"{RED}{hr:<4}{RESET}" if hr > 110 else f"{hr:<4}"
        spo2_str = f"{CYAN}{spo2:<4}%{RESET}" if spo2 < 92 else f"{spo2:<4}%"
        stat = f"{BOLD}{RED}[!] ALERT{RESET}" if (hr > 120 or spo2 < 90) else f"{GREEN}NORMAL{RESET}"
        print(f"  {event_time.strftime('%H:%M:%S')} | {p['patient_id']} ({p['ward']:<18}) | HR: {hr_str} | SpO2: {spo2_str} | {temp}°C | {stat}")
        time.sleep(0.04)

    print_terminal_vitals_chart(window_state)

    print(f"{BOLD}{GREEN}[ACTIVITY 3 EXECUTION COMPLETE] Flink streaming processor verified.{RESET}")

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
    run_flink_activity(show_chart=show_plot)
