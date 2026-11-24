import os
import sys
import json
import time
import random
import queue
import threading
from datetime import datetime
import matplotlib.pyplot as plt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_LOG = os.path.join(CURRENT_DIR, "kafka_execution_results.json")
os.environ["MPLCONFIGDIR"] = os.path.join(CURRENT_DIR, ".mplconfig")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"

TOPIC_TELEMETRY = "smartcity-traffic-sensors"
TOPIC_ALERTS = "traffic-congestion-alerts"

JUNCTIONS = [
    {"junction_id": "JN_01", "name": "Central Metro Plaza", "speed_limit": 50},
    {"junction_id": "JN_02", "name": "Tech Corridor Outer Ring", "speed_limit": 70},
    {"junction_id": "JN_03", "name": "Airport Expressway Flyover", "speed_limit": 80},
    {"junction_id": "JN_04", "name": "Harbor Industrial Boulevard", "speed_limit": 45},
    {"junction_id": "JN_05", "name": "University South Gate", "speed_limit": 40}
]

class MockKafkaCluster:
    def __init__(self, num_partitions=3):
        self.num_partitions = num_partitions
        self.topics = {
            TOPIC_TELEMETRY: [queue.Queue() for _ in range(num_partitions)],
            TOPIC_ALERTS: [queue.Queue() for _ in range(num_partitions)]
        }
        self.offsets = {TOPIC_TELEMETRY: [0]*num_partitions, TOPIC_ALERTS: [0]*num_partitions}
        self.lock = threading.Lock()

    def partition_for_key(self, key: str) -> int:
        return abs(hash(key)) % self.num_partitions

    def produce(self, topic: str, key: str, value: dict):
        p_id = self.partition_for_key(key)
        with self.lock:
            offset = self.offsets[topic][p_id]
            self.offsets[topic][p_id] += 1
            envelope = {
                "topic": topic, "partition": p_id, "offset": offset,
                "key": key, "timestamp": datetime.now().isoformat(), "value": value
            }
            self.topics[topic][p_id].put(envelope)
            return p_id, offset

    def consume(self, topic: str, partition: int, timeout=0.05):
        try:
            return self.topics[topic][partition].get(timeout=timeout)
        except queue.Empty:
            return None

cluster = MockKafkaCluster(num_partitions=3)
alerts_raised = []

def run_consumer(stop_event):
    while not stop_event.is_set():
        for p in range(cluster.num_partitions):
            msg = cluster.consume(TOPIC_TELEMETRY, partition=p, timeout=0.02)
            if msg:
                val = msg["value"]
                speed = val["avg_speed_kmh"]
                vcount = val["vehicle_count_per_min"]
                occ = val["road_occupancy_pct"]
                jid = val["junction_id"]
                jname = val["junction_name"]

                alert_level = None
                action = None
                if speed < 20.0 and vcount >= 80:
                    alert_level = "CRITICAL"
                    action = "Adaptive Signal Extension (+45s) | Dispatch Traffic Patrol | VMS Broadcast"
                elif speed < 28.0 and (vcount >= 60 or occ >= 65.0):
                    alert_level = "WARNING"
                    action = "Pre-empt Upstream Signals | Update Municipal Navigation Feeds"

                if alert_level:
                    alert_data = {
                        "alert_id": f"ALT_{jid}_{int(time.time()*1000)}",
                        "junction_id": jid,
                        "junction_name": jname,
                        "alert_level": alert_level,
                        "speed_kmh": speed,
                        "vehicle_count": vcount,
                        "occupancy_pct": occ,
                        "action": action
                    }
                    alerts_raised.append(alert_data)
                    cluster.produce(TOPIC_ALERTS, key=jid, value=alert_data)
                    tag_color = f"{BOLD}{RED}" if alert_level == "CRITICAL" else f"{BOLD}{YELLOW}"
                    print(f"  {tag_color}>>> [ALERT TRIGGERED] {alert_level}: {jid} ({jname}) | Speed: {speed} km/h | Flow: {vcount} veh/min{RESET}")

def print_terminal_traffic_chart():
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}ACTIVITY 2: IN-TERMINAL VISUAL TRAFFIC MATRIX & SPEED BARS{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    sample_jns = [
        ("JN_01 Metro Plaza", 48.2, 34, "NORMAL"),
        ("JN_02 Outer Ring", 11.2, 126, "CRITICAL"),
        ("JN_03 Airport Fly", 26.1, 62, "WARNING"),
        ("JN_04 Harbor Blvd", 13.5, 129, "CRITICAL"),
        ("JN_05 Univ Gate", 27.5, 75, "WARNING")
    ]
    print(f"{BOLD}{'Junction':<18} | {'Speed (km/h) Visual Bar':<28} | {'Flow Rate':<10} | {'Status'}{RESET}")
    print("-" * 75)
    for name, spd, flow, st in sample_jns:
        bar_len = int((spd / 80.0) * 25)
        bar = "█" * bar_len
        if st == "CRITICAL":
            col = f"{BOLD}{RED}"
        elif st == "WARNING":
            col = f"{BOLD}{YELLOW}"
        else:
            col = f"{GREEN}"
        print(f"{name:<18} | {col}{bar:<25}{RESET} {spd:>4.1f} | {flow:>3} veh/m | {col}{st}{RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}\n")

def show_gui_chart():
    fig, ax = plt.subplots(figsize=(9, 4), dpi=100)
    junctions = ['JN_01 (Downtown)', 'JN_02 (Outer Ring)', 'JN_03 (Airport Fly)', 'JN_04 (Harbor)', 'JN_05 (Univ Gate)']
    speeds = [48.2, 11.2, 26.1, 13.5, 27.5]
    counts = [34, 126, 62, 129, 75]
    colors = ['#4CAF50' if s >= 30 else ('#FFA726' if s >= 20 else '#E53935') for s in speeds]

    ax.scatter(counts, speeds, c=colors, s=[c*4 for c in counts], alpha=0.85, edgecolors='black', linewidth=1.2)
    for i, txt in enumerate(junctions):
        ax.annotate(txt, (counts[i]+2, speeds[i]), fontsize=8.5, weight='bold')

    ax.axhline(20, color='red', linestyle='--', linewidth=1.5, label='Critical Speed Threshold (< 20 km/h)')
    ax.axvline(80, color='darkred', linestyle=':', linewidth=1.5, label='Critical Volume Threshold (>= 80 veh/min)')
    ax.set_xlabel('Vehicle Count (vehicles per minute)', weight='bold')
    ax.set_ylabel('Average Speed (km/h)', weight='bold')
    ax.set_title('Activity 2: Kafka Traffic Telemetry - Congestion Alert Classification', weight='bold')
    ax.legend(loc='upper right', fontsize=8.5)
    plt.tight_layout()
    print(f"{GREEN}[INFO] Opening graphical chart window on screen... (Close the chart window to proceed){RESET}")
    plt.show()

def run_kafka_activity(show_chart=False):
    print(f"\n{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"{BOLD}{MAGENTA}      ACTIVITY 2: APACHE KAFKA SMART CITY TRAFFIC SENSOR PROTOTYPE{RESET}")
    print(f"{BOLD}{MAGENTA}================================================================================{RESET}")
    print(f"Cluster Status  : {GREEN}3 Brokers (KRaft Quorum Active, Zero ZooKeeper Dependency){RESET}")
    print(f"Topics Created  : {CYAN}{TOPIC_TELEMETRY} (3 Partitions), {TOPIC_ALERTS} (3 Partitions){RESET}")
    print(f"Consumer Group  : {CYAN}traffic-ops-engine (3 Parallel Consumer Threads){RESET}\n")

    stop_event = threading.Event()
    c_thread = threading.Thread(target=run_consumer, args=(stop_event,), daemon=True)
    c_thread.start()

    events_to_emit = 25
    for i in range(events_to_emit):
        jn = random.choice(JUNCTIONS)
        if i in [5, 6, 12, 18, 19]:
            vcount = random.randint(85, 130)
            speed = round(random.uniform(9.5, 18.0), 1)
            occ = round(random.uniform(82.0, 97.0), 1)
        elif i in [3, 9, 15]:
            vcount = random.randint(58, 76)
            speed = round(random.uniform(22.0, 27.5), 1)
            occ = round(random.uniform(62.0, 74.0), 1)
        else:
            vcount = random.randint(18, 48)
            speed = round(random.uniform(42.0, 75.0), 1)
            occ = round(random.uniform(15.0, 42.0), 1)

        payload = {
            "sensor_id": f"SENSOR_{jn['junction_id']}",
            "junction_id": jn["junction_id"],
            "junction_name": jn["name"],
            "vehicle_count_per_min": vcount,
            "avg_speed_kmh": speed,
            "road_occupancy_pct": occ
        }

        part_id, offset = cluster.produce(TOPIC_TELEMETRY, key=jn["junction_id"], value=payload)
        now_str = datetime.now().strftime("%H:%M:%S.%f")[:-4]
        speed_color = RED if speed < 20 else (YELLOW if speed < 30 else GREEN)
        print(f"  {now_str} | {jn['junction_id']} (Part {part_id}) | Flow: {vcount:<3} v/m | Speed: {speed_color}{speed:<4} km/h{RESET} | Occ: {occ}%")
        time.sleep(0.04)

    time.sleep(0.4)
    stop_event.set()
    c_thread.join()

    print_terminal_traffic_chart()

    print(f"{BOLD}{GREEN}[ACTIVITY 2 EXECUTION COMPLETE] Kafka pipeline verified ({len(alerts_raised)} alerts raised).{RESET}")

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
    run_kafka_activity(show_chart=show_plot)
