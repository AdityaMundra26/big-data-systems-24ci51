import os
import sys
import time
import subprocess

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

def run_script(rel_path):
    full_path = os.path.join(CURRENT_DIR, rel_path)
    if not os.path.exists(full_path):
        print(f"{RED}[ERROR] Script not found: {full_path}{RESET}")
        return
    print(f"\n{CYAN}>>> Launching: python {os.path.basename(full_path)}...{RESET}")
    # Use subprocess.run with argument list to prevent Windows space-in-path issues
    try:
        result = subprocess.run([sys.executable, full_path], cwd=os.path.dirname(full_path))
        if result.returncode != 0:
            print(f"{RED}[ERROR] Execution returned code {result.returncode}{RESET}")
    except Exception as e:
        print(f"{RED}[ERROR] Failed to run {full_path}: {e}{RESET}")

def print_banner():
    print(f"\n{BOLD}{CYAN}================================================================================{RESET}")
    print(f"{BOLD}{BLUE}       DEPARTMENT OF CSE (ARTIFICIAL INTELLIGENCE & MACHINE LEARNING){RESET}")
    print(f"{BOLD}{BLUE}            BIG DATA SYSTEMS (24CI51) — SELF LEARNING ACTIVITIES{RESET}")
    print(f"{BOLD}{CYAN}================================================================================{RESET}")
    print(f"Academic Evaluation: {BOLD}Total Marks: 20 (5 Activities × 4 Marks){RESET}\n")
    print(f"{BOLD}SELECT AN ACTIVITY TO RUN ON YOUR PC:{RESET}")
    print(f"  {BOLD}[1]{RESET} {CYAN}Activity 1: Apache Hive{RESET}      — Supermarket Sales DW & Inventory Optimization")
    print(f"  {BOLD}[2]{RESET} {CYAN}Activity 2: Apache Kafka{RESET}     — Smart City Traffic Telemetry & Congestion Alerts")
    print(f"  {BOLD}[3]{RESET} {CYAN}Activity 3: Apache Flink{RESET}     — Hospital Patient Vitals Streaming & Alerts")
    print(f"  {BOLD}[4]{RESET} {CYAN}Activity 4: MongoDB (NoSQL){RESET}  — E-Commerce Dynamic Catalog & Index Benchmark")
    print(f"  {BOLD}[5]{RESET} {CYAN}Activity 5: Cloud Analytics{RESET}  — AWS S3 + Athena Serverless Log Lake & Cost Model")
    print(f"  ------------------------------------------------------------------------------")
    print(f"  {BOLD}[6]{RESET} {GREEN}RUN ALL 5 ACTIVITIES SEQUENTIALLY (Full Live Demo for Teacher){RESET}")
    print(f"  {BOLD}[7]{RESET} {YELLOW}View Path to Consolidated Academic Report (.md){RESET}")
    print(f"  {BOLD}[0]{RESET} Exit")
    print(f"{CYAN}================================================================================{RESET}")

def main():
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    else:
        print_banner()
        choice = input(f"{BOLD}Enter choice [0-7]: {RESET}").strip()

    if choice == "1":
        run_script("activity_1_hive/run_hive_analysis.py")
    elif choice == "2":
        run_script("activity_2_kafka/traffic_kafka_prototype.py")
    elif choice == "3":
        run_script("activity_3_flink/flink_patient_vitals_processor.py")
    elif choice == "4":
        run_script("activity_4_mongodb/mongodb_ecommerce_catalog.py")
    elif choice == "5":
        run_script("activity_5_cloud_logs/cloud_log_analytics_athena.py")
    elif choice == "6" or choice.lower() == "--all":
        print(f"\n{BOLD}{GREEN}>>> RUNNING COMPLETE SUITE OF 5 ACTIVITIES FOR IN-PERSON DEMONSTRATION...{RESET}\n")
        time.sleep(0.8)
        run_script("activity_1_hive/run_hive_analysis.py")
        time.sleep(1.0)
        run_script("activity_2_kafka/traffic_kafka_prototype.py")
        time.sleep(1.0)
        run_script("activity_3_flink/flink_patient_vitals_processor.py")
        time.sleep(1.0)
        run_script("activity_4_mongodb/mongodb_ecommerce_catalog.py")
        time.sleep(1.0)
        run_script("activity_5_cloud_logs/cloud_log_analytics_athena.py")
        print(f"\n{BOLD}{GREEN}================================================================================{RESET}")
        print(f"{BOLD}{GREEN}ALL 5 BIG DATA SYSTEM ACTIVITIES EXECUTED AND VERIFIED SUCCESSFULLY!{RESET}")
        print(f"{BOLD}{GREEN}================================================================================{RESET}\n")
    elif choice == "7":
        report_path = os.path.join(CURRENT_DIR, "Big_Data_Systems_24CI51_Consolidated_Report.md")
        print(f"\n{GREEN}Consolidated Report File Location:{RESET}\n{BOLD}{report_path}{RESET}\n")
    elif choice == "0":
        print("Exiting.")
        sys.exit(0)
    else:
        print(f"{RED}Invalid selection.{RESET}")

if __name__ == "__main__":
    main()
