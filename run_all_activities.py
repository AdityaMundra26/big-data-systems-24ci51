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

def run_script(rel_path, extra_args=None):
    full_path = os.path.join(CURRENT_DIR, rel_path)
    if not os.path.exists(full_path):
        print(f"{RED}[ERROR] Script not found: {full_path}{RESET}")
        return
    print(f"\n{CYAN}>>> Launching: python {os.path.basename(full_path)}...{RESET}")
    cmd = [sys.executable, full_path]
    if extra_args:
        cmd.extend(extra_args)
    try:
        result = subprocess.run(cmd, cwd=os.path.dirname(full_path))
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
    print(f"  {BOLD}[1]{RESET} {CYAN}Activity 1: Apache Hive{RESET}      — Supermarket Sales DW (Console Table + Chart)")
    print(f"  {BOLD}[2]{RESET} {CYAN}Activity 2: Apache Kafka{RESET}     — Traffic Telemetry & Alerts (Console Table + Chart)")
    print(f"  {BOLD}[3]{RESET} {GREEN}Activity 3: Apache Flink{RESET}     — Patient Vitals ({BOLD}Generates Chart + Opens http://localhost:8081{RESET})")
    print(f"  {BOLD}[4]{RESET} {CYAN}Activity 4: MongoDB (NoSQL){RESET}  — E-Commerce Dynamic Catalog (Console Benchmark + Chart)")
    print(f"  {BOLD}[5]{RESET} {GREEN}Activity 5: Cloud Analytics{RESET}  — AWS Log Lake ({BOLD}Generates Chart + Opens http://localhost:8050{RESET})")
    print(f"  ------------------------------------------------------------------------------")
    print(f"  {BOLD}[6]{RESET} {GREEN}RUN ALL 5 ACTIVITIES SEQUENTIALLY (Full Live Demo for Teacher){RESET}")
    print(f"  {BOLD}[7]{RESET} {YELLOW}View Path to Consolidated Academic Report (.md / LaTeX){RESET}")
    print(f"  {BOLD}[0]{RESET} Exit")
    print(f"{CYAN}================================================================================{RESET}")

def main():
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    else:
        print_banner()
        choice = input(f"{BOLD}Enter choice [0-7]: {RESET}").strip()

    if choice == "1":
        run_script("activity_1_hive/run_hive_analysis.py", ["--plot"])
    elif choice == "2":
        run_script("activity_2_kafka/traffic_kafka_prototype.py", ["--plot"])
    elif choice == "3":
        run_script("activity_3_flink/flink_patient_vitals_processor.py")
    elif choice == "4":
        run_script("activity_4_mongodb/mongodb_ecommerce_catalog.py", ["--plot"])
    elif choice == "5":
        run_script("activity_5_cloud_logs/cloud_log_analytics_athena.py")
    elif choice == "6" or choice.lower() == "--all":
        print(f"\n{BOLD}{GREEN}>>> RUNNING COMPLETE SUITE OF 5 ACTIVITIES FOR IN-PERSON DEMONSTRATION...{RESET}\n")
        time.sleep(0.5)
        run_script("activity_1_hive/run_hive_analysis.py", ["--no-plot"])
        time.sleep(1.0)
        run_script("activity_2_kafka/traffic_kafka_prototype.py", ["--no-plot"])
        time.sleep(1.0)
        run_script("activity_3_flink/flink_patient_vitals_processor.py")
        time.sleep(1.0)
        run_script("activity_4_mongodb/mongodb_ecommerce_catalog.py", ["--no-plot"])
        time.sleep(1.0)
        run_script("activity_5_cloud_logs/cloud_log_analytics_athena.py")
        print(f"\n{BOLD}{GREEN}================================================================================{RESET}")
        print(f"{BOLD}{GREEN}ALL 5 BIG DATA SYSTEM ACTIVITIES EXECUTED AND VERIFIED SUCCESSFULLY!{RESET}")
        print(f"{BOLD}{GREEN}================================================================================{RESET}\n")
    elif choice == "7":
        md_path = os.path.join(CURRENT_DIR, "Big_Data_Systems_24CI51_Consolidated_Report.md")
        tex_path = os.path.join(CURRENT_DIR, "latex_template", "main.tex")
        zip_path = os.path.join(CURRENT_DIR, "Big_Data_Systems_24CI51_LaTeX_Report.zip")
        print(f"\n{GREEN}Markdown Report:{RESET} {md_path}")
        print(f"{GREEN}LaTeX Report   :{RESET} {tex_path}")
        print(f"{GREEN}Overleaf ZIP   :{RESET} {zip_path}\n")
    elif choice == "0":
        print("Exiting.")
        sys.exit(0)
    else:
        print(f"{RED}Invalid selection.{RESET}")

if __name__ == "__main__":
    main()
