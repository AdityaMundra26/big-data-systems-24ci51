import os
import matplotlib.pyplot as plt
import numpy as np

IMG_DIR = r"C:\Users\Aditya Mundra\.gemini\antigravity\scratch\big_data_systems_24ci51\latex_template\images"
os.makedirs(IMG_DIR, exist_ok=True)

# Set clean aesthetic styling
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['axes.labelsize'] = 11

# 1. OVERALL BIG DATA SYSTEMS ARCHITECTURE
fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
ax.axis('off')

boxes = [
    ("Activity 1: Batch Warehouse", "Apache Hive (ORC + Tez)\nSupermarket Sales DW\nPartitioned & Bucketed", 0.05, 0.55, "#E1F5FE", "#0288D1"),
    ("Activity 2: Messaging Hub", "Apache Kafka (KRaft)\nSmart City Traffic IoT\n3-Partition Consumer Engine", 0.38, 0.55, "#FFF8E1", "#FFA000"),
    ("Activity 3: Stream Processing", "Apache Flink (DataStream)\nICU Patient Vitals\n20s Tumbling Event Windows", 0.71, 0.55, "#FCE4EC", "#D81B60"),
    ("Activity 4: Dynamic NoSQL Store", "MongoDB (WiredTiger)\nPolymorphic Product Catalog\nCompound B-Tree Indexing", 0.22, 0.12, "#E8F5E9", "#388E3C"),
    ("Activity 5: Serverless Cloud Lake", "AWS S3 + Glue + Athena\nPresto SQL Serverless Logs\n95.4% Cost Reduction", 0.56, 0.12, "#EDE7F6", "#512DA8")
]

for title, desc, x, y, bg, border in boxes:
    ax.text(x + 0.12, y + 0.28, title, ha='center', va='center', weight='bold', color=border, fontsize=11)
    bbox_props = dict(boxstyle='round,pad=0.8', facecolor=bg, edgecolor=border, linewidth=1.8)
    ax.text(x + 0.12, y + 0.12, desc, ha='center', va='center', bbox=bbox_props, fontsize=9.5, color='#212121')

# Add interconnecting lines / arrows
ax.annotate("", xy=(0.38, 0.68), xytext=(0.29, 0.68), arrowprops=dict(arrowstyle="->", lw=1.5, color="#555555"))
ax.annotate("", xy=(0.71, 0.68), xytext=(0.62, 0.68), arrowprops=dict(arrowstyle="->", lw=1.5, color="#555555"))
ax.annotate("", xy=(0.34, 0.40), xytext=(0.20, 0.52), arrowprops=dict(arrowstyle="->", lw=1.5, color="#555555"))
ax.annotate("", xy=(0.68, 0.40), xytext=(0.78, 0.52), arrowprops=dict(arrowstyle="->", lw=1.5, color="#555555"))

plt.title("Overall Big Data Systems Integrated Architecture (Course 24CI51)", fontsize=13, weight='bold', pad=15)
plt.tight_layout()
plt.savefig(os.path.join(IMG_DIR, "overall_architecture.png"), bbox_inches='tight')
plt.close()
print("Generated: overall_architecture.png")

# 2. ACTIVITY 1: HIVE HOURLY SALES VELOCITY & PEAK HOURS
fig, ax1 = plt.subplots(figsize=(9, 4.5), dpi=300)
hours = list(range(24))
# Real data from our execution
rev = [3406.82, 1695.29, 1964.13, 2070.43, 2049.72, 3268.65, 6745.81, 17761.36, 26226.74, 21133.20,
       26226.32, 33222.61, 34222.02, 25608.52, 23038.18, 20542.50, 29114.00, 41240.52, 47537.90,
       43894.92, 21923.69, 14629.01, 8009.84, 3513.05]
tx = [107, 56, 61, 62, 58, 112, 206, 539, 808, 673, 804, 1044, 1021, 764, 708, 659, 925, 1323, 1498, 1328, 694, 493, 220, 111]

ax1.bar(hours, [r/1000 for r in rev], color='#1976D2', alpha=0.75, width=0.6, label='Revenue ($ in Thousands)')
ax1.set_xlabel('Hour of the Day (00:00 - 23:00)', weight='bold')
ax1.set_ylabel('Hourly Revenue ($K)', color='#1976D2', weight='bold')
ax1.set_xticks(hours)
ax1.set_xticklabels([f"{h:02d}" for h in hours], rotation=45)

ax2 = ax1.twinx()
ax2.plot(hours, tx, color='#D32F2F', marker='o', linewidth=2, label='Transaction Count')
ax2.set_ylabel('Transaction Count', color='#D32F2F', weight='bold')
ax2.grid(False)

# Highlight Evening Rush Peak
ax1.axvspan(16.5, 20.5, color='#FFCDD2', alpha=0.35, label='Evening Peak Rush (5-8 PM)')
ax1.axvspan(10.5, 14.5, color='#BBDEFB', alpha=0.35, label='Mid-Day Peak (11 AM-2 PM)')

plt.title('Activity 1: Apache Hive Sales Analysis - Hourly Revenue & Traffic Windows', weight='bold', pad=12)
fig.legend(loc='upper left', bbox_to_anchor=(0.14, 0.90), fontsize=8.5)
plt.tight_layout()
plt.savefig(os.path.join(IMG_DIR, "hive_hourly_sales.png"), bbox_inches='tight')
plt.close()
print("Generated: hive_hourly_sales.png")

# 3. ACTIVITY 2: KAFKA CONGESTION TELEMETRY & ALERTS
fig, ax = plt.subplots(figsize=(9, 4), dpi=300)
junctions = ['JN_01 (Downtown)', 'JN_02 (Outer Ring)', 'JN_03 (Airport Fly)', 'JN_04 (Harbor)', 'JN_05 (Univ Gate)']
speeds = [48.2, 18.7, 10.8, 16.0, 10.3]
counts = [34, 111, 92, 97, 134]
colors = ['#4CAF50' if s >= 30 else ('#FFA726' if s >= 20 else '#E53935') for s in speeds]

scatter = ax.scatter(counts, speeds, c=colors, s=[c*4 for c in counts], alpha=0.85, edgecolors='black', linewidth=1.2)
for i, txt in enumerate(junctions):
    ax.annotate(txt, (counts[i]+2, speeds[i]), fontsize=8.5, weight='bold')

ax.axhline(20, color='red', linestyle='--', linewidth=1.5, label='Critical Speed Threshold (< 20 km/h)')
ax.axvline(80, color='darkred', linestyle=':', linewidth=1.5, label='Critical Volume Threshold (>= 80 veh/min)')
ax.set_xlabel('Vehicle Count (vehicles per minute)', weight='bold')
ax.set_ylabel('Average Speed (km/h)', weight='bold')
ax.set_title('Activity 2: Kafka Traffic Telemetry - Congestion Alert Classification', weight='bold')
ax.legend(loc='upper right', fontsize=8.5)
plt.tight_layout()
plt.savefig(os.path.join(IMG_DIR, "kafka_traffic_alerts.png"), bbox_inches='tight')
plt.close()
print("Generated: kafka_traffic_alerts.png")

# 4. ACTIVITY 3: FLINK PATIENT VITALS MONITORING
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 4.5), sharex=True, dpi=300)
time_ticks = [f"14:00:{i:02d}" for i in range(0, 60, 4)]
# Synthetic patient heart rate with an acute tachycardia surge
hr_series = [72, 75, 78, 82, 85, 128, 134, 130, 92, 84, 76, 73, 71, 70, 72]
# SpO2 series with a momentary dip
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
plt.savefig(os.path.join(IMG_DIR, "flink_vitals_stream.png"), bbox_inches='tight')
plt.close()
print("Generated: flink_vitals_stream.png")

# 5. ACTIVITY 4: MONGODB BENCHMARK (COLLSCAN vs IXSCAN)
fig, ax = plt.subplots(figsize=(7, 4), dpi=300)
categories = ['Full Collection Scan\n(COLLSCAN)', 'Compound B-Tree Index\n(IXSCAN -> FETCH)']
docs_examined = [5004, 484]
bar_colors = ['#E53935', '#43A047']

bars = ax.bar(categories, docs_examined, color=bar_colors, width=0.45, edgecolor='black', lw=1.2)
ax.set_ylabel('Total Documents Examined from Disk', weight='bold')
ax.set_title('Activity 4: MongoDB Query Performance Benchmark (explain())', weight='bold')

for bar in bars:
    yval = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2.0, yval + 100, f"{int(yval):,} docs", ha='center', va='bottom', weight='bold')

ax.text(0.5, 3000, "90.3% Document Scan Reduction\nwith { category: 1, price: -1 }", 
        ha='center', va='center', bbox=dict(boxstyle='round,pad=0.6', facecolor='#FFF9C4', edgecolor='#FBC02D', lw=1.5), fontsize=10, weight='bold')

plt.ylim(0, 5800)
plt.tight_layout()
plt.savefig(os.path.join(IMG_DIR, "mongodb_benchmark.png"), bbox_inches='tight')
plt.close()
print("Generated: mongodb_benchmark.png")

# 6. ACTIVITY 5: CLOUD LOG ANALYTICS ARCHITECTURE & COST COMPARISON
fig, ax = plt.subplots(figsize=(8, 4), dpi=300)
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
plt.savefig(os.path.join(IMG_DIR, "cloud_cost_comparison.png"), bbox_inches='tight')
plt.close()
print("Generated: cloud_cost_comparison.png")

# 7. CLOUD WORKFLOW DIAGRAM
fig, ax = plt.subplots(figsize=(9, 4), dpi=300)
ax.axis('off')
steps = [
    ("1. Ingestion", "Kinesis Firehose\nBuffers server logs\ninto S3 Raw bucket", 0.05, 0.4, "#E3F2FD", "#1976D2"),
    ("2. Cataloging", "AWS Glue Crawler\nInfers schema & SerDe\nRegisters partitions", 0.28, 0.4, "#FFF3E0", "#FB8C00"),
    ("3. Transformation", "Parquet Conversion\nSnappy Columnar Format\n75% storage savings", 0.51, 0.4, "#E8F5E9", "#43A047"),
    ("4. Query & BI", "Amazon Athena\nPresto SQL Engine\nQuickSight Dashboards", 0.74, 0.4, "#F3E5F5", "#8E24AA")
]
for title, desc, x, y, bg, border in steps:
    ax.text(x + 0.10, y + 0.35, title, ha='center', va='center', weight='bold', color=border, fontsize=10.5)
    bbox_props = dict(boxstyle='round,pad=0.7', facecolor=bg, edgecolor=border, linewidth=1.5)
    ax.text(x + 0.10, y + 0.15, desc, ha='center', va='center', bbox=bbox_props, fontsize=9, color='#212121')

for x in [0.22, 0.45, 0.68]:
    ax.annotate("", xy=(x + 0.05, 0.55), xytext=(x - 0.02, 0.55), arrowprops=dict(arrowstyle="->", lw=1.8, color="#424242"))

plt.title("Cloud-Based Serverless Log Analytics Workflow (AWS)", fontsize=12, weight='bold', pad=12)
plt.tight_layout()
plt.savefig(os.path.join(IMG_DIR, "cloud_log_workflow.png"), bbox_inches='tight')
plt.close()
print("Generated: cloud_log_workflow.png")
