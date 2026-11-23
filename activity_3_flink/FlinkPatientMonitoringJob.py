"""
==============================================================================
PRODUCTION PYFLINK DATASTREAM APPLICATION: CLINICAL PATIENT VITALS MONITORING
==============================================================================
Framework: Apache Flink (PyFlink 1.17+)
Deployment Mode: Standalone / Flink on Kubernetes / YARN
"""

import json
from pyflink.common import WatermarkStrategy, Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

# 1. Custom JSON Deserialization / Map Function
def parse_vitals_telemetry(raw_json: str):
    data = json.loads(raw_json)
    return (
        data["patient_id"],
        data["patient_name"],
        data["ward"],
        int(data["heart_rate_bpm"]),
        float(data["spo2_pct"]),
        float(data["temperature_c"]),
        int(data["timestamp_epoch"]) * 1000  # Milliseconds for Flink EventTime
    )

# 2. Keyed Process Function for Immediate Vital Anomaly Detection
class VitalsThresholdAlertFunction(KeyedProcessFunction):
    def process_element(self, value, ctx):
        patient_id, name, ward, hr, spo2, temp, ts = value
        alerts = []

        # Tachycardia / Bradycardia
        if hr > 120:
            alerts.append(f"CRITICAL TACHYCARDIA ({hr} BPM)")
        elif hr < 45:
            alerts.append(f"CRITICAL BRADYCARDIA ({hr} BPM)")

        # Hypoxemia
        if spo2 < 90.0:
            alerts.append(f"SEVERE HYPOXIA (SpO2 {spo2}%)")
        elif spo2 < 92.0:
            alerts.append(f"LOW OXYGEN SATURATION (SpO2 {spo2}%)")

        # Temperature
        if temp > 38.5:
            alerts.append(f"HIGH FEVER ({temp}°C)")

        if alerts:
            alert_msg = {
                "alert_type": "IMMEDIATE_ANOMALY",
                "patient_id": patient_id,
                "patient_name": name,
                "ward": ward,
                "anomalies": alerts,
                "timestamp": ts
            }
            # Emit to side output or main stream
            yield json.dumps(alert_msg)

# 3. Process Window Function for Tumbling Window Aggregation (e.g. 30s)
class VitalsWindowSummaryFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = 0
        total_hr = 0
        min_spo2 = 100.0
        max_temp = 0.0
        abnormal_count = 0
        patient_name = ""
        ward = ""

        for el in elements:
            _, pname, w, hr, spo2, temp, _ = el
            patient_name = pname
            ward = w
            count += 1
            total_hr += hr
            min_spo2 = min(min_spo2, spo2)
            max_temp = max(max_temp, temp)
            if hr > 110 or hr < 50 or spo2 < 92.0 or temp > 38.5:
                abnormal_count += 1

        summary = {
            "summary_type": "WINDOW_ROLLUP",
            "patient_id": key,
            "patient_name": patient_name,
            "ward": ward,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "readings_count": count,
            "abnormal_events": abnormal_count,
            "avg_heart_rate": round(total_hr / count, 1) if count else 0,
            "min_spo2": min_spo2,
            "max_temperature": max_temp,
            "status": "CRITICAL" if abnormal_count >= 2 else ("WARNING" if abnormal_count == 1 else "STABLE")
        }
        return [json.dumps(summary)]

def main():
    # Initialize Flink Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds for exactly-once

    # Configure Kafka Consumer Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("patient-vitals-raw") \
        .set_group_id("flink-clinical-monitor-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Watermark Strategy with Bounded Out-of-Orderness (3 seconds tolerance)
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Time.seconds(3)) \
        .with_timestamp_assigner(lambda event, timestamp: event[6])

    # Build Pipeline
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaVitalsSource") \
        .map(parse_vitals_telemetry, output_type=Types.ROW([
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.LONG()
        ])) \
        .assign_timestamps_and_watermarks(watermark_strategy)

    # Branch A: Immediate Alerts
    immediate_alerts = stream.key_by(lambda x: x[0]).process(VitalsThresholdAlertFunction())
    immediate_alerts.print()

    # Branch B: 30-Second Windowed Aggregate Summary
    window_summaries = stream.key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
        .process(VitalsWindowSummaryFunction())
    window_summaries.print()

    # Execute Job
    env.execute("Flink Patient Telemetry Real-Time Monitoring Job")

if __name__ == "__main__":
    main()
