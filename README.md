# 🎛 Industrial Sensor Medallion Lakehouse (Microsoft Fabric)

https://img.shields.io/badge/Python-3.10+-blue
https://img.shields.io/badge/Microsoft%20Fabric-Lakehouse-0078D4
https://img.shields.io/badge/Spark-PySpark-FC4D0D
https://img.shields.io/badge/Data-Industrial%20Telemetry-grey
https://img.shields.io/badge/Architecture-Medallion-green
https://img.shields.io/badge/PowerBI-DirectLake-F2C811
https://img.shields.io/badge/Status-Completed-brightgreen

End-to-end Medallion Architecture (Bronze → Silver → Gold) implementation for industrial telemetry using Microsoft Fabric, featuring incremental ingestion, Spark-based data validation, quarantine handling, and DirectLake KPI reporting.

---

## 📝 Executive Summary

This project simulates an industrial sensor telemetry platform and implements a production-style Lakehouse architecture:

- Incremental, idempotent ingestion of micro-batch telemetry files  
- Spark-based data contract validation with quarantine logic  
- Business-ready aggregates in a Gold layer  
- DirectLake semantic model powering KPI dashboards  
- Governance metric: Dirty Rate %

The system demonstrates scalable tabular analytics without GPU dependency, executed entirely on CPU using Spark in Microsoft Fabric.

---

## 🏗 Architecture Overview

### 🥉 Bronze Layer — Incremental Ingestion

**Purpose:** Load raw telemetry micro-batches safely and idempotently.  
**Source:** CSV micro-batches  
**Destination:** `dbo.bronze_telemetry`  
**Engineering Feature:** `dbo.bronze_ingestion_log`

The ingestion log ensures that if the notebook reruns, previously ingested files are not duplicated.

### 🥈 Silver Layer — Governance & Validation

**Purpose:** Enforce data contracts and isolate invalid records.

**Outputs:**
- `dbo.silver_telemetry` — Clean records  
- `dbo.dirty_telemetry` — Quarantined records (with `dirty_reason` column)

**Validation examples:**
- Negative pressure values  
- Out-of-range temperature readings  
- Physically impossible sensor combinations  

Invalid records are quarantined for review instead of failing the pipeline.

### 🥇 Gold Layer — Business Aggregates

**Purpose:** Create analytics-ready tables.  
**Output:** `dbo.gold_device_5min`

**Aggregations include:**
- 5-minute event counts  
- Average temperature, pressure, humidity  
- Vibration flags  
- Average energy consumption  

This table powers the BI layer.

---

## 📊 KPI Dashboard (DirectLake)

A DirectLake semantic model is built from the Gold layer.

### Example Measures

```DAX
Total Events = SUM(gold_device_5min[events_in_window])
High Vibration Windows = SUM(gold_device_5min[high_vibration_flag])
Average Energy (kW) = AVERAGE(gold_device_5min[energy_avg])

Clean Rows = COUNTROWS(silver_telemetry)
Dirty Rows = COUNTROWS(dirty_telemetry)

Dirty Rate % =
    DIVIDE([Dirty Rows], [Dirty Rows] + [Clean Rows])
```

### Dashboard Features

- Total Events (5-min windows)  
- High Vibration Windows  
- Average Energy (kW)  
- Dirty Rate %  
- Device slicer  
- Energy trend over time  

---

## 🔁 Telemetry Simulation

A local Python script generates continuous micro-batches of telemetry data.

### Fields Generated

- `device_id`  
- `event_time`  
- `temperature_c`  
- `pressure_kpa`  
- `humidity_pct`  
- `vibration_rms`  
- `energy_kw`  

Bad records are intentionally injected to test governance logic.

### ▶️ Run Locally

```bash
.\.venv\Scripts\python.exe src\01_generate_telemetry_stream.py
```

Stop with: **Ctrl + C**

---

# ▶️ How to Run Locally

## 1. Create Virtual Environment

```bash
python -m venv .venv
```

## 2. Activate Environment

### Windows:
```bash
.\.venv\Scripts\activate
```

### Mac/Linux:
```bash
source .venv/bin/activate
```

## 3. Install Dependencies
```bash
pip install -r requirements.txt
```

## 4. Run Telemetry Simulation

```bash
python src/01_generate_telemetry_stream.py
```

Stop with `Ctrl + C`.

This continuously generates micro-batch telemetry files.

---

# 🧠 Design Decisions

## Why 5-Minute Windows?

Industrial telemetry is typically aggregated in fixed time intervals to:

- Smooth sensor noise  
- Enable operational monitoring  
- Support shift-level reporting  
- Reduce computational overhead  

Five-minute windows balance granularity and performance.

---

## Why Quarantine Instead of Drop?

Invalid records are routed to a **quarantine table**, not discarded, to:

- Preserve auditability  
- Enable root cause analysis  
- Support data governance reporting  
- Avoid silent data loss  

---

## Why Medallion Architecture?

The Bronze → Silver → Gold structure ensures:

- Raw data preservation  
- Clear transformation stages  
- Reproducibility  
- Separation of engineering and business logic  

---

# ⚠️ Limitations & Future Improvements

### Current Limitations

- No orchestration layer (e.g., scheduled Fabric pipelines)  
- No automated testing framework  
- No schema evolution handling  
- No streaming ingestion (micro-batch only)  
- No monitoring/alerting system  

### Future Enhancements

- Add Fabric Data Pipelines for scheduled execution  
- Implement Delta table optimization  
- Add unit tests for validation logic  
- Implement real-time streaming ingestion  
- Integrate alerting for abnormal KPIs  

---

## 🛠 Fabric Implementation Steps

1. Create Fabric Workspace  
2. Create Lakehouse  
3. Upload telemetry CSV files to: `Files/bronze/telemetry/`  
4. Execute notebooks in the following order:
   - `01_bronze_ingestion`
   - `02_silver_validation`
   - `03_gold_aggregates`
5. Create DirectLake semantic model  
6. Build KPI dashboard  

---

## 📂 Repository Structure

```
Industrial-Sensor-Medallion-Lakehouse/
│
├── src/
│   ├── 01_generate_telemetry_stream.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_validation.py
│   └── 03_gold_aggregates.py
│
├── README.md
├── .gitignore
└── requirements.txt
```

---

## 🚀 Engineering Highlights

- Idempotent ingestion via ingestion log  
- Explicit DAX measures (no implicit aggregations)  
- Data contract enforcement with quarantine routing  
- Medallion architecture discipline  
- DirectLake BI delivery  
- Governance KPI (Dirty Rate %)  

---

## 🖥 Execution Environment

- Microsoft Fabric (Trial / Pro)  
- Spark (PySpark)  
- DirectLake Semantic Model  
- Power BI Service  
- Python 3.x (local telemetry simulation)

All processing executed on CPU with Spark — no GPU dependency.

---

## 🎯 What This Project Demonstrates

- Production-style Lakehouse design  
- Incremental micro-batch ingestion  
- Data governance implementation  
- Analytics engineering best practices  
- Business-facing KPI delivery  
- Industrial telemetry modeling  

---

## 👤 Author

**Nforche Divine Ako**  
Industrial Data & Analytics Engineering