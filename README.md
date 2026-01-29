#Redset Streaming Project

This project implements a streaming pipeline for the Redset dataset.
It replays real Redshift query workloads in accelerated time and sends
cleaned query events to Kafka for further analysis.

---

#Project Implementation

The pipeline consists of three main steps:

1. Ingestion
   - Load parquet data using DuckDB
   - Convert to pandas DataFrame

2. Cleaning
   - Remove invalid, aborted, cached, and system-only queries
   - Remove zero-work queries
   - Tag queries as read or write

3. Streaming
   - Select a 24-hour window from the dataset
   - Group queries by hour
   - Replay one hour of data in one minute
   - Stream query events to Kafka

---

#Requirements

- Python 3.9+
- DuckDB
- Pandas
- Kafka (running locally)
- confluent-kafka

Install dependencies:
```bash
pip install -r requirements.txt
```
---

#Dataset setup
Due to GitHub file size limits, the dataset is not included in the repository.
Steps to run locally:
Download sample_0.01.parquet
Place it at:
data/raw/sample_0.01.parquet
