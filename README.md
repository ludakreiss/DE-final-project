# Table Flippers: A Redset Streaming Project
<p align="center">
  <img src="/ui/logo.png" alt="Fries" title="Table Flippers GO!" width="50%">
</p>

This project implements a streaming pipeline for the [Redset dataset](https://github.com/amazon-science/redset/).
It replays real Redshift query workloads in accelerated time and sends cleaned query events to Kafka for further analysis.
These events are then consumed by a metrics engine that builds fact and metric tables, stores them in DuckDB, and powers a live dashboard for performance analysis and optimization insights.

---

# Project Implementation

The pipeline consists of the following main steps:

### 1. Ingestion
- Load Parquet data using DuckDB  
- Convert the data into a pandas DataFrame  

### 2. Cleaning
- Remove invalid, aborted, cached, and system-only queries  
- Remove zero-work queries  
- Tag queries as read or write  

### 3. Streaming
- Select a 24-hour window from the dataset  
- Group queries by hour  
- Replay one hour of data in one minute  
- Stream query events into Kafka  

### 4. Metrics Processing
- Consume streaming query events from Kafka  
- Group events into one-minute batches representing one hour of workload  
- Build a fact table with one row per query  
- Derive metric tables (KPIs, fingerprints, scans, joins, optimization signals)  

### 5. Metrics Storage
- Continuously read cleaned Parquet data  
- Select dashboard-relevant columns  
- Derive additional columns (cost, redundancy, total execution time)  
- Write fact and metric tables into a single DuckDB file  
- Replace the database atomically on each run to ensure consistency  

### 6. Visualization
- Connect the dashboard directly to DuckDB  
- Apply filters on fact or pre-aggregated metric tables  
- Avoid heavy computations in the UI layer  
- Visualize performance KPIs, fingerprint patterns, and optimization opportunities  
- Provide unified filtering across all dashboard views  
---

# Project Tree
```
DE-final-project
    ├── LICENSE
    ├── README.md
    ├── artifacts
    ├── data
    │   ├── consumed
    │   │   └── cleaned_consumed.parquet
    │   └── raw
    │       └── sample_0.01.parquet
    ├── pyproject.toml
    ├── requirements.txt
    ├── src
    │   ├── __pycache__
    │   │   ├── cleaning.cpython-313.pyc
    │   │   ├── ingestion.cpython-313.pyc
    │   │   └── streaming.cpython-313.pyc
    │   ├── constants.py
    │   ├── main.py
    │   ├── metrics
    │   │   ├── metrics.py
    │   │   └── metrics_consumer.py
    │   ├── optimization.py
    │   ├── pipeline
    │   │   ├── cleaning.py
    │   │   ├── ingestion.py
    │   │   └── streaming.py
    │   └── ui
    │       ├── css.cpython-313.pyc
    │       ├── dashboard.py
    │       ├── data_access.py
    │       ├── db_helpers.py
    │       ├── logo.png
    │       ├── styles.py
    │       └── ui_config.py
    └── uv.lock
```
# Requirements

* Python 3.9+
* DuckDB
* Pandas
* Kafka (running locally)
* confluent-kafka

## Install dependencies:
```bash
pip install -r requirements.txt
```
---

# Dataset setup
Due to GitHub file size limits, the dataset is not included in the repository.
### Steps to run locally:

#### Manually
1. Download `sample_0.01.parquet`
2. Place it in `data/raw/`

#### Other ways

* Using `PowerShell`
```
 Invoke-WebRequest https://s3.amazonaws.com/redshift-downloads/redset/serverless/sample_0.01.parquet `
  -OutFile data/raw/sample_0.01.parquet
```
* Using `curl`
```
curl -o data/raw/sample_0.01.parquet \
  https://s3.amazonaws.com/redshift-downloads/redset/serverless/sample_0.01.parquet
```

* Using `wegt`
```
wget -O data/raw/sample_0.01.parquet \
  https://s3.amazonaws.com/redshift-downloads/redset/serverless/sample_0.01.parquet
```
---

# References
Below are some sources that were used in order to implement this project
## Repositories 
* [Snowset workload traces and analysis](https://github.com/resource-disaggregation/snowset)
* [Cloud Analytics Benchmark (CAB)](https://github.com/awslabs/cloud-analytics-benchmark)
* [Trino distributed query engine](https://github.com/trinodb/trino)
* [Apache Flink training and streaming examples](https://github.com/apache/flink-training)
* [Netflix Atlas monitoring system](https://github.com/Netflix/atlas)
* [Bytewax streaming and replay framework](https://github.com/bytewax/bytewax)
* [AWS cost anomaly detection examples](https://github.com/aws-samples/aws-cost-anomaly-detection)
* [DuckDB query execution engine](https://github.com/duckdb/duckdb)
* [BigQuery cost estimation utilities](https://github.com/doitintl/bigquery-cost-estimator)
* [Amazon Redshift utilities and performance tools](https://github.com/awslabs/amazon-redshift-utils)
* [PostgreSQL database system](https://github.com/postgres/postgres)
* [Facebook Prophet time-series analysis library](https://github.com/facebook/prophet)
* [Statsmodels statistical modeling library](https://github.com/statsmodels/statsmodels)
* [Twitter AnomalyDetection toolkit](https://github.com/twitter/AnomalyDetection)
* [Prometheus monitoring and alerting system](https://github.com/prometheus/prometheus)

## Aditional links
* [Amazon Redshift pricing](https://aws.amazon.com/redshift/pricing/)
* [Amazon Redshift monitoring guide (2024)](https://www.eyer.ai/blog/amazon-redshift-monitoring-guide-2024/)
* [Cost-efficient and elastic analytics with Amazon Redshift Serverless (research paper)](https://arxiv.org/pdf/2403.02286)
* [Amazon Redshift Serverless capacity management documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-capacity.html)
* [Optimizing Amazon Redshift performance](https://www.chaosgenius.io/blog/optimizing-redshift-performance/)
* [Amazon Redshift query optimization techniques](https://www.eyer.ai/blog/12-amazon-redshift-query-optimization-techniques/)
