import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_PATH = os.path.join(BASE_DIR, "data", "raw", "sample_0.01.parquet")
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "redset-group"
KAFKA_GROUP_ID = "redset-group"
CLEANED_PATH = os.path.join(BASE_DIR, "data", "consumed", "cleaned_consumed.parquet")
METRICS_DIR = os.path.join(BASE_DIR, "src", "metrics")
ARTIFACT_DIR =  os.path.join(BASE_DIR, "artifacts")
DB_PATH = os.path.join(ARTIFACT_DIR, "metrics.duckdb")
COST = 0.4278
NO_OF_RPUS = 128
