"""
metrics.py (DuckDB version)

Reads the continuously-updated cleaned_consumed.parquet and writes ONE DuckDB file:
  metrics/metrics.duckdb

Tables inside metrics.duckdb:
  - fact (1 row per query execution; filterable by timestamp/query_type/fingerprint)
  - kpis
  - query_type_counts
  - time_series_hour
  - fingerprint_stats
  - optimization_actions
  - top_joins
  - top_perm_tables

Writes are atomic: build a temp DB and os.replace() it in place.
"""

import os
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd


# -------------------- Paths --------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CLEANED_PATH = os.path.join(BASE_DIR, "data", "consumed", "cleaned_consumed.parquet")
METRICS_DIR = os.path.join(BASE_DIR, "metrics")
DB_PATH = os.path.join(METRICS_DIR, "metrics.duckdb")



# -------------------- Helpers --------------------
def safe_read_parquet(path: str, retries: int = 3, sleep_s: float = 0.2) -> pd.DataFrame:
    for _ in range(retries):
        try:
            if not os.path.exists(path):
                return pd.DataFrame()
            return pd.read_parquet(path)
        except Exception:
            time.sleep(sleep_s)
    return pd.DataFrame()


def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def atomic_write_duckdb(tables: dict[str, pd.DataFrame], db_path: str) -> None:
    """
    Write ALL tables into a temp DuckDB and atomically swap it into place.
    This guarantees the dashboard always sees a consistent snapshot.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    tmp_db = db_path + ".tmp"
    if os.path.exists(tmp_db):
        os.remove(tmp_db)

    con = duckdb.connect(tmp_db)
    try:
        for name, df in tables.items():
            # Ensure consistent column order/type is kept as-is from pandas
            con.register("df_tmp", df)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df_tmp")
            con.unregister("df_tmp")
    finally:
        con.close()

    os.replace(tmp_db, db_path)


# -------------------- Optimization suggestion hook --------------------
def suggest_optimizations(df: pd.DataFrame) -> pd.DataFrame:
    # condition: execution time > 5 seconds
    slow = df["execution_duration_ms"] > 5000

    out = (
        df.loc[slow, ["timestamp", "query_id", "fingerprint", "query_type"]]
          .drop_duplicates(subset=["timestamp", "query_id", "fingerprint"])
          .copy()
    )

    suggested_optimizations = {
        "select": [
            "avoid select *",
            "early filtering",
            "good dist/sort keys",
            "use materialized views",
        ],
        "copy": [
            "prefer COPY bulk loads",
            "file compression",
            "etl-specific WLM",
        ],
        "insert": [
            "batch inserts",
            "use COPY instead",
        ],
        "update": [
            "group updates",
            "key-based predicates",
        ],
        "delete": [
            "bulk deletes",
            "CTAS instead of delete",
            "reindex interleaved",
        ],
        "analyze": [
            "post-change ANALYZE",
        ],
        "ctas": [
            "explicit keys/encodings",
            "ANALYZE new table",
        ],
        "unload": [
            "optimize inner SELECT",
            "multiple compressed files",
        ],
        "other": [
            "WLM by workload",
            "enable concurrency scaling",
            "monitor table health",
        ],
    }

    out["query_type"] = out["query_type"].astype("string").str.upper()
    # Add debugging
    # for key, value in suggested_optimizations.items():
    #     print(f"Key: {key}, Type: {type(value)}, Value: {value}")
    # Or check a specific problematic qt
    # test_qt = "copy"  # Replace with actual problematic qt
    # print(f"For qt={test_qt}: {suggested_optimizations.get(test_qt)}")
    # print(f"Type: {type(suggested_optimizations.get(test_qt))}")
    out["suggested_action"] = out["query_type"].apply(
        lambda qt: ", ".join(suggested_optimizations.get(qt.lower(), [])))



    return out

# -------------------- Preparation / normalization --------------------


def prepare_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    df = ensure_cols(
        df,
        [
            "arrival_timestamp",
            "query_id",
            "query_type",
            "feature_fingerprint",
            "mbytes_scanned",
            "execution_duration_ms",
            "queue_duration_ms",
            "compile_duration_ms",
            "num_joins",
            "num_perm_tables",
        ],
    )

    df["timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors="coerce")

    df["query_type"] = df["query_type"].astype("string")
    df["feature_fingerprint"] = df["feature_fingerprint"].astype("string")

    df["mbytes_scanned"] = pd.to_numeric(df["mbytes_scanned"], errors="coerce").fillna(0)

    df["execution_duration_ms"] = pd.to_numeric(df["execution_duration_ms"], errors="coerce").fillna(0)
    df["queue_duration_ms"] = pd.to_numeric(df["queue_duration_ms"], errors="coerce").fillna(0)
    df["compile_duration_ms"] = pd.to_numeric(df["compile_duration_ms"], errors="coerce").fillna(0)

    df["num_joins"] = pd.to_numeric(df["num_joins"], errors="coerce").fillna(0)
    df["num_perm_tables"] = pd.to_numeric(df["num_perm_tables"], errors="coerce").fillna(0)

    df["fingerprint"] = df["feature_fingerprint"].str.slice(0, 10)

    df = df.sort_values("timestamp", na_position="last")
    df["is_redundant"] = df["fingerprint"].duplicated(keep="first")

    return df


# -------------------- Builders --------------------
def build_fact(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    total_time_ms = df["queue_duration_ms"] + df["compile_duration_ms"] + df["execution_duration_ms"]
    charged_seconds = (total_time_ms / 1000.0).clip(lower=60.0)
    cost = 128 * 0.42788424 * charged_seconds

    fact = df[
        [
            "timestamp",
            "query_id",
            "query_type",
            "fingerprint",
            "feature_fingerprint",
            "mbytes_scanned",
            "execution_duration_ms",
            "queue_duration_ms",
            "compile_duration_ms",
            "num_joins",
            "num_perm_tables",
            "is_redundant",
        ]
    ].copy()

    fact["total_time_ms"] = total_time_ms
    fact["charged_seconds"] = charged_seconds
    fact["cost"] = cost
    fact["computed_at"] = computed_at
    return fact


def build_kpis(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    data_max_ts = df["timestamp"].max()

    total_time_ms = df["queue_duration_ms"] + df["compile_duration_ms"] + df["execution_duration_ms"]
    charged_seconds = (total_time_ms / 1000.0).clip(lower=60.0)
    cost = 128 * 0.42788424 * charged_seconds

    total_queries = int(len(df))
    redundant_queries = int(df["is_redundant"].sum())
    total_tb_scanned = float(df["mbytes_scanned"].sum() / 1_000_000)

    avg_total_time_ms = float(total_time_ms.mean()) if total_queries else 0.0
    avg_cost_per_query = float(cost.mean()) if total_queries else 0.0
    total_cost = float(cost.sum()) if total_queries else 0.0

    fp_counts = df["fingerprint"].value_counts(dropna=False)
    avg_fingerprint_repetition = float(fp_counts.mean()) if len(fp_counts) else 0.0

    return pd.DataFrame(
        [
            {
                "computed_at": computed_at,
                "data_max_ts": data_max_ts,
                "total_queries": total_queries,
                "redundant_queries": redundant_queries,
                "total_tb_scanned": total_tb_scanned,
                "avg_total_time_ms": avg_total_time_ms,
                "avg_cost_per_query": avg_cost_per_query,
                "total_cost": total_cost,
                "avg_fingerprint_repetition": avg_fingerprint_repetition,
            }
        ]
    )


def build_query_type_counts(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    qt = (
        df.groupby("query_type", dropna=False)
        .agg(
            count=("query_id", "count"),
            redundant_count=("is_redundant", "sum"),
            total_mb_scanned=("mbytes_scanned", "sum"),
        )
        .reset_index()
    )
    qt.insert(0, "computed_at", computed_at)
    return qt


def build_time_series_hour(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    tmp = df.copy()
    tmp["hour_of_day"] = tmp["timestamp"].dt.hour

    tsh = (
        tmp.groupby("hour_of_day", dropna=False)
        .agg(
            total_queries=("query_id", "count"),
            unique_fingerprints=("fingerprint", "nunique"),
        )
        .reset_index()
    )
    tsh.insert(0, "computed_at", computed_at)
    return tsh


def build_fingerprint_stats(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    fp = (
        df.groupby("fingerprint", dropna=False)
        .agg(
            frequency=("query_id", "count"),
            avg_exec_time_sec=("execution_duration_ms", lambda s: s.mean() / 1000.0),
            total_mb=("mbytes_scanned", "sum"),
        )
        .reset_index()
    )
    fp.insert(0, "computed_at", computed_at)
    return fp


def build_optimization_actions(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    actions = suggest_optimizations(df)
    actions = ensure_cols(actions, ["timestamp", "query_id", "fingerprint", "suggested_action"])
    actions.insert(0, "computed_at", computed_at)
    return actions


def build_top_joins(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    top = df.loc[:, [
        "timestamp",
        "query_id",
        "fingerprint",
        "num_joins",
        "execution_duration_ms",
    ]].copy()

    # Avoid division by zero
    exec_sec = (top["execution_duration_ms"] / 1000.0).replace(0, pd.NA)

    top["joins_per_sec"] = top["num_joins"] / exec_sec

    top = (
        top
        .dropna(subset=["joins_per_sec"])
        .sort_values("joins_per_sec", ascending=False)
        .head(10)
    )

    top.insert(0, "computed_at", computed_at)
    return top



def build_top_perm_tables(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    top = df.loc[:, [
        "timestamp",
        "query_id",
        "fingerprint",
        "num_perm_tables",
        "execution_duration_ms",
    ]].copy()

    exec_sec = (top["execution_duration_ms"] / 1000.0).replace(0, pd.NA)

    top["perm_tables_per_sec"] = top["num_perm_tables"] / exec_sec

    top = (
        top
        .dropna(subset=["perm_tables_per_sec"])
        .sort_values("perm_tables_per_sec", ascending=False)
        .head(10)
    )

    top.insert(0, "computed_at", computed_at)
    return top



# -------------------- Orchestration --------------------
def compute_and_write_metrics() -> None:
    raw = safe_read_parquet(CLEANED_PATH)
    if raw.empty:
        return

    df = prepare_df(raw)
    computed_at = datetime.now(timezone.utc)

    # OPTIONAL: keep fact bounded to last 24h (recommended for long runs).
    # cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=24)
    # df = df[df["timestamp"] >= cutoff]

    tables = {
        "fact": build_fact(df, computed_at),
        "kpis": build_kpis(df, computed_at),
        "query_type_counts": build_query_type_counts(df, computed_at),
        "time_series_hour": build_time_series_hour(df, computed_at),
        "fingerprint_stats": build_fingerprint_stats(df, computed_at),
        "optimization_actions": build_optimization_actions(df, computed_at),
        "top_joins": build_top_joins(df, computed_at),
        "top_perm_tables": build_top_perm_tables(df, computed_at),
    }

    atomic_write_duckdb(tables, DB_PATH)


def main() -> None:
    print("metrics.py started; reading:", CLEANED_PATH)
    print("writing duckdb snapshot to:", DB_PATH)
    while True:
        compute_and_write_metrics()
        time.sleep(60)


if __name__ == "__main__":
    main()
