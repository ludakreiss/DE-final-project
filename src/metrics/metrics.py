import os
import time
from datetime import datetime, timezone
import duckdb
import pandas as pd
from src.constants import CLEANED_PATH, ARTIFACT_DIR, DB_PATH
from src.optimization import suggest_optimizations



def safe_read_parquet(path: str, retries: int = 3, sleep_s: float = 0.2) -> pd.DataFrame:
    '''
    safely transforms a parquet file into a pandas Dataframe 
    with retries in the case there is no parquet file
    
    :param path: path of parquet file
    :type path: str
    :param retries: # of attempts reading parquet file
    :type retries: int
    :param sleep_s: pause in between retries
    :type sleep_s: float
    :return: Dataframe of the parquet file
    :rtype: DataFrame
    '''
    for _ in range(retries):
        try:
            if not os.path.exists(path):
                return pd.DataFrame()
            return pd.read_parquet(path)
        except Exception:
            time.sleep(sleep_s)
    return pd.DataFrame()


def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    '''
    making sure that the Dataframe has the required columns
    
    :param df: Dataframe 
    :type df: pd.DataFrame
    :param cols: required columns
    :type cols: list[str]
    :return: Dataframe with the necessary columns
    :rtype: DataFrame
    '''
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def atomic_write_duckdb(tables: dict[str, pd.DataFrame], db_path: str) -> None:
    '''
    write all of the tables into a temp DuckDB and atomically swap it into place.
    This is done by having a temporary DuckDB file which is then replaced by a new 
    DuckDB file after completing a new round of table creation
    
    :param tables: tables of the DuckDB
    :type tables: dict[str, pd.DataFrame]
    :param db_path: path of the DuckDB
    :type db_path: str
    '''
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    tmp_db = db_path + ".tmp"
    if os.path.exists(tmp_db):
        os.remove(tmp_db)

    con = duckdb.connect(tmp_db)
    try:
        for name, df in tables.items():
            con.register("df_tmp", df)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df_tmp")
            con.unregister("df_tmp")
    finally:
        con.close()
    os.replace(tmp_db, db_path)


def prepare_df(raw: pd.DataFrame) -> pd.DataFrame:
    '''
    Chooses the specific columns to have from the cleaned 
    parquet file and include it in the new Dataframe
    
    :param raw: Dataframe of parquet file
    :type raw: pd.DataFrame
    :return: Dataframe of specfically-chosen columns of the parquet fiel
    :rtype: DataFrame
    '''
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

    # in the case that there are still N/A values in any of these columns
    df["mbytes_scanned"] = pd.to_numeric(df["mbytes_scanned"], errors="coerce").fillna(0) 
    df["execution_duration_ms"] = pd.to_numeric(df["execution_duration_ms"], errors="coerce").fillna(0) 
    df["queue_duration_ms"] = pd.to_numeric(df["queue_duration_ms"], errors="coerce").fillna(0)
    df["compile_duration_ms"] = pd.to_numeric(df["compile_duration_ms"], errors="coerce").fillna(0)
    df["num_joins"] = pd.to_numeric(df["num_joins"], errors="coerce").fillna(0)
    df["num_perm_tables"] = pd.to_numeric(df["num_perm_tables"], errors="coerce").fillna(0)

    # this is from the fact that the first 10 chars of a fingerprint
    # represent significant information 
    df["fingerprint"] = df["feature_fingerprint"].str.slice(0, 10)

    df = df.sort_values("timestamp")
    df["is_redundant"] = df["fingerprint"].duplicated(keep="first")

    return df


def build_fact(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    '''
    The fact table consists of original queries from the parquet file 
    in addition to new information relating to said queries. This table is 
    used for when the user would like to filter based on time, query type, or fingerprint.
    
    :param df: queries
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: fact table 
    :rtype: DataFrame
    '''
    total_time_ms = df["queue_duration_ms"] + df["compile_duration_ms"] + df["execution_duration_ms"]
    charged_seconds = (total_time_ms / 1000.0).clip(lower=60.0)
    cost = 128 * 0.42788424 * charged_seconds

    # from the Dataframe being passed
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

    # new information
    fact["total_time_ms"] = total_time_ms
    fact["charged_seconds"] = charged_seconds
    fact["cost"] = cost
    fact["computed_at"] = computed_at
    return fact


def build_kpis(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    '''
    kpis for dashboard for when no filtering is applied
    
    :param df: queries
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: kpi values
    :rtype: DataFrame
    '''
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
    '''
    compute per–query-type counts, redundancy, and scanned data
    
    :param df: queries 
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: aggregated metrics by query type
    :rtype: DataFrame
    '''
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
    '''
    build hourly query and fingerprint counts
    
    :param df: queries
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: hourly aggregated metrics
    :rtype: DataFrame
    '''
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
    """
    compute fingerprint-level query statistics

    :param df: queries
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: fingerprint-level frequency, latency, and scan metrics
    :rtype: pd.DataFrame
    """
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
    '''
    build optimization action recommendations for slow queries

    :param df: queries
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: optimization recommendations per query
    :rtype: pd.DataFrame
    '''
    actions = suggest_optimizations(df)
    actions = ensure_cols(actions, ["timestamp", "query_id", "fingerprint", "suggested_action"])
    actions.insert(0, "computed_at", computed_at)
    return actions

# NOT USED!!!!
def build_top_joins(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    '''
    observe the top  10 queries that have high rate of joins per second
    
    :param df: queries
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: queries with high rate of joins/sec in descending order
    :rtype: DataFrame
    '''
    top = df.loc[:, [
        "timestamp",
        "query_id",
        "fingerprint",
        "num_joins",
        "execution_duration_ms",
    ]].copy()

    # division by zero
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


# NOT USED!!!!
def build_top_perm_tables(df: pd.DataFrame, computed_at: datetime) -> pd.DataFrame:
    '''
    observe the top 10 queries that have high rate of tables accessed per second
    
    :param df: queries
    :type df: pd.DataFrame
    :param computed_at: time the queries were processed
    :type computed_at: datetime
    :return: queries with high rate of tables accsessed/sec in descending order
    :rtype: DataFrame
    '''
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

def compute_and_write_metrics() -> None:
    '''
    takes in a cleaned parquet file and computes metrics from it
    '''
    raw = safe_read_parquet(CLEANED_PATH)
    if raw.empty:
        return

    df = prepare_df(raw)
    computed_at = datetime.now(timezone.utc)


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
    print("writing duckdb snapshot to:", ARTIFACT_DIR)
    while True:
        compute_and_write_metrics()
        time.sleep(60) # to match with the kafka streaming time as well


if __name__ == "__main__":
    main()
