import pandas as pd
from db_helpers import query_df, placeholders
from ui_config import ACTIONS_LIMIT


def load_filter_domains():
    '''
    gets available query_types, fingerprints, and min/max timestamp for the sidebar filtering
    '''
    domains = query_df("""
        SELECT
            MIN(timestamp) AS min_ts,
            MAX(timestamp) AS max_ts
        FROM fact
    """)
    min_ts = domains["min_ts"].iloc[0]
    max_ts = domains["max_ts"].iloc[0]

    qt = query_df("SELECT DISTINCT query_type FROM fact ORDER BY query_type")
    fps = query_df("SELECT DISTINCT fingerprint FROM fact ORDER BY fingerprint")

    return (
        qt["query_type"].dropna().astype(str).tolist(),
        fps["fingerprint"].dropna().astype(str).tolist(),
        min_ts,
        max_ts,
    )


def filters_active(qt_filtered, qt_not_filtered, filtered_fp, time_range, full_range):
    '''
    Verify if there are filters that change the original state
    
    :param qt_filtered: query types that are left after filtering
    :param qt_not_filtered: complete set of query types
    :param filtered_fp: fingerprint types that are left after filtering
    :param time_range: specfied time-range
    :param full_range: time-range of the full dataset
    '''
    filters_applied = False
    if set(qt_filtered) != set(qt_not_filtered):
        filters_applied = True
    elif filtered_fp != "All":
        filters_applied = True
    elif time_range != full_range:
        filters_applied = True
    return filters_applied


def load_fact_filtered(qt_filtered, filtered_fp, time_range):
    '''
    Gets only rows needed from fact table
    
    :param qt_filtered: queries that are left after filtering
    :param filtered_fp: fingerprints that are left after filtering
    :param time_range: specified time-range
    '''
    start_ts, end_ts = time_range
    type_list = list(qt_filtered) if qt_filtered else []

    sql = f"""
        SELECT
            timestamp,
            query_id,
            query_type,
            fingerprint,
            mbytes_scanned,
            execution_duration_ms,
            queue_duration_ms,
            compile_duration_ms,
            num_joins,
            num_perm_tables,
            is_redundant,
            charged_seconds,
            cost
        FROM fact
        WHERE timestamp BETWEEN ? AND ?
          AND query_type IN ({placeholders(len(type_list))})
          AND (? = 'All' OR fingerprint = ?)
    """
    # Specify parameters for call tables
    params = [start_ts, end_ts, *type_list, filtered_fp, filtered_fp]
    df = query_df(sql, params)

    # Match of DuckDB tables to dashboard expectations
    df["duration_sec"] = pd.to_numeric(df["execution_duration_ms"], errors="coerce").fillna(0) / 1000.0
    df["mb_scanned"] = pd.to_numeric(df["mbytes_scanned"], errors="coerce").fillna(0)
    df["is_redundant"] = df["is_redundant"].fillna(False).astype(bool)

    return df

# Load optimization suggestions linked to specific query patterns
def load_actions_filtered(qt_filtered, filtered_fp, time_range):
    start_ts, end_ts = time_range
    type_list = list(qt_filtered) if qt_filtered else []

    sql = f"""
        SELECT
            a.timestamp,
            a.query_id,
            a.fingerprint,
            a.suggested_action
        FROM optimization_actions a
        JOIN fact f
          ON f.query_id = a.query_id
        WHERE a.timestamp BETWEEN ? AND ?
          AND f.query_type IN ({placeholders(len(type_list))})
          AND (? = 'All' OR a.fingerprint = ?)
        ORDER BY a.timestamp DESC
        LIMIT {ACTIONS_LIMIT}
    """
    params = [start_ts, end_ts, *type_list, filtered_fp, filtered_fp]
    return query_df(sql, params)
