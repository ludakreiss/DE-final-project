import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data quality and semantic cleaning to Redset queries.
    This function removes invalid, aborted, cached, and non-meaningful queries.
    """

    # Keep only rows with valid timestamps and non-negative metrics
    df = df[
        df["arrival_timestamp"].notna() &
        (df["execution_duration_ms"] >= 0) &
        (df["mbytes_scanned"] >= 0)
    ]

    # Remove queries that were aborted or served from cache
    df = df[
        df["was_aborted"].eq(False) &
        df["was_cached"].eq(False)
    ]

    # Remove queries that did no actual work
    df = df[
        ~(
            (df["execution_duration_ms"] == 0) &
            (df["mbytes_scanned"] == 0) &
            (df["num_scans"].fillna(0) == 0)
        )
    ]

    # Remove queries that only accessed system tables and no user tables
    df = df[
        ~(
            (df["num_system_tables_accessed"] > 0) &
            (df["num_permanent_tables_accessed"] == 0)
        )
    ]

    # Classify queries as read or write based on query type
    df["query_class"] = df["query_type"].apply(
        lambda x: "write" if x in ("INSERT", "CTAS", "COPY") else "read"
    )

    # Reset index after filtering
    return df.reset_index(drop=True)