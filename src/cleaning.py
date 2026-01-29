import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data quality and semantic cleaning to Redset queries.
    This function removes invalid, aborted, cached, and non-meaningful queries.
    """

    #basic validity checks
    df = df[
        df["arrival_timestamp"].notna() &
        (df["execution_duration_ms"] >= 0) &
        (df["mbytes_scanned"] >= 0)
    ]

    #remove aborted and cached queries
    df = df[
        (df["was_aborted"] == False) &
        (df["was_cached"] == False)
    ]

    #remove zero-work queries
    df = df[
        ~(
            (df["execution_duration_ms"] == 0) &
            (df["mbytes_scanned"] == 0) &
            (df["num_scans"].fillna(0) == 0)
        )
    ]

    #remove system-only queries
    df = df[
        ~(
            (df["num_system_tables_accessed"] > 0) &
            (df["num_permanent_tables_accessed"] == 0)
        )
    ]

    #tag query type
    df["query_class"] = df["query_type"].apply(
        lambda x: "write" if x in ("INSERT", "CTAS", "COPY") else "read"
    )

    return df.reset_index(drop=True)
