import pandas as pd

SUGGESTED_OPTIMIZATIONS = {
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


def suggest_optimizations(df: pd.DataFrame) -> pd.DataFrame:
    '''
    recommendations for optimizing slow queries passed to the function.
    
    :param df: queries
    :type df: pd.DataFrame
    :return: queries with their optimization recommendations
    :rtype: DataFrame
    '''
    # condition: execution time > 5 seconds
    slow = df["execution_duration_ms"] > 5000

    slow_queries = (
        df.loc[slow, ["timestamp", "query_id", "fingerprint", "query_type"]]
          .drop_duplicates(subset=["timestamp", "query_id", "fingerprint"])
          .copy()
    )

    slow_queries["query_type"] = slow_queries["query_type"].astype("string")

    # given a slow query, recommend its respective optimization
    slow_queries["suggested_action"] = slow_queries["query_type"].apply(
        lambda qt: ", ".join(SUGGESTED_OPTIMIZATIONS.get(qt.lower(), [])))
    return slow_queries