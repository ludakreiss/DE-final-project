import duckdb
import pandas as pd

def load_raw_data(parquet_path: str) -> pd.DataFrame:
    """
    Load raw Redset data from a parquet file using DuckDB.
    This function performs no cleaning, only loading.
    """
    con = duckdb.connect(database=":memory:")

    df = con.execute(f"""
        SELECT *
        FROM read_parquet('{parquet_path}')
    """).df()

    df["arrival_timestamp"] = pd.to_datetime(
        df["arrival_timestamp"], errors="coerce"
    )

    return df
