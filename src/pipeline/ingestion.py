import duckdb
import pandas as pd


def load_raw_data(parquet_path: str) -> pd.DataFrame:
    """
    Load raw Redset data from a parquet file using DuckDB.
    This function performs no cleaning, only loading.
    """

    # Create an in-memory DuckDB connection
    con = duckdb.connect(database=":memory:")

    # Read all records from the parquet file into a DataFrame
    df = con.execute(f"""
        SELECT *
        FROM read_parquet('{parquet_path}')
    """).df()

    # Convert arrival_timestamp to datetime, coercing invalid values to NaT
    df["arrival_timestamp"] = pd.to_datetime(
        df["arrival_timestamp"], errors="coerce"
    )

    # Return the loaded DataFrame without further processing
    return df