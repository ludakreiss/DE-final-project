import duckdb
import pandas as pd
from constants import DB_PATH


def query_df(sql: str, params=None) -> pd.DataFrame:
    '''
    execute a SQL query against DuckDB and return the result as a Dataframe

    :param sql: SQL query to execute
    :type sql: str
    :param params: optional query parameters
    :return: query result as a pandas Dataframe
    :rtype: pd.DataFrame
    '''
    with duckdb.connect(DB_PATH, read_only=True) as con:
        if params is None:
            return con.execute(sql).df()
        return con.execute(sql, params).df()


def placeholders(n: int) -> str:
    '''
    placeholders for parameterized queries
    
    :param n: number of placeholders
    :type n: int
    :return: conseqcutive placeholders if they are more than one
    :rtype: str
    '''
    return ",".join(["?"] * max(n, 1))
