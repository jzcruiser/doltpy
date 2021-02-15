import logging
from typing import List, Mapping

import pandas as pd  # type: ignore

from doltpy.cli import Dolt
from doltpy.shared.helpers import rows_to_columns

logger = logging.getLogger(__name__)


def read_columns(dolt: Dolt, table: str, as_of: str = None) -> Mapping[str, list]:
    """
    Read the contents of a table formatted into columns, optionally `AS OF` a commit, commit ref, or date.
    :param dolt:
    :param table: the table to read.
    :param as_of: a commit, commit ref, or date.
    :return: `Mapping[str, list]` mapping column names to lists of values.
    """
    return read_columns_sql(dolt, _get_read_table_asof_query(table, as_of))


def read_rows(dolt: Dolt, table: str, as_of: str = None) -> List[dict]:
    """
    Read the contents of a table formatted into rows, optionally `AS OF` a commit, commit ref, or date.
    :param dolt:
    :param table: the table to read.
    :param as_of: a commit, commit ref, or date.
    :return: `List[dict]`, each `dict` representing a row.
    """
    return read_rows_sql(dolt, _get_read_table_asof_query(table, as_of))


def read_pandas(dolt: Dolt, table: str, as_of: str = None) -> pd.DataFrame:
    """
    Read the contents of a table formatted as a `pandas.DataFrame`, optionally `AS OF` a commit, commit ref, or date.
    :param dolt:
    :param table: the table to read.
    :param as_of: a commit, commit ref, or date.
    :return: `pd.DataFrame` representing the table.
    """
    return read_pandas_sql(dolt, _get_read_table_asof_query(table, as_of))


def _get_read_table_asof_query(table: str, as_of: str = None) -> str:
    base_query = f"SELECT * FROM `{table}`"
    return f'{base_query} AS OF "{as_of}"' if as_of else base_query


def read_columns_sql(dolt: Dolt, sql: str) -> Mapping[str, list]:
    """
    Execute a SQL query and format the result as a mapping of columns to lists of values.
    :param dolt:
    :param sql: the SQL query to execute.
    :return: `Mapping[str, list]` representing the return value mapping columns to lists of values..
    """
    rows = _read_table_sql(dolt, sql)
    columns = rows_to_columns(rows)
    return columns


def read_rows_sql(dolt: Dolt, sql: str) -> List[dict]:
    """
    Execute a SQL query and format the result as a list of rows, each represented by a `dict`.
    :param dolt:
    :param sql: the SQL query to execute.
    :return: `List[dict]` representing the return value mapping as list of `dict`, one for each row.
    """
    return _read_table_sql(dolt, sql)


def read_pandas_sql(dolt: Dolt, sql: str) -> pd.DataFrame:
    """
    Execute a SQL query and format the result as `pandas.DataFrame`.
    :param dolt:
    :param sql: the SQL query to execute.
    :return: `pd.DataFrame` representing the return value mapping as list of `dict`, one for each row.
    """
    rows = _read_table_sql(dolt, sql)
    return pd.DataFrame(rows)


def _read_table_sql(dolt: Dolt, sql: str) -> List[dict]:
    return dolt.sql(sql, result_format="csv")
