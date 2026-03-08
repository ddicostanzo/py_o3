"""Base class for executing parameterized SQL queries via pyodbc."""
from __future__ import annotations

import logging
from collections.abc import Generator, Iterable
from pathlib import Path

import pyodbc
from pyodbc import Connection

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"


def _resolve_query_path(relative_path: str) -> str:
    """Resolve a query file path relative to the sql/queries/ directory.

    Parameters
    ----------
    relative_path : str
        Path relative to the queries directory (e.g., 'Aura/patient.sql')

    Returns
    -------
    str
        Absolute path to the query file

    Raises
    ------
    FileNotFoundError
        If the resolved path does not exist
    """
    resolved = _QUERIES_DIR / relative_path
    if not resolved.is_file():
        raise FileNotFoundError(f"Query file not found: {resolved}")
    return str(resolved)


class Datatable:
    """
    Base class for parameterized SQL query execution via pyodbc.

    Reads a SQL query from a file and provides generator-based and
    batch retrieval methods for query results.

    The caller is responsible for managing the connection lifecycle.
    This class does not close or otherwise manage the provided connection.

    Parameters
    ----------
    connection : pyodbc.Connection
        an active pyodbc connection to the target database
    query_location : str
        the file path to the SQL query to execute
    """

    def __init__(self, connection: Connection, query_location: str):
        self.connection = connection
        self.query_location = query_location
        with open(query_location) as query:
            self.query = query.read()

    def _get_data(
        self,
        num_results: int = None,
        params: tuple = None,
    ) -> Iterable[pyodbc.Row] | Generator[pyodbc.Row, None, None]:
        logging.info(f"Executing query from {self.query_location}")
        if num_results is None:
            return self._data_generator(params)
        else:
            return self._data_rows(num_results, params)

    def _data_generator(self, params: tuple = None):
        try:
            cursor = self.connection.cursor()
            if params is not None:
                yield from cursor.execute(self.query, params)
            else:
                yield from cursor.execute(self.query)
        except pyodbc.Error as e:
            raise RuntimeError(
                f"Error executing query from '{self.query_location}': {e}"
            ) from e

    def _data_rows(self, num_results: int, params: tuple = None):
        try:
            cursor = self.connection.cursor()
            if params is not None:
                rows = cursor.execute(self.query, params).fetchmany(num_results)
            else:
                rows = cursor.execute(self.query).fetchmany(num_results)
        except pyodbc.Error as e:
            raise RuntimeError(
                f"Error executing query from '{self.query_location}': {e}"
            ) from e

        return rows


if __name__ == "__main__":
    pass
