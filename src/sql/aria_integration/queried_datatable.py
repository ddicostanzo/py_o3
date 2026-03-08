"""Base class for executing parameterized SQL queries via pyodbc."""
from __future__ import annotations

import logging
from collections.abc import Generator, Iterable
from contextlib import closing

import pyodbc
from pyodbc import Connection


class Datatable:
    """
    Base class for parameterized SQL query execution via pyodbc.

    Reads a SQL query from a file and provides generator-based and
    batch retrieval methods for query results.

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

    def _get_data(self, num_results: int = None) -> Iterable[pyodbc.Row] | Generator[pyodbc.Row, None, None]:
        logging.info(f"Executing query from {self.query_location}")
        if num_results is None:
            return self._data_generator()
        else:
            return self._data_rows(num_results)

    def _data_generator(self):
        try:
            with closing(self.connection) as conn, closing(conn.cursor()) as cursor:
                yield from cursor.execute(self.query)
        except pyodbc.Error as e:
            raise pyodbc.Error(
                f"Error executing query from '{self.query_location}': {e}"
            ) from e

    def _data_rows(self, num_results: int):
        try:
            with closing(self.connection) as conn, closing(conn.cursor()) as cursor:
                rows = cursor.execute(self.query).fetchmany(num_results)
        except pyodbc.Error as e:
            raise pyodbc.Error(
                f"Error executing query from '{self.query_location}': {e}"
            ) from e

        return rows


if __name__ == "__main__":
    pass
