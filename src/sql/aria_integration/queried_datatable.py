"""Base class for executing parameterized SQL queries via pyodbc."""
from __future__ import annotations

import logging
from collections.abc import Generator, Iterable
from pathlib import Path

import pyodbc
from pyodbc import Connection

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"


class Datatable:
    """
    Base class for parameterized SQL query execution via pyodbc.

    Reads a SQL query from a file and provides generator-based and
    batch retrieval methods for query results.

    Subclasses should set a ``_QUERY_FILE`` class variable with the path
    relative to ``sql/queries/`` (e.g., ``'Aura/patient.sql'``) and pass
    it to ``super().__init__()`` as ``query_location``. The base class
    resolves it to an absolute path automatically.

    The caller is responsible for managing the connection lifecycle.
    This class does not close or otherwise manage the provided connection.

    Parameters
    ----------
    connection : pyodbc.Connection
        An active pyodbc connection to the target database.
    query_location : str
        Path to the SQL query file. Relative paths are resolved against
        the ``sql/queries/`` directory; absolute paths are used as-is.

    Raises
    ------
    FileNotFoundError
        If the resolved query file does not exist.
    """

    def __init__(self, connection: Connection, query_location: str):
        self.connection = connection
        self.query_location = self.__resolve_path(query_location)
        with open(self.query_location) as query:
            self.query = query.read()

    @staticmethod
    def __resolve_path(query_location: str) -> str:
        path = Path(query_location)
        if path.is_absolute():
            resolved = path
        else:
            resolved = _QUERIES_DIR / query_location
        if not resolved.is_file():
            raise FileNotFoundError(f"Query file not found: {resolved}")
        return str(resolved)

    def _get_data(
        self,
        num_results: int | None = None,
        params: tuple[str, ...] | None = None,
    ) -> Iterable[pyodbc.Row] | Generator[pyodbc.Row, None, None]:
        logging.info(f"Executing query from {self.query_location}")
        if num_results is None:
            return self.__data_generator(params)
        else:
            return self.__data_rows(num_results, params)

    def __data_generator(
        self, params: tuple[str, ...] | None = None
    ) -> Generator[pyodbc.Row, None, None]:
        try:
            cursor = self.connection.cursor()
            execute_args = (self.query, params) if params is not None else (self.query,)
            yield from cursor.execute(*execute_args)
        except pyodbc.Error as e:
            raise RuntimeError(
                f"Error executing query from '{self.query_location}': {e}"
            ) from e

    def __data_rows(
        self, num_results: int, params: tuple[str, ...] | None = None
    ) -> list[pyodbc.Row]:
        try:
            cursor = self.connection.cursor()
            execute_args = (self.query, params) if params is not None else (self.query,)
            rows = cursor.execute(*execute_args).fetchmany(num_results)
        except pyodbc.Error as e:
            raise RuntimeError(
                f"Error executing query from '{self.query_location}': {e}"
            ) from e

        return rows


if __name__ == "__main__":
    pass
