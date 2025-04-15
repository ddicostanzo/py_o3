from __future__ import annotations

from contextlib import closing
from typing import Iterable, Generator

import pyodbc
from pyodbc import Connection


class Datatable:
    def __init__(self, connection: Connection, query_location: str):
        self.connection = connection
        with open(query_location, 'r') as query:
            self.query = query.read()

    def _get_data(self, num_results: int = None) -> Iterable[pyodbc.Row] | Generator[pyodbc.Row, None, None]:
        if num_results is None:
            return self._data_generator()
        else:
            return self._data_rows(num_results)

    def _data_generator(self):
        with closing(self.connection) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(self.query):
                yield row

    def _data_rows(self, num_results: int):
        with closing(self.connection) as conn:
            cursor = conn.cursor()
            rows = cursor.execute(self.query).fetchmany(num_results)

        return rows


if __name__ == "__main__":
    pass
