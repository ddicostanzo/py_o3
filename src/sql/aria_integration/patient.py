"""Patient data query executor for the Aria data warehouse."""
from collections.abc import Generator, Iterable

from pyodbc import Connection, Row

from sql.aria_integration.queried_datatable import Datatable


class Patient(Datatable):
    """
    Query executor for patient demographic data from the Aria data warehouse.

    Loads and executes the patient SQL query against the provided connection.
    """

    _QUERY_FILE: str = 'Aura/patient.sql'

    def __init__(self, connection: Connection):
        super().__init__(connection, self._QUERY_FILE)

    def get_data(self, num_results: int | None = None) -> Iterable[Row] | Generator[Row, None, None]:
        """
        Execute the patient demographics query.

        Parameters
        ----------
        num_results : int, optional
            Maximum number of rows to return. If None, returns a generator.

        Returns
        -------
        Iterable[pyodbc.Row] | Generator[pyodbc.Row, None, None]
            Query results as a list (if num_results specified) or generator.
        """
        return self._get_data(num_results=num_results)


if __name__ == "__main__":
    pass
