"""Patient information query executor for the Aria data warehouse."""
from collections.abc import Generator, Iterable

from pyodbc import Connection, Row

from sql.aria_integration.queried_datatable import Datatable


class PatientInformation(Datatable):
    """
    Query executor for patient address and clinical information from the Aria
    data warehouse.

    Executes a parameterized query filtered by medical record number (MRN).

    Parameters
    ----------
    connection : pyodbc.Connection
        An active pyodbc connection to the Aura data warehouse.
    """

    _QUERY_FILE: str = 'Aura/patient_information.sql'

    def __init__(self, connection: Connection):
        super().__init__(connection, self._QUERY_FILE)

    def get_data(
        self, mrn: str, num_results: int | None = None
    ) -> Iterable[Row] | Generator[Row, None, None]:
        """
        Execute the patient information query for a specific MRN.

        Parameters
        ----------
        mrn : str
            The medical record number to query.
        num_results : int, optional
            Maximum number of rows to return. If None, returns a generator.

        Returns
        -------
        Iterable[pyodbc.Row] | Generator[pyodbc.Row, None, None]
            Query results as a list (if num_results specified) or generator.
        """
        if not mrn:
            raise ValueError("mrn must be a non-empty string")
        return self._get_data(num_results=num_results, params=(mrn,))


if __name__ == "__main__":
    pass
