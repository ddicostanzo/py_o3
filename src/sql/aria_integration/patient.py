"""Patient data query executor for the Aria data warehouse."""
from pyodbc import Connection

from sql.aria_integration.queried_datatable import Datatable, _resolve_query_path


class Patient(Datatable):
    """
    Query executor for patient demographic data from the Aria data warehouse.

    Loads and executes the patient SQL query against the provided connection.
    """

    _QUERY_FILE = 'Aura/patient.sql'

    def __init__(self, connection: Connection):
        super().__init__(connection, _resolve_query_path(self._QUERY_FILE))

    def get_data(self, num_results: int = None):
        return self._get_data(num_results=num_results)


if __name__ == "__main__":
    pass
