"""Patient data query executor for the Aria data warehouse."""
from pyodbc import Connection
from sql.aria_integration.queried_datatable import Datatable


class Patient(Datatable):
    """
    Query executor for patient demographic data from the Aria data warehouse.

    Loads and executes the patient SQL query against the provided connection.
    """

    def __init__(self, connection: Connection):
        super().__init__(connection, './sql/queries/Aura/patient.sql')

    def get_data(self):
        return self._get_data()


if __name__ == "__main__":
    pass
