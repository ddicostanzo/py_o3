from pyodbc import Connection
from src.sql.aria_integration.queried_datatable import Datatable


class Patient(Datatable):
    def __init__(self, connection: Connection):
        super().__init__(connection, './sql/queries/Aura/patient.sql')

    def get_data(self):
        return self._get_data()


if __name__ == "__main__":
    pass
