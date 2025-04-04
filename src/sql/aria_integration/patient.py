from pyodbc import Connection
from contextlib import closing
from sql.aria_integration.queried_datatable import Datatable


class Patient(Datatable):
    def __init__(self, connection: Connection):
        super().__init__(connection)
        with open('./sql/queries/Aura/patient.sql') as query:
            self.query = query.read()

    def get_data(self, num_results: int = None):
        with closing(self.connection) as conn:
            cursor = conn.cursor()
            cursor.execute(self.query)
            if num_results is None:
                rows = cursor.fetchall()
            else:
                rows = cursor.fetchmany(num_results)

        return rows


if __name__ == "__main__":
    pass
