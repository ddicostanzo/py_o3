from pyodbc import Connection


class Datatable:
    def __init__(self, connection: Connection):
        self.connection = connection


if __name__ == "__main__":
    pass
