from api.data_model import O3DataModel
from src.sql_interface.table_generator import KeyElementTableCreator
from src.helpers.enums import SupportedSQLServers


if __name__ == "__main__":

    test = O3DataModel("./Resources/O3_20250128.json", clean=True)
    sql_test = KeyElementTableCreator(SupportedSQLServers.MSSQL, test.key_elements["Diagnosis and Staging"])
    print(sql_test.sql_table(True))
    print()

