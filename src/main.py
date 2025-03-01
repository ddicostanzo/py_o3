from api.data_model import O3DataModel
from src.sql_interface.table_generator import KeyElementTableCreator, StandardListTableCreator
from src.helpers.enums import SupportedSQLServers


if __name__ == "__main__":

    test = O3DataModel("./Resources/O3_20250128.json", clean=True)
    sql_test = KeyElementTableCreator(SupportedSQLServers.MSSQL, test.key_elements["Diagnosis and Staging"])
    _com = sql_test.sql_table(True)
    sv_test = test.standard_value_lists['Dose Units']
    sv_sql = StandardListTableCreator(SupportedSQLServers.MSSQL, 'Dose Units', sv_test).sql_table()
    print(_com)
    print()

