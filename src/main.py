from api.data_model import O3DataModel
from src.sql_interface.table_generator import KeyElementTableCreator, StandardListTableCreator
from src.helpers.enums import SupportedSQLServers


def create_tables(file_name, clean, server_type, phi_allowed):
    model = O3DataModel(file_name, clean=clean)

    tables = {}
    for table_name, data in model.key_elements.items():
        tables[table_name] = KeyElementTableCreator(server_type, data).sql_table(phi_allowed=phi_allowed)

    for table_name, data in model.standard_value_lists.items():
        tables[table_name] = StandardListTableCreator(server_type, table_name, data).sql_table()

    return tables


if __name__ == "__main__":

    file_name = './Resources/O3_20250128.json'
    clean = True
    server_type = SupportedSQLServers.MSSQL
    phi_allowed = True
    tables = create_tables(file_name, clean, server_type, phi_allowed)

    print()

