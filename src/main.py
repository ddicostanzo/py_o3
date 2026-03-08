"""Entry point script demonstrating O3 model parsing and SQL generation workflow."""
import logging

from api.workflow import (
    create_model,
    create_standard_value_lookup_table,
    create_tables,
    foreign_key_constraints,
)
from helpers.enums import ServerToConnect, SupportedSQLServers
from sql.connection.mssql import MSSQLConnection
from sql.data_model_to_sql.table_generator import LookupTableCreator, PatientIdentifierHash

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


if __name__ == "__main__":

    o3_schema: str = './Resources/O3_20250128.json'
    clean_file: bool = True
    o3_model = create_model(file_location=o3_schema, clean=clean_file)
    # sub, pred, cat = get_table_names_from_relationships(o3_model)
    # validate_names_in_relationships(sub, pred, model)

    sql_server_type: SupportedSQLServers = SupportedSQLServers.MSSQL
    is_phi_allowed: bool = True
    tables: dict[str, str] = create_tables(o3_model, sql_server_type, is_phi_allowed)

    lookup_table: LookupTableCreator = create_standard_value_lookup_table(o3_model, sql_server_type)
    tables['StandardValueLookup'] = lookup_table.sql_table()

    patient_id_hash: PatientIdentifierHash = PatientIdentifierHash(sql_server_type, "PatientIdentifierHash")
    tables['PatientIdentifierHash'] = patient_id_hash.sql_table()

    insert_commands: list[str] = lookup_table.insert_commands()
    fk_commands: list[str] = foreign_key_constraints(o3_model, sql_server_type)
    fk_commands.append(patient_id_hash.foreign_key)

    location: str = '../Sql_Commands/test.txt'

    o3_db = MSSQLConnection.create_connection(ServerToConnect.O3)
    aura_db = MSSQLConnection.create_connection(ServerToConnect.Aura)

    # Aria integration is available via sql.aria_integration.patient.Patient
    # but PHI must not be logged — use secure data handling practices instead

    # write_sql_to_text(location, [v for _, v in tables.items()], write_mode='w')
    # write_sql_to_text(location, insert_commands, write_mode='a')
    # write_sql_to_text(location, fk_commands, write_mode='a')
