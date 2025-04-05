from api.data_model import O3DataModel
from sql.data_model_to_sql.table_generator import KeyElementTableCreator, StandardListTableCreator, \
    LookupTableCreator, PatientIdentifierHash
from sql.data_model_to_sql.foreign_keys import ForeignKeysConstraints
from src.helpers.enums import SupportedSQLServers
from sql.connection.mssql import MSSQLConnection
from helpers.enums import ServerToConnect


def create_key_element_tables(model: O3DataModel,
                              sql_type: SupportedSQLServers,
                              phi_allowed: bool) -> dict:
    """
    Creates the sql commands for the given data model, sql type, and PHI flag

    Parameters
    ----------
    model: O3DataModel
        The O3 model to create the tables for
    sql_type: SupportedSQLServers
        The enumerated supported SQL servers
    phi_allowed: bool
        Whether the table and data model should be generated to store PHI or not
    Returns
    -------
        dict[str, str]
            A dictionary with the table name as a key and value as the sql command
    """
    _tables: dict = {}
    for table_name, data in model.key_elements.items():
        _tables[table_name] = KeyElementTableCreator(sql_type, data).sql_table(phi_allowed=phi_allowed)

    return _tables


def create_individual_standard_value_tables(model: O3DataModel,
                                            sql_type: SupportedSQLServers,) -> dict:

    """
    Creates individual tables for each standard value list in the model

    Parameters
    ----------
    model: O3DataModel
        The data model to use
    sql_type: SupportedSQLServers
        Supported SQL server type

    Returns
    -------
        dict[str, str]
            A dictionary with key of table name and value as sql command
    """
    _tables: dict = {}
    for table_name, data in model.standard_value_lists.items():
        _tables[table_name] = StandardListTableCreator(sql_type, table_name, data).sql_table()

    return _tables


def create_standard_value_lookup_table(model: O3DataModel,
                                       sql_type: SupportedSQLServers) -> LookupTableCreator:

    """
    Creates a single table for the standard value lists that acts as a lookup table

    Parameters
    ----------
    model: O3DataModel
        The O3 model to use to construct the list
    sql_type: SupportedSQLServers
        The sql server type to create the command for
    Returns
    -------
        dict[str, str]
            The dictionary with key of "StandardValueLookup" and value of the sql command
    """
    items: list = []
    for _, values in model.standard_value_lists.items():
        items.extend(values)

    return LookupTableCreator(sql_type, items)


def create_tables(model: O3DataModel, sql_type: SupportedSQLServers, phi_allowed: bool) -> dict:
    """
    Parses the O3 model to create the necessary tables for database instantiation

    Parameters
    ----------
    model: O3DataModel
        The O3 data model to use to create the tables from
    sql_type: SupportedSQLServers
        The sql server type to create the commands for
    phi_allowed: bool
        Whether PHI can be stored in the DB or not

    Returns
    -------
        dict[str, str]
            A dictionary that contains all commands necessary to instantiate the database.
            The key is the table name and the value is the sql command

    """
    _tables = create_key_element_tables(model, sql_type, phi_allowed)
    return _tables


def create_model(file_location: str, clean: bool) -> O3DataModel:
    """
    Instantiates an O3 data model from a JSON schema

    Parameters
    ----------
    file_location: str
        the file location of the JSON data model schema
    clean: bool
        a flag to clean the model during reading and parsing for common typos or omissions

    Returns
    -------
        O3DataModel
            the instantiated data model
    """
    return O3DataModel(file_location, clean=clean)


def get_table_names_from_relationships(model: O3DataModel) -> tuple[set[str], set[str], set[str]]:
    """
    Gets the table names and relationships from the data model

    Parameters
    ----------
    model: O3DataModel
        the data model to accrue the relationships from

    Returns
    -------
        tuple[set[str], set[str], set[str]]
            a tuple with the subject names, predicate names, and relationship categories as sets
    """
    subject_names = set()
    predicate_names = set()
    relationship_categories = set()

    for _, ke in model.key_elements.items():
        for rel in ke.relationships:
            subject_names.add(rel.subject_element)
            predicate_names.add(rel.predicate_element)
            relationship_categories.add(rel.relationship_category)

    return subject_names, predicate_names, relationship_categories


def test_names_in_relationships(subject: str, predicate: str, model: O3DataModel) -> None:
    """
    Tests the key elements in the table names to be sure they exist

    Parameters
    ----------
    subject: str
        the subject table name
    predicate: str
        the predicate table name
    model: O3DataModel
        the model to test the tables against

    Returns
    -------
        None
    """
    for _, ke in model.key_elements.items():
        if ke.string_code not in subject:
            print(f"String Code {ke.string_code} not in sub table")

        if ke.string_code not in predicate:
            print(f"String Code {ke.string_code} not in predicate table")


def foreign_key_constraints(model: O3DataModel, sql_type: SupportedSQLServers) -> list[str]:
    """
    Generates foreign key constraints from the data model using Child Of relationships

    Parameters
    ----------
    model: O3DataModel
        the data model to accrue the relationships from
    sql_type: SupportedSQLServers
        the sql server type to create the commands for

    Returns
    -------
    list[str]
        A list of the foreign key constraints as SQL commands
    """
    _commands = []
    for _, ke in model.key_elements.items():
        for rel in ke.child_of_relationships:
            _commands.append(ForeignKeysConstraints(rel, sql_type).column_creation_text)

    return _commands


def write_sql_to_text(file_location: str, commands: list[str], **kwargs) -> None:
    """
    Writes the SQL command to text file

    Parameters
    ----------
    file_location: str
        The file location to write the SQL command to
    commands: list[str]
        The SQL commands to write
    kwargs
        write_mode: str
            The open function mode to use. Defaults to 'a' if not provided

    Returns
    -------
    None
    """
    with open(file_location, kwargs.get('write_mode', 'a')) as file:
        for command in commands:
            file.writelines(command)


if __name__ == "__main__":

    o3_schema: str = './Resources/O3_20250128.json'
    clean_file: bool = True
    o3_model: O3DataModel = create_model(file_location=o3_schema, clean=clean_file)
    # sub, pred, cat = get_table_names_from_relationships(o3_model)
    # test_names_in_relationships(sub, pred, model)

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
    o3_connection = o3_db.connection()
    aura_db = MSSQLConnection.create_connection(ServerToConnect.Aura)

    from sql.aria_integration.patient import Patient
    pat = Patient(aura_db.connection())
    for row in pat.get_data():
        print(row)
        print()

    # write_sql_to_text(location, [v for _, v in tables.items()], write_mode='w')
    # write_sql_to_text(location, insert_commands, write_mode='a')
    # write_sql_to_text(location, fk_commands, write_mode='a')

    print()

