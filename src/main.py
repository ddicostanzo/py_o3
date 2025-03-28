from api.data_model import O3DataModel
from src.sql_interface.table_generator import KeyElementTableCreator, StandardListTableCreator, LookupTableCreator
from src.sql_interface.foreign_keys import ForeignKeysConstraints
from src.helpers.enums import SupportedSQLServers


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
                                       sql_type: SupportedSQLServers) -> dict:

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

    return {'StandardValueLookup': LookupTableCreator(sql_type, items).sql_table()}


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
    _tables.update(create_standard_value_lookup_table(model, sql_type))

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


def foreign_key_constraints(model: O3DataModel, sql_type: SupportedSQLServers):
    _commands = []
    for _, ke in model.key_elements.items():
        for rel in ke.child_of_relationships:
            _commands.append(ForeignKeysConstraints(rel, sql_type).column_creation_text)

    return _commands


if __name__ == "__main__":

    o3_schema = './Resources/O3_20250128.json'
    clean_file = True
    o3_model = create_model(file_location=o3_schema, clean=clean_file)
    sub, pred, cat = get_table_names_from_relationships(o3_model)
    # test_names_in_relationships(sub, pred, model)

    sql_server_type = SupportedSQLServers.MSSQL
    is_phi_allowed = True
    tables = create_tables(o3_model, sql_server_type, is_phi_allowed)
    fk_commands = foreign_key_constraints(o3_model, sql_server_type)

    location = 'U:/CodeRepository/Dominic/O3/Sql_Commands/test.txt'
    for k, v in tables.items():
        with open(location, 'a') as file:
            file.writelines(v)
            file.writelines('\n')

    for com in fk_commands:
        with open(location, 'a') as file:
            file.write(com)

    print()

