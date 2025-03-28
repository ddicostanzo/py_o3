from api.data_model import O3DataModel
from src.sql_interface.table_generator import KeyElementTableCreator, StandardListTableCreator, LookupTableCreator
from src.sql_interface.foreign_keys import ForeignKeysConstraints
from src.helpers.enums import SupportedSQLServers


def create_key_element_tables(model: O3DataModel,
                              server_type: SupportedSQLServers,
                              phi_allowed: bool):
    _tables: dict = {}
    for table_name, data in model.key_elements.items():
        _tables[table_name] = KeyElementTableCreator(server_type, data).sql_table(phi_allowed=phi_allowed)

    return _tables


def create_individual_standard_value_tables(model: O3DataModel,
                                            server_type: SupportedSQLServers,):

    _tables: dict = {}
    for table_name, data in model.standard_value_lists.items():
        _tables[table_name] = StandardListTableCreator(server_type, table_name, data).sql_table()

    return _tables


def create_standard_value_lookup_table(model: O3DataModel,
                                       server_type: SupportedSQLServers):

    items: list = []
    for _, values in model.standard_value_lists.items():
        items.extend(values)

    return {'StandardValueLookup': LookupTableCreator(server_type, items).sql_table()}


def create_tables(model: O3DataModel, server_type: SupportedSQLServers, phi_allowed: bool):
    tables = create_key_element_tables(model, server_type, phi_allowed)
    tables.update(create_standard_value_lookup_table(model, server_type))

    return tables


def create_model(file_name, clean):
    return O3DataModel(file_name, clean=clean)


def get_table_names_from_relationships(model):
    subject_names = set()
    predicate_names = set()
    relationship_categories = set()

    for _, ke in model.key_elements.items():
        for rel in ke.relationships:
            subject_names.add(rel.subject_element)
            predicate_names.add(rel.predicate_element)
            relationship_categories.add(rel.relationship_category)

    return subject_names, predicate_names, relationship_categories


def test_names_in_relationships(sub, pred, model):
    for _, ke in model.key_elements.items():
        if ke.string_code not in sub:
            pass
            # print(f"String Code {ke.string_code} not in sub table")

        if ke.string_code not in pred:
            pass
            # print(f"String Code {ke.string_code} not in predicate table")


def foreign_key_constraints(model, sql_server_type):
    _commands = []
    for _, ke in model.key_elements.items():
        for rel in ke.child_of_relationships:
            _commands.append(ForeignKeysConstraints(rel, sql_server_type).column_creation_text)

    return _commands


if __name__ == "__main__":

    file_name = './Resources/O3_20250128.json'
    clean = True
    model = create_model(file_name=file_name, clean=clean)
    sub, pred, cat = get_table_names_from_relationships(model)
    # test_names_in_relationships(sub, pred, model)

    server_type = SupportedSQLServers.MSSQL
    phi_allowed = True
    tables = create_tables(model, server_type, phi_allowed)
    fk_commands = foreign_key_constraints(model, server_type)

    location = 'U:/CodeRepository/Dominic/O3/Sql_Commands/test.txt'
    for k, v in tables.items():
        with open(location, 'a') as file:
            file.writelines(v)
            file.writelines('\n')

    for com in fk_commands:
        with open(location, 'a') as file:
            file.write(com)

    print()

