from api.data_model import O3DataModel
from src.sql_interface.table_generator import KeyElementTableCreator, StandardListTableCreator
from src.sql_interface.foreign_keys import ForeignKeysConstraints
from src.helpers.enums import SupportedSQLServers


def create_model(file_name, clean):
    return O3DataModel(file_name, clean=clean)


def create_tables(model, server_type, phi_allowed):
    tables = {}
    for table_name, data in model.key_elements.items():
        tables[table_name] = KeyElementTableCreator(server_type, data).sql_table(phi_allowed=phi_allowed)

    for table_name, data in model.standard_value_lists.items():
        tables[table_name] = StandardListTableCreator(server_type, table_name, data).sql_table()

    return tables


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
        for rel in ke.relationships:
            _commands.append(ForeignKeysConstraints(rel, sql_server_type).column_creation_text)

    return _commands

if __name__ == "__main__":

    file_name = './Resources/O3_20250128.json'
    clean = True
    model = create_model(file_name=file_name, clean=clean)
    sub, pred, cat = get_table_names_from_relationships(model)
    test_names_in_relationships(sub, pred, model)

    server_type = SupportedSQLServers.MSSQL
    phi_allowed = True
    tables = create_tables(model, server_type, phi_allowed)
    fk_commands = foreign_key_constraints(model, server_type)

    print()

