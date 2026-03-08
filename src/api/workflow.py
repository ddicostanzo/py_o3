"""Workflow functions for O3 model creation, SQL generation, and file output."""
from __future__ import annotations

import logging

from api.data_model import O3DataModel
from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.foreign_keys import ForeignKeysConstraints
from sql.data_model_to_sql.table_generator import (
    KeyElementTableCreator,
    LookupTableCreator,
    StandardListTableCreator,
)


def create_key_element_tables(model: O3DataModel,
                              sql_type: SupportedSQLServers,
                              phi_allowed: bool) -> dict[str, str]:
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
    for data in model.key_elements.values():
        _tables[data.key_element_name] = KeyElementTableCreator(sql_type, data, phi_allowed=phi_allowed).sql_table()

    return _tables


def create_individual_standard_value_tables(model: O3DataModel,
                                            sql_type: SupportedSQLServers) -> dict[str, str]:

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
        LookupTableCreator
            The lookup table creator instance with methods to generate SQL table and INSERT commands
    """
    items: list = []
    for values in model.standard_value_lists.values():
        items.extend(values)

    return LookupTableCreator(sql_type, items)


def create_tables(model: O3DataModel, sql_type: SupportedSQLServers, phi_allowed: bool) -> dict[str, str]:
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

    for ke in model.key_elements.values():
        for rel in ke.relationships:
            subject_names.add(rel.subject_element)
            predicate_names.add(rel.predicate_element)
            relationship_categories.add(rel.relationship_category)

    return subject_names, predicate_names, relationship_categories


def validate_names_in_relationships(subject: set[str], predicate: set[str], model: O3DataModel) -> None:
    """
    Validates the key elements in the table names to be sure they exist

    Parameters
    ----------
    subject: set[str]
        the subject table names
    predicate: set[str]
        the predicate table names
    model: O3DataModel
        the model to validate the tables against

    Returns
    -------
        None
    """
    for ke in model.key_elements.values():
        if ke.string_code not in subject:
            logging.warning(f"String Code {ke.string_code} not in subject table names")

        if ke.string_code not in predicate:
            logging.warning(f"String Code {ke.string_code} not in predicate table names")


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
    for ke in model.key_elements.values():
        for rel in ke.child_of_relationships:
            _commands.append(ForeignKeysConstraints(rel, sql_type).column_creation_text)

    return _commands


def write_sql_to_text(file_location: str, commands: list[str], write_mode: str = 'a') -> None:
    """
    Writes the SQL command to text file

    Parameters
    ----------
    file_location: str
        The file location to write the SQL command to
    commands: list[str]
        The SQL commands to write
    write_mode: str
        The open function mode to use. Must be 'w' or 'a' (default: 'a')

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If write_mode is not 'w' or 'a'
    """
    if write_mode not in ('w', 'a'):
        raise ValueError(f"write_mode must be 'w' or 'a', got {write_mode!r}")

    with open(file_location, write_mode) as file:
        for command in commands:
            file.writelines(command)


if __name__ == "__main__":
    pass
