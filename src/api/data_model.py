from collections.abc import Iterator

from base.o3_attribute import O3Attribute
from src.base.o3_key_element import O3KeyElement
import json
import pathlib

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.base.o3_key_element import O3KeyElement
    from base.o3_standard_value import O3StandardValue
    from pathlib import Path


class O3DataModel:
    """
    The O3 data model that is instantiated by a JSON schema
    that can be downloaded from: https://aapmbdsc.azurewebsites.net/
    """
    def __init__(self, json_file, **kwargs):
        """
        The constructor of this class that takes the JSON file.

        Parameters
        ----------
        json_file: str
            The JSON schema downloaded from https://aapmbdsc.azurewebsites.net/
        kwargs
            clean: bool
                attempts to clean common errors in the typed data of the key element and
                its attributes
        """
        super().__init__()

        path: Path = pathlib.Path.joinpath(pathlib.Path.cwd(), json_file)
        path.absolute()

        if not path.exists():
            raise FileExistsError(f"Path not found: {path}")

        if not path.is_file():
            raise TypeError(f"Is not a file: {path}")

        self.json_file: Path = path
        self.json_obj: dict = {}
        self.key_elements: dict[str, O3KeyElement] = {}
        self.__standard_value_lists: dict[str, list[O3StandardValue]] = {}
        self.__value_data_types: set[str] = set()
        self.__sql_data_types: set[str] = set()
        self.__value_priority: set[str] = set()
        self.__reference_system_for_standard_values = set()
        self.__allow_nulls: set[str] = set()

        self.__json_to_dictionary()
        self.__create_key_elements(**kwargs)

    def __json_to_dictionary(self) -> None:
        """
        Creates the json_obj dictionary from the json file.

        Returns
        -------
        None
        """
        with open(self.json_file, 'r') as file:
            _json_text = file.read()
            # The O3 JSON contains escape characters with the Unicode encoded +.
            # This removes those and provides "Other" as a category.
            _json_text = _json_text.replace('(\\u002B Other)', "Other")
            _json_text = _json_text.replace('(\\u002BOther)', "Other")
            self.json_obj = json.loads(_json_text)

    def __create_key_elements(self, **kwargs) -> None:
        """
        Reads and creates the O3 elements contained in the JSON dictionary

        Parameters
        ----------
        kwargs
            clean: bool
                passes the clean kwarg to the O3KeyElement constructor

        Returns
        -------
        None
        """
        for obj in self.json_obj:
            _element = O3KeyElement(obj, **kwargs)
            self.key_elements[_element.key_element_name] = _element

    def __key_element_generator(self) -> Iterator[O3KeyElement]:
        """
        A generator object used for passing the O3KeyElement objects

        Yields
        -------
        O3KeyElement
            Key elements from the instantiated model
        """
        for _, ke in self.key_elements.items():
            yield ke

    def __attribute_generator(self) -> Iterator[O3Attribute]:
        """
        A generator object used for accessing the O3Attribute objects in the O3KeyElements

        Yields
        -------
        O3Attribute
            the O3Attribute objects from the instantiated model
        """
        for ke in self.__key_element_generator():
            for ele_attr in ke.list_attributes:
                yield ele_attr

    def __read_property_from_attribute(self, collection, attribute_name) -> None:
        """
        Adds the properties from the attributes of each key element to the passed set

        Parameters
        ----------
        collection: set[str]
            the set to add the properties to
        attribute_name: str
            the name of the attribute to pass to the set

        Returns
        -------
            None
        """
        for ke in self.__key_element_generator():
            for ele_attr in ke.list_attributes:
                collection.add(getattr(ele_attr, attribute_name))

    def __read_standard_values(self) -> None:
        """
        Reads the standard value lists from the attributes of each key element and adds to a dictionary

        Returns
        -------
            None
        """
        for ele_attr in self.__attribute_generator():
            if len(ele_attr.standard_values_list) > 0:
                self.__standard_value_lists[ele_attr.value_name] = ele_attr.standard_values_list

    def __read_value_data_types(self) -> None:
        """
        Reads the value data types from the attributes of each key element and adds to the appropriate set

        Returns
        -------
            None
        """
        self.__read_property_from_attribute(self.__value_data_types, 'value_data_type')

    def __read_sql_data_types(self) -> None:
        """
        Reads the sql data types from the attributes of each key element and adds to the appropriate set

        Returns
        -------
            None
        """
        self.__read_property_from_attribute(self.__sql_data_types, 'sql_data_type')

    def __read_value_priority(self) -> None:
        """
        Reads the value priority from the attributes of each key element and adds to the appropriate set

        Returns
        -------
            None
        """
        self.__read_property_from_attribute(self.__value_priority, 'value_priority')

    def __read_reference_system_for_standard_values(self) -> None:
        """
        Reads the reference system for standard values from the attributes of each key element
        and adds to the appropriate set

        Returns
        -------
            None
        """
        self.__read_property_from_attribute(self.__reference_system_for_standard_values,
                                            'reference_system_for_values')

    def __read_allow_nulls(self) -> None:
        """
        Reads the allow nulls from the attributes of each key element and adds to the appropriate set

        Returns
        -------
            None
        """
        self.__read_property_from_attribute(self.__allow_nulls, 'allow_null_values')

    @property
    def standard_value_lists(self) -> dict[str, list[O3StandardValue]]:
        """
        Retrieves all standard value lists for the model in a dictionary

        Returns
        -------
            dict[str, list[str]]
                A dictionary of the standard value lists with name of the list as the key and items as the value
        """
        if len(self.__standard_value_lists) == 0:
            self.__read_standard_values()

        return self.__standard_value_lists

    @property
    def value_data_types(self) -> set[str]:
        """
        Retrieves all unique value data types for the model.

        Returns
        -------
            set[str]
                the unique value data types as a set
        """
        if len(self.__value_data_types) == 0:
            self.__read_value_data_types()

        return self.__value_data_types

    @property
    def sql_data_types(self) -> set[str]:
        """
        Retrieves all unique sql data types for the model.

        Returns
        -------
            set[str]
                the unique sql data types as a set
        """
        if len(self.__sql_data_types) == 0:
            self.__read_sql_data_types()

        return self.__sql_data_types

    @property
    def value_priority(self) -> set[str]:
        """
        Retrieves all unique value priorities for the model.

        Returns
        -------
            set[str]
                the unique value priorities as a set
        """
        if len(self.__value_priority) == 0:
            self.__read_value_priority()

        return self.__value_priority

    @property
    def reference_systems_for_standard_values(self) -> set[str]:
        """
        Retrieves all reference systems for standard values from the attributes of each key element

        Returns
        -------
            set[str]
                the reference systems for standard values as a set
        """
        if len(self.__reference_system_for_standard_values) == 0:
            self.__read_reference_system_for_standard_values()

        return self.__reference_system_for_standard_values

    @property
    def allow_nulls(self) -> set[str]:
        """
        Retrieves all allow nulls from the attributes of each key element and adds to a set

        Returns
        -------
            set[str]
                the unique allow null values as a set
        """
        if len(self.__allow_nulls) == 0:
            self.__read_allow_nulls()

        return self.__allow_nulls


if __name__ == "__main__":
    pass
