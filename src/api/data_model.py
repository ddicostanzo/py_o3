"""O3 data model parser that reads the O3 JSON schema into Python objects."""
from __future__ import annotations

import json
import pathlib
from collections.abc import Iterator
from typing import TYPE_CHECKING

from base.o3_attribute import O3Attribute
from base.o3_key_element import O3KeyElement

if TYPE_CHECKING:
    from pathlib import Path

    from base.o3_key_element import O3KeyElement
    from base.o3_standard_value import O3StandardValue


class O3DataModel:
    """
    The O3 data model that is instantiated by a JSON schema
    that can be downloaded from: https://aapmbdsc.azurewebsites.net/
    """

    def __init__(self, json_file: str, **kwargs):
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

        json_path = pathlib.Path(json_file)
        if json_path.is_absolute():
            path: Path = json_path
        else:
            path: Path = pathlib.Path.cwd() / json_file

        if not path.exists():
            raise FileNotFoundError(f"Path not found: {path}")

        if not path.is_file():
            raise TypeError(f"Is not a file: {path}")

        self.json_file: Path = path
        self.json_obj: dict = {}
        self._init_caches()

        self.__json_to_dictionary()
        self.__create_key_elements(**kwargs)

    def _init_caches(self) -> None:
        """
        Initialize the lazy property caches and key_elements dict.
        Shared by __init__ and from_dict to avoid duplicating
        name-mangled attribute names.
        """
        self.key_elements: dict[str, O3KeyElement] = {}
        self.__standard_value_lists: dict[str, list[O3StandardValue]] | None = None
        self.__value_data_types: set[str] | None = None
        self.__value_priority: set[str] | None = None
        self.__reference_system_for_standard_values: set[str] | None = None
        self.__allow_nulls: set[str] | None = None

    @classmethod
    def from_dict(cls, data: list[dict], **kwargs) -> O3DataModel:
        """
        Create an O3DataModel from an in-memory list of dictionaries,
        bypassing the filesystem.

        Parameters
        ----------
        data : list[dict]
            the JSON-parsed list of key element dictionaries
        kwargs
            clean: bool
                attempts to clean common errors in the typed data of the key element and
                its attributes

        Returns
        -------
        O3DataModel
            the instantiated data model
        """
        instance = object.__new__(cls)
        instance.json_file = None
        instance.json_obj = data
        instance._init_caches()
        instance._O3DataModel__create_key_elements(**kwargs)
        return instance

    def __json_to_dictionary(self) -> None:
        """
        Creates the json_obj dictionary from the json file.

        Returns
        -------
        None
        """
        with open(self.json_file) as file:
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
        yield from self.key_elements.values()

    def __attribute_generator(self) -> Iterator[O3Attribute]:
        """
        A generator object used for accessing the O3Attribute objects in the O3KeyElements

        Yields
        -------
        O3Attribute
            the O3Attribute objects from the instantiated model
        """
        for ke in self.__key_element_generator():
            yield from ke.list_attributes

    def __read_standard_values(self) -> None:
        """
        Reads the standard value lists from the attributes of each key element and adds to a dictionary

        Returns
        -------
            None
        """
        self.__standard_value_lists = {}
        for ele_attr in self.__attribute_generator():
            if len(ele_attr.standard_values_list) > 0:
                self.__standard_value_lists[ele_attr.value_name] = ele_attr.standard_values_list

    def __collect_all_attribute_properties(self) -> None:
        """
        Iterates all attributes once and populates all attribute property caches simultaneously.

        Returns
        -------
            None
        """
        if self.__value_data_types is not None:
            return

        self.__value_data_types = set()
        self.__value_priority = set()
        self.__reference_system_for_standard_values = set()
        self.__allow_nulls = set()

        for ele_attr in self.__attribute_generator():
            self.__value_data_types.add(ele_attr.value_data_type)
            self.__value_priority.add(ele_attr.value_priority)
            self.__reference_system_for_standard_values.add(ele_attr.reference_system_for_values)
            self.__allow_nulls.add(ele_attr.allow_null_values)

    @property
    def standard_value_lists(self) -> dict[str, list[O3StandardValue]]:
        """
        Retrieves all standard value lists for the model in a dictionary

        Returns
        -------
            dict[str, list[O3StandardValue]]
                A dictionary of the standard value lists with name of the list as the key and items as the value
        """
        if self.__standard_value_lists is None:
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
        if self.__value_data_types is None:
            self.__collect_all_attribute_properties()

        return self.__value_data_types

    @property
    def value_priority(self) -> set[str]:
        """
        Retrieves all unique value priorities for the model.

        Returns
        -------
            set[str]
                the unique value priorities as a set
        """
        if self.__value_priority is None:
            self.__collect_all_attribute_properties()

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
        if self.__reference_system_for_standard_values is None:
            self.__collect_all_attribute_properties()

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
        if self.__allow_nulls is None:
            self.__collect_all_attribute_properties()

        return self.__allow_nulls


if __name__ == "__main__":
    pass
