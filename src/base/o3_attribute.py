from __future__ import annotations

from src.base.o3_standard_value import O3StandardValue
from src.base.o3_element import O3Element
import warnings

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from src.base.o3_key_element import O3KeyElement


class O3Attribute(O3Element):
    """
    The O3 Attribute class that manages the individual attributes for each element.
    """
    def __init__(self, key_element: "O3KeyElement", item_dict: dict, **kwargs):
        """
        Instantiates an O3Attribute object using the parent key element and dictionary containing the attribute.

        Parameters
        ----------
        key_element: O3Element
            the parent element of the attribute
        item_dict: dict
            the parsed JSON dictionary for this attribute
        kwargs
            clean: bool
                the flag for determining if data cleaning should take place
        """
        super().__init__(item_dict)

        self.key_element: "O3KeyElement" = key_element
        self.__possible_value_data_types: list[str] = ['Boolean', 'Binary', 'Date', 'Decimal', 'Integer', 'String']
        self.value_data_type: str = item_dict['ValueDataType']
        self.standard_values_use: str = item_dict['StandardValuesUse']
        self.standard_values_list: list[O3StandardValue] = [O3StandardValue(self.key_element,
                                                            self,
                                                            x) for x in item_dict['StandardValuesList']]
        self.reference_system_for_values: str = item_dict['ReferenceSystemForValues']
        self.allow_null_values: str = item_dict['AllowNullValues']
        self.value_example: str = item_dict['ValueExample']

        if kwargs.get('clean', True):
            self.__check_reference_system(item_dict)
            self.__clean_standard_values_list()
            self.__clean_value_data_types()

    def __check_reference_system(self, item_dict: dict) -> None:
        """
        Part of the clean routine that will evaluate if the reference system for standard values in inserted
        into the standard value list. If so, pushes it to the reference_system_for_values property.

        Parameters
        ----------
        item_dict: dict
            the parsed JSON dictionary for this attribute which contains a key of "StandardValuesList"

        Returns
        -------
        None
        """
        if self.reference_system_for_values is None:
            if any(['Reference System' in x for x in item_dict['StandardValuesList']]):
                for sv in item_dict['StandardValuesList']:
                    if 'Reference System' in sv:
                        self.reference_system_for_values = sv.split(': ')[-1].split('{')[0].strip()
                        break

    def __clean_standard_values_list(self) -> None:
        """
        Part of the clean routine. Evaluates if the reference system or Current ICD Standard are
        in the standard value list and removes them.

        Returns
        -------
        None
        """
        for i, item in enumerate(self.standard_values_list):
            if 'Reference System' in item.value_name or 'Current ICD standard' in item.value_name:
                self.standard_values_list.pop(i)

    def __clean_value_data_types(self) -> None:
        """
        Part of the clean routine. Evaluates different data types and sets them to a standard. Capitalization,
        acronyms, date, and numeric data types are all processed.

        Returns
        -------
        None
        """
        if len(self.standard_values_list) > 0:
            self.value_data_type = "String"

        if len(self.standard_values_list) == 0 and self.value_data_type == "":
            self.value_data_type = "String"
            warnings.warn(f"Setting value data type to string for attribute: {self.value_name}.", UserWarning)

        if self.value_data_type not in self.__possible_value_data_types:
            if self.value_data_type == "Int":
                self.value_data_type = "Integer"
            if self.value_data_type == "Numeric":
                self.value_data_type = "Decimal"
            if "Date" in self.value_name:
                self.value_data_type = "Date"
            if self.value_data_type == "string":
                self.value_data_type = "String"


if __name__ == "__main__":
    pass
