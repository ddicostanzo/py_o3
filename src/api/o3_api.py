from base.o3_key_element import O3KeyElement
import json
import pathlib


class O3DataModel:
    def __init__(self, json_file):
        super().__init__()

        path = pathlib.Path.joinpath(pathlib.Path.cwd(), json_file)
        path.absolute()

        if not path.exists():
            raise FileExistsError(f"Path not found: {path}")

        if not path.is_file():
            raise TypeError(f"Is not a file: {path}")

        self.json_file = path
        self.json_obj = None
        self.key_elements = {}
        self.__standard_value_lists = {}
        self.__value_data_types = set()
        self.__value_priority = set()
        self.__reference_system_for_standard_values = set()
        self.__allow_nulls = set()

        self.__read_json()
        self.__load_elements()

    def __read_json(self):
        with open(self.json_file, 'r') as file:
            _json_text = file.read()
            # The O3 JSON contains escape characters with the Unicode encoded +.
            # This removes those and provides "Other" as a category.
            _json_text = _json_text.replace('(\\u002B Other)', "Other")
            _json_text = _json_text.replace('(\\u002BOther)', "Other")
            self.json_obj = json.loads(_json_text)

    def __load_elements(self):
        for obj in self.json_obj:
            _element = O3KeyElement(obj)
            self.key_elements[_element.key_element_name] = _element

    def __read_all_key_elements(self):
        for _, ke in self.key_elements.items():
            yield ke

    def __read_all_attributes(self):
        for ke in self.__read_all_key_elements():
            for ele_attr in ke.list_attributes:
                yield ele_attr

    def __read_property_from_attribute(self, collection, attribute_name):
        for ke in self.__read_all_key_elements():
            for ele_attr in ke.list_attributes:
                collection.add(getattr(ele_attr, attribute_name))

    def __read_standard_values(self):
        for ele_attr in self.__read_all_attributes():
            if len(ele_attr.standard_values_list) > 0:
                self.__standard_value_lists[ele_attr.value_name] = ele_attr.standard_values_list

    def __read_value_data_types(self):
        self.__read_property_from_attribute(self.__value_data_types, 'value_data_type')

    def __read_value_priority(self):
        self.__read_property_from_attribute(self.__value_priority, 'value_priority')

    def __read_reference_system_for_standard_values(self):
        self.__read_property_from_attribute(self.__reference_system_for_standard_values,
                                            'reference_system_for_values')

    def __read_allow_nulls(self):
        self.__read_property_from_attribute(self.__allow_nulls, 'allow_null_values')

    @property
    def standard_value_lists(self):
        if len(self.__standard_value_lists) == 0:
            self.__read_standard_values()

        return self.__standard_value_lists

    @property
    def value_data_types(self):
        if len(self.__value_data_types) == 0:
            self.__read_value_data_types()

        return self.__value_data_types

    @property
    def value_priority(self):
        if len(self.__value_priority) == 0:
            self.__read_value_priority()

        return self.__value_priority

    @property
    def reference_systems_for_standard_values(self):
        if len(self.__reference_system_for_standard_values) == 0:
            self.__read_reference_system_for_standard_values()

        return self.__reference_system_for_standard_values

    @property
    def allow_nulls(self):
        if len(self.__allow_nulls) == 0:
            self.__read_allow_nulls()

        return self.__allow_nulls


if __name__ == "__main__":
    pass
