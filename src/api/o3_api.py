from base.o3_key_element import O3KeyElement
import json
import os


class O3DataModel:
    def __init__(self, json_file):
        super().__init__()

        if ~os.path.exists(json_file):
            raise FileExistsError(f"File not found: {json_file}")

        if ~os.path.isfile(json_file):
            raise TypeError(f"Is not a file: {json_file}")

        self.json_file = json_file
        self.json_obj = None
        self.key_elements = {}
        self.__standard_value_lists = {}

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

    @property
    def standard_value_lists(self):
        if len(self.__standard_value_lists) == 0:
            self.__read_standard_values()

        return self.__standard_value_lists

    def __read_standard_values(self):
        for _, ke in self.key_elements.items():
            assert isinstance(ke, O3KeyElement)
            for ele_attr in ke.list_attributes:
                if len(ele_attr.standard_values_list) > 0:
                    self.__standard_value_lists[ele_attr.value_name] = ele_attr.standard_values_list


if __name__ == "__main__":
    pass
