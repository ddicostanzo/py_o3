from base.o3_key_element import O3KeyElement
import json
import os


class O3DataModel:
    def __init__(self, json_file):
        super().__init__()

        assert os.path.exists(json_file), f"File not found: {json_file}"
        assert os.path.isfile(json_file), f"Is not a file: {json_file}"

        self.json_file = json_file
        self.key_elements = {}
        self.json_obj = None
        self.__read_json()
        self.__load_elements()

    def __read_json(self):
        with open(self.json_file) as file:
            self.json_obj = json.load(file)

    def __load_elements(self):
        for obj in self.json_obj:
            _element = O3KeyElement(obj)
            self.key_elements[_element.key_element_name] = _element


if __name__ == "__main__":
    pass
