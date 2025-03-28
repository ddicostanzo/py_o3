from src.base.o3_standard_value import O3StandardValue
from src.base.o3_element import O3Element
import warnings


class O3Attribute(O3Element):
    def __init__(self, key_element, item_dict, **kwargs):

        super().__init__(item_dict)

        self.key_element = key_element
        self.__possible_value_data_types = ['Boolean', 'Binary', 'Date', 'Decimal', 'Integer', 'String']
        self.value_data_type = item_dict['ValueDataType']
        self.standard_values_use = item_dict['StandardValuesUse']
        self.standard_values_list = [O3StandardValue(self.key_element,
                                                     self,
                                                     x) for x in item_dict['StandardValuesList']]
        self.reference_system_for_values = item_dict['ReferenceSystemForValues']
        self.allow_null_values = item_dict['AllowNullValues']
        self.value_example = item_dict['ValueExample']

        if kwargs.get('clean', True):
            self.__check_reference_system(item_dict)
            self.__clean_standard_values_list()
            self.__clean_value_data_types()

    def __check_reference_system(self, item_dict):
        if self.reference_system_for_values is None:
            if any(['Reference System' in x for x in item_dict['StandardValuesList']]):
                for sv in item_dict['StandardValuesList']:
                    if 'Reference System' in sv:
                        self.reference_system_for_values = sv.split(': ')[-1].split('{')[0].strip()
                        break

    def __clean_standard_values_list(self):
        for i, item in enumerate(self.standard_values_list):
            if 'Reference System' in item.value_name or 'Current ICD standard' in item.value_name:
                self.standard_values_list.pop(i)

    def __clean_value_data_types(self):
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
