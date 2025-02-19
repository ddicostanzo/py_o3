from base.o3_standard_value import O3StandardValue
from base.o3_element import O3Element


class O3Attribute(O3Element):
    def __init__(self, item_dict):
        super().__init__(item_dict)
        self.value_data_type = item_dict['ValueDataType']
        self.standard_values_use = item_dict['StandardValuesUse']
        self.standard_values_list = item_dict['StandardValuesList']
        self.standard_values_list = [O3StandardValue(x) for x in self.standard_values_list]
        self.reference_system_for_values = item_dict['ReferenceSystemForValues']
        self.allow_null_values = item_dict['AllowNullValues']
        self.value_example = item_dict['ValueExample']
        self.__check_reference_system(item_dict)
        self.__clean_standard_values_list()

    def __check_reference_system(self, item_dict):
        if self.reference_system_for_values is None:
            if any(['Reference System' in x for x in item_dict['StandardValuesList']]):
                for sv in item_dict['StandardValuesList']:
                    if 'Reference System' in sv:
                        self.reference_system_for_values = sv.split(': ')[-1].split('{')[0].strip()
                        break

    def __clean_standard_values_list(self):
        if len(self.standard_values_list) == 1 and 'Reference System' in self.standard_values_list[0].value_name:
            self.standard_values_list.pop(0)


if __name__ == "__main__":
    pass
