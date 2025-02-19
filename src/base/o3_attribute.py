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


if __name__ == "__main__":
    pass
