from base.o3_standard_value import O3StandardValue
from base.o3_element import O3Element
import warnings


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
        self.__clean_value_data_types()
        self.__sql_data_types = {"Boolean": "bit",
                                 "DICOM Image": "varbinary",
                                 "Date": "datetime2",
                                 "Decimal": "decimal(19,9)",
                                 "Integer": "int",
                                 "String": "varchar(max)"
                                 }

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

    @property
    def sql_field_name(self):
        return ''.join(self.string_code.split('_')[1:])

    def __clean_value_data_types(self):

        if len(self.standard_values_list) > 0:
            self.value_data_type = "String"
        elif len(self.standard_values_list) == 0 and self.value_data_type == "":
            self.value_data_type = "String"
            warnings.warn(f"Setting value data type to string for attribute: {self.value_name}.", UserWarning)

        if self.value_data_type not in ['Boolean', 'DICOM Image', 'Date', 'Decimal', 'Integer', 'String']:
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
