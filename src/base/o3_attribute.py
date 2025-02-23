from base.o3_standard_value import O3StandardValue
from base.o3_element import O3Element
import warnings


class O3Attribute(O3Element):
    def __init__(self, item_dict, **kwargs):

        super().__init__(item_dict)

        self.__possible_value_data_types = ['Boolean', 'Binary', 'Date', 'Decimal', 'Integer', 'String']
        self.value_data_type = item_dict['ValueDataType']
        self.sql_data_type = None
        self.standard_values_use = item_dict['StandardValuesUse']
        self.standard_values_list = item_dict['StandardValuesList']
        self.standard_values_list = [O3StandardValue(x) for x in self.standard_values_list]
        self.reference_system_for_values = item_dict['ReferenceSystemForValues']
        self.allow_null_values = item_dict['AllowNullValues']
        self.value_example = item_dict['ValueExample']
        self.__set_sql_data_type_methods = [self.__set_date_for_iso8601,
                                            self.__set_standard_values_sql,
                                            self.__set_empty_value_types,
                                            self.__set_date_sql_data_type,
                                            self.__set_string_sql_data_type,
                                            self.__set_int_sql_data_type,
                                            self.__set_decimal_sql_data_type,
                                            self.__set_bool_sql_data_type,
                                            self.__set_binary_sql_data_type]

        if kwargs.get('clean', True):
            self.__check_reference_system(item_dict)
            self.__clean_standard_values_list()
            self.__clean_value_data_types()

        self.__set_sql_data_types()

        self.__sql_data_types = {"MSSQL": {"Boolean": "bit",
                                           "Binary": "varbinary",
                                           "Date": "datetime2",
                                           "Decimal": "decimal(19,9)",
                                           "Integer": "int",
                                           "String": "varchar(max)"
                                           },
                                 "PSQL": {"Boolean": "boolean",
                                          "Binary": "bytea",
                                          "Date": "timestamptz",
                                          "Decimal": "numeric(19,9)",
                                          "Integer": "integer",
                                          "String": "text"
                                          }
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
    def __sql_field_name(self):
        return ''.join(self.string_code.split('_')[1:])

    def __sql_field_type(self, sql_server):
        if sql_server not in self.__sql_data_types.keys():
            raise KeyError(f"Provided SQL server {sql_server} is not supported. Only MSSQL and PSQL are supported.")
        data_types = self.__sql_data_types[sql_server]
        return data_types[self.sql_data_type]

    def __set_date_for_iso8601(self):
        if self.reference_system_for_values is None:
            return
        if 'ISO 8601' in self.reference_system_for_values:
            self.sql_data_type = 'Date'

    def __set_standard_values_sql(self):
        if len(self.standard_values_list) > 0:
            self.sql_data_type = "String"

    def __set_empty_value_types(self):
        if self.value_data_type == "":
            self.sql_data_type = "String"

    def __set_date_sql_data_type(self):
        if "date".lower() in self.value_name.lower():
            self.sql_data_type = "Date"
        if self.value_data_type == "Date":
            self.sql_data_type = "Date"

    def __set_string_sql_data_type(self):
        if "String".lower() == self.value_data_type.lower():
            self.sql_data_type = "String"

    def __set_binary_sql_data_type(self):
        if "DICOM".lower() in self.value_data_type.lower():
            self.sql_data_type = "Binary"

    def __set_int_sql_data_type(self):
        if "Integer" in self.value_data_type or "Int" in self.value_data_type:
            self.sql_data_type = "Integer"

    def __set_bool_sql_data_type(self):
        if "Boolean".lower() == self.value_data_type.lower():
            self.sql_data_type = "Boolean"

    def __set_decimal_sql_data_type(self):
        if "Decimal" in self.value_data_type or "Numeric" in self.value_data_type:
            self.sql_data_type = "Decimal"

    def __set_sql_data_types(self):
        for _method in self.__set_sql_data_type_methods:
            _method()
            if self.sql_data_type is not None:
                break

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

    def create_sql_field_text(self, sql_server):
        return f'{self.__sql_field_name} {self.__sql_field_type(sql_server)}'


if __name__ == "__main__":
    pass
