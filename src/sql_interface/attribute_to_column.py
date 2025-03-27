import warnings

from helpers.string_helpers import clean_table_and_column_names
from src.helpers.enums import SupportedSQLServers
from src.base.o3_attribute import O3Attribute
from src.sql_interface.sql_type_from_o3_data_type import sql_data_types


class AttributeToSQLColumn:
    def __init__(self, attribute: O3Attribute, phi_allowed: bool, sql_server_type: SupportedSQLServers):
        if sql_server_type not in SupportedSQLServers:
            raise KeyError(f"Provided SQL server {sql_server_type} is not supported. "
                           f"Only MSSQL and PSQL are supported.")

        self.attribute = attribute
        self.allow_phi = phi_allowed
        self.sql_server = sql_server_type
        self.column_data_type = None
        self.column_nullable = None
        self.__set_data_type_methods = [self.__set_date_for_iso8601,
                                        self.__set_standard_values_data_type,
                                        self.__set_empty_value_types,
                                        self.__set_string_data_type,
                                        self.__set_int_data_type,
                                        self.__set_decimal_data_type,
                                        self.__set_bool_data_type,
                                        self.__set_binary_data_type,
                                        self.__set_date_data_type]
        self.__set_data_types()
        self.__set_nullable()

    @property
    def column_name(self):
        if len(self.attribute.standard_values_list) > 0:
            return clean_table_and_column_names(''.join(self.attribute.string_code.split('_')[1:]) + 'Id')
        return clean_table_and_column_names(''.join(self.attribute.string_code.split('_')[1:]))

    @property
    def column_creation_text(self):
        return f'{self.column_name} {self.__sql_field_type} {self.column_nullable}'

    @property
    def __sql_field_type(self):
        data_types = sql_data_types[self.sql_server]
        return data_types[self.column_data_type]

    def __set_nullable(self):

        if self.attribute.allow_null_values in ['Yes', 'True', True, 'Yes, if diagnosis is for secondary cancer',
                                                'Yes, except when intervention is TURP']:
            self.column_nullable = 'NULL'

        if self.attribute.allow_null_values in ['No']:
            self.column_nullable = 'NOT NULL'

        if self.allow_phi:
            if self.attribute.allow_null_values == 'No for systems alloing PHI. Yes for systems not allowing PHI':
                self.column_nullable = 'NOT NULL'
            if self.attribute.allow_null_values == 'No for systems allowing PHI. Yes for systems not allowing PHI':
                self.column_nullable = 'NOT NULL'
            if self.attribute.allow_null_values == 'Yes for systems allowing PHI. No for systems not allowing PHI':
                self.column_nullable = 'NULL'
            if self.attribute.string_code == 'Patient_MRN':
                self.column_nullable = 'NOT NULL'
            if self.attribute.string_code == 'Patient_AnonPatID':
                self.column_nullable = 'NULL'
        else:
            if self.attribute.allow_null_values == 'No for systems alloing PHI. Yes for systems not allowing PHI':
                self.column_nullable = 'NULL'
            if self.attribute.allow_null_values == 'No for systems allowing PHI. Yes for systems not allowing PHI':
                self.column_nullable = 'NULL'
            if self.attribute.allow_null_values == 'Yes for systems allowing PHI. No for systems not allowing PHI':
                self.column_nullable = 'NOT NULL'
            if self.attribute.string_code == 'Patient_MRN':
                self.column_nullable = 'NULL'
            if self.attribute.string_code == 'Patient_AnonPatID':
                self.column_nullable = 'NOT NULL'

        if self.column_nullable is None:
            warnings.warn(f"No SQL nullable field set using logic. Defaulting to NULL for {self}")
            self.column_nullable = 'NULL'

    def __set_date_for_iso8601(self):
        if self.attribute.reference_system_for_values is None:
            return
        if 'ISO 8601' in self.attribute.reference_system_for_values:
            self.column_data_type = 'Date'

    def __set_standard_values_data_type(self):
        if len(self.attribute.standard_values_list) > 0:
            self.column_data_type = "String"

    def __set_empty_value_types(self):
        if self.attribute.value_data_type == "":
            self.column_data_type = "String"

    def __set_date_data_type(self):
        if "date".lower() in self.attribute.value_name.lower():
            self.column_data_type = "Date"
        if self.attribute.value_data_type == "Date":
            self.column_data_type = "Date"

    def __set_string_data_type(self):
        if "String".lower() == self.attribute.value_data_type.lower():
            self.column_data_type = "String"

    def __set_binary_data_type(self):
        if "DICOM".lower() in self.attribute.value_data_type.lower():
            self.column_data_type = "Binary"

    def __set_int_data_type(self):
        if "Integer" in self.attribute.value_data_type or "Int" in self.attribute.value_data_type:
            self.column_data_type = "Integer"

    def __set_bool_data_type(self):
        if "Boolean".lower() == self.attribute.value_data_type.lower():
            self.column_data_type = "Boolean"

    def __set_decimal_data_type(self):
        if "Decimal" in self.attribute.value_data_type or "Numeric" in self.attribute.value_data_type:
            self.column_data_type = "Decimal"

    def __set_data_types(self):
        for _method in self.__set_data_type_methods:
            _method()
            if self.column_data_type is not None:
                break


if __name__ == "__main__":
    pass
