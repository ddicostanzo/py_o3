from __future__ import annotations
import warnings

from helpers.string_helpers import leave_only_letters_numbers_or_underscore
from sql_interface.data_model_to_sql.sql_type_from_o3_data_type import sql_data_types
from src.helpers.enums import SupportedSQLServers

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.base.o3_attribute import O3Attribute


class AttributeToSQLColumn:
    """
    The class that handles conversion of O3 attributes to SQL columns.
    """
    def __init__(self, attribute: "O3Attribute", phi_allowed: bool, sql_server_type: SupportedSQLServers):
        """
        Instantiates the object that will convert the Attribute to a SQL column.

        Parameters
        ----------
        attribute: O3Attribute
            The Attribute to convert.
        phi_allowed: bool
            the flag for allowing PHI in the database
        sql_server_type: SupportedSQLServers
            the SQL server type
        """
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
    def column_name(self) -> str:
        """
        The column name of this attribute
        """
        if len(self.attribute.standard_values_list) > 0:
            return leave_only_letters_numbers_or_underscore(''.join(self.attribute.string_code.split('_')[1:]) + 'Id')
        return leave_only_letters_numbers_or_underscore(''.join(self.attribute.string_code.split('_')[1:]))

    @property
    def column_creation_text(self) -> str:
        """
        The SQL command to create the column.
        """
        return f'{self.column_name} {self.__sql_field_type} {self.column_nullable}'

    @property
    def __sql_field_type(self) -> str:
        """
        The SQL column data type of this attribute
        """
        data_types = sql_data_types[self.sql_server]
        return data_types[self.column_data_type]

    def __set_nullable(self) -> None:
        """
        Sets the column's nullable flag based on different logic associated with PHI and the attribute setting
        """
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

    def __set_date_for_iso8601(self) -> None:
        """
        Sets the column data type to Date when the reference system includes ISO 8601
        """
        if self.attribute.reference_system_for_values is None:
            return
        if 'ISO 8601' in self.attribute.reference_system_for_values:
            self.column_data_type = 'Date'

    def __set_standard_values_data_type(self) -> None:
        """
        Sets the column data type to String if the standard value list has values
        """
        if len(self.attribute.standard_values_list) > 0:
            self.column_data_type = "String"

    def __set_empty_value_types(self) -> None:
        """
        Sets the column data type to String if value data type is blank
        """
        if self.attribute.value_data_type == "":
            self.column_data_type = "String"

    def __set_date_data_type(self) -> None:
        """
        If the attribute value name includes the word date, change the column_data_type to Date. If the
        attribute value data type is Date, set the column_data_type to Date.
        """
        if "date" in self.attribute.value_name.lower():
            self.column_data_type = "Date"
        if self.attribute.value_data_type == "Date":
            self.column_data_type = "Date"

    def __set_string_data_type(self) -> None:
        """
        If the value data type of the attribute is of type "String" set the column_data_type to String
        """
        if "string" == self.attribute.value_data_type.lower():
            self.column_data_type = "String"

    def __set_binary_data_type(self) -> None:
        """
        If the value data type of the attribute is set to DICOM, set the column_data_type to Binary
        """
        if "dicom" in self.attribute.value_data_type.lower():
            self.column_data_type = "Binary"

    def __set_int_data_type(self) -> None:
        """
        If the attribute value data type is Int or Integer, set the column_data_type to Integer
        """
        if "Integer" in self.attribute.value_data_type or "Int" in self.attribute.value_data_type:
            self.column_data_type = "Integer"

    def __set_bool_data_type(self) -> None:
        """
        If the attribute value data type is set to Boolean, set the column_data_type to Boolean
        """
        if "boolean" == self.attribute.value_data_type.lower():
            self.column_data_type = "Boolean"

    def __set_decimal_data_type(self) -> None:
        """
        If the attribute value data type is set to Decimal or Numeric, set the column_data_type to Decimal
        """
        if "decimal" in self.attribute.value_data_type.lower() or "numeric" in self.attribute.value_data_type.lower():
            self.column_data_type = "Decimal"

    def __set_data_types(self) -> None:
        """
        Using the list of methods to set the column data type, loop through them all to set the appropriate column
        data type.
        """
        for _method in self.__set_data_type_methods:
            _method()
            if self.column_data_type is not None:
                break


if __name__ == "__main__":
    pass
