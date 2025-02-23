from base.o3_element import O3Element
from base.o3_attribute import O3Attribute
from base.o3_relationship import O3Relationship


class O3KeyElement(O3Element):
    def __init__(self, item_dict, **kwargs):
        super().__init__(item_dict['keyelementdetail'])

        self.supported_sql_servers = ['MSSQL', 'PSQL']

        self.key_element_name = item_dict['KeyElementName']
        self.is_longitudinal_key_element = item_dict['keyelementdetail']['IsLongitudinalKeyElement']

        self.list_attributes = []
        for this_attr in item_dict['list_attributes']:
            self.list_attributes.append(O3Attribute(this_attr, **kwargs))

        self.dictionary_attributes = {x.value_name: x for x in self.list_attributes}

        self.list_relationships = []
        for this_relationship in item_dict['list_relationships']:
            if this_relationship["SubjectElement"] == "Subject Element":
                continue
            self.list_relationships.append(O3Relationship(this_relationship, **kwargs))

    @property
    def __sql_field_name(self):
        return self.key_element_name

    def __sql_identity_field(self, sql_server):
        if sql_server not in self.supported_sql_servers:
            raise KeyError(f"Provided SQL server {sql_server} is not supported. Only MSSQL and PSQL are supported.")

        if sql_server == 'MSSQL':
            return f'{self.__sql_field_name}Id INT Identity(1, 1)'
        else:
            return f'{self.__sql_field_name}Id SERIAL PRIMARY KEY'

    def __str__(self):
        return self.key_element_name


if __name__ == "__main__":
    pass
