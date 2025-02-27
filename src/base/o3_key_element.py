from base.o3_element import O3Element
from base.o3_attribute import O3Attribute
from base.o3_relationship import O3Relationship


class O3KeyElement(O3Element):

    def __init__(self, item_dict, **kwargs):
        """
        Instantiation of the O3 Key Element class

        Parameters
        ----------
        item_dict
            refers to the deserialized JSON item dictionary associated with the specific element
        kwargs
            clean: bool
                provides an avenue to attempt to clean common errors in the typed data of the key element and
                its attributes
        """
        super().__init__(item_dict['keyelementdetail'])

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

    def __str__(self):
        return self.key_element_name


if __name__ == "__main__":
    pass
