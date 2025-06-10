from base.o3_element import O3Element
from base.o3_attribute import O3Attribute
from base.o3_relationship import O3Relationship


class O3KeyElement(O3Element):
    """
    The Key Element class. Instantiates an object using the parsed JSON dictionary. Will create attributes
    and relationships.
    """

    def __init__(self, item_dict: dict, **kwargs):
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
            self.list_attributes.append(O3Attribute(self, this_attr, **kwargs))

        self.dictionary_attributes = {x.value_name: x for x in self.list_attributes}

        self.relationships = []
        for this_relationship in item_dict['list_relationships']:
            if this_relationship["SubjectElement"] == "Subject Element":
                continue
            self.relationships.append(O3Relationship(this_relationship, **kwargs))

    def __str__(self):
        return self.key_element_name

    def __repr__(self):
        return (f"Value Name: {self.key_element_name}, Value Type: {self.value_type}, "
                f"String Code: {self.string_code}, Numeric Code: {self.numeric_code}, "
                f"ValuePriority: {self.value_priority}")

    @property
    def child_of_relationships(self):
        """
        The relationships of this Key Element where the subject element matches this string code and the
        relationship category is "ChildElement-Of".

        Returns
        -------
            list[O3Relationship]
                The child of relationships for this key element
        """
        _child_of_relationships = []
        for this_relationship in self.relationships:
            if this_relationship.relationship_category == "ChildElement-Of":
                if this_relationship.subject_element == self.string_code:
                    _child_of_relationships.append(this_relationship)

        return _child_of_relationships

    @property
    def instance_of_relationships(self):
        """
        The relationships of this Key Element where the subject element matches this string code and the
        relationship category is "InstanceAssociated-with".

        Returns
        -------
            list[O3Relationship]
                the InstanceAssociated-with relationships for this key element
        """
        _instance_of_relationships = []
        for this_relationship in self.relationships:
            if this_relationship.relationship_category == "InstanceAssociated-with":
                if this_relationship.predicate_element == self.string_code:
                    _instance_of_relationships.append(this_relationship)

        return _instance_of_relationships


if __name__ == "__main__":
    pass
