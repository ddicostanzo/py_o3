from o3_element import O3Element
from o3_attribute import O3Attribute


class O3KeyElement(O3Element):
    def __init__(self, item_dict):
        super().__init__(item_dict['keyelementdetail'])
        self.key_element_name = item_dict['KeyElementName']
        self.is_longitudinal_key_element = item_dict['keyelementdetail']['IsLongitudinalKeyElement']

        self.list_attributes = []
        for this_attr in item_dict['list_attributes']:
            self.list_attributes.append(O3Attribute(this_attr))

        self.dictionary_attributes = {x.value_name: x for x in self.list_attributes}

    def __str__(self):
        return self.key_element_name


if __name__ == "__main__":
    pass
