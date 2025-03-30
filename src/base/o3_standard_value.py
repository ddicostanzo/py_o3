from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from base.o3_attribute import O3Attribute
    from base.o3_key_element import O3KeyElement


class O3StandardValue:
    """
    The O3 standard value class.
    """
    def __init__(self, key_element: O3KeyElement, attrib: O3Attribute,  item: str):
        self.key_element = key_element
        self.attribute = attrib
        self.numeric_code = item.split('{')[-1].replace('{', '').replace('}', '')
        if ';' in self.numeric_code:
            self.numeric_code = self.numeric_code.split(';')[0].strip()
        self.value_name = ' '.join([x.strip() for x in item.split('{')[:-1]])

    def __str__(self):
        return self.value_name

    def __repr__(self):
        return f"Value Name: {self.value_name}, Numeric Code: {self.numeric_code}"


if __name__ == "__main__":
    pass
