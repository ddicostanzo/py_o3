class O3StandardValue:
    def __init__(self, key_element, attrib,  item):
        self.key_element = key_element
        self.attribute = attrib
        self.numeric_code = item.split('{')[-1].replace('{', '').replace('}', '')
        if ';' in self.numeric_code:
            self.numeric_code = self.numeric_code.split(';')[0].strip()
        self.value_name = ' '.join([x.strip() for x in item.split('{')[:-1]])

    def __str__(self):
        return self.value_name


if __name__ == "__main__":
    pass
