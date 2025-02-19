class O3StandardValue:
    def __init__(self, item):
        self.numeric_code = item.split('{')[-1].replace('{', '').replace('}', '')
        self.value_name = ' '.join([x.strip() for x in item.split('{')[:-1]])

    def __str__(self):
        return self.value_name


if __name__ == "__main__":
    pass
