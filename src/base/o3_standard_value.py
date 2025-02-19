class O3StandardValue:
    def __init__(self, item):
        self.numeric_code = item.split(' ')[1].replace('{', '').replace('}', '')
        self.value_name = item.split(' ')[0]

    def __str__(self):
        return self.value_name


if __name__ == "__main__":
    pass
