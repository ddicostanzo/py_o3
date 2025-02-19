class O3Element:
    def __init__(self, item_dict):
        self.value_name = item_dict['ValueName']
        self.value_type = item_dict['ValueType']
        self.string_code = item_dict['StringCode']
        self.numeric_code = item_dict['NumericCode']
        self.definition = item_dict['Definition']
        self.value_priority = item_dict['ValuePriority']
        self.more_than_one_value_allowed = item_dict['MoreThanOneValueAllowed']
        self.sct_id = item_dict['SCTID']
        self.ncitc = item_dict['NCITC']
        self.ncimt = item_dict['NCIMT']

    def __str__(self):
        return self.value_name


if __name__ == "__main__":
    pass
