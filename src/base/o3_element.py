class O3Element:
    """
    The base class for O3 key elements and attributes.
    """
    def __init__(self, item_dict: dict):
        """
        Instantiates the O3 Element using the parsed JSON file's dictionary of data

        Parameters
        ----------
        item_dict: dict
            The JSON dictionary containing the O3 element data.
        """
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

    def __repr__(self):
        return (f"Value Name: {self.value_name}, Value Type: {self.value_type}, String Code: {self.string_code}, "
                f"Numeric Code: {self.numeric_code}, ValuePriority: {self.value_priority}")


if __name__ == "__main__":
    pass
