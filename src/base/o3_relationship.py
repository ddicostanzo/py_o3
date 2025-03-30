
class O3Relationship:
    """
    The O3 relationship class.
    """
    def __init__(self, item_dict: dict, **kwargs):
        """
        Instantiates an O3 relationship object.

        Parameters
        ----------
        item_dict: dict
            the JSON parsed dictionary object containing the relationship data.

        kwargs
            unused at present
        """
        super().__init__()

        self.subject_element = item_dict['SubjectElement']
        self.relationship_category = item_dict['RelationshipCategory']
        self.predicate_element = item_dict['PredicateElement']
        self.cardinality = item_dict['Cardinality']

    def __str__(self):
        return f'{self.subject_element} {self.relationship_category} {self.predicate_element}'

    def __repr__(self):
        return (f'Subject Element: {self.subject_element}, Relationship Category: {self.relationship_category}, '
                f'Predicate Element: {self.predicate_element}, Cardinality: {self.cardinality}')


if __name__ == '__main__':
    pass
