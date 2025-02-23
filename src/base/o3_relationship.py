
class O3Relationship:
    def __init__(self, item_dict, **kwargs):
        super().__init__()

        self.subject_element = item_dict['SubjectElement']
        self.relationship_category = item_dict['RelationshipCategory']
        self.predicate_element = item_dict['PredicateElement']
        self.cardinality = item_dict['Cardinality']

    def __str__(self):
        return f'{self.subject_element} {self.relationship_category} {self.predicate_element}'

if __name__ == '__main__':
    pass
