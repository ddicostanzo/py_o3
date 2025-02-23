from src.base.o3_key_element import O3KeyElement


class O3Relationship:
    def __init__(self, item_dict, **kwargs):
        super().__init__(item_dict)

        self.subject_element = item_dict['SubjectElement']
        self.relationship_category = item_dict['RelationshipCategory']
        self.predicate_element = item_dict['PredicateElement']
        self.cardinality = item_dict['Cardinality']