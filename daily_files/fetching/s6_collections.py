from typing import Iterable

class S6Collection():
    shortname: str
    concept_id: str
    priority: int
    
    def __init__(self, shortname: str, concept_id: str, priority: int) -> None:
        self.shortname = shortname
        self.concept_id = concept_id
        self.priority = priority

class S6Collections():
    
    S6_COLLECTIONS: Iterable[S6Collection] = [
            S6Collection('JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08', 'C2619443998-POCLOUD', 1),
            S6Collection('JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08_UNVALIDATED', 'C2619444006-POCLOUD', 2),
            S6Collection('JASON_CS_S6A_L2_ALT_LR_RED_OST_STC_F', 'C1968980609-POCLOUD', 3),
        ]