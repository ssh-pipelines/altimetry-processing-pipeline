from dataclasses import dataclass
from typing import Iterable


@dataclass
class CollectionMeta:
    source: str
    source_url: str
    reference: str


class AllCollections:
    collections: Iterable[CollectionMeta] = {
        "C2619443998-POCLOUD": CollectionMeta(
            "Sentinel-6A MF Jason-CS L2 P4 Altimeter Low Resolution (LR) NTC Reduced Ocean Surface Topography F08",
            "https://podaac.jpl.nasa.gov/dataset/JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08",
            "https://doi.org/10.5067/S6AP4-2LRNTR-F08",
        ),
        "C2619444006-POCLOUD": CollectionMeta(
            "Sentinel-6A MF Jason-CS L2 P4 Altimeter Low Resolution (LR) NTC Reduced Ocean Surface Topography (Unvalidated) F08",
            "https://podaac.jpl.nasa.gov/dataset/JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08_UNVALIDATED",
            "https://doi.org/10.5067/S6AP4-2LRNUR-F08",
        ),
        "C1968979561-POCLOUD": CollectionMeta(
            "Sentinel-6A MF Jason-CS L2 P4 Altimeter Low Resolution (LR) STC Reduced Ocean Surface Topography",
            "https://podaac.jpl.nasa.gov/dataset/JASON_CS_S6A_L2_ALT_LR_RED_OST_STC_F",
            "https://doi.org/10.5067/S6AP4-2LRST",
        ),
        "C2901523432-POCLOUD": CollectionMeta(
            "Integrated Multi-Mission Ocean Altimeter Data for Climate Research Version 5.2",
            "https://podaac.jpl.nasa.gov/dataset/MERGED_TP_J1_OSTM_OST_CYCLES_V52",
            "https://doi.org/10.5067/ALTUG-TJ152",
        ),
    }
