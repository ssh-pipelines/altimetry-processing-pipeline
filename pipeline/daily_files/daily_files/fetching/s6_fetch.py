from datetime import datetime
import logging
import re
from typing import Iterable
from daily_files.fetching.cmr_query import CMRGranule
from daily_files.fetching.podaac_s3_fetch import PodaacS3Fetcher


class S6Collection:
    shortname: str
    concept_id: str
    priority: int

    def __init__(self, shortname: str, concept_id: str, priority: int) -> None:
        self.shortname = shortname
        self.concept_id = concept_id
        self.priority = priority


class S6Collections:
    S6_COLLECTIONS: Iterable[S6Collection] = [
        S6Collection("JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08", "C2619443998-POCLOUD", 1),
        S6Collection(
            "JASON_CS_S6A_L2_ALT_LR_RED_OST_NTC_F08_UNVALIDATED",
            "C2619444006-POCLOUD",
            2,
        ),
        S6Collection("JASON_CS_S6A_L2_ALT_LR_RED_OST_STC_F", "C1968979561-POCLOUD", 3),
    ]


class S6Fetch(PodaacS3Fetcher):
    """
    Sentinel 6 data is obtained from multiple collections with different levels of processing
    validity. We want to select granules from collections with the highest level of validity available.
    This is accomplished through some collection priority logic, where a lower priority value
    is more desirable.
    """

    date: datetime
    priority_granules: dict
    granules: Iterable[CMRGranule]

    def __init__(self, date: datetime):
        """
        Sets self.granules from inhereted cmr_query method
        Uses intermediary self.priority_granules dict to select by priority
            where key is cycle_pass and value is (collection priority, CMRGranule object)
        """
        super().__init__()
        self.date = date
        self.priority_granules = {}
        self.granules = self.select_priority_granules()

    def select_priority_granules(self):
        """
        Query for multiple S6 collections and select granules based on collection
        priorities as defined in daily_files.fetching.s6_collections.S6_Collections
        """
        cycle_pass_pattern = "_\d{3}_\d{3}_"
        for collection in S6Collections.S6_COLLECTIONS:
            logging.info(f"Querying for collection {collection.shortname}")
            granules = self.cmr_query(collection.concept_id, self.date)
            for granule in granules:
                # Extract cycle_pass from granule file name
                cycle_pass = re.search(cycle_pass_pattern, granule.title).group(0)[1:-1]
                # Get current highest priority granule for this cycle_pass
                queue_status = self.priority_granules.get(cycle_pass, (100, None))
                # Update if current collection has higher priority
                if queue_status[0] > collection.priority:
                    self.priority_granules.update({cycle_pass: (collection.priority, granule)})

        # Return the list of CMR_Granule objects
        return [granule for cycle_pass, (priority_val, granule) in sorted(self.priority_granules.items())]
