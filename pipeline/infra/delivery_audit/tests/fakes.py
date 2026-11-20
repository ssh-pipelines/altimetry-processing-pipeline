"""Offline stand-ins for S3 and CMR. A fake paginator serves bucket listings and
a fake HTTP session serves UMM-JSON pages, so nothing leaves the process."""

from datetime import timedelta

from audit import SEED_COPY_TIME

AT = "NASA-SSH_alt_ref_at_v1_1_"
SG = "NASA-SSH_alt_ref_simple_grid_v1_1_"
INDICATORS = ("GMSL", "ENSO", "IOD", "PDO")
OLD = SEED_COPY_TIME - timedelta(days=30)


def umm_item(ur, size=100, revision="2026-11-16T16:23:49.169Z", ext=".nc"):
    return {
        "meta": {"revision-date": revision},
        "umm": {
            "GranuleUR": ur,
            "DataGranule": {
                "ArchiveAndDistributionInformation": [
                    {"Name": f"{ur}{ext}.md5", "SizeInBytes": 70},
                    {"Name": f"{ur}{ext}", "SizeInBytes": size},
                ]
            },
        },
    }


class FakeS3:
    def __init__(self, keys, fail_prefixes=()):
        # keys: {key: (size, last_modified)}
        self.keys = keys
        self.fail_prefixes = fail_prefixes

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        if Prefix in self.fail_prefixes:
            raise RuntimeError(f"AccessDenied listing {Prefix}")
        contents = [
            {"Key": k, "Size": s, "LastModified": t}
            for k, (s, t) in sorted(self.keys.items())
            if k.startswith(Prefix)
        ]
        # Two pages, so callers must not assume one.
        half = len(contents) // 2
        return [{"Contents": contents[:half]}, {"Contents": contents[half:]}, {}]


class FakeResponse:
    def __init__(self, items, hits, search_after=None, status=200):
        self._items = items
        self.status = status
        self.headers = {"CMR-Hits": str(hits)}
        if search_after:
            self.headers["CMR-Search-After"] = search_after

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"{self.status} Server Error")

    def json(self):
        return {"items": self._items}


class FakeSession:
    def __init__(self, by_short_name, hits=None, status=None):
        # by_short_name: {short_name: [page_of_items, ...]}
        # hits: {short_name: reported CMR-Hits}, defaulting to the true total
        # status: {short_name: HTTP status} to fail a collection
        self.pages = by_short_name
        self.hits = hits or {}
        self.status = status or {}
        self.calls = []

    def get(self, url, params, headers, timeout):
        short_name = params["short_name"]
        self.calls.append((short_name, dict(headers)))
        pages = self.pages.get(short_name, [[]])
        idx = int(headers.get("CMR-Search-After", "0"))
        more = idx + 1 < len(pages)
        hits = self.hits.get(short_name, sum(len(p) for p in pages))
        return FakeResponse(
            pages[idx], hits, str(idx + 1) if more else None, self.status.get(short_name, 200)
        )


def reference_keys():
    """A clean NASA-SSH line: one file per collection, all ingested."""
    p = "products/NASA-SSH/"
    k = {
        f"{p}along_track/2026/{AT}20261112.nc": (100, OLD),
        f"{p}simple_grid/2026/{SG}20261109.nc": (100, OLD),
        f"{p}indicators/indicators.nc": (100, OLD),
    }
    for ind in INDICATORS:
        k[f"{p}indicators/NASA_SSH_{ind}_INDICATOR.txt"] = (100, OLD)
        k[f"{p}indicators/NASA_SSH_{ind}_INDICATOR.mp"] = (100, OLD)
    return k


def reference_cmr():
    pages = {
        "NASA_SSH_REF_ALONGTRACK_V11": [[umm_item(f"{AT}20261112")]],
        "NASA_SSH_REF_SIMPLE_GRID_V11": [[umm_item(f"{SG}20261109")]],
    }
    for ind in INDICATORS:
        pages[f"NASA_SSH_{ind}_INDICATOR"] = [[umm_item(f"NASA_SSH_{ind}_INDICATOR", ext=".txt")]]
    return pages

