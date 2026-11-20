"""Audit published product lines against what PO.DAAC has ingested into CMR.

Every released file in a delivered collection should be a CMR granule whose
GranuleUR is the filename without its extension. A file is reported as

    missing        no granule in CMR
    stale          the S3 object was modified after the granule was last ingested
    size mismatch  the granule's file size differs from the S3 object's
    pending        missing or stale, but published within the grace, so delivery
                   and ingest may still be under way
    CMR only       a granule with no released file (listed with --verbose)

A collection with no released files is a problem too: a delivered family whose
prefix is empty means the declaration or the layout is wrong.

The collections are the `delivery` entries in `utilities/products.yaml`. Each is
audited on its own; one that cannot be audited is reported beside the others'
results rather than stopping them. Read-only on both sides: lists the bucket and
queries the public CMR search API, which needs no Earthdata login for these
collections.

Hand runs:
    uv run python pipeline/infra/delivery_audit/audit.py --bucket nasa-ssh-staging-prod --profile <profile>
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from utilities.pipeline_layout import (
    published_along_track_prefix,
    published_indicators_prefix,
    published_simple_grid_prefix,
)
from utilities.products import registered_contributors
from utilities.source_profile import (
    Delivery,
    get_published_product,
    list_delivered_products,
    list_published_products,
)

CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
CMR_PROVIDER = "POCLOUD"
CMR_PAGE_SIZE = 2000

# Seeding products/ copied every object, so S3 LastModified on anything released
# before then is the copy time rather than its publish time. Modification times up
# to here are not evidence of a re-publication. Matches DELIVERY_NOT_BEFORE in
# delivery-scripts.
SEED_COPY_TIME = datetime(2026, 10, 26, 21, 26, tzinfo=timezone.utc)

# Problem granules listed per collection in the email before collapsing to a count.
REPORT_CAP = 50


class PartialCmrResults(RuntimeError):
    """CMR returned fewer granules than it reported. Reconciling a truncated
    listing would report real granules as missing."""


@dataclass(frozen=True)
class ReleasedFile:
    granule_ur: str
    key: str
    size: int
    last_modified: datetime


@dataclass(frozen=True)
class CmrGranule:
    granule_ur: str
    size: int | None
    revision_date: datetime


@dataclass(frozen=True)
class Collection:
    """One CMR collection and the released files it should hold."""
    published_product: str
    short_name: str
    prefix: str
    suffix: str
    name_prefix: str = ""


@dataclass
class AuditResult:
    collection: Collection
    released: int = 0
    ingested: int = 0
    missing: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    size_mismatch: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    cmr_only: list[str] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return self.released == 0

    @property
    def problems(self) -> int:
        return len(self.missing) + len(self.stale) + len(self.size_mismatch) + int(self.empty)


@dataclass
class AuditError:
    """A collection that could not be audited, reported beside the others."""
    collection: Collection
    error: str


AuditOutcome = AuditResult | AuditError


def _collection(identity: str, delivery: Delivery) -> Collection:
    if delivery.family == "along_track":
        return Collection(identity, delivery.collection, published_along_track_prefix(identity), ".nc")
    if delivery.family == "simple_grid":
        return Collection(identity, delivery.collection, published_simple_grid_prefix(identity), ".nc")
    if delivery.family == "indicators":
        # Each indicator is its own collection, holding the current file and every
        # archive/{name}/ copy as separate granules, all named after the collection.
        return Collection(
            identity, delivery.collection, published_indicators_prefix(identity), ".txt", delivery.collection
        )
    raise ValueError(f"Published product '{identity}' delivers unknown family '{delivery.family}'")


def collections_for(identity: str) -> list[Collection]:
    """The CMR collections a published line delivers to. A line with no
    `delivery` declaration is refused: nothing of it is in CMR to audit."""
    if identity in list_published_products():
        delivery = get_published_product(identity).delivery
    elif registered_contributors(identity):
        # An along-track-only line like NASA-SSH_S3B has no published_products:
        # entry, and so no delivery.
        delivery = ()
    else:
        raise ValueError(f"No registered source resolves to published product '{identity}'.")
    if not delivery:
        raise ValueError(f"Published product '{identity}' declares no PO.DAAC delivery.")
    return [_collection(identity, d) for d in delivery]


def resolve_collections(published_products: list[str] | None = None) -> list[Collection]:
    """Collections for the named lines, or for every delivering line when none
    are named, so a line declared for delivery is audited without a schedule edit."""
    identities = list_delivered_products() if published_products is None else published_products
    return [c for identity in identities for c in collections_for(identity)]


def list_released(s3_client, bucket: str, collection: Collection) -> dict[str, ReleasedFile]:
    files = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=collection.prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if not name.endswith(collection.suffix) or not name.startswith(collection.name_prefix):
                continue
            ur = name[: -len(collection.suffix)]
            if ur in files:
                # Delivery flattens paths, so a second file with this name would
                # overwrite the first at PO.DAAC.
                raise ValueError(f"{obj['Key']} and {files[ur].key} share a granule name")
            files[ur] = ReleasedFile(ur, obj["Key"], obj["Size"], obj["LastModified"])
    return files


def _parse_cmr_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _granule_from_umm(item: dict) -> CmrGranule:
    umm = item["umm"]
    ur = umm["GranuleUR"]
    size = None
    for f in umm.get("DataGranule", {}).get("ArchiveAndDistributionInformation", []):
        # The data file shares the GranuleUR stem; the other entry is its .md5.
        if f.get("Name", "").startswith(ur) and not f["Name"].endswith(".md5"):
            size = f.get("SizeInBytes")
            break
    return CmrGranule(ur, size, _parse_cmr_time(item["meta"]["revision-date"]))


def fetch_cmr_granules(session, short_name: str) -> dict[str, CmrGranule]:
    granules = {}
    fetched = 0
    hits = None
    headers = {}
    while True:
        resp = session.get(
            CMR_GRANULES_URL,
            params={"short_name": short_name, "provider": CMR_PROVIDER, "page_size": CMR_PAGE_SIZE},
            headers=headers,
            timeout=120,
        )
        resp.raise_for_status()
        if hits is None:
            hits = int(resp.headers["CMR-Hits"])
        items = resp.json().get("items", [])
        fetched += len(items)
        for item in items:
            g = _granule_from_umm(item)
            granules[g.granule_ur] = g
        search_after = resp.headers.get("CMR-Search-After")
        if len(items) < CMR_PAGE_SIZE or not search_after:
            break
        headers = {"CMR-Search-After": search_after}
    if fetched != hits or len(granules) != hits:
        raise PartialCmrResults(
            f"{short_name}: CMR reported {hits} granules, returned {fetched} ({len(granules)} distinct)"
        )
    return granules


def reconcile(
    collection: Collection,
    released: dict[str, ReleasedFile],
    ingested: dict[str, CmrGranule],
    now: datetime,
    grace: timedelta,
    not_before: datetime = SEED_COPY_TIME,
) -> AuditResult:
    result = AuditResult(collection, released=len(released), ingested=len(ingested))
    for ur in sorted(released):
        f = released[ur]
        g = ingested.get(ur)
        if g is None:
            (result.pending if now - f.last_modified < grace else result.missing).append(ur)
            continue
        if f.last_modified > not_before and f.last_modified > g.revision_date:
            (result.pending if now - f.last_modified < grace else result.stale).append(ur)
            continue
        if g.size is not None and g.size != f.size:
            result.size_mismatch.append(ur)
    result.cmr_only = sorted(set(ingested) - set(released))
    return result


def audit_collection(
    collection: Collection, s3_client, bucket: str, http_session, now: datetime, grace: timedelta
) -> AuditResult:
    released = list_released(s3_client, bucket, collection)
    ingested = fetch_cmr_granules(http_session, collection.short_name)
    return reconcile(collection, released, ingested, now, grace)


def run_audit(
    collections: list[Collection], s3_client, bucket: str, http_session, now: datetime, grace: timedelta
) -> list[AuditOutcome]:
    outcomes: list[AuditOutcome] = []
    for collection in collections:
        try:
            outcomes.append(audit_collection(collection, s3_client, bucket, http_session, now, grace))
        except Exception as e:
            outcomes.append(AuditError(collection, f"{type(e).__name__}: {e}"))
    return outcomes


def _listed(label: str, urs: list[str], cap: int) -> list[str]:
    lines = [f"  {label}: {ur}" for ur in urs[:cap]]
    if len(urs) > cap:
        lines.append(f"  {label}: ... {len(urs) - cap} more ({len(urs)} total)")
    return lines


def render_report(
    outcomes: list[AuditOutcome], bucket: str, grace: timedelta, verbose: bool = False, cap: int = REPORT_CAP
) -> tuple[str, str]:
    """The notification subject and body. Errors lead, then problems, then the
    per-collection counts, so a clean collection never pushes a problem off-screen."""
    errors = [o for o in outcomes if isinstance(o, AuditError)]
    results = [o for o in outcomes if isinstance(o, AuditResult)]
    problems = sum(r.problems for r in results)
    lines_ = sorted({o.collection.published_product for o in outcomes})

    if errors:
        subject = f"Delivery audit incomplete: {len(errors)} of {len(outcomes)} collections not audited"
    elif problems:
        subject = f"Delivery audit: {problems} problems"
    else:
        subject = "Delivery audit clean"

    body = [
        f"Published products: {', '.join(lines_)}",
        f"Bucket: {bucket}",
        f"Grace: {grace.total_seconds() / 3600:g} h",
        "",
    ]
    for e in errors:
        body.append(f"{e.collection.short_name}: could not audit: {e.error}")
    if errors:
        body.append("")

    for r in results:
        c = r.collection
        body.append(
            f"{c.short_name}: released {r.released}, in CMR {r.ingested}, missing {len(r.missing)}, "
            f"stale {len(r.stale)}, size mismatch {len(r.size_mismatch)}, pending {len(r.pending)}, "
            f"CMR only {len(r.cmr_only)}"
        )
        if r.empty:
            body.append(f"  no released files under {c.prefix}")
        body += _listed("missing", r.missing, cap)
        body += _listed("stale", r.stale, cap)
        body += _listed("size mismatch", r.size_mismatch, cap)
        if verbose:
            body += _listed("pending", r.pending, cap)
            body += _listed("CMR only", r.cmr_only, cap)

    body.append("")
    if errors:
        body.append(f"{len(errors)} collections could not be audited; {problems} problems in the rest")
    elif problems:
        body.append(f"{problems} problems")
    else:
        body.append("Audit clean")
    return subject, "\n".join(body)


def main(argv: list[str] | None = None, s3_client=None, http_session=None, now: datetime | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit released products against PO.DAAC's CMR granules.")
    parser.add_argument("--bucket", required=True, help="Bucket holding products/ (e.g. nasa-ssh-staging-prod)")
    parser.add_argument(
        "--product",
        action="append",
        dest="products",
        help="Published product line to audit; repeatable (default: every line declaring a delivery)",
    )
    parser.add_argument("--profile", default=None, help="AWS profile name")
    parser.add_argument(
        "--grace-hours",
        type=float,
        default=12,
        help="Files published within this many hours are pending rather than missing or stale (default: 12)",
    )
    parser.add_argument("--verbose", action="store_true", help="Also list pending and CMR-only granules")
    args = parser.parse_args(argv)

    try:
        collections = resolve_collections(args.products)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    if s3_client is None:
        import boto3

        s3_client = boto3.Session(profile_name=args.profile).client("s3")
    if http_session is None:
        import requests

        http_session = requests.Session()
    now = now or datetime.now(timezone.utc)
    grace = timedelta(hours=args.grace_hours)

    outcomes = run_audit(collections, s3_client, args.bucket, http_session, now, grace)
    _, body = render_report(outcomes, args.bucket, grace, verbose=args.verbose, cap=sys.maxsize)
    print(body)
    if any(isinstance(o, AuditError) for o in outcomes):
        return 2
    return 1 if any(o.problems for o in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
