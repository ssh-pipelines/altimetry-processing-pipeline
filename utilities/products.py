"""Which **Product** an artifact belongs to, and which **Published product** a
**Source** feeds.

Two resolution directions, deliberately distinct functions rather than one with
a union parameter — they take different kinds of thing and resolve by different
routes:

    along_track_product(profile)              Source   -> Product   (product_type)
    published_along_track_product(identity)   identity -> Product   (via its Sources)

`published_products:` in `utilities/products.yaml` declares only what no Source
can supply. Everything `product_type` already determines is derived here.
"""

from __future__ import annotations

from utilities.source_profile import (
    Product,
    SourceCommon,
    get_product,
    get_published_product,
    get_registered_sources,
    get_source_profile,
)

# Published identity per `product_type`. Many-to-one and one-to-one fall out of
# the same rule, so there is nothing per-source to keep in sync: a `reference`
# Source publishes to the shared reference line, a `high_latitude` Source to its
# own satellite line.
_IDENTITY_BY_PRODUCT_TYPE = {
    "reference": lambda source: "NASA-SSH",
    "high_latitude": lambda source: f"NASA-SSH_{source}",
}


def _along_track_product_name(product_type: str) -> str:
    return f"along_track_{product_type}"


def along_track_product(profile: SourceCommon) -> Product:
    """The Product a Source's along-track daily files belong to. Writers need it
    for the in-file identity attrs, not only for the filename."""
    return get_product(_along_track_product_name(profile.product_type))


def published_identity(profile: SourceCommon) -> str:
    """The published identity a Source's along-track files are released under.

    Derived from `product_type`, never declared per source: no Source's published
    identity departs from its product type, so a per-source field would be a
    hand-maintained restatement of a derivable fact.
    """
    try:
        rule = _IDENTITY_BY_PRODUCT_TYPE[profile.product_type]
    except KeyError:
        raise ValueError(
            f"Source '{profile.source}' has product_type "
            f"'{profile.product_type}', which maps to no published identity. "
            f"Known: {sorted(_IDENTITY_BY_PRODUCT_TYPE)}"
        ) from None
    return rule(profile.source)


def contributing_sources(identity: str) -> list[str]:
    """The **enabled** Sources that contribute to a published identity.

    The production-membership question: what a product-keyed run should actually
    fan out over. Use `registered_contributors` when resolving names rather than
    planning work — a staged-but-disabled Source still has a published identity.
    """
    return [s for s in registered_contributors(identity) if get_source_profile(s).enabled]


def registered_contributors(identity: str) -> list[str]:
    """Every registered Source resolving to a published identity, enabled or not."""
    matches = []
    for source in get_registered_sources():
        profile = get_source_profile(source)
        try:
            resolved = published_identity(profile)
        except ValueError:
            continue
        if resolved == identity:
            matches.append(source)
    return matches


def published_along_track_product(identity: str) -> Product:
    """The Product a published identity's along-track files belong to.

    Resolved over all *registered* Sources, not just enabled ones: resolving a
    name must not depend on a production fact, or every key function would break
    for a staged-but-disabled source — the normal state for the high-latitude
    satellites.
    """
    sources = registered_contributors(identity)
    if not sources:
        raise ValueError(
            f"No registered source resolves to published product '{identity}', "
            f"so it has no along-track product family."
        )
    products = {along_track_product(get_source_profile(s)).name for s in sources}
    if len(products) > 1:
        # Cannot fire while identity derives *from* product_type (that makes the
        # inverse a function). Written down because it pins the assumption that
        # makes the inverse well-defined.
        raise ValueError(
            f"Sources {sources} resolve to published product '{identity}' but "
            f"disagree on their along-track product: {sorted(products)}"
        )
    return get_product(products.pop())


def published_simple_grid_product(identity: str) -> Product:
    """The Product a published identity's simple grids belong to.

    Declared rather than derived: the grid a published line publishes is named
    after no single source (the reference grid is composed from several, the
    deferred composite from several lines), so `published_products:` has to say.
    """
    published = get_published_product(identity)
    if published.simple_grid is None:
        raise ValueError(
            f"Published product '{identity}' declares no simple_grid, so it "
            f"publishes no gridded product."
        )
    return get_product(published.simple_grid)


def publishes_simple_grid(identity: str) -> bool:
    """Whether a published line publishes a grid — and so has a **Product tier**.

    `published_simple_grid_product` raises both for an identity absent from
    `published_products:` and for one declaring no `simple_grid`; both mean there
    is nothing to grid, which is one answer to this question. Read by `start_run`
    for the `gridded` flag the parent branches on, and by `run_summary` to decide
    which tiers to reconcile.
    """
    try:
        published_simple_grid_product(identity)
    except ValueError:
        return False
    return True
