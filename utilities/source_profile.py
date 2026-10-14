from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as date_type
from functools import lru_cache
from pathlib import Path
from typing import Type, TypeVar

import yaml

SOURCES_DIR = Path(__file__).parent / "sources"
PRODUCTS_PATH = Path(__file__).parent / "products.yaml"


@dataclass(frozen=True)
class CollectionConfig:
    """One entry in a source's collections list. Fields default to empty/None
    because not every source uses every field (CMR vs THREDDS vs S3, with
    metadata fields varying by source documentation availability)."""
    shortname: str = ""
    concept_id: str = ""
    priority: int = 1
    thredds_collection: str | None = None
    thredds_version: str | None = None
    source_label: str = ""
    source_url: str = ""
    reference: str = ""


# The CF global attributes a product owns. `product_version` is derived from
# `version` rather than declared, so the filename and the in-file version cannot
# disagree. `references` / `source_url` are product-owned on the gridded path and
# source-filled on the along-track path, so both families declare them (empty is
# legal for these two — see ALLOW_EMPTY_GLOBAL_ATTRS in the daily_files schema).
PRODUCT_GLOBAL_ATTRS = (
    "title",
    "summary",
    "id",
    "processing_level",
    "product_short_name",
    "product_version",
    "references",
    "source_url",
)


@dataclass(frozen=True)
class Product:
    """A produced data artifact's wire format. Versions, filename templates, and
    in-file identity are owned by the product, not by individual sources —
    multiple sources contribute to the same product and must share its naming
    convention and its identity.

    Filename templates accept `{source}`, `{version}`, and `{YYYYMMDD}`
    placeholders.

    The identity fields default to empty so a product whose artifacts carry no CF
    identity (the ENSO grids) needs no declaration; every product that writes
    them declares them all, pinned by a registry test.
    """
    name: str
    version: str
    filename_template: str
    title: str = ""
    summary: str = ""
    id: str = ""
    processing_level: str = ""
    product_short_name: str = ""
    references: str = ""
    source_url: str = ""

    @property
    def product_version(self) -> str:
        """The CF `product_version` spelling of `version`: `v1_1` → `V1.1`."""
        return "V" + self.version.lstrip("vV").replace("_", ".")

    def global_attrs(self) -> dict[str, str]:
        """The product-owned CF global attributes, for a writer to merge over the
        attrs shared by every NASA-SSH product."""
        return {attr: getattr(self, attr) for attr in PRODUCT_GLOBAL_ATTRS}


@dataclass(kw_only=True, frozen=True)
class SourceCommon:
    """Source-identity fields. Every stage's config inherits from this."""
    source: str
    product_type: str
    discovery_type: str = "cmr"
    unify: bool = False
    start_date: date_type
    end_date: date_type | None = None
    collections: list[CollectionConfig] = field(default_factory=list)
    source_bucket: str | None = None
    source_prefix_pattern: str | None = None
    source_filename_pattern: str | None = None
    cycle_index_key: str | None = None
    ground_speed: float = 5.745
    intermission_bias: float = 0.0


T = TypeVar("T", bound=SourceCommon)


@lru_cache(maxsize=None)
def _load_products_raw() -> dict:
    with open(PRODUCTS_PATH) as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=None)
def get_product(name: str) -> Product:
    raw = _load_products_raw()
    products = raw.get("products", {})
    if name not in products:
        raise ValueError(
            f"Product '{name}' not configured. Available: {sorted(products)}"
        )
    return Product(name=name, **products[name])


@lru_cache(maxsize=None)
def _load_yaml(source: str) -> dict:
    path = SOURCES_DIR / f"{source}.yaml"
    if not path.exists():
        available = sorted(p.stem for p in SOURCES_DIR.glob("*.yaml"))
        raise ValueError(
            f"Source '{source}' is not configured (no {path.name}). "
            f"Available: {available}"
        )
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _parse_common_section(common: dict) -> dict:
    """Apply lightweight type conversions to a 'common' YAML section."""
    parsed = {**common}
    if "start_date" in parsed and isinstance(parsed["start_date"], str):
        parsed["start_date"] = date_type.fromisoformat(parsed["start_date"])
    if "end_date" in parsed and isinstance(parsed["end_date"], str):
        parsed["end_date"] = date_type.fromisoformat(parsed["end_date"])
    if "collections" in parsed and parsed["collections"] is not None:
        parsed["collections"] = [
            CollectionConfig(**c) for c in parsed["collections"]
        ]
    return parsed


def load_source_config(
    dataclass_cls: Type[T],
    stage: str | None,
    source: str,
) -> T:
    """Load and merge a source's profile into the given stage dataclass.

    Reads utilities/sources/{source}.yaml, merges `common` + (optional) stage
    section, and instantiates dataclass_cls(**merged). Vanilla dataclass
    construction provides validation: missing required fields raise TypeError.
    """
    raw = _load_yaml(source)
    common = _parse_common_section(raw.get("common", {}) or {})
    stage_section = (raw.get(stage) or {}) if stage else {}

    merged = {"source": source, **common, **stage_section}
    return dataclass_cls(**merged)


def get_source_profile(source: str) -> SourceCommon:
    """Convenience: load just the source-identity fields (no stage section)."""
    return load_source_config(SourceCommon, None, source)


def list_sources_for_stage(stage: str | None) -> list[str]:
    """Return source names whose YAML has a section for this stage (or all
    sources if stage is None)."""
    sources = []
    for path in sorted(SOURCES_DIR.glob("*.yaml")):
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        if stage is None or stage in raw:
            sources.append(path.stem)
    return sources


def get_registered_sources() -> list[str]:
    """All sources known to the registry."""
    return list_sources_for_stage(None)


def clear_caches() -> None:
    """Test helper: clear all module-level caches."""
    _load_yaml.cache_clear()
    _load_products_raw.cache_clear()
    get_product.cache_clear()
