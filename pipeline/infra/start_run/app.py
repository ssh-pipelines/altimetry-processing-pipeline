"""Open a product-keyed pipeline run.

The trigger names a **Published product**; everything the parent state machine
needs to fan out over that line is derived here, because this is the only state
that runs exactly once before either tier and ASL can read neither the Source
registry nor a clock:

  - **contributors** — the `enabled` Sources feeding the line. A thin wrapper
    over `utilities.products.contributing_sources`, not a second implementation
    of the derivation rule.
  - **run_id** — minted once and handed to both tiers, so a run is traceable
    end to end across `pipeline_runs/{source}/` and `pipeline_runs/{product}/`.
    pipeline_init used to mint its own, which was sufficient only while one run
    meant one source.
  - **product_jobs_key** — the product tier's manifest address. Computed here
    rather than in `merge_manifests` so that Lambda needs no layout knowledge
    and can stay a zip target.
  - **gridded** — whether this line has a product tier at all. A single
    high-latitude satellite publishes along-track only, and the Choice that
    skips the gridded tier for it cannot consult `published_products:` itself.

This is also where the run's invocation parameters are recorded, since the
parent is where they arrive. Each pipeline_init writes its own sidecar for what
the window resolved to per Source; this one records how the run was *asked* for.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

from utilities.pipeline_layout import jobs_manifest_key, run_params_key
from utilities.products import (
    contributing_sources,
    publishes_simple_grid,
    registered_contributors,
)

s3 = boto3.client("s3")


def resolve_contributors(published_product: str) -> list[str]:
    """The enabled Sources feeding a published line.

    Both empty cases raise, and they are distinguished because the fixes differ.
    Neither can be allowed to return an empty fan-out: a run over zero
    contributors succeeds having produced nothing, and reads downstream as a
    quiet week rather than as a misconfiguration.
    """
    contributors = contributing_sources(published_product)
    if contributors:
        return contributors

    registered = registered_contributors(published_product)
    if not registered:
        raise ValueError(
            f"No registered source resolves to published product "
            f"'{published_product}'. Check the trigger input against the "
            f"source registry's product_type rules."
        )
    raise ValueError(
        f"Published product '{published_product}' has registered sources "
        f"{registered} but none are enabled, so the run would fan out over "
        f"nothing. Set `enabled: true` on the contributor that should run."
    )


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event.get("bucket") or os.environ.get("BUCKET_NAME")
    if bucket is None:
        raise ValueError("bucket not provided in event or BUCKET_NAME env var.")

    published_product = event.get("published_product")
    if published_product is None:
        raise ValueError(
            "published_product job parameter missing. Triggers are product-keyed; "
            "a `source` input is the pre-Phase-4 shape."
        )

    contributors = resolve_contributors(published_product)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    force_update = event.get("force_update", False)

    envelope = {
        "published_product": published_product,
        "bucket": bucket,
        "run_id": run_id,
        "contributors": contributors,
        "product_jobs_key": jobs_manifest_key(published_product, run_id),
        # The product tier exists iff the line publishes a grid; `run_summary`
        # reads the same predicate to decide which tiers to reconcile.
        "gridded": publishes_simple_grid(published_product),
        "start": event.get("start"),
        "end": event.get("end"),
        "force_update": force_update,
    }

    logging.info(
        f"Run {run_id} for {published_product}: contributors={contributors}, "
        f"gridded={envelope['gridded']}"
    )

    params_key = run_params_key(published_product, run_id)
    s3.put_object(
        Bucket=bucket,
        Key=params_key,
        Body=json.dumps(
            {
                "published_product": published_product,
                "run_id": run_id,
                "contributors": contributors,
                "start": event.get("start"),
                "end": event.get("end"),
                "force_update": force_update,
                "defaults_used": not (event.get("start") or event.get("end")),
            }
        ),
        ContentType="application/json",
    )
    logging.info(f"Wrote run params to s3://{bucket}/{params_key}")

    return envelope
