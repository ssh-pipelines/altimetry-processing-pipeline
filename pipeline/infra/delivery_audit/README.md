# delivery_audit

Weekly audit of released products against PO.DAAC's CMR granules. See
[ADR 0013](../../../docs/adr/0013-podaac-delivery-in-pipeline.md).

Not part of the pipeline. An EventBridge Scheduler schedule invokes it, mirrored as
`delivery-audit` under `stage_schedules:` in `utilities/schedules.yaml`. Ingest lags delivery
by hours, so an audit at the end of a run would see that run's files as pending.

```
delivery_audit/
├── Dockerfile
├── README.md
├── app.py           handler: validates the event, runs the audit, emails the report
├── audit.py         collections, S3 listing, CMR paging, reconciliation, report; hand-run CLI
└── tests/
    ├── fakes.py     offline S3 paginator and CMR session
    ├── test_app.py
    └── test_audit.py
```

## What it checks

The collections are the `delivery:` entries of every line in `published_products:`
(`utilities/products.yaml`), six for `NASA-SSH` today. A line with no entry is published but
not delivered, and is not audited. Declaring one is all it takes to audit a new line.

For each collection it lists the released files under the family's `products/` prefix, pages
through the collection's CMR granules (UMM-JSON, `CMR-Search-After`), and matches GranuleUR
to the filename stem. Each indicator is its own collection, holding the current `.txt` and
every `archive/{IND}/` copy; `.mp` files and `indicators.nc` are not granules.

| Reported | Meaning |
|---|---|
| missing | no granule in CMR |
| stale | the S3 object was modified after the granule's `revision-date` |
| size mismatch | the granule's `SizeInBytes` differs from the S3 object's |
| pending | missing or stale, but published within the grace |
| CMR only | a granule with no released file; listed only with `verbose` |
| no released files | the family's prefix is empty, so the declaration or layout is wrong |

Missing, stale, size mismatch and an empty prefix are problems. `SEED_COPY_TIME` ignores
modification times up to the `products/` seed copy, which reset every object's LastModified;
it mirrors `DELIVERY_NOT_BEFORE` in `delivery-scripts`.

Each collection is audited on its own. If one cannot be audited (a CMR error, an S3 error, a
CMR listing shorter than its `CMR-Hits` header) it is reported as "could not audit" next to
the other collections' results. A truncated listing is refused rather than reconciled, since
it would report real granules as missing.

## Event

```json
{"grace_hours": 192, "redeliver": false}
```

| Key | |
|---|---|
| `grace_hours` | Required. Files published within it are pending. 192 (8 days) while climateelvis delivers weekly; 12 once the pipeline delivers each run. No default, so forgetting the cutover edit cannot leave a stale value in place |
| `published_products` | Optional list narrowing the audit; default is every delivering line |
| `redeliver` | Must be `false` or absent. Refused until the delivery Lambda exists |
| `bucket` | Optional; defaults to `BUCKET_NAME` |
| `verbose` | Optional; also lists pending and CMR-only granules |

Unknown keys are refused, so a typo in the schedule's input fails the run instead of taking a
default.

## Notification

One email to `SNS_TOPIC_ARN` on every outcome:

- `Delivery audit clean`
- `Delivery audit: N problems`, listing GranuleURs per collection, capped at 50 per category
  with a count past that
- `Delivery audit incomplete: N of M collections not audited`, with each error ahead of the
  other collections' results. The handler then raises, for the Lambda-errors alarm
- `Delivery audit could not run`, when the event or the declaration is bad. Also raised

So a missing email means the schedule or the Lambda stopped.

## Hand runs

```bash
uv run python pipeline/infra/delivery_audit/audit.py --bucket nasa-ssh-staging-prod --profile <profile> --grace-hours 192
```

`--product` is repeatable. The CLI lists every problem uncapped and exits 0 when clean, 1 on
problems, and 2 when a collection could not be audited or a named line has no delivery.

## Deployment

Prod only (`deploy_stages: [prod]` in `utilities/targets.yaml`): it audits the prod bucket
against PO.DAAC. Runs outside a VPC so it reaches `cmr.earthdata.nasa.gov`. Containerized to
ship `utilities`, like `run_summary` and `start_run`.

| | |
|---|---|
| Env | `SNS_TOPIC_ARN`, `BUCKET_NAME` |
| IAM | `s3:ListBucket` on the prod bucket with an `s3:prefix` condition of `products/*`; `sns:Publish` on the topic |
| Memory / timeout | 512 MB / 5 min to start. A run is ~12.5k keys and ~10 CMR pages; time the first hand run and size from that, leaving room for high-latitude lines |

## Tests

`./scripts/test.sh delivery_audit`. S3 and CMR are stubbed in `tests/fakes.py`.
