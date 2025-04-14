# Finalizer

NOTE: finalizer is currently directly invoked but will be redeployed to better support handling of longer date ranges.

Inputs:

- start_date: "YYYY-MM-DD"
- end_date: "YYYY-MM-DD"

Will iterate through date range between start and end dates, selecting the P2 file from the appropriate source for that date, and applying the offset if necessary. Also makes any required metadata adjustments and saves the finalized P3 daily file using the `NASA_SSH` prefix