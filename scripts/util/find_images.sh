#!/usr/bin/env bash
set -eo pipefail

find pipeline -name Dockerfile -print0 | while IFS= read -r -d '' file; do
    dirname "$file" | xargs basename
done