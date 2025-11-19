#!/usr/bin/env bash
set -eo pipefail

# Fail in real mode if env not set
if [ -z "$DRY_RUN" ]; then
    : "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set}"
    : "${AWS_REGION:?AWS_REGION not set}"
fi

REGISTRY="${AWS_ACCOUNT_ID:-<ACCOUNT_ID>}.dkr.ecr.${AWS_REGION:-us-west-2}.amazonaws.com"

if [ -z "$DRY_RUN" ]; then
    echo "Logging in to ECR: $REGISTRY" >&2   # send message to stderr
    aws ecr get-login-password --region "$AWS_REGION" \
        | docker login --username AWS --password-stdin "$REGISTRY"
else
    echo "[DRY-RUN] Would log in to ECR: $REGISTRY" >&2  # send message to stderr
fi

# **only output the registry to stdout**
echo "$REGISTRY"