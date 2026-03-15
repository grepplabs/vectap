#!/usr/bin/env bash
set -euo pipefail

./vectap tap \
  --namespace vector \
  --type kubernetes \
  "$@"
