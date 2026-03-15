#!/usr/bin/env bash
set -euo pipefail

./vectap --config examples/vectap-tlmr.yaml tap \
  --source th-cons-d01,tr-cons-d01 \
  --local-filter '+component.kind:*' \
  "$@"
