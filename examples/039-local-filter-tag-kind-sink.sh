#!/usr/bin/env bash
set -euo pipefail

./vectap --config examples/vectap-tlmr.yaml tap \
  --source tr-cons-d01 \
  --local-filter '+tags.component_kind:sink' \
  "$@"
