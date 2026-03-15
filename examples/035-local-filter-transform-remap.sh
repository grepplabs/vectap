#!/usr/bin/env bash
set -euo pipefail

./vectap --config examples/vectap-tlmr.yaml tap \
  --source tr-cons-d01 \
  --local-filter '+component.kind:transform' \
  --local-filter '+component.type:remap' \
  --outputs-of 'attributor_logs,telemetry-routing-engine-6ecb588d-3413-44ff-a314-ff4db7b49ca8-metrics-remap' \
  "$@"
