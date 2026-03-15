#!/usr/bin/env bash
set -euo pipefail

./vectap --config examples/vectap-tlmr.yaml tap \
  --source tr-cons-d01 \
  --local-filter '+tags.component_id:destination--5ab249fa-d51f-4adf-b41d-bf6d58af115c--ea3f37ba-6d79-407a-9570-73eff93b47af--true' \
  --local-filter '-tags.component_id:debug-view' \
  --local-filter '-tags.component_id:prom_exporter' \
  --local-filter '+tags.component_kind:sink' \
  --local-filter '+tags.component_type:opentelemetry' \
  --local-filter '+tags.host:vector-0' \
  "$@"
