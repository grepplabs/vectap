#!/usr/bin/env bash
set -euo pipefail

./vectap --config examples/vectap-tlmr.yaml tap \
  --source tr-cons-d01 \
  --local-filter '+component.kind:source' \
  --local-filter '+component.type:opentelemetry' \
  --local-filter '+name:http_server_*' \
  "$@"
