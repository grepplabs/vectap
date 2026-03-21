#!/usr/bin/env bash
set -euo pipefail

# server-side filtered.
./vectap --config examples/vectap-tlmr.yaml tap \
  --source tr-cons-d01 \
  --inputs-of 'destination-ea3f37ba-6d79-407a-9570-73eff93b47af-bouncer' \
  "$@"
