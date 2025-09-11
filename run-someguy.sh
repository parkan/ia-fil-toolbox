#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run-someguy.sh <endpoint1>[,<endpoint2>...] [--] [extra someguy flags...]
#
# Examples:
#   ./run-someguy.sh https://ia.dcentnetworks.nl
#   ./run-someguy.sh https://ia.dcentnetworks.nl,https://trustless-gateway.link -- --listen-address :8190 --dht disabled

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <trustless-endpoint[,endpoint2,...]> [--] [someguy flags]" >&2
  exit 2
fi

BROKER_CSV="$1"; shift || true
EXTRA_ARGS=("$@")

# Split comma-separated endpoints into an array
IFS=',' read -r -a ENDPOINTS <<< "$BROKER_CSV"
if [[ ${#ENDPOINTS[@]} -eq 0 ]]; then
  echo "no endpoints parsed from: $BROKER_CSV" >&2
  exit 2
fi

# Generate one throwaway PeerID per endpoint, then remove the key (we keep only the PeerID string)
declare -a PIDS
for i in "${!ENDPOINTS[@]}"; do
  name="tmp-throwaway-$$_$i"
  # ipfs key gen prints the PeerID on the last line in recent kubo
  pid="$(ipfs key gen -t ed25519 "$name" | tail -n1)"
  # ensure we leave no private material behind
  ipfs key rm "$name" >/dev/null
  PIDS+=("$pid")
  echo "endpoint[$i]=${ENDPOINTS[$i]}  ->  peerID[$i]=$pid" >&2
done

# Build the argument list: pairs of --http-block-provider-endpoints and --http-block-provider-peerids
declare -a ARGS
for i in "${!ENDPOINTS[@]}"; do
  ARGS+=( --http-block-provider-endpoints "${ENDPOINTS[$i]}" )
done
for i in "${!PIDS[@]}"; do
  ARGS+=( --http-block-provider-peerids    "${PIDS[$i]}" )
done

# Sensible defaults you can override via EXTRA_ARGS:
# - listen on loopback (change to ":8190" to bind all interfaces)
# - disable DHT if you're purely proxying via HTTP trustless gateways
DEFAULTS=( --listen-address "127.0.0.1:8190" --dht disabled )

echo "starting someguy..." >&2
exec someguy "${DEFAULTS[@]}" "${ARGS[@]}" "${EXTRA_ARGS[@]}"

