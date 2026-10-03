#!/usr/bin/env bash
# Download the real ClarkNet-HTTP traces (ITA) into data/raw/access_log.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RAW="$ROOT/data/raw"
mkdir -p "$RAW"

AUG_URL="${CLARKNET_AUG_URL:-https://ita.ee.lbl.gov/traces/clarknet_access_log_Aug28.gz}"
SEP_URL="${CLARKNET_SEP_URL:-https://ita.ee.lbl.gov/traces/clarknet_access_log_Sep4.gz}"
AUG_GZ="$RAW/clarknet_access_log_Aug28.gz"
SEP_GZ="$RAW/clarknet_access_log_Sep4.gz"
OUT="$RAW/access_log"

need_download() {
  local gz="$1"
  # Real traces are ~20MB compressed; skip tiny/failed leftovers.
  [[ -f "$gz" ]] && [[ "$(stat -c%s "$gz" 2>/dev/null || echo 0)" -gt 10000000 ]]
}

is_clarknet() {
  [[ -f "$OUT" ]] || return 1
  local lines n
  lines="$(head -n 5 "$OUT" || true)"
  n="$(wc -l < "$OUT" | tr -d ' ')"
  [[ "$n" -ge 1000000 ]] || return 1
  echo "$lines" | grep -q "1995" || return 1
  echo "$lines" | grep -qv "2026" || return 1
}

fetch() {
  local url="$1" dest="$2"
  echo "[clarknet] downloading $(basename "$dest")"
  curl -L --fail --retry 5 --retry-delay 3 --progress-bar -o "$dest.partial" "$url"
  mv "$dest.partial" "$dest"
}

if is_clarknet; then
  echo "[clarknet] already present: $OUT ($(wc -l < "$OUT" | tr -d ' ') lines)"
  exit 0
fi

need_download "$AUG_GZ" || fetch "$AUG_URL" "$AUG_GZ"
need_download "$SEP_GZ" || fetch "$SEP_URL" "$SEP_GZ"

echo "[clarknet] decompressing and concatenating into access_log"
gunzip -c "$AUG_GZ" "$SEP_GZ" > "$OUT"

if ! is_clarknet; then
  echo "[clarknet] ERROR: downloaded file does not look like ClarkNet 1995 CLF" >&2
  head -n 3 "$OUT" >&2 || true
  exit 1
fi

echo "[clarknet] ready: $OUT"
echo "[clarknet] lines=$(wc -l < "$OUT" | tr -d ' ') size=$(du -h "$OUT" | cut -f1)"
head -n 2 "$OUT"
