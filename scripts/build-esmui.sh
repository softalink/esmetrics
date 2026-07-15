#!/usr/bin/env bash
# Rebuilds the vendored EsMetrics UI (rebranded upstream vmui) from source
# and syncs the dist into crates/esmetrics/assets/esmui/.
#
# This automates the procedure documented in
# crates/esmetrics/assets/esmui/PATCHES.md: copy pristine upstream source,
# apply the rebrand patch, npm build, verify, replace the vendored dist.
# Day-to-day `cargo build` does NOT need this — the built assets are
# committed and embedded at compile time; run this only when re-vendoring
# (upstream version bump) or changing the rebrand patch.
#
# Usage: ./scripts/build-esmui.sh [path-to-VictoriaMetrics-checkout]
#   Default checkout: $VM_CHECKOUT, else /home/test/refsrc/VictoriaMetrics.
#   The checkout's HEAD should match VM_TAG in ./UPSTREAM (warned if not).
set -euo pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
CHECKOUT=${1:-${VM_CHECKOUT:-/home/test/refsrc/VictoriaMetrics}}
ASSETS="$HERE/crates/esmetrics/assets/esmui"
PATCH="$ASSETS/patches/rebrand.patch"
SRC="$CHECKOUT/app/vmui/packages/vmui"

[ -d "$SRC" ] || { echo "error: vmui source not found at $SRC" >&2; exit 1; }
[ -f "$PATCH" ] || { echo "error: rebrand patch not found at $PATCH" >&2; exit 1; }
command -v npm >/dev/null || { echo "error: npm not found" >&2; exit 1; }

# Warn (don't fail) if the checkout isn't at the pinned upstream tag —
# re-vendoring a new version is exactly when they will differ.
PINNED=$(sed -n 's/^VM_TAG=//p' "$HERE/UPSTREAM")
ACTUAL=$(git -C "$CHECKOUT" describe --tags --exact-match 2>/dev/null || echo "<not a tag>")
if [ "$PINNED" != "$ACTUAL" ]; then
    echo "warning: checkout is at $ACTUAL, UPSTREAM pins $PINNED" >&2
    echo "         (fine if you are re-vendoring; update UPSTREAM afterwards)" >&2
fi

WORK=$(mktemp -d /tmp/build-esmui-XXXX)
trap 'rm -rf "$WORK"' EXIT

echo "=== copying pristine source"
cp -r "$SRC" "$WORK/vmui-src"
cd "$WORK/vmui-src"
rm -rf node_modules

# Upstream's Makefile copies the MetricsQL docs in before building.
if [ ! -f src/assets/MetricsQL.md ]; then
    cp "$CHECKOUT/docs/victoriametrics/MetricsQL.md" src/assets/MetricsQL.md
fi

echo "=== applying rebrand patch"
git init -q && git add -A && git -c user.email=b@b -c user.name=b commit -qm pristine
git apply --check "$PATCH"
git apply "$PATCH"

echo "=== building (npm install && npm run build)"
npm install --no-audit --no-fund --loglevel=error
npm run build

DIST="$WORK/vmui-src/build"
[ -f "$DIST/index.html" ] || { echo "error: build produced no index.html in $DIST" >&2; exit 1; }

echo "=== verifying dist"
grep -q "EsMetrics" "$DIST/index.html" \
    || { echo "error: rebrand missing: no 'EsMetrics' in index.html" >&2; exit 1; }
grep -lq "graph|vmui|esmui" "$DIST"/assets/index-*.js \
    || { echo "error: /esmui API-base regex missing from the JS bundle" >&2; exit 1; }
if grep -rn "victoriametrics\.com" "$DIST" \
    | grep -v "docs\.victoriametrics\.com\|play\.victoriametrics\.com" | grep -q .; then
    echo "error: unexpected victoriametrics.com reference in dist:" >&2
    grep -rn "victoriametrics\.com" "$DIST" \
        | grep -v "docs\.victoriametrics\.com\|play\.victoriametrics\.com" >&2
    exit 1
fi

echo "=== syncing dist into $ASSETS"
find "$ASSETS" -mindepth 1 \
    -not -path "$ASSETS/patches*" -not -name PATCHES.md -delete
cp -r "$DIST"/. "$ASSETS/"

echo "=== done; review with: git status crates/esmetrics/assets/esmui"
git -C "$HERE" status --short crates/esmetrics/assets/esmui | head -20
echo "Rebuild + test: cargo test -p esmetrics"
