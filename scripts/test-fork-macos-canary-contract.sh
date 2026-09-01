#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
workflow="$repo_root/.github/workflows/fork-macos-canary.yml"

[[ -f "$workflow" ]]
grep -Fq 'workflow_dispatch:' "$workflow"
grep -Fq "if: github.repository == 'Mushkrot/buzz'" "$workflow"
grep -Fq 'runs-on: macos-latest' "$workflow"
grep -Fq 'TARGET: aarch64-apple-darwin' "$workflow"
grep -Fq 'MAX_DMG_BYTES: "314572800"' "$workflow"
grep -Fq 'actions/upload-artifact@' "$workflow"
grep -Fq 'retention-days: 1' "$workflow"
grep -Fq '"createUpdaterArtifacts": false' "$workflow"
grep -Fq 'signature=unsigned' "$workflow"
grep -Fq 'feature_set=default' "$workflow"
grep -Fq 'rm -rf --' "$workflow"

if [[ $(grep -Fc 'uses: actions/upload-artifact@' "$workflow") -ne 1 ]]; then
  echo "fork canary must upload exactly one artifact" >&2
  exit 1
fi

# The fork canary is deliberately manual and non-publishing. Any of these
# capabilities would either create unbounded runs or persist build payloads.
if grep -Eq '(^|[[:space:]])(push|pull_request|schedule|repository_dispatch):|actions/cache|actions/upload-pages-artifact|gh release|contents:[[:space:]]*write|packages:[[:space:]]*write|actions:[[:space:]]*write|id-token:[[:space:]]*write' "$workflow"; then
  echo "fork canary gained an automatic trigger, cache, release, or write capability" >&2
  exit 1
fi

if grep -Eq 'retention-days:[[:space:]]*[2-9][0-9]*|macos-[0-9]+|macos-.*(large|xlarge)|matrix:|x86_64-apple-darwin|mesh-llm' "$workflow"; then
  echo "fork canary exceeded the one-day ARM64/default-feature policy" >&2
  exit 1
fi

on_block=$(
  awk '
    /^on:$/ { in_on = 1; next }
    in_on && /^[^[:space:]#]/ { exit }
    in_on && NF && $0 !~ /^[[:space:]]*#/ {
      gsub(/[[:space:]]/, "")
      print
    }
  ' "$workflow"
)
if [[ "$on_block" != "workflow_dispatch:" ]]; then
  echo "fork canary must have workflow_dispatch as its only trigger" >&2
  printf 'found on block:\n%s\n' "$on_block" >&2
  exit 1
fi

echo "fork macOS canary contract passed"
