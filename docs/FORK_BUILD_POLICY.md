# Fork build policy

This document defines how the `Mushkrot/buzz` fork produces test builds for
the owner's Apple Silicon Mac. It is intentionally separate from the upstream
release lanes and from any future branded product release.

## Default path

Development and tests happen in the Linux checkout. When a macOS application
build is needed, run **Fork macOS ARM64 Canary** manually in GitHub Actions and
select the exact branch or tag to build. The workflow targets
`aarch64-apple-darwin`, which is the right architecture for an M4 Mac.

The canary uses the normal desktop feature set, produces an unsigned DMG, and
does not include the optional Mesh-LLM native runtime. Mesh-LLM requires a
separate, explicitly reviewed build policy because its native build payload is
large. The first-launch Gatekeeper warning for an unsigned test app is
expected; this lane is not a distributable signed release.

## Zero-cost and storage rules

The following rules are part of the workflow contract, not suggestions:

1. The fork stays public and uses only the standard `macos-latest` hosted
   runner. Larger runners are not allowed in this lane.
2. The workflow is manual (`workflow_dispatch`) only. A new run cancels an
   older run in the same lane, so repeated clicks cannot accumulate parallel
   builds.
3. Actions Cache is not used. The macOS runner is temporary; its Cargo,
   pnpm, Tauri, and build directories disappear with the job.
4. Exactly one ARM64 DMG is uploaded as one Actions artifact. Only a checksum
   and a small build-provenance text file accompany it.
5. The artifact is retained for one day. There is no GitHub Release, package,
   updater manifest, or other durable publishing step.
6. The DMG is capped at 300 MiB. A growth beyond that limit fails the job
   instead of silently increasing stored output.
7. The job has read-only repository permissions and no signing, notarization,
   or provider credentials.

The runner workspace and an uploaded artifact are different things: the first
is ephemeral, while the second is retained by GitHub. This is why the workflow
has no cache and keeps the sole artifact small and short-lived. “Free” still
requires monitoring the repository's total Actions storage if other workflows
are changed later.

## Before and after a run

Run the policy check locally before changing the workflow:

```sh
./scripts/test-fork-macos-canary-contract.sh
```

After a run, inspect the exact fork rather than assuming that a green job left
no storage:

```sh
gh api repos/Mushkrot/buzz/actions/artifacts?per_page=100
gh api repos/Mushkrot/buzz/actions/cache/usage
gh run download <run-id> --repo Mushkrot/buzz --name <artifact-name>
```

The expected steady state is one recent ARM64 artifact (or none after its
one-day expiry) and zero Actions caches. If an older artifact appears, inspect
its exact name and delete only that artifact through GitHub; do not add a broad
cleanup job with write permissions.

## What this policy does not cover

This lane is for our fork's test builds. It is not the upstream signed canary,
the normal desktop release process, or a future renamed/rebranded product.
Those remain separate decisions. A later branded product lane may use a
different repository and a different release policy; it must not quietly turn
this test lane into a permanent artifact store.

The upstream release documentation is in
[`RELEASING.md`](../RELEASING.md). The fork-specific workflow is
[`fork-macos-canary.yml`](../.github/workflows/fork-macos-canary.yml).
