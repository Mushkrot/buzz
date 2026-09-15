# Buzz fork development plan

This is the canonical phase plan for the current development track. It
records implementation state and acceptance gates; it does not replace the
product vision, the remote-agent protocol, or the release documentation.

Status values:

- **Complete** — implementation, relevant automated checks, and the required
  user-visible verification are finished.
- **Implemented, verification pending** — the code exists, but the real
  workflow still needs to be exercised before the phase can close.
- **Planned** — not yet implemented.

## Current baseline

The product baseline remains the complete Buzz surface, with incremental
changes that are independently useful and testable. Development happens in the
Linux checkout. macOS test builds are produced manually by the short-lived
GitHub Actions canary; they are unsigned test artifacts, not releases.

The current verified desktop fix is commit
`672daa92593f0223146e3f9691727c32b1c038fc`: DM creation bookkeeping events
(`dm_created`) are hidden from the message timeline while ordinary
user-authored JSON remains visible.

## Phases

### 1. Build and release hygiene — Complete

- Use the manual GitHub macOS ARM64 canary for test builds.
- Avoid Actions caches, releases, updater metadata, and long-lived artifacts.
- Keep the build policy and release instructions consistent with the workflow.

Acceptance: the fork canary contract check passes and the repository has no
unintended build-storage path.

### 2. Execution profile vertical slice — Complete

The desktop profile shows the effective execution facts for an agent:
location, harness, model provider, and model. Manual configuration remains the
control mechanism; automatic routing is deliberately out of scope for this
phase.

Acceptance: the displayed values match the configured runtime and do not expose
provider credentials or invent missing values.

### 3. Managed-agent transfer foundation — Implemented, verification pending

The relay, database, ACP harness, desktop target bootstrap, and Kubernetes
provider now contain the transfer foundation:

- fenced transfer state and append-only journal;
- signed coordinator requests and durable delivery with retry;
- target bootstrap after relay delivery;
- source shutdown only after target activation;
- launch-data parity between local and provider-managed starts;
- provider protocol negotiation and staged executable execution;
- inactivity self-termination support and the Kubernetes provider/image path.

The implementation is not considered complete until the real cross-runtime
workflow passes. Unit and integration tests are supporting evidence, not the
completion gate by themselves.

### 4. Fresh desktop regression verification — Complete

- Build `0.5.20-fork.11` was produced by the fork canary.
- Agents and direct messages opened normally on macOS.
- A newly created DM with Honey rendered the agent response without the
  technical `dm_created` JSON event.
- The targeted formatter tests, desktop typecheck/build, and canary build
  passed.

Acceptance: the new-DM scenario is user-visible and the original user-authored
content path remains intact.

### 5. Cross-runtime transfer verification — Next

Run an autonomous end-to-end check with disposable test agents and an isolated
server/runtime target. Verify:

1. a target that is offline receives and bootstraps the durable request;
2. the target becomes active before the source is stopped;
3. the source and target converge to one live instance;
4. messages and transfer state are not duplicated or lost;
5. retry and restart recovery work after a temporary delivery failure;
6. the Runtime profile reports the actual execution location and configuration;
7. temporary test data and agents are removed afterward.

This phase closes only with runtime evidence plus the relevant automated
checks. No manual intervention is required unless authentication or physical
machine access becomes unavoidable.

### 6. Remote-agent hardening — Planned after phase 5

Review any failures from the live transfer exercise, then close the remaining
protocol hardening gates, especially clean-exit/restart semantics, the
shutdown grace budget, and the provider's explicit `inactivity_seconds: 0`
opt-out. Add regression tests for every repaired invariant and update the
remote-agent specification only from verified implementation facts.

### 7. Release and documentation closeout — Planned

After phases 5–6 pass:

- update the phase status and implementation correspondence;
- run the relevant repository quality gates;
- create a new short-lived canary only when code changed;
- record the exact commit, checks, and artifact expiry;
- keep no durable release or build artifact unless a separate release decision
  explicitly authorizes one.

## Operating rules

- A new canary is made only after code changes or when a fresh user-visible
  build is needed.
- A green build or unit test never substitutes for the real user workflow.
- Failed or skipped checks remain open in this plan with their evidence and
  next action.
