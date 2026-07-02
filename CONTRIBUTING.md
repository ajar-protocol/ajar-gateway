# Contributing to ajar-gateway

Start with the org-wide [CONTRIBUTING.md](https://github.com/ajar-protocol/.github/blob/main/CONTRIBUTING.md) and [AGENTS.md](https://github.com/ajar-protocol/.github/blob/main/AGENTS.md). They apply fully here.

Use the org-wide [TESTING.md](https://github.com/ajar-protocol/.github/blob/main/TESTING.md) for unit, conformance, adversarial, and end-to-end evidence expectations.

This repo is the reference site-side server. Phase 1 implementation is kicking off with the read layer; actions follow in Phase 3. The backlog is planning tasks T1.1-T1.15, with T3 work later. ADR-009's Rust-core stack decision is to be finalized at kickoff.

## Wanted now

- Phase 1 Gateway issues and PRs tied to T1 task IDs.
- Reverse-proxy skeleton, manifest serving, content negotiation, harvesting, extraction, signing, policy, console, and audit-log work.
- Test plans that prove fail-closed behavior and no serve-path LLM calls.
- Documentation that keeps owner controls and deployment paths clear.

## Gated work

- Action runtime, mandates, receipts, and payment behavior wait for Phase 3 unless a Phase 1 task explicitly prepares an interface.
- Platform plugin work belongs in its owning repo when that repo publishes.
- Kernel fallback behavior belongs in `ajar-kernel` when Phase 2 opens.

## How work is tracked

Open issues in this repo. Tasks are aggregated on the org Project board, [Ajar Roadmap](https://github.com/orgs/ajar-protocol/projects).

Use task IDs in titles, for example `T1.6: implement template clustering`. One task is one PR. Keep behavior, tests, and docs in the same PR when the Definition of Done requires them.

The DoD is binding. Demonstrate it with tests, recorded output, or documented evidence in the PR.

## Gateway invariants

- Owners decide what is exposed and signed.
- No LLM calls in the serving path.
- All signing goes through the signer module.
- Verification, parse, expiry, and policy failures fail closed with registry error codes.
- No telemetry in owner-deployed software.
