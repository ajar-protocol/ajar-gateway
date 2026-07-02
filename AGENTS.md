# AGENTS.md - Gateway Architecture Contract

The org-wide `.github/AGENTS.md` and `.github/ENGINEERING.md` bind fully here.
This file adds the Gateway architecture contract.
ADR-009 selected the Rust core direction; issue #15 may still finalize stack mechanics.
The architecture rules below are stack-agnostic and survive that closure.

## Mission

The Gateway is the reference server proving an owner can go from site to agent-ready in under one hour.
Owner sovereignty is the product.
Judge every implementation choice by whether it preserves owner control, auditability, and safe defaults.

## Architecture contract

The bounded modules are the Gateway modules named in `ajar/docs/02-ARCHITECTURE.md` section 3.1.
Keep these module names in code, docs, tests, and telemetry-free logs:

- Harvester
- Extractor
- Inducer
- Action Drafter
- Policy Engine
- Signer
- Serving Layer
- Freshness
- Console

Dependency direction is fixed:

```text
Serving Layer -> Policy Engine -> content store <- conversion pipeline
```

The conversion pipeline is Harvester, Extractor, Inducer, Action Drafter, Freshness, and owner review flow.
The conversion pipeline NEVER runs in the serve path.
The conversion pipeline produces prepared artifacts that the serve path reads.
The serve path reads prepared stores and applies policy.
The serve path does not harvest, crawl, render, induce, cluster, or draft.

The Harvester owns Tier-1 structure recovery.
It reads CMS data, database adapters, sitemaps, RSS, JSON-LD, OpenGraph, and OpenAPI inputs.
It MUST expose harvest collectors through an interface.
Each collector MUST have a test double.

The Extractor owns Tier-2 deterministic extraction.
It may crawl, strip boilerplate, transform HTML to semantic markdown, and cluster templates off the serve path.
Extraction rule formats are an extension interface.
Generated rules are data, not code executed from untrusted input.

The Inducer owns Tier-3 build-time LLM assist.
It may label sample pages and draft extraction rules or manifest text.
It MUST NOT be reachable from a request-serving path.
It MUST NOT publish anything.
Its output is data validated against schemas before entering the content store.
Nothing model-generated is executable.

The Action Drafter emits candidate Actions only.
Draft Actions NEVER auto-publish.
Owner approval, wiring confirmation, risk assignment, gates, and signing are required before exposure.

The Policy Engine is a pure decision function:

```text
(request context, policy doc) -> verdict
```

No I/O is allowed inside policy decisions.
No clock read, storage read, network call, signature operation, logging write, mutation, rendering, or LLM call is allowed inside the decision function.
Inject all inputs before evaluation.
Policy decisions MUST be unit-testable in isolation.

The Signer is the only module that touches private keys.
All other modules request signatures through the Signer interface.
Key stores are extension interfaces, including OS keystore and HSM implementations.
Key material MUST NOT cross a module boundary in memory as raw bytes accessible to other modules.
All signing, key rotation, key id resolution, revocation publication, and canonicalization flow through the Signer.
Bypassing the Signer is forbidden.

The Serving Layer owns HTTP content negotiation, semantic Views, SIMULATE, propose, commit, 402 metering, origin proxying, and receipt responses.
It MAY proxy to the origin where the Gateway deployment shape requires it.
It MUST NOT do per-request rendering.
It MUST NOT call the Inducer.
It MUST NOT mutate conversion artifacts during a read.

Freshness owns CMS event hooks, content-hash diffing, drift detection, and re-induction triggers.
Freshness output is queued work and prepared artifacts.
Freshness does not run inside a request decision.

The Console owns owner review, approval, policy editing, coverage reports, key ceremonies, kill switch, and audit-log viewing.
Console actions are owner actions.
Console defaults MUST deny exposure and deny publication.

Extension interfaces exist from day one for:

- harvest collectors
- rendering engine at the CDP boundary
- settlement adapters
- key stores
- storage backends

Each extension interface gets its own module or trait.
Each extension interface gets at least one test double.
Concrete implementations live at the edges and are wired at startup.
Core Gateway logic depends on interfaces only.

Rendering engines are process-isolated behind CDP per ADR-010.
Settlement adapters implement the PAY profile boundary.
Storage backends store prepared artifacts, audit events, receipts, policies, and freshness metadata.
Revocation transport is an interface when revocation behavior lands.

Serve path budget:

- zero LLM calls
- zero per-request rendering
- zero conversion-pipeline work
- no network fan-out per request beyond the origin proxy
- read-only state from prepared stores, except audit writes and protocol-required state transitions
- bounded body sizes
- bounded timeouts
- bounded memory

## Quality specifics

Typed Gateway errors MUST map to registry codes at every HTTP boundary.
Every 4xx and 5xx response MUST carry `Ajar-Error-Code`.
Wire responses MUST NOT expose stack traces, dependency errors, filesystem paths, or internals.
Use `.github/TESTING.md` for evidence expectations.
Conformance vectors are the acceptance tests for protocol behavior.
Anything touching `ajar/docs/04-SECURITY-MODEL.md` threats T1, T2, T3, or T6 requires adversarial tests in the same PR.
Anything touching signatures, key custody, canonicalization, policy, exposure, mandate checks, offers, commits, or receipts requires fail-closed tests.
Request-serving code MUST include timeout, rate-limit, and body-size coverage where feasible.

## Owner-sovereignty implementation rules

Drafts NEVER auto-publish.
Exposure defaults deny.
Fresh installs expose nothing.
First approval may expose read-only public content only.
`/account/**`-class paths are unexposable without explicit owner override and a warning in the Console.
Authenticated paths are unexposable without explicit owner override and a warning in the Console.
Personalized paths are unexposable without explicit owner override and a warning in the Console.
The kill switch MUST be honored before policy evaluation.
The audit log write cannot be disabled by config.
Owner approval is required before signing a Manifest, View, Action, Offer policy, or public exposure rule.
Owner policy may raise protocol floors.
Owner policy MUST NOT lower protocol floors.

## Never do this

- Never add a serve-path LLM call.
- Never add per-request rendering.
- Never run harvesting, extraction, induction, clustering, or drafting on a request path.
- Never bypass the Signer.
- Never let raw private key bytes cross from the Signer or key-store boundary into another module.
- Never make a policy decision with side effects.
- Never add a public endpoint without citing the spec section, planning task, or ADR that requires it.
- Never weaken an exposure default.
- Never auto-publish a draft Action, View, policy, or Manifest.
- Never log secrets, key material, mandate contents, receipt contents, or View content at INFO.
- Never introduce telemetry or phone-home behavior.

## Stack conventions (ADR-018)

Decided in `ajar/DECISIONS.md` ADR-018; issue #15 is closed.

- Toolchain: pinned stable Rust via `rust-toolchain.toml`. MSRV = the stable release pinned there. Do not float the channel.
- Layout: Cargo workspace, one crate per bounded module: `signer`, `policy-engine`, `serving`, `harvester`, `extractor`, `inducer`, `store`, and the `ajar-gateway` binary crate. The crate graph MUST match the dependency direction in this file; a core crate MUST NOT depend on an edge crate.
- HTTP stack: tokio + axum + tower. Framework code stays inside the `serving` crate.
- TLS: rustls. OpenSSL is forbidden.
- Crypto: ed25519-dalek and the RFC 8785 JCS implementation live in the `signer` crate only. No other crate may depend on a crypto library.
- Serialization: serde and serde_json.
- Errors: thiserror-typed per crate. `anyhow` is forbidden in library crates.
- Safety: `#![forbid(unsafe_code)]` in every crate. A future signer exception requires its own documented justification and human review.
- Format: `cargo fmt --check` clean, default rustfmt config.
- Lint: `cargo clippy --all-targets -- -D warnings` clean. A suppression requires an inline justification comment.
- Tests: `cargo test` for units; conformance harness per `.github/TESTING.md` when it lands.
- Dependencies: `cargo deny check` clean in CI (license allowlist and advisories).
- Generated code declares its generator and inputs and is never hand-edited.
