# ajar-gateway - Reference Server (Site-Side)

Self-hosted software that lets a site owner publish a signed, policy-governed Ajar surface for an existing website. This is the reference implementation, not the only valid implementation.

Status: T1.1 Gateway skeleton is implemented. The workspace now contains the ADR-018 crate layout and a standalone reverse-proxy binary. The read-layer backlog is [planning/tasks/phase-1-gateway.md](https://github.com/ajar-protocol/planning/blob/main/tasks/phase-1-gateway.md); actions follow in [planning/tasks/phase-3-actions.md](https://github.com/ajar-protocol/planning/blob/main/tasks/phase-3-actions.md).

## What it does
Wraps an existing site as a reverse proxy, CMS plugin, or edge worker. It produces the signed manifest at `/.well-known/ajar.json`, semantic Views on the same URLs via content negotiation, draft Actions awaiting owner approval, Owner Policy enforcement, 402 metering, receipts, an audit log, and the owner Console. The owner's key signs site-side artifacts; without it the Gateway cannot publish.

## Key documents
- [`CONVERSION-PIPELINE.md`](CONVERSION-PIPELINE.md): how sites become agent-readable: Tier 1 source tapping, Tier 2 template-clustered extraction, and Tier 3 build-time LLM rule induction. LLMs are never in the serve path.
- Protocol contract: [`ajar`](https://github.com/ajar-protocol/ajar) spec; compatibility judged only by [`conformance`](https://github.com/ajar-protocol/conformance)
- Owner model: [ajar/docs/05-OWNER-CONTROL.md](https://github.com/ajar-protocol/ajar/blob/main/docs/05-OWNER-CONTROL.md)

## Non-negotiables
Owner sovereignty (drafts never auto-publish; safe defaults; authenticated areas unexposable without explicit override) · zero serve-path LLM calls · no telemetry (ADR-014) · fail closed with registry error codes · single audited signing module.

## Deployment shapes (all conformance-equal)
Standalone binary/Docker reverse proxy · CMS plugins (`ajar-woocommerce`, publishes at Stage 4, may embed or front this Gateway) · edge/CDN workers · native library for greenfield sites. The Next.js path in `ajar-examples` (publishes across Phases 1-3 demos) implements Ajar without a Gateway.

Stack: Rust core per ADR-009; mechanics per ADR-018 (pinned stable toolchain, per-module crate workspace, tokio/axum, rustls, signer-confined crypto) — see AGENTS.md. License: Apache-2.0. Commercial managed offerings of this software are welcome (ADR-011).

## T1.1 implementation status

Implemented:

- `crates/serving`: axum/hyper reverse proxy plus separate admin server.
- `crates/ajar-gateway`: binary config loading, startup, graceful shutdown, and JSON stderr logs.
- `crates/store`, `crates/harvester`, and `crates/signer`: documented extension-interface traits with test doubles.

Stubbed crate boundaries only:

- `crates/policy-engine`
- `crates/extractor`
- `crates/inducer`

Not implemented in T1.1:

- Manifest or View serving.
- Owner Console.
- Policy evaluation in the serve path.
- Signing ceremonies or key custody.
- Conversion pipeline execution.

## Runtime budgets

- Default upstream response-header timeout: `30000` ms.
- Default request body limit: `10485760` bytes.
- Concurrency budget: bounded by Tokio and the host listener backlog in T1.1; no internal unbounded queues are introduced.
- Memory budget: request and response bodies stream through the proxy; declared oversized bodies are rejected before proxying and streaming bodies are capped.
- Network budget: one upstream origin request per proxied browser request; admin endpoints are local-only by deployment convention.

See [`gateway.toml.example`](gateway.toml.example) and [`docs/DEPLOY.md`](docs/DEPLOY.md) for deployment details.
