# ajar-gateway - Reference Server (Site-Side)

Self-hosted software that lets a site owner publish a signed, policy-governed Ajar surface for an existing website. This is the reference implementation, not the only valid implementation.

Status: T1.2/T1.3 read serving is implemented on top of the T1.1 reverse proxy. The Gateway can optionally serve a prepared, already-signed manifest and Views from disk while remaining a pure proxy when no `store_dir` is configured. The read-layer backlog is [planning/tasks/phase-1-gateway.md](https://github.com/ajar-protocol/planning/blob/main/tasks/phase-1-gateway.md); actions follow in [planning/tasks/phase-3-actions.md](https://github.com/ajar-protocol/planning/blob/main/tasks/phase-3-actions.md).

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

## Make Your Site Agent-Ready

1. Produce signed artifacts using the current protocol tooling. Today, use
   `../ajar/tools/signing_profile.py` as the reference for signing canonical
   manifest and View objects.
2. Put the prepared artifacts in one directory:

   ```text
   <store_dir>/
     manifest.json
     views/<name>.json
     view-index.json
   ```

3. Set `store_dir` in `gateway.toml`.
4. Restart the Gateway.

With `store_dir` set, the Gateway serves `/.well-known/ajar.json`, advertises it
on proxied HTML responses with `rel="ajar-manifest"`, serves the View index at
`manifest.views.index`, and serves same-URL Views for requests that explicitly
accept `application/ajar+json` or `text/markdown`. Browsers and unsupported
`Accept` headers continue to see the origin site.

See [`docs/CONTENT-STORE.md`](docs/CONTENT-STORE.md) for the store contract.

## T1.2/T1.3 implementation status

Implemented:

- `crates/serving`: axum/hyper reverse proxy plus separate admin server.
- `crates/ajar-gateway`: binary config loading, startup, graceful shutdown, and JSON stderr logs.
- `crates/store`: prepared content store loading and validation behind the storage backend boundary.
- `crates/harvester` and `crates/signer`: documented extension-interface traits with test doubles.
- Manifest serving at `/.well-known/ajar.json` when `store_dir` is configured.
- Same-URL View negotiation for `application/ajar+json` and deterministic `text/markdown`.

Stubbed crate boundaries only:

- `crates/policy-engine`
- `crates/extractor`
- `crates/inducer`

Still not implemented:

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
