# ajar-gateway - Reference Server (Site-Side)

Self-hosted software that lets a site owner publish a signed, policy-governed Ajar surface for an existing website. This is the reference implementation, not the only valid implementation.

Status: Phase 1 implementation is kicking off. The read-layer backlog is [planning/tasks/phase-1-gateway.md](https://github.com/ajar-protocol/planning/blob/main/tasks/phase-1-gateway.md); actions follow in [planning/tasks/phase-3-actions.md](https://github.com/ajar-protocol/planning/blob/main/tasks/phase-3-actions.md).

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

Stack: ADR-009 (Rust core, proposed; finalize at Phase-1 kickoff). License: Apache-2.0. Commercial managed offerings of this software are welcome (ADR-011).
