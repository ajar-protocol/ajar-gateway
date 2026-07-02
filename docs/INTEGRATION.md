# Gateway Integration Model

The Gateway is distributed as an engine plus platform adapters. Integrators
choose the deployment tier that fits their platform, but every tier is
conformance-equal. The engine owns Ajar protocol ceremony. Adapters own platform
integration.

## Who integrates how

| Integrator | Recommended tier | Why |
|---|---|---|
| Platform plugin team for WordPress, Shopify, Laravel, or another CMS/framework | Sidecar plus contracts | The adapter can stay native to the platform while the Gateway engine handles protocol behavior. |
| SaaS or greenfield product already using Rust | Embedded library | The product can consume Gateway crates directly and wire callbacks as native traits. |
| Site owner or operator who wants a standalone service | Direct | The binary or Docker reverse proxy is explicit, inspectable, and operationally simple. |
| Edge or CDN integration team | Sidecar plus contracts, or embedded where the runtime permits | The adapter controls edge-specific config while preserving Gateway protocol ownership. |

### Tier 1: direct

Run `ajar-gateway` as a binary or Docker reverse proxy. This is the shape for
owners and operators who want to operate the server explicitly.

The Gateway reads its config, binds the public listener, binds the admin
listener, optionally loads a prepared content store, and proxies unsupported
requests to the configured origin.

### Tier 2: sidecar plus contracts

This is the primary platform-plugin path. The adapter writes prepared artifacts
into the content-store contract, manages Gateway config, and triggers reload
through the admin control API.

Examples include `ajar-woocommerce`, `ajar-shopify`, future Laravel packages,
and edge-worker management layers. Adapter teams do not need Rust.

The engine owns:

- Manifest lifecycle.
- Content negotiation.
- Signing ceremony.
- Risk floors.
- Offer and commit state machine in Phase 3.
- Receipts.
- Registry error codes.

The adapter owns:

- Platform data collection.
- Platform UI and owner approval UX.
- Platform credentials.
- Business logic for future action callbacks.
- Freshness triggers.

### Tier 3: embedded library

Rust products can consume Gateway crates natively. Later C-ABI or FFI adapters
may expose the same engine boundary for other runtimes, following ADR-009.

Embedded integrators get native callback traits for actions. They still do not
implement protocol ceremony, and there is no opt-out from ceremony enforcement.

## The write seam: prepared content store

The write seam is the prepared content store documented in
[`CONTENT-STORE.md`](CONTENT-STORE.md). Adapters write complete, already-signed
artifacts:

```text
<store_dir>/
  manifest.json
  views/<name>.json
  view-index.json
```

The serve path never harvests, crawls, renders, induces, drafts, or signs
content. It reads the prepared store into memory and serves only validated
artifacts.

### Atomic update recipe

Adapters should write a full replacement store to a temporary directory on the
same filesystem, validate their own write set, rename it into place atomically,
then call `POST /reload` on the admin listener.

Never mutate a live store in place. A partial write can fail validation and
keeps the old store serving, but in-place mutation makes operator diagnosis and
rollback harder.

### Versioning promise

The content-store format is a versioned public interface. Compatible evolution
is additive. Breaking changes require a major version and a migration note.

## The control API

The control API is exposed only on the admin listener. Bind it to a private
interface such as `127.0.0.1` or a private network. Do not publish it on the
public internet.

| Method | Path | Semantics | Success | Failure |
|---|---|---|---|---|
| `GET` | `/healthz` | Process-local liveness check. | `200 {"status":"ok"}` | Listener failure is a process failure. |
| `GET` | `/metrics` | Owner-local text counters. | `200 text/plain` | Listener failure is a process failure. |
| `POST` | `/reload` | Reloads and validates `store_dir`, then atomically swaps the in-memory store. | `200 {"status":"reloaded","views":N,"manifest_sequence":S}` | `409 application/problem+json`; old store keeps serving. |
| `GET` | `/reload` | Not supported. | None. | `405 Method Not Allowed`. |

`POST /reload` is fail-closed. If `store_dir` is not configured, it returns
`409` with detail `no content store configured`. If the new store fails load or
validation, the response detail is the typed validation message. Filesystem
paths, stack traces, dependency internals, and secrets are not exposed.

A reload whose manifest sequence is lower than the currently served sequence is
rejected with `409`; this prevents accidental rollback and mirrors the protocol
sequence semantics.

Example:

```sh
curl -i -X POST http://127.0.0.1:9090/reload
```

Successful reloads log `store_reloaded` and increment
`store_reloads_total`. Failed reloads log `store_reload_failed` and increment
`store_reload_failures_total`.

## Guarantees matrix

| Responsibility | Engine owns | Adapter owns |
|---|---|---|
| Manifest lifecycle | Yes | No |
| Negotiation | Yes | No |
| Signing ceremony | Yes, through the Signer | No |
| Risk floors | Yes | No |
| Offer and commit state machine | Yes, Phase 3 | No |
| Receipts | Yes | No |
| Registry error codes | Yes | No |
| Audit | Yes | No |
| Content generation | No | Yes |
| Platform UI and UX | No | Yes |
| Platform credentials | No | Yes |
| Action business logic | No | Yes, through simulate and commit callbacks |
| Freshness triggers | No | Yes |

## Action binding

Phase 3 design intent, not yet implemented.

Adapters will register per-action callback URLs in the store action config. The
engine will perform the full SIMULATE, propose, and commit ceremony, then call
the adapter callback only for business evaluation.

Callback requests will be signed by the engine. Callback responses will be
schema-validated. Callback failures will fail closed.

The embedded tier gets the same shape as native traits:
`ActionHandler::simulate` and `ActionHandler::commit`.

## What adapters must not do

- Implement Ajar protocol ceremony themselves.
- Expose the admin port publicly.
- Hold signing keys; the Signer owns key custody.
- Bypass reload by mutating the live store in place.
- Lower protocol floors or skip risk ceremony for convenience.
- Return unvalidated business results directly to clients.
