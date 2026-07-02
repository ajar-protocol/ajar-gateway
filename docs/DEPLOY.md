# Deploying ajar-gateway

This build is a standalone reverse proxy for an existing HTTP origin. By
default it is a pure proxy. When `store_dir` is configured, it also serves an
already-signed Ajar manifest, View index, and semantic Views from a prepared
content store.

## Clean VM prerequisites

- Linux VM with outbound access to the Rust crate registry for build time.
- Rust stable `1.94`, or Docker with BuildKit.
- An existing HTTP origin reachable from the Gateway host.

## Binary path

1. Copy `gateway.toml.example` to `/etc/ajar-gateway/gateway.toml`.
2. Set `origin_url`, `listen_addr`, and `admin_addr`.
3. Optional: set `store_dir` to a prepared content store directory.
4. Build the binary:

   ```sh
   cargo build --release -p ajar-gateway
   ```

5. Start the Gateway:

   ```sh
   AJAR_GATEWAY_CONFIG=/etc/ajar-gateway/gateway.toml \
     ./target/release/ajar-gateway
   ```

6. Verify:

   ```sh
   curl -i http://127.0.0.1:9090/healthz
   curl -i http://127.0.0.1:9090/metrics
   curl -i http://127.0.0.1:8081/
   curl -i http://127.0.0.1:8081/.well-known/ajar.json
   ```

The process exits non-zero if the config file is missing, invalid, or cannot be
bound completely. Startup is all-or-nothing: both public and admin listeners
must bind before serving begins.

## Docker path

1. Copy and edit `gateway.toml.example` as `gateway.toml`.
2. Build the image:

   ```sh
   docker build -t ajar-gateway:t1.3 .
   ```

3. Run the image:

   ```sh
   docker run --rm \
     -p 8081:8081 \
     -p 9090:9090 \
     -v "$PWD/gateway.toml:/etc/ajar-gateway/gateway.toml:ro" \
     ajar-gateway:t1.3
   ```

## Operational notes

- The public listener proxies browser requests to `origin_url`.
- If `store_dir` is omitted, `/.well-known/ajar.json` and all content URLs pass
  through to the origin unchanged.
- If `store_dir` is configured, the Gateway loads all prepared artifacts into
  memory at startup and performs no per-request disk reads. Replace the store
  atomically, then call `POST /reload` on the admin listener to hot-reload the
  prepared artifacts without restarting.
- The prepared store layout is documented in
  [`CONTENT-STORE.md`](CONTENT-STORE.md).
- The admin listener is separate and should remain bound to a private interface.
- The admin listener exposes `GET /healthz`, `GET /metrics`, and `POST /reload`.
- Hop-by-hop request and response headers are stripped at the proxy boundary.
- Request and response bodies stream through the Gateway; large bodies are not
  buffered into memory.
- `max_body_bytes` and `request_timeout_ms` are explicit deployment budgets.
- Metrics are owner-local text counters only; the Gateway does not emit
  telemetry or phone-home traffic.

## Make your site agent-ready

Prepare signed artifacts:

```text
<store_dir>/
  manifest.json
  views/<name>.json
  view-index.json
```

Today, use `../ajar/tools/signing_profile.py` as the signing reference for
canonical Ajar artifacts. After the files are in place, point `store_dir` at the
directory and start the Gateway. The public listener will serve:

- `GET /.well-known/ajar.json` as the signed manifest.
- `GET <manifest.views.index>` as `view-index.json`.
- `GET <view.url>` with `Accept: application/ajar+json` as the signed View.
- `GET <view.url>` with `Accept: text/markdown` as deterministic markdown
  derived from the signed View.

To update a running Gateway, write the replacement store to a temporary
directory, rename the complete store into place, then reload:

```sh
curl -i -X POST http://127.0.0.1:9090/reload
```

Successful reloads return `200` with the new View count and manifest sequence.
Invalid stores, missing `store_dir`, and lower manifest sequences return
`409 application/problem+json`; the old in-memory store keeps serving.
