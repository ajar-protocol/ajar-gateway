# Deploying ajar-gateway

This T1.1 build is a standalone reverse proxy for an existing HTTP origin. It
does not expose manifests, semantic Views, Actions, policy editing, or signing
ceremonies yet; those modules are present as crate boundaries and documented
extension interfaces.

## Clean VM prerequisites

- Linux VM with outbound access to the Rust crate registry for build time.
- Rust stable `1.94`, or Docker with BuildKit.
- An existing HTTP origin reachable from the Gateway host.

## Binary path

1. Copy `gateway.toml.example` to `/etc/ajar-gateway/gateway.toml`.
2. Set `origin_url`, `listen_addr`, and `admin_addr`.
3. Build the binary:

   ```sh
   cargo build --release -p ajar-gateway
   ```

4. Start the Gateway:

   ```sh
   AJAR_GATEWAY_CONFIG=/etc/ajar-gateway/gateway.toml \
     ./target/release/ajar-gateway
   ```

5. Verify:

   ```sh
   curl -i http://127.0.0.1:9090/healthz
   curl -i http://127.0.0.1:9090/metrics
   curl -i http://127.0.0.1:8081/
   ```

The process exits non-zero if the config file is missing, invalid, or cannot be
bound completely. Startup is all-or-nothing: both public and admin listeners
must bind before serving begins.

## Docker path

1. Copy and edit `gateway.toml.example` as `gateway.toml`.
2. Build the image:

   ```sh
   docker build -t ajar-gateway:t1.1 .
   ```

3. Run the image:

   ```sh
   docker run --rm \
     -p 8081:8081 \
     -p 9090:9090 \
     -v "$PWD/gateway.toml:/etc/ajar-gateway/gateway.toml:ro" \
     ajar-gateway:t1.1
   ```

## Operational notes

- The public listener proxies browser requests to `origin_url`.
- The admin listener is separate and should remain bound to a private interface.
- Hop-by-hop request and response headers are stripped at the proxy boundary.
- Request and response bodies stream through the Gateway; large bodies are not
  buffered into memory.
- `max_body_bytes` and `request_timeout_ms` are explicit deployment budgets.
- Metrics are owner-local text counters only; the Gateway does not emit
  telemetry or phone-home traffic.
