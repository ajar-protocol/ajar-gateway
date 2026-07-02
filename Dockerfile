FROM rust:1.94-slim AS build
WORKDIR /workspace
COPY . .
RUN cargo build --release -p ajar-gateway

FROM debian:bookworm-slim AS runtime
RUN useradd --system --user-group --home /nonexistent --shell /usr/sbin/nologin ajar
COPY --from=build /workspace/target/release/ajar-gateway /usr/local/bin/ajar-gateway
COPY gateway.toml.example /etc/ajar-gateway/gateway.toml.example
USER ajar
EXPOSE 8081 9090
ENTRYPOINT ["/usr/local/bin/ajar-gateway"]
CMD ["--config", "/etc/ajar-gateway/gateway.toml"]
