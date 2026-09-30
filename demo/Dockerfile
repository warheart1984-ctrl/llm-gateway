# Builds the gateway and the demo's mock provider into one small image.
# Build context is the repository root (see demo/compose.yaml).

FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin llm-gateway --example mock_upstream \
 && cp target/release/llm-gateway target/release/examples/mock_upstream /usr/local/bin/

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 gateway \
 && mkdir -p /var/lib/llm-gateway && chown gateway /var/lib/llm-gateway
COPY --from=build /usr/local/bin/llm-gateway /usr/local/bin/mock_upstream /usr/local/bin/
USER gateway
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/llm-gateway"]
