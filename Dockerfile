# Build:  docker build -t nodra .
# Run:    docker run --rm -v $PWD/jobs:/jobs nodra plan /jobs/job.sql
FROM rust:1.95-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libssl-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked --features azure,kafka-ssl \
    && cp target/release/nodra /usr/local/bin/nodra

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 nodra
COPY --from=build /usr/local/bin/nodra /usr/local/bin/nodra
USER 10001
ENTRYPOINT ["nodra"]
CMD ["help"]
