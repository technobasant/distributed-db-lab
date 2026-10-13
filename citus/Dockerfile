# Citus publishes amd64-only images, and an emulated database makes every
# timing in this lab meaningless on Apple Silicon. Build the release tag from
# source on the official PostgreSQL image instead, so it runs natively on arm64
# and amd64 alike. CI overrides CITUS_IMAGE with citusdata/citus on amd64.
FROM postgres:17-bookworm AS build
ARG CITUS_VERSION=v14.2.0
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates git autoconf flex bison \
        postgresql-server-dev-17 libcurl4-openssl-dev liblz4-dev libzstd-dev \
        libkrb5-dev libssl-dev libicu-dev libselinux1-dev libxslt1-dev \
        libpam0g-dev libreadline-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${CITUS_VERSION}" https://github.com/citusdata/citus.git /src
WORKDIR /src
RUN ./configure && make -j"$(nproc)" && make install DESTDIR=/out

FROM postgres:17-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
        libcurl4 liblz4-1 libzstd1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /
