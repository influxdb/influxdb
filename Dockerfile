#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.84
FROM rust:${RUST_VERSION}-slim-bookworm as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes binutils build-essential curl pkg-config libssl-dev clang lld git patchelf protobuf-compiler zstd \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Build influxdb3
COPY . /influxdb3
WORKDIR /influxdb3

ARG CARGO_INCREMENTAL=yes
ARG CARGO_NET_GIT_FETCH_WITH_CLI=false
ARG PROFILE=release
ARG FEATURES=aws,gcp,azure,jemalloc_replacing_malloc,system-py
ARG PACKAGE=influxdb3
ARG PBS_DATE=unset
ARG PBS_VERSION=unset
ARG PBS_TARGET=unset
ENV CARGO_INCREMENTAL=$CARGO_INCREMENTAL \
    CARGO_NET_GIT_FETCH_WITH_CLI=$CARGO_NET_GIT_FETCH_WITH_CLI \
    PROFILE=$PROFILE \
    FEATURES=$FEATURES \
    PACKAGE=$PACKAGE \
    PBS_TARGET=$PBS_TARGET \
    PBS_DATE=$PBS_DATE \
    PBS_VERSION=$PBS_VERSION

# obtain python-build-standalone and configure PYO3_CONFIG_FILE
RUN \
  sed -i "s/^readonly TARGETS=.*/readonly TARGETS=${PBS_TARGET}/" ./.circleci/scripts/fetch-python-standalone.bash && \
  ./.circleci/scripts/fetch-python-standalone.bash /influxdb3/python-artifacts "${PBS_DATE}" "${PBS_VERSION}" && \
  tar -C /influxdb3/python-artifacts -zxf /influxdb3/python-artifacts/all.tar.gz "./${PBS_TARGET}" && \
  sed -i 's#tmp/workspace#influxdb3#' "/influxdb3/python-artifacts/${PBS_TARGET}/pyo3_config_file.txt" && \
  cat "/influxdb3/python-artifacts/${PBS_TARGET}/pyo3_config_file.txt"

RUN \
  --mount=type=cache,id=influxdb3_rustup,sharing=locked,target=/usr/local/rustup \
  --mount=type=cache,id=influxdb3_registry,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,id=influxdb3_git,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,id=influxdb3_target,sharing=locked,target=/influxdb3/target \
    du -cshx /usr/local/rustup /usr/local/cargo/registry /usr/local/cargo/git /influxdb3/target && \
    PYO3_CONFIG_FILE="/influxdb3/python-artifacts/$PBS_TARGET/pyo3_config_file.txt" cargo build --target-dir /influxdb3/target --package="$PACKAGE" --profile="$PROFILE" --no-default-features --features="$FEATURES" && \
    objcopy --compress-debug-sections "target/$PROFILE/$PACKAGE" && \
    cp "/influxdb3/target/$PROFILE/$PACKAGE" "/root/$PACKAGE" && \
    patchelf --set-rpath '$ORIGIN/python/lib:$ORIGIN/../lib/influxdb3/python/lib' "/root/$PACKAGE" && \
    cp -a "/influxdb3/python-artifacts/$PBS_TARGET/python" /root/python && \
    du -cshx /usr/local/rustup /usr/local/cargo/registry /usr/local/cargo/git /influxdb3/target


FROM debian:bookworm-slim

RUN apt update \
    && apt install --yes ca-certificates gettext-base libssl3 wget curl --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log} \
    && groupadd --gid 1500 influxdb3 \
    && useradd --uid 1500 --gid influxdb3 --shell /bin/bash --create-home influxdb3

RUN mkdir /var/lib/influxdb3 && \
    chown influxdb3:influxdb3 /var/lib/influxdb3

RUN mkdir -p /usr/lib/influxdb3
COPY --from=build /root/python /usr/lib/influxdb3/python
RUN chown -R root:root /usr/lib/influxdb3

RUN mkdir /plugins && \
    chown influxdb3:influxdb3 /plugins

USER influxdb3

RUN mkdir ~/.influxdb3

ARG PACKAGE=influxdb3
ENV PACKAGE=$PACKAGE
ENV INFLUXDB3_PLUGIN_DIR=/plugins
ENV STANDALONE_ROOT=/usr/lib/influxdb3/python

COPY --from=build "/root/$PACKAGE" "/usr/bin/$PACKAGE"
COPY docker/entrypoint.sh /usr/bin/entrypoint.sh

EXPOSE 8181

# TODO: Make this and other env vars not specific to IOx
ENV INFLUXDB_IOX_OBJECT_STORE=file
ENV INFLUXDB_IOX_DB_DIR=/var/lib/influxdb3
ENV LOG_FILTER=info

ENTRYPOINT ["/usr/bin/entrypoint.sh"]

CMD ["serve"]
