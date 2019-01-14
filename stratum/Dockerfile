# Multistage docker build, requires docker 17.05
# runtime stage
FROM debian:9.4

# builder stage
FROM rust:latest as builder

RUN set -ex && \
    apt-get update && \
    apt-get --no-install-recommends --yes install \
        clang \
        libclang-dev \
        llvm-dev \
        libncurses5 \
        libncursesw5 \
        libssl-dev \
        libssl1.1 \
        cmake \
        git \
        vim \
        net-tools \
        procps

WORKDIR /stratum
COPY grin-pool /usr/local/bin/
COPY grin-pool.toml /usr/local/bin/
COPY grin-pool.toml /stratum
COPY run.sh /

ENV RUST_BACKTRACE=1
ENV PATH=/usr/local/bin:$PATH
EXPOSE 9103
EXPOSE 9105
EXPOSE 9107
CMD ["/run.sh"]
