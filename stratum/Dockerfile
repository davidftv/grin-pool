# Multistage docker build, requires docker 17.05
# runtime stage
FROM debian:9.4

COPY stratum/target/*/grin-pool /usr/local/bin/
COPY grin-pool.toml /usr/local/bin/
COPY grin-pool.toml /stratum
COPY run.sh /

ENV RUST_BACKTRACE=1
ENV PATH=/usr/local/bin:$PATH
EXPOSE 9103
EXPOSE 9105
EXPOSE 9107
CMD ["/run.sh"]
