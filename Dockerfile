FROM rust:1.37-buster
RUN mkdir -p /dbfish
WORKDIR /dbfish
COPY . /dbfish
RUN cargo build --release

FROM debian:buster

COPY --from=0 /dbfish/target/release/dbfish /
CMD /dbfish
