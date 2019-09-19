FROM rust:1.37-buster
RUN mkdir -p /dbfish
WORKDIR /dbfish
COPY . /dbfish
RUN cargo build --release

FROM ubuntu:19.04

COPY --from=0 /dbfish/target/release/dbfish /
CMD /dbfish
