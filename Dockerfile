FROM rust:1.45-buster
RUN mkdir -p /dbfish
WORKDIR /dbfish
COPY . /dbfish
RUN cargo build --release

FROM debian:buster
RUN apt-get update && apt-get install libssl1.1

COPY --from=0 /dbfish/target/release/dbfish /
CMD /dbfish
