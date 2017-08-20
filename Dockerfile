FROM ubuntu:16.04

RUN \
  apt-get update && \
  apt-get install -y \
    build-essential \
    curl \
    file \
    locales \
    sudo

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN curl -s https://static.rust-lang.org/rustup.sh | sh -s -- \
  --channel=nightly --prefix=/usr

RUN mkdir /app
WORKDIR /app

# Workaround to build and cache dependencies first
# https://github.com/rust-lang/cargo/issues/1891#issuecomment-279781302
# COPY Cargo.toml Cargo.lock /app/
# RUN mkdir /app/src && echo "fn main() {}" > /app/src/main.rs && cargo build --release

COPY . /app
RUN cargo build --release

CMD ["bash", "entrypoint.sh"]
