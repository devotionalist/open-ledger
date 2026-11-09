# Static musl binary produced on the host, then packed into an empty image
# so the build does not pull a Rust toolchain.
#
#   rustup target add x86_64-unknown-linux-musl
#   cargo build --release --target x86_64-unknown-linux-musl -p openledgerd
#   docker build -t openledgerd:local .
FROM scratch
COPY target/x86_64-unknown-linux-musl/release/openledgerd /usr/local/bin/openledgerd
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/openledgerd"]
