//! The module resolves jemalloc's `je_*` ctl functions from the host valkey-server at load time, for
//! `dma_libfabric`'s extent hooks. A Linux cdylib already permits undefined symbols, since the
//! dynamic loader resolves them when valkey `dlopen`s the module, but macOS's linker is strict by
//! default and a local dev build fails to link — so defer resolution to load time there too.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
    }
}
