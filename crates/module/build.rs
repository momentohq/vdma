//! The module resolves jemalloc's `je_*` ctl functions from the host valkey-server at load time, for
//! `src/memory`'s extent hooks. A Linux cdylib already permits undefined symbols, since the
//! dynamic loader resolves them when valkey `dlopen`s the module, but macOS's linker is strict by
//! default and a local dev build fails to link — so defer resolution to load time there too.

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
    }

    println!("cargo:rerun-if-changed=.vdma-always-rebuild");
    println!("cargo:rustc-env=VDMA_BUILD_ID={:08x}", build_id());
}

/// A number per build, for a startup log
fn build_id() -> u32 {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (since.as_secs() as u32).wrapping_mul(0x0000_01b3) ^ since.subsec_nanos()
}
