//! Generates libfabric FFI bindings with bindgen and compiles wrappers for its `static inline`
//! functions, which call through `fid` op-tables and aren't otherwise linkable.

use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let header = manifest_dir.join("wrapper.h");
    let static_fns = out_dir.join("static_fns.c");

    // Locate libfabric and emit its link directives.
    let libfabric = pkg_config::Config::new()
        .probe("libfabric")
        .expect("libfabric not found via pkg-config");

    println!("cargo:rerun-if-changed={}", header.display());

    let mut builder = bindgen::Builder::default()
        .header(header.to_string_lossy())
        .wrap_static_fns(true)
        .wrap_static_fns_path(&static_fns)
        .allowlist_function("fi_.*")
        .allowlist_type("fi_.*|fid_.*|fid")
        .allowlist_var("FI_.*")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));
    for include in &libfabric.include_paths {
        builder = builder.clang_arg(format!("-I{}", include.display()));
    }

    builder
        .generate()
        .expect("generate libfabric bindings")
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("write bindings.rs");

    // Compile the generated wrappers.
    let mut wrappers = cc::Build::new();
    wrappers.file(&static_fns).include(&manifest_dir);
    // The wrappers cover every static inline function, including deprecated ones.
    wrappers.flag_if_supported("-Wno-deprecated-declarations");
    for include in &libfabric.include_paths {
        wrappers.include(include);
    }
    wrappers.compile("libfabric_static_wrappers");
}
