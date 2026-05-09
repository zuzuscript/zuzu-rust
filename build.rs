use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR should be set"));
    let source = PathBuf::from("src/runtime/stdlib/clib_ffi.c");
    let object = out_dir.join("clib_ffi.o");
    let archive = out_dir.join("libzuzu_clib_ffi.a");

    let cc_status = Command::new("cc")
        .arg("-O2")
        .arg("-fPIC")
        .arg("-c")
        .arg(&source)
        .arg("-o")
        .arg(&object)
        .status()
        .expect("failed to run cc for clib ffi bridge");
    assert!(cc_status.success(), "cc failed for clib ffi bridge");

    let ar_status = Command::new("ar")
        .arg("crus")
        .arg(&archive)
        .arg(&object)
        .status()
        .expect("failed to run ar for clib ffi bridge");
    assert!(ar_status.success(), "ar failed for clib ffi bridge");

    println!("cargo:rerun-if-changed={}", source.display());
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=zuzu_clib_ffi");
    println!("cargo:rustc-link-lib=ffi");
}
