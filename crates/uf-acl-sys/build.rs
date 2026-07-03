use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let lib_dir = env::var("UF_ACL_SHIM_LIB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| manifest_dir.join("../../build/lib"));
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=uf_acl_shim");
}
