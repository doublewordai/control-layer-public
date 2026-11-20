
use std::{fs, path::Path, process::Command};

fn cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dwctl-model-provisioning"))
        .args(args)
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join(".."))
        .output()
        .unwrap()
}

#[test]
fn committed_schema_matches_the_model_document_type() {
    let output = cli(&["schema"]);
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let committed = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../model-provisioning.schema.json")).unwrap();
    assert!(
        String::from_utf8_lossy(&output.stdout) == committed,
        "model-provisioning.schema.json is out of date; regenerate it with\n  \
         cargo run -p dwctl --bin dwctl-model-provisioning -- schema > model-provisioning.schema.json"
    );
}

#[test]
fn example_catalog_is_valid() {
    let output = cli(&["validate", "examples/model-provisioning"]);
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}
