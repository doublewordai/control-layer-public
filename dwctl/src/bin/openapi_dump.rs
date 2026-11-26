
use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Parser;

type Document = (&'static str, fn() -> String);

const DOCUMENTS: [Document; 2] = [("admin.json", dwctl::openapi::admin_json), ("ai.json", dwctl::openapi::ai_json)];

#[derive(Debug, Parser)]
#[command(about = "Write or check dwctl's committed OpenAPI documents")]
struct Args {
    #[arg(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/openapi"))]
    output_dir: PathBuf,

    #[arg(long)]
    check: bool,
}

#[allow(clippy::print_stdout, reason = "this CLI's output is its result")]
fn main() -> Result<()> {
    let args = Args::parse();

    if args.check {
        check(&args.output_dir)?;
    } else {
        write(&args.output_dir)?;
    }

    Ok(())
}

#[allow(clippy::print_stdout, reason = "this CLI's output is its result")]
fn write(output_dir: &Path) -> Result<()> {
    fs::create_dir_all(output_dir).with_context(|| format!("create {}", output_dir.display()))?;

    for (name, generate) in DOCUMENTS {
        let path = output_dir.join(name);
        fs::write(&path, generate()).with_context(|| format!("write {}", path.display()))?;
        println!("wrote {}", path.display());
    }

    Ok(())
}

#[allow(clippy::print_stdout, reason = "this CLI's output is its result")]
fn check(output_dir: &Path) -> Result<()> {
    let mut stale = Vec::new();

    for (name, generate) in DOCUMENTS {
        let path = output_dir.join(name);
        let committed = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        if committed != generate() {
            stale.push(path);
        }
    }

    if !stale.is_empty() {
        let paths = stale.iter().map(|path| path.display().to_string()).collect::<Vec<_>>().join(", ");
        bail!("stale OpenAPI document(s): {paths}; run `cargo run -p dwctl --bin openapi-dump`");
    }

    println!("OpenAPI documents are current");
    Ok(())
}
