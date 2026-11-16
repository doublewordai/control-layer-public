use std::path::PathBuf;

use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use dwctl::{account_limits::AccountLimitsCatalog, model_provisioning::Catalog, org_overlays::OrgCatalog};

#[derive(Debug, Parser)]
#[command(about = "Validate and describe dwctl model provisioning catalogs")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate { directory: PathBuf },
    ValidateOrgOverlays {
        directory: PathBuf,
        #[arg(long)]
        models: PathBuf,
    },
    #[command(about = "Validate account limits and their model references against the complete model catalog, without a database")]
    ValidateAccountLimits {
        directory: PathBuf,
        #[arg(long)]
        models: PathBuf,
        #[arg(long)]
        org_overlays: Option<PathBuf>,
    },
    OrgSchema,
    AccountSchema,
    Schema,
}

#[allow(clippy::print_stdout, reason = "this CLI's output is its result")]
fn main() -> Result<()> {
    match Args::parse().command {
        Command::Validate { directory } => {
            Catalog::load(&directory)?;
            println!("model provisioning catalog is valid: {}", directory.display());
        }
        Command::ValidateOrgOverlays { directory, models } => {
            ensure!(directory.is_dir(), "org overlay directory does not exist: {}", directory.display());
            ensure!(models.is_dir(), "model directory does not exist: {}", models.display());
            let overlays = OrgCatalog::load(&directory)?;
            overlays.validate_models(&Catalog::load(&models)?)?;
            println!("org overlay catalog is valid: {}", directory.display());
        }
        Command::ValidateAccountLimits {
            directory,
            models,
            org_overlays,
        } => {
            ensure!(
                directory.is_dir(),
                "account limits directory does not exist: {}",
                directory.display()
            );
            let accounts = AccountLimitsCatalog::load(&directory)?;
            accounts.validate_models(&Catalog::load(&models)?)?;
            if let Some(org_overlays) = org_overlays {
                accounts.validate_against_overlays(&OrgCatalog::load(&org_overlays)?)?;
            }
            println!("account limits are valid: {}", directory.display());
        }
        Command::OrgSchema => println!("{}", OrgCatalog::json_schema()?),
        Command::AccountSchema => println!("{}", AccountLimitsCatalog::json_schema()?),
        Command::Schema => println!("{}", Catalog::json_schema()?),
    }
    Ok(())
}
