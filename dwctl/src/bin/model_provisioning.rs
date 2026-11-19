use std::path::PathBuf;

use anyhow::{Result, bail, ensure};
use clap::{Parser, Subcommand};
use dwctl::{
    account_limits::{AccountLimitsCatalog, check_api_prices},
    model_provisioning::Catalog,
    org_overlays::OrgCatalog,
};

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
    CheckApiPrices {
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
        #[arg(long)]
        account_limits: Option<PathBuf>,
        #[arg(long)]
        org_overlays: Option<PathBuf>,
    },
    OrgSchema,
    AccountSchema,
    Schema,
}

#[allow(clippy::print_stdout, clippy::print_stderr, reason = "this CLI's output is its result")]
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
        Command::CheckApiPrices {
            database_url,
            account_limits,
            org_overlays,
        } => {
            ensure!(
                account_limits.is_some() || org_overlays.is_some(),
                "give --account-limits, --org-overlays or both"
            );
            let accounts = account_limits.as_deref().map(AccountLimitsCatalog::load).transpose()?;
            let overlays = org_overlays.as_deref().map(OrgCatalog::load).transpose()?;
            let conflicts = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(check_api_prices(&database_url, accounts.as_ref(), overlays.as_ref()))?;
            if !conflicts.is_empty() {
                for conflict in &conflicts {
                    eprintln!("{conflict}");
                }
                bail!(
                    "{} declared price(s) compete with prices set through the admin API",
                    conflicts.len()
                );
            }
            println!("no declared price competes with a price set through the admin API");
        }
        Command::OrgSchema => println!("{}", OrgCatalog::json_schema()?),
        Command::AccountSchema => println!("{}", AccountLimitsCatalog::json_schema()?),
        Command::Schema => println!("{}", Catalog::json_schema()?),
    }
    Ok(())
}
