use std::process::ExitCode;

use anyhow::Context;
use clap::{Parser, Subcommand};
use dwctl::config::Args;
use dwctl::log_redaction::redact_secrets;
use dwctl::{Application, Config, migrations, telemetry};

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[used]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000\0";

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C, shutting down gracefully...");
        },
        _ = terminate => {
            tracing::info!("Received SIGTERM, shutting down gracefully...");
        },
    }
}

fn main() -> ExitCode {
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .map_err(anyhow::Error::from)
        .and_then(|runtime| runtime.block_on(async_main()));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            report_error(&error);
            ExitCode::FAILURE
        }
    }
}

fn report_error(error: &anyhow::Error) {
    if tracing::dispatcher::has_been_set() {
        tracing::error!(error = format!("{error:#}"), "dwctl exited with an error");
    } else {
        #[allow(clippy::print_stderr, reason = "logging failed to start")]
        {
            eprintln!("Error: {}", redact_secrets(&format!("{error:?}")));
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    args: Args,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Migrate {
        #[arg(long)]
        check: bool,
    },
}

async fn async_main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let args = cli.args;

    let config = match Config::load(&args) {
        Ok(config) => config,
        Err(error) => {
            let log_format = Config::figment(&args).extract_inner("log_format").unwrap_or_default();
            let _ = telemetry::init_telemetry(false, log_format);
            return Err(anyhow::Error::from(error).context("Failed to load configuration"));
        }
    };

    let tracer_provider = telemetry::init_telemetry(config.enable_otel_export, config.log_format)?;

    config.batches.validate();

    if args.validate {
        if let Some(provider) = tracer_provider {
            let _ = provider.shutdown();
        }
        #[allow(clippy::print_stdout, reason = "the result of `dwctl --validate`")]
        {
            println!("Configuration is valid.");
        }
        return Ok(());
    }

    tracing::debug!("{:?}", args);

    if let Some(Command::Migrate { check }) = cli.command {
        let result = migrations::run_command(&config, check).await;
        if let Some(provider) = tracer_provider {
            let _ = provider.shutdown();
        }
        return result.map(|_| ()).context("dwctl migrate failed");
    }

    let shutdown = shutdown_signal();
    Application::new_with_config_path(config, Some(args.config.clone()), tracer_provider)
        .await?
        .serve(shutdown)
        .await
}
