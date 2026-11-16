use axum::{Router, routing::get};
use clap::Parser as _;
use onwards::{
    AppState, build_metrics_layer_and_handle, build_metrics_router, build_router, client,
    config::Config,
    create_openai_sanitizer,
    strict::build_strict_router,
    target::{Targets, WatchedFile},
    telemetry,
};
use tokio::net::TcpListener;
use tracing::{info, instrument};

mod server;
#[tokio::main]
#[instrument]
pub async fn main() -> anyhow::Result<()> {
    let tracer_provider = telemetry::init_telemetry()?;

    let result = run().await;

    if let Some(provider) = tracer_provider
        && let Err(e) = provider.shutdown()
    {
        tracing::error!(error = %e, "Failed to shutdown tracer provider");
    }

    result
}

async fn run() -> anyhow::Result<()> {
    let config = Config::parse().validate()?;
    info!("Starting AI Gateway with config: {:?}", config);

    let targets = Targets::from_config_file(&config.targets)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create targets from config: {}", e))?;

    let strict_mode = targets.strict_mode;

    if config.watch {
        targets
            .receive_updates(WatchedFile(config.targets.clone()))
            .await?;
    }

    let shutdown_signal = server::shutdown_signal()?;
    let (prometheus_layer, metrics_router) = if config.metrics {
        let (prometheus_layer, prometheus_handle) =
            build_metrics_layer_and_handle(config.metrics_prefix.clone());

        let metrics_router = build_metrics_router(prometheus_handle);
        (Some(prometheus_layer), metrics_router)
    } else {
        info!("Metrics endpoint disabled");
        (None, Router::new())
    };

    let app_state = AppState::new(targets)
        .with_sse_buffer_limit(config.sse_buffer_limit)
        .with_response_transform(create_openai_sanitizer());

    let mut router = if strict_mode {
        use onwards::strict::handlers::models_handler;
        info!("Strict mode enabled - using typed request validation");
        Router::new()
            .route("/models", get(models_handler::<client::HyperClient>))
            .with_state(app_state.clone())
            .nest("/v1", build_strict_router(app_state))
    } else {
        build_router(app_state)
    };
    if let Some(prometheus_layer) = prometheus_layer {
        router = router.layer(prometheus_layer)
    };
    let bind_addr = format!("0.0.0.0:{}", config.port);
    let listener = TcpListener::bind(&bind_addr).await?;
    let metrics_addr = format!("0.0.0.0:{}", config.metrics_port);
    let metrics_listener = TcpListener::bind(&metrics_addr).await?;
    info!("AI Gateway listening on {}", bind_addr);
    info!("Metrics and health endpoint listening on {}", metrics_addr);

    server::serve(
        listener,
        router,
        metrics_listener,
        metrics_router,
        &config,
        shutdown_signal,
    )
    .await
}
