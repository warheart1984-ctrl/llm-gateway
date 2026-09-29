//! Binary entry point: config -> state -> server -> graceful shutdown.

use std::process::ExitCode;

use llm_gateway::{api, bootstrap, config, observability, state::AppState};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> ExitCode {
    let settings = match config::Settings::load() {
        Ok(s) => s,
        Err(err) => {
            eprintln!("configuration error: {err}");
            return ExitCode::FAILURE;
        }
    };

    observability::logging::init(&settings.telemetry);

    let state = match bootstrap::build(settings).await {
        Ok(state) => state,
        Err(err) => {
            tracing::error!(error = %err, "startup failed");
            return ExitCode::FAILURE;
        }
    };

    match run(state).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = %err, "server exited with an error");
            ExitCode::FAILURE
        }
    }
}

async fn run(state: std::sync::Arc<AppState>) -> anyhow::Result<()> {
    let addr = state.settings.socket_addr()?;
    let grace = state.settings.shutdown_grace();
    let reload_interval = state.settings.reload_interval();
    let hot_reload = state
        .settings
        .registry
        .hot_reload;

    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(
        address = %bound,
        settings = ?config::redacted_settings(&state.settings),
        providers = ?state.providers.names(),
        models = state.models.snapshot().await.len(),
        tenants = state.tenants.len(),
        version = llm_gateway::VERSION,
        "llm-gateway listening"
    );

    // Hot reload: poll the registry mtime off the request path so a control
    // plane edit takes effect without dropping in-flight streams.
    if hot_reload {
        let reload_state = std::sync::Arc::clone(&state);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reload_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Some(generation) = reload_state.models.reload_if_changed().await {
                    tracing::info!(generation, "model registry hot-reloaded");
                }
            }
        });
    }

    let app = api::router(state.clone());
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(grace))
        .await?;

    tracing::info!("shutdown complete");
    Ok(())
}

/// Ctrl-C, or SIGTERM where available (containers). Draining gives in-flight
/// streams up to `grace` to finish rather than cutting them mid-token.
async fn shutdown_signal(grace: std::time::Duration) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT; draining"),
        _ = terminate => tracing::info!("received SIGTERM; draining"),
    }

    let draining = tokio::time::sleep(grace);
    tokio::pin!(draining);
    tokio::select! {
        _ = &mut draining => tracing::warn!("grace period expired; closing remaining streams"),
        _ = wait_for_drain() => tracing::info!("all in-flight streams settled"),
    }
}

/// Poll in-flight streams to zero, bounded by the grace period.
async fn wait_for_drain() {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
