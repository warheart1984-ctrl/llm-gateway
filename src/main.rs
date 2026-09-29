//! Binary entry point: config -> state -> server -> graceful shutdown.

use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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
        tenants = state.current_tenants().len(),
        version = llm_gateway::VERSION,
        "llm-gateway listening"
    );

    // Hot reload: poll the registry mtime off the request path so a control
    // plane edit takes effect without dropping in-flight streams. A tenant
    // file that fails to load keeps the previous registry serving; the mtime
    // is remembered either way so a broken file is not re-litigated every tick.
    if hot_reload {
        let reload_state = std::sync::Arc::clone(&state);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reload_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let tenants_path = reload_state.settings.registry.tenants_path.clone();
            let mut tenants_mtime = tenants_file_mtime(&tenants_path);
            loop {
                ticker.tick().await;
                if let Some(generation) = reload_state.models.reload_if_changed().await {
                    tracing::info!(generation, "model registry hot-reloaded");
                }
                let mtime = tenants_file_mtime(&tenants_path);
                if mtime.is_some() && mtime != tenants_mtime {
                    tenants_mtime = mtime;
                    match reload_state.reload_tenants() {
                        Ok(tenants) => {
                            tracing::info!(tenants, "tenant registry hot-reloaded")
                        }
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "tenant registry reload failed; keeping previous generation"
                            )
                        }
                    };
                }
            }
        });
    }

    let app = api::router(state.clone());
    let serve = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(grace, state.inflight.clone()));

    // The serve future completes only when every stream has finished, which a
    // stream that will not settle can delay indefinitely. Race it against the
    // hard deadline: signal + grace. Whoever wins, `run` returns and the
    // runtime drop aborts whatever is left, so the process cannot outlive
    // `grace` after a shutdown signal.
    tokio::select! {
        result = serve => {
            result?;
            tracing::info!("shutdown complete");
        }
        _ = force_close_after_grace(grace) => {
            tracing::warn!("grace period expired; closing remaining streams");
        }
    }

    Ok(())
}

/// Ctrl-C, or SIGTERM where available (containers). Shared by the drain and the
/// hard deadline so both clocks start at the same instant.
async fn terminate_signal() {
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
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Signal the server to stop accepting, then give in-flight streams up to
/// `grace` to settle rather than cutting them mid-token.
async fn shutdown_signal(grace: std::time::Duration, inflight: Arc<std::sync::atomic::AtomicU64>) {
    terminate_signal().await;
    tracing::info!("received shutdown signal; draining in-flight streams");

    let drain = wait_for_drain(inflight);
    tokio::pin!(drain);
    tokio::select! {
        _ = &mut drain => tracing::info!("all in-flight streams settled"),
        _ = tokio::time::sleep(grace) => {
            tracing::warn!("grace period expired; remaining streams will be closed")
        }
    }
}

/// Hard shutdown deadline: the OS signal, then `grace`. Resolving this aborts
/// the serve future (and with it any connection tasks still running), so a
/// wedged stream cannot pin the process.
async fn force_close_after_grace(grace: std::time::Duration) {
    terminate_signal().await;
    tokio::time::sleep(grace).await;
}

/// Modified timestamp of the tenant file, for the hot-reload poll. `None` when
/// the file is missing or unreadable, which the poll treats as "no change".
fn tenants_file_mtime(path: &std::path::Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).ok()?.modified().ok()
}

/// Poll the live stream counter to zero. Every stream struct bumps it on
/// construction and drops it on teardown, so this is exact rather than a fixed
/// sleep: a ten-stream shutdown ends in milliseconds when they settle, and only
/// genuinely stuck streams consume the grace budget.
async fn wait_for_drain(inflight: Arc<std::sync::atomic::AtomicU64>) {
    loop {
        if inflight.load(Ordering::SeqCst) == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_returns_immediately_when_nothing_is_in_flight() {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            wait_for_drain(counter),
        )
        .await
        .expect("an idle drain must not wait");
    }

    #[tokio::test]
    async fn drain_returns_once_the_last_stream_settles() {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(1));
        let late_settler = Arc::clone(&counter);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            late_settler.fetch_sub(1, Ordering::SeqCst);
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            wait_for_drain(counter),
        )
        .await
        .expect("drain must return once the counter reaches zero");
    }
}
