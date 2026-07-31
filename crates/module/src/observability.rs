//! `tracing` setup: a `tracing-cache` span cache making the module's APIs inspectable in memory,
//! optionally exposed to remote consoles via `tracing-console-host`. Lifecycle events still go to
//! valkey's native log through [`crate::valkey_logger`].

use std::sync::Arc;

use configuration::{ConsoleHostConfiguration, ObservabilityConfiguration};
use tracing::level_filters::LevelFilter;
use tracing_cache::{
    ChanceHandle, ChancePredicate, Driver, LevelHandle, LevelPredicate, SpanCache,
};

/// Closed-span ring-buffer capacity.
const CACHE_CAPACITY: usize = 16384;

/// A chance gate over a level filter, so the console host can adjust both at runtime.
type Cache = SpanCache<ChancePredicate<LevelPredicate>>;

/// Install the global tracing subscriber, and the console host when one is configured.
///
/// The subscriber is always installed but filtered off, pinning the global max-level hint to OFF so
/// events are discarded at the callsite rather than processed and forwarded — which the hot path
/// depends on. The driver is spawned only alongside a console host, since it has no consumer
/// otherwise; the host then raises the level and chance at runtime through the handles. Idempotent:
/// a pre-existing global subscriber is left in place.
pub fn install(observability: &ObservabilityConfiguration) {
    let level = LevelPredicate::with_filter(LevelFilter::OFF);
    let level_handle = level.handle();
    let chance = ChancePredicate::new(level, 100.0);
    let chance_handle = chance.handle();

    let (cache, driver) = SpanCache::with_predicate(CACHE_CAPACITY, chance);
    let cache = Arc::new(cache);

    // The driver loop and console host are async, so they get a dedicated runtime.
    if let Some(console_host) = observability.console_host.clone() {
        let served_cache = cache.clone();
        std::thread::spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(run(
                    driver,
                    served_cache,
                    level_handle,
                    chance_handle,
                    console_host,
                )),
                Err(error) => eprintln!("dma module: failed to start tracing runtime: {error}"),
            }
        });
    }

    if let Err(error) = tracing::subscriber::set_global_default(cache) {
        // A pre-existing global subscriber means the cache never receives spans, so surface it to
        // valkey's journal rather than swallowing it or panicking the server.
        eprintln!("dma module: failed to install tracing subscriber (spans will be lost): {error}");
    }
}

/// Drive the cache and serve the console host until the listener stops.
async fn run(
    driver: Driver,
    cache: Arc<Cache>,
    level_handle: LevelHandle,
    chance_handle: ChanceHandle,
    console_host: ConsoleHostConfiguration,
) {
    let host = async {
        let result =
            tracing_console_host::serve(cache, level_handle, chance_handle, console_host.listen)
                .await;
        if let Err(error) = result {
            eprintln!("dma module: tracing console host stopped: {error}");
        }
    };
    tokio::join!(driver.run(), host);
}
