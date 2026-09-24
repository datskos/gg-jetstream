#![deny(missing_docs)]
//! Trait-based framework for building structured observers on top of Jetstreamer's firehose.
//!
//! # Overview
//! Plugins let you react to every block, transaction, reward, entry, and stats update emitted
//! by [`jetstreamer_firehose`](https://crates.io/crates/jetstreamer-firehose). Combined with
//! the
//! [`JetstreamerRunner`](https://docs.rs/jetstreamer/latest/jetstreamer/struct.JetstreamerRunner.html),
//! they provide a high-throughput analytics pipeline capable of exceeding 2.7 million
//! transactions per second on the right hardware. All events originate from Old Faithful's CAR
//! archive and are streamed over the network into your local runner.
//!
//! The framework offers:
//! - A [`Plugin`] trait with async hook points for each data type.
//! - [`PluginRunner`] for coordinating multiple plugins with shared ClickHouse connections
//!   (used internally by `JetstreamerRunner`).
//! - Built-in plugins under [`plugins`] that demonstrate common batching strategies and
//!   metrics.
//! - See `JetstreamerRunner` in the `jetstreamer` crate for the easiest way to run plugins.
//!
//! # ClickHouse Integration
//! Jetstreamer plugins are typically paired with ClickHouse for persistence. Runner instances
//! honor the following environment variables:
//! - `JETSTREAMER_CLICKHOUSE_DSN` (default `http://localhost:8123`): HTTP(S) DSN handed to
//!   every plugin that requests a database handle.
//! - `JETSTREAMER_CLICKHOUSE_MODE` (default `auto`): toggles the bundled ClickHouse helper.
//!   Set to `remote` to opt out of spawning the helper while still writing to a cluster,
//!   `local` to always spawn, or `off` to disable ClickHouse entirely.
//!
//! When the mode is `auto`, Jetstreamer inspects the DSN at runtime and only launches the
//! embedded helper for local endpoints, enabling native clustering workflows out of the box.
//!
//! ## Write Durability
//! Writes issued by the runner and the bundled plugins are never silently dropped. Inserts
//! use `async_insert` with `wait_for_async_insert=1` (an acknowledgment means durably
//! flushed), failures are retried with exponential backoff for up to 10 minutes, in-flight
//! write tasks are tracked and drained at shutdown (so runtime teardown never cancels a
//! batch mid-delivery), and a write that is still failing after the horizon aborts the run
//! with a message that includes the exact command to resume from the lowest unprocessed
//! slot. Retries provide at-least-once
//! delivery: every bundled table is a `ReplacingMergeTree` keyed on its logical identity, so
//! replayed batches deduplicate on merge — query with `FINAL` (or tolerate transient
//! duplicates) when reading while ingestion is active.
//!
//! # Batching ClickHouse Writes
//! ClickHouse (and any sinks you invoke inside hook handlers) can apply backpressure on large
//! numbers of tiny inserts. Plugins should buffer work locally and flush in batches on a
//! cadence that matches their workload. The default [`PluginRunner`] configuration triggers
//! stats pulses every 100 slots, which offers a reasonable heartbeat without thrashing the
//! database. The bundled [`plugins::program_tracking::ProgramTrackingPlugin`] mirrors this
//! approach by accumulating `ProgramEvent` rows per worker thread and issuing a single batch
//! insert every 1,000 slots. Adopting a similar strategy keeps long-running replays responsive
//! even under peak throughput.
//!
//! # Ordering Guarantees
//! Also note that because Jetstreamer spawns parallel threads that process different subranges of
//! the overall slot range at the same time, while each thread sees a purely sequential view of
//! transactions, downstream services such as databases that consume this data will see writes in a
//! fairly arbitrary order, so you should design your database tables and shared data structures
//! accordingly.
//!
//! # Examples
//! ## Defining a Plugin
//! ```no_run
//! use std::sync::Arc;
//! use clickhouse::Client;
//! use futures_util::FutureExt;
//! use jetstreamer_firehose::firehose::TransactionData;
//! use jetstreamer_plugin::{Plugin, PluginFuture};
//!
//! struct CountingPlugin;
//!
//! impl Plugin for CountingPlugin {
//!     fn name(&self) -> &'static str { "counting" }
//!
//!     fn on_transaction<'a>(
//!         &'a self,
//!         _thread_id: usize,
//!         _db: Option<Arc<Client>>,
//!         transaction: &'a TransactionData,
//!     ) -> PluginFuture<'a> {
//!         async move {
//!             println!("saw tx {} in slot {}", transaction.signature, transaction.slot);
//!             Ok(())
//!         }
//!         .boxed()
//!     }
//! }
//! # let _plugin = CountingPlugin;
//! ```
//!
//! ## Running Plugins with `PluginRunner`
//! ```no_run
//! use std::sync::Arc;
//! use jetstreamer_firehose::epochs;
//! use jetstreamer_plugin::{Plugin, PluginRunner};
//!
//! struct LoggingPlugin;
//!
//! impl Plugin for LoggingPlugin {
//!     fn name(&self) -> &'static str { "logging" }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut runner = PluginRunner::new("http://localhost:8123", 1, false, false, None);
//!     runner.register(Box::new(LoggingPlugin));
//!     let runner = Arc::new(runner);
//!
//!     let (start, _) = epochs::epoch_to_slot_range(800);
//!     let (_, end_inclusive) = epochs::epoch_to_slot_range(805);
//!     runner
//!         .clone()
//!         .run(start..(end_inclusive + 1), false)
//!         .await?;
//!     Ok(())
//! }
//! ```

/// Global runtime metrics shared with frontends such as the CLI `--tui` mode.
pub mod metrics;
/// Built-in plugin implementations that ship with Jetstreamer.
pub mod plugins;

/// Horizon-native plugin framework: zero-copy plugins over `.jet` archives.
pub mod horizon;

const LOG_MODULE: &str = "jetstreamer::runner";

use std::{
    fmt::Display,
    future::Future,
    hint,
    ops::Range,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{
    BlockData, EntryData, Handler, RewardsData, Stats, StatsTracking, TransactionData, firehose,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{signal, sync::broadcast};
use url::Url;

/// Re-exported statistics types produced by [`firehose`].
pub use jetstreamer_firehose::firehose::{
    FirehoseErrorContext, Stats as FirehoseStats, ThreadStats,
};

// Global totals snapshot used to compute overall TPS/ETA between pulses.
static LAST_TOTAL_SLOTS: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_TXS: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_TIME_NS: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_LOCK: AtomicBool = AtomicBool::new(false);
#[inline]
fn monotonic_nanos_since(origin: std::time::Instant) -> u64 {
    origin.elapsed().as_nanos() as u64
}

/// Convenience alias for the boxed future returned by plugin hooks.
pub type PluginFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>
            + Send
            + 'a,
    >,
>;

/// Trait implemented by plugins that consume firehose events.
///
/// Override [`Plugin::wants_entries`] and [`Plugin::wants_rewards`] to opt out of
/// unused notifications. If no plugin wants a notification, the runner omits
/// that firehose handler entirely. Built-in plugins opt out of both.
///
/// See the crate-level documentation for usage examples.
pub trait Plugin: Send + Sync + 'static {
    /// Human-friendly plugin name used in logs and persisted metadata.
    fn name(&self) -> &'static str;

    /// Whether to receive entry callbacks. Defaults to true for compatibility
    /// with existing plugins that override `on_entry`. Evaluated once per run.
    fn wants_entries(&self) -> bool {
        true
    }

    /// Whether to receive reward callbacks. Defaults to true for compatibility
    /// with existing plugins that override `on_reward`. Evaluated once per run.
    fn wants_rewards(&self) -> bool {
        true
    }

    /// Semantic version for the plugin; defaults to `1`.
    fn version(&self) -> u16 {
        1
    }

    /// Deterministic identifier derived from [`Plugin::name`].
    fn id(&self) -> u16 {
        let hash = Sha256::digest(self.name());
        let mut res = 1u16;
        for byte in hash {
            res = res.wrapping_mul(31).wrapping_add(byte as u16);
        }
        res
    }

    /// Called for every transaction seen by the firehose.
    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Called for every block observed by the firehose.
    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Called for every entry observed by the firehose when entry notifications are enabled.
    fn on_entry<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _entry: &'a EntryData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Called for reward updates associated with processed blocks.
    fn on_reward<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _reward: &'a RewardsData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Called whenever a firehose thread encounters an error before restarting.
    fn on_error<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _error: &'a FirehoseErrorContext,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Invoked once before the firehose starts streaming events.
    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }

    /// Invoked once after the firehose finishes or shuts down.
    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
}

/// Coordinates plugin execution and ClickHouse persistence.
///
/// See the crate-level documentation for usage examples.
#[derive(Clone)]
pub struct PluginRunner {
    plugins: Arc<Vec<Arc<dyn Plugin>>>,
    clickhouse_dsn: String,
    num_threads: usize,
    sequential: bool,
    reverse: bool,
    buffer_window_bytes: Option<u64>,
    db_update_interval_slots: u64,
    tui: bool,
}

impl PluginRunner {
    /// Creates a new runner that writes to `clickhouse_dsn` using `num_threads`.
    ///
    /// When `sequential` is `true`, firehose runs with one worker and `num_threads` is used as
    /// ripget parallel download concurrency. When `reverse` is `true`, epochs in the slot range
    /// are streamed from highest to lowest; this implies sequential mode and activates it
    /// automatically if not already set.
    pub fn new(
        clickhouse_dsn: impl Display,
        num_threads: usize,
        sequential: bool,
        reverse: bool,
        buffer_window_bytes: Option<u64>,
    ) -> Self {
        Self {
            plugins: Arc::new(Vec::new()),
            clickhouse_dsn: clickhouse_dsn.to_string(),
            num_threads: std::cmp::max(1, num_threads),
            sequential,
            reverse,
            buffer_window_bytes,
            db_update_interval_slots: 100,
            tui: false,
        }
    }

    /// Enables TUI support: stats pulses are always tracked (even without ClickHouse) so the
    /// frontend has data to render.
    pub fn set_tui(&mut self, tui: bool) {
        self.tui = tui;
    }

    /// Registers an additional plugin.
    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        Arc::get_mut(&mut self.plugins)
            .expect("cannot register plugins after the runner has started")
            .push(Arc::from(plugin));
    }

    /// Runs the firehose across the specified slot range, optionally writing to ClickHouse.
    pub async fn run(
        self: Arc<Self>,
        slot_range: Range<u64>,
        clickhouse_enabled: bool,
    ) -> Result<(), PluginRunnerError> {
        jetstreamer_firehose::epochs::validate_parallel_download_config()
            .map_err(PluginRunnerError::Configuration)?;
        let db_update_interval = self.db_update_interval_slots.max(1);
        let plugin_handles: Arc<Vec<PluginHandle>> = Arc::new(
            self.plugins
                .iter()
                .cloned()
                .map(PluginHandle::from)
                .collect(),
        );

        let clickhouse = if clickhouse_enabled {
            let client = Arc::new(
                build_clickhouse_client(&self.clickhouse_dsn)
                    .with_setting("async_insert", "1")
                    // Wait for the async buffer to flush before acking: an ack then means
                    // durably written, so every failure is visible to the retry layer and
                    // nothing can be lost in a post-ack flush failure. Retries may still
                    // double-commit on ambiguous timeouts; ReplacingMergeTree absorbs that.
                    .with_setting("wait_for_async_insert", "1"),
            );
            ensure_clickhouse_tables(client.as_ref()).await?;
            upsert_plugins(client.as_ref(), plugin_handles.as_ref()).await?;
            Some(client)
        } else {
            None
        };

        for handle in plugin_handles.iter() {
            if let Err(error) = handle
                .plugin
                .on_load(clickhouse.clone())
                .await
                .map_err(|e| e.to_string())
            {
                return Err(PluginRunnerError::PluginLifecycle {
                    plugin: handle.name,
                    stage: "on_load",
                    details: error,
                });
            }
        }

        let shutting_down = Arc::new(AtomicBool::new(false));
        let slot_buffer: Arc<DashMap<u16, Vec<PluginSlotRow>, ahash::RandomState>> =
            Arc::new(DashMap::with_hasher(ahash::RandomState::new()));
        let clickhouse_enabled = clickhouse.is_some();
        let slots_since_flush = Arc::new(AtomicU64::new(0));
        // Handler futures can move between runtime threads. Index by the logical
        // firehose worker, not OS thread-local storage; each lock is worker-local.
        let worker_metrics: Arc<Vec<Arc<Mutex<WorkerMetrics>>>> = Arc::new(
            (0..self.num_threads)
                .map(|_| Arc::new(Mutex::new(WorkerMetrics::default())))
                .collect(),
        );

        let on_block = {
            let worker_metrics = worker_metrics.clone();
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let slot_buffer = slot_buffer.clone();
            let slots_since_flush = slots_since_flush.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize, block: BlockData| {
                let worker_metrics = worker_metrics[thread_id].clone();
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let slot_buffer = slot_buffer.clone();
                let slots_since_flush = slots_since_flush.clone();
                let shutting_down = shutting_down.clone();
                async move {
                    let (transactions, tally) = worker_metrics
                        .lock()
                        .expect("worker metrics poisoned")
                        .finish_block(block.slot());
                    metrics::note_thread_transactions(thread_id, transactions);
                    if shutting_down.load(Ordering::SeqCst) {
                        log::debug!(
                            target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                            "ignoring block while shutdown is in progress"
                        );
                        return Ok(());
                    }
                    let block = Arc::new(block);
                    if !plugin_handles.is_empty() {
                        for handle in plugin_handles.iter() {
                            let db = clickhouse.clone();
                            if let Err(err) = handle
                                .plugin
                                .on_block(thread_id, db.clone(), block.as_ref())
                                .await
                            {
                                log::error!(
                                    target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                                    "plugin {} on_block error: {}",
                                    handle.name,
                                    err
                                );
                                continue;
                            }
                            if let (Some(db_client), BlockData::Block { slot, .. }) =
                                (clickhouse.clone(), block.as_ref())
                            {
                                if clickhouse_enabled {
                                    slot_buffer
                                        .entry(handle.id)
                                        .or_default()
                                        .push(PluginSlotRow {
                                            plugin_id: handle.id as u32,
                                            slot: *slot,
                                        });
                                } else if let Err(err) =
                                    record_plugin_slot(db_client, handle.id, *slot).await
                                {
                                    log::error!(
                                        target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                                        "failed to record plugin slot for {}: {}",
                                        handle.name,
                                        err
                                    );
                                }
                            }
                        }
                        if clickhouse_enabled {
                            let current = slots_since_flush
                                .fetch_add(1, Ordering::Relaxed)
                                .wrapping_add(1);
                            if current.is_multiple_of(db_update_interval)
                                && let Some(db_client) = clickhouse.clone()
                            {
                                let buffer = slot_buffer.clone();
                                spawn_tracked_write(async move {
                                    flush_slot_buffer(db_client, buffer).await;
                                });
                            }
                        }
                    }
                    if let Some(db_client) = clickhouse.clone() {
                        match block.as_ref() {
                            BlockData::Block {
                                slot,
                                executed_transaction_count,
                                block_time,
                                ..
                            } => {
                                let slot = *slot;
                                let executed_transaction_count = *executed_transaction_count;
                                let block_time = *block_time;
                                spawn_tracked_write(async move {
                                    retry_clickhouse_write("slot status", || {
                                        record_slot_status(
                                            Arc::clone(&db_client),
                                            slot,
                                            thread_id,
                                            executed_transaction_count,
                                            tally.votes,
                                            tally.non_votes,
                                            block_time,
                                        )
                                    })
                                    .await;
                                });
                            }
                            BlockData::PossibleLeaderSkipped { .. } => {}
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        };

        let on_transaction = {
            let worker_metrics = worker_metrics.clone();
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize, transaction: TransactionData| {
                let worker_metrics = worker_metrics[thread_id].clone();
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let shutting_down = shutting_down.clone();
                async move {
                    worker_metrics
                        .lock()
                        .expect("worker metrics poisoned")
                        .record_transaction(transaction.slot, transaction.is_vote);
                    if plugin_handles.is_empty() {
                        return Ok(());
                    }
                    if shutting_down.load(Ordering::SeqCst) {
                        log::debug!(
                            target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                            "ignoring transaction while shutdown is in progress"
                        );
                        return Ok(());
                    }
                    for handle in plugin_handles.iter() {
                        if let Err(err) = handle
                            .plugin
                            .on_transaction(thread_id, clickhouse.clone(), &transaction)
                            .await
                        {
                            log::error!(
                                target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                                "plugin {} on_transaction error: {}",
                                handle.name,
                                err
                            );
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        };

        let on_entry =
            make_entry_handler(&plugin_handles, clickhouse.clone(), shutting_down.clone());

        let on_reward =
            make_reward_handler(&plugin_handles, clickhouse.clone(), shutting_down.clone());

        let on_error = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize, context: FirehoseErrorContext| {
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let shutting_down = shutting_down.clone();
                async move {
                    if plugin_handles.is_empty() {
                        return Ok(());
                    }
                    if shutting_down.load(Ordering::SeqCst) {
                        log::debug!(
                            target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                            "ignoring error callback while shutdown is in progress"
                        );
                        return Ok(());
                    }
                    let context = Arc::new(context);
                    for handle in plugin_handles.iter() {
                        if let Err(err) = handle
                            .plugin
                            .on_error(thread_id, clickhouse.clone(), context.as_ref())
                            .await
                        {
                            log::error!(
                                target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                                "plugin {} on_error error: {}",
                                handle.name,
                                err
                            );
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        };

        let total_slot_count = slot_range.end.saturating_sub(slot_range.start);

        let total_slot_count_capture = total_slot_count;
        let run_origin = std::time::Instant::now();
        // Reset global rate snapshot for a new run.
        SNAPSHOT_LOCK.store(false, Ordering::Relaxed);
        LAST_TOTAL_SLOTS.store(0, Ordering::Relaxed);
        LAST_TOTAL_TXS.store(0, Ordering::Relaxed);
        LAST_TOTAL_TIME_NS.store(monotonic_nanos_since(run_origin), Ordering::Relaxed);
        metrics::init(if self.sequential { 1 } else { self.num_threads });
        metrics::set_run_slot_range(slot_range.start, slot_range.end);
        // Stats pulses drive both the log lines and the TUI, so track them whenever either
        // consumer is active.
        let stats_tracking = (clickhouse.is_some() || self.tui).then(|| {
            let shutting_down = shutting_down.clone();
            let thread_progress_max: Arc<DashMap<usize, f64, ahash::RandomState>> = Arc::new(DashMap::with_hasher(ahash::RandomState::new()));
            StatsTracking {
        on_stats: {
            let thread_progress_max = thread_progress_max.clone();
            let total_slot_count = total_slot_count_capture;
            move |thread_id: usize, stats: Stats| {
                let shutting_down = shutting_down.clone();
                let thread_progress_max = thread_progress_max.clone();
                async move {
                    if shutting_down.load(Ordering::SeqCst) {
                                log::debug!(
                                    target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                                    "skipping stats write during shutdown"
                                );
                                return Ok(());
                            }
                            let finish_at = stats
                                .finish_time
                                .unwrap_or_else(std::time::Instant::now);
                            let elapsed_since_start = finish_at
                                .saturating_duration_since(stats.start_time)
                                .as_nanos()
                                .max(1) as u64;
                            let total_slots = stats.slots_processed;
                            let total_txs = stats.transactions_processed;
                            let now_ns = monotonic_nanos_since(run_origin);
                            // Serialize snapshot updates so every pulse measures deltas from the
                            // previous pulse (regardless of which thread emitted it) using a
                            // monotonic clock shared across threads.
                            let (delta_slots, delta_txs, delta_time_ns) = {
                                while SNAPSHOT_LOCK
                                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                                    .is_err()
                                {
                                    hint::spin_loop();
                                }
                                let prev_slots = LAST_TOTAL_SLOTS.load(Ordering::Relaxed);
                                let prev_txs = LAST_TOTAL_TXS.load(Ordering::Relaxed);
                                let prev_time_ns = LAST_TOTAL_TIME_NS.load(Ordering::Relaxed);
                                LAST_TOTAL_SLOTS.store(total_slots, Ordering::Relaxed);
                                LAST_TOTAL_TXS.store(total_txs, Ordering::Relaxed);
                                LAST_TOTAL_TIME_NS.store(now_ns, Ordering::Relaxed);
                                SNAPSHOT_LOCK.store(false, Ordering::Release);
                                let delta_slots = total_slots.saturating_sub(prev_slots);
                                let delta_txs = total_txs.saturating_sub(prev_txs);
                                let delta_time_ns = now_ns.saturating_sub(prev_time_ns).max(1);
                                (delta_slots, delta_txs, delta_time_ns)
                            };
                            let delta_secs = (delta_time_ns as f64 / 1e9).max(1e-9);
                            let mut slot_rate = delta_slots as f64 / delta_secs;
                            let mut tps = delta_txs as f64 / delta_secs;
                            if slot_rate <= 0.0 && total_slots > 0 {
                                slot_rate =
                                    total_slots as f64 / (elapsed_since_start as f64 / 1e9);
                            }
                            if tps <= 0.0 && total_txs > 0 {
                                tps = total_txs as f64 / (elapsed_since_start as f64 / 1e9);
                            }
                            let thread_stats = &stats.thread_stats;
                            let processed_slots = stats.slots_processed.min(total_slot_count);
                            let progress_fraction = if total_slot_count > 0 {
                                processed_slots as f64 / total_slot_count as f64
                            } else {
                                1.0
                            };
                            let overall_progress = (progress_fraction * 100.0).clamp(0.0, 100.0);
                            let thread_total_slots = thread_stats
                                .initial_slot_range
                                .end
                                .saturating_sub(thread_stats.initial_slot_range.start);
                            let thread_progress_raw = if thread_total_slots > 0 {
                                (thread_stats.slots_processed as f64 / thread_total_slots as f64)
                                    .clamp(0.0, 1.0)
                                    * 100.0
                            } else {
                                100.0
                            };
                            let thread_progress = *thread_progress_max
                                .entry(thread_id)
                                .and_modify(|max| {
                                    if thread_progress_raw > *max {
                                        *max = thread_progress_raw;
                                    }
                                })
                                .or_insert(thread_progress_raw);
                            let mut overall_eta = None;
                            if slot_rate > 0.0 {
                                let remaining_slots =
                                    total_slot_count.saturating_sub(processed_slots);
                                overall_eta = Some(human_readable_duration(
                                    remaining_slots as f64 / slot_rate,
                                ));
                            }
                            if overall_eta.is_none() {
                                if progress_fraction > 0.0 && progress_fraction < 1.0 {
                                    if let Some(elapsed_total) = finish_at
                                        .checked_duration_since(stats.start_time)
                                        .map(|d| d.as_secs_f64())
                                        && elapsed_total > 0.0 {
                                            let remaining_secs =
                                                elapsed_total * (1.0 / progress_fraction - 1.0);
                                            overall_eta = Some(human_readable_duration(remaining_secs));
                                        }
                                } else if progress_fraction >= 1.0 {
                                    overall_eta = Some("0s".into());
                                }
                            }
                            metrics::record_pulse(metrics::PulseSnapshot {
                                progress_pct: overall_progress,
                                eta: overall_eta.clone(),
                                tps,
                                slots_processed: processed_slots,
                                blocks_processed: stats.blocks_processed,
                                transactions_processed: stats.transactions_processed,
                                entries_processed: stats.entries_processed,
                                rewards_processed: stats.rewards_processed,
                                total_slots: total_slot_count,
                                elapsed_secs: elapsed_since_start as f64 / 1e9,
                            });
                            let slots_display = human_readable_count(processed_slots);
                            let blocks_display = human_readable_count(stats.blocks_processed);
                            let txs_display = human_readable_count(stats.transactions_processed);
                            let tps_display = human_readable_count(tps.ceil() as u64);
                            log::info!(
                                target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                                "{overall_progress:.1}% | ETA: {} | {tps_display} TPS | {slots_display} slots | {blocks_display} blocks | {txs_display} txs | thread: {thread_progress:.1}%",
                                overall_eta.unwrap_or_else(|| "n/a".into()),
                            );
                            Ok(())
                        }
                        .boxed()
                    }
                },
                tracking_interval_slots: 100,
            }
        });

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let mut firehose_future = Box::pin(firehose(
            self.num_threads as u64,
            self.sequential,
            self.reverse,
            self.buffer_window_bytes,
            slot_range,
            Some(on_block),
            Some(on_transaction),
            on_entry,
            on_reward,
            Some(on_error),
            stats_tracking,
            Some(shutdown_tx.subscribe()),
        ));

        let firehose_result = tokio::select! {
            res = &mut firehose_future => res,
            ctrl = signal::ctrl_c() => {
                match ctrl {
                    Ok(()) => log::info!(
                        target: LOG_MODULE,
                        "CTRL+C received; initiating shutdown"
                    ),
                    Err(err) => log::error!(
                        target: LOG_MODULE,
                        "failed to listen for CTRL+C: {}",
                        err
                    ),
                }
                shutting_down.store(true, Ordering::SeqCst);
                let _ = shutdown_tx.send(());
                firehose_future.await
            }
        };

        // Preserve metrics for a partial block on shutdown or a terminal error.
        for (thread_id, worker) in worker_metrics.iter().enumerate() {
            let transactions = worker
                .lock()
                .expect("worker metrics poisoned")
                .take_transactions();
            if transactions != 0 {
                metrics::note_thread_transactions(thread_id, transactions);
            }
        }

        // Drain outstanding fire-and-forget writes (including any parked in retry backoff)
        // before flushing final state; past this point runtime teardown and the embedded
        // ClickHouse shutdown cannot cancel a delivery.
        drain_outstanding_writes().await;

        if clickhouse_enabled && let Some(db_client) = clickhouse.clone() {
            flush_slot_buffer(db_client, slot_buffer.clone()).await;
        }

        for handle in plugin_handles.iter() {
            if let Err(error) = handle
                .plugin
                .on_exit(clickhouse.clone())
                .await
                .map_err(|e| e.to_string())
            {
                log::error!(
                    target: LOG_MODULE,
                    "plugin {} on_exit error: {}",
                    handle.name,
                    error
                );
            }
        }

        match firehose_result {
            Ok(()) => Ok(()),
            Err((error, slot)) => Err(PluginRunnerError::Firehose {
                details: error.to_string(),
                slot,
            }),
        }
    }
}

fn build_clickhouse_client(dsn: &str) -> Client {
    let mut client = Client::default();
    if let Ok(mut url) = Url::parse(dsn) {
        // The ClickHouse client clears URL query parameters when building requests.
        // Configure the database explicitly so queries and inserts use the DSN's database.
        if let Some((_, database)) = url.query_pairs().find(|(key, _)| key == "database") {
            client = client.with_database(database.into_owned());
        }
        let username = url.username().to_string();
        let password = url.password().map(|value| value.to_string());
        if !username.is_empty() || password.is_some() {
            let _ = url.set_username("");
            let _ = url.set_password(None);
        }
        client = client.with_url(url.as_str());
        if !username.is_empty() {
            client = client.with_user(username);
        }
        if let Some(password) = password {
            client = client.with_password(password);
        }
    } else {
        client = client.with_url(dsn);
    }
    client
}

#[cfg(test)]
mod clickhouse_dsn_tests {
    use super::build_clickhouse_client;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn clickhouse_dsn_database_is_sent_in_requests() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(read, 0, "connection closed before request headers");
                    request.extend_from_slice(&buffer[..read]);
                }
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                String::from_utf8(request).unwrap()
            });
            let client = build_clickhouse_client(&format!(
                "http://jetstreamer:test-password@{address}/?database=jetstreamer%2Dtest"
            ));
            client.query("SELECT 1").execute().await.unwrap();
            let request = server.await.unwrap();
            let target = request
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap();
            let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
            assert!(
                url.query_pairs()
                    .any(|(key, value)| { key == "database" && value == "jetstreamer-test" }),
                "database missing from request: {target}"
            );
        })
        .await
        .expect("ClickHouse mock request timed out");
    }
}

/// Errors that can arise while running plugins against the firehose.
#[derive(Debug, Error)]
pub enum PluginRunnerError {
    /// Invalid runtime or download configuration.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// ClickHouse client returned an error.
    #[error("clickhouse error: {0}")]
    Clickhouse(#[from] clickhouse::error::Error),
    /// Firehose streaming failed at the specified slot.
    #[error("firehose error at slot {slot}: {details}")]
    Firehose {
        /// Human-readable description of the firehose failure.
        details: String,
        /// Slot where the firehose encountered the error.
        slot: u64,
    },
    /// Lifecycle hook on a plugin returned an error.
    #[error("plugin {plugin} failed during {stage}: {details}")]
    PluginLifecycle {
        /// Name of the plugin that failed.
        plugin: &'static str,
        /// Lifecycle stage where the failure occurred.
        stage: &'static str,
        /// Textual error details.
        details: String,
    },
}

#[derive(Clone)]
struct PluginHandle {
    plugin: Arc<dyn Plugin>,
    id: u16,
    name: &'static str,
    version: u16,
}

fn make_entry_handler(
    handles: &[PluginHandle],
    clickhouse: Option<Arc<Client>>,
    shutting_down: Arc<AtomicBool>,
) -> Option<impl Handler<EntryData>> {
    let plugin_handles: Arc<Vec<_>> = Arc::new(
        handles
            .iter()
            .filter(|handle| handle.plugin.wants_entries())
            .cloned()
            .collect(),
    );
    if plugin_handles.is_empty() {
        return None;
    }
    Some(move |thread_id: usize, entry: EntryData| {
        let plugin_handles = plugin_handles.clone();
        let clickhouse = clickhouse.clone();
        let shutting_down = shutting_down.clone();
        async move {
            if shutting_down.load(Ordering::SeqCst) {
                log::debug!(
                    target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                    "ignoring entry while shutdown is in progress"
                );
                return Ok(());
            }
            for handle in plugin_handles.iter() {
                if let Err(err) = handle
                    .plugin
                    .on_entry(thread_id, clickhouse.clone(), &entry)
                    .await
                {
                    log::error!(
                        target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                        "plugin {} on_entry error: {}",
                        handle.name,
                        err
                    );
                }
            }
            Ok(())
        }
        .boxed()
    })
}

fn make_reward_handler(
    handles: &[PluginHandle],
    clickhouse: Option<Arc<Client>>,
    shutting_down: Arc<AtomicBool>,
) -> Option<impl Handler<RewardsData>> {
    let plugin_handles: Arc<Vec<_>> = Arc::new(
        handles
            .iter()
            .filter(|handle| handle.plugin.wants_rewards())
            .cloned()
            .collect(),
    );
    if plugin_handles.is_empty() {
        return None;
    }
    Some(move |thread_id: usize, reward: RewardsData| {
        let plugin_handles = plugin_handles.clone();
        let clickhouse = clickhouse.clone();
        let shutting_down = shutting_down.clone();
        async move {
            if shutting_down.load(Ordering::SeqCst) {
                log::debug!(
                    target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                    "ignoring reward while shutdown is in progress"
                );
                return Ok(());
            }
            for handle in plugin_handles.iter() {
                if let Err(err) = handle
                    .plugin
                    .on_reward(thread_id, clickhouse.clone(), &reward)
                    .await
                {
                    log::error!(
                        target: &format!("{LOG_MODULE}::T{thread_id:03}"),
                        "plugin {} on_reward error: {}",
                        handle.name,
                        err
                    );
                }
            }
            Ok(())
        }
        .boxed()
    })
}

impl From<Arc<dyn Plugin>> for PluginHandle {
    fn from(plugin: Arc<dyn Plugin>) -> Self {
        let id = plugin.id();
        let name = plugin.name();
        let version = plugin.version();
        Self {
            plugin,
            id,
            name,
            version,
        }
    }
}

#[cfg(test)]
mod optional_callback_tests {
    use super::*;

    #[derive(Default)]
    struct Observer {
        entries: AtomicU64,
        rewards: AtomicU64,
    }

    // Deliberately retain the default interests, as existing external plugins do.
    impl Plugin for Observer {
        fn name(&self) -> &'static str {
            "observer"
        }
        fn on_entry<'a>(
            &'a self,
            _: usize,
            _: Option<Arc<Client>>,
            _: &'a EntryData,
        ) -> PluginFuture<'a> {
            async move {
                self.entries.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            .boxed()
        }
        fn on_reward<'a>(
            &'a self,
            _: usize,
            _: Option<Arc<Client>>,
            _: &'a RewardsData,
        ) -> PluginFuture<'a> {
            async move {
                self.rewards.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            .boxed()
        }
    }

    struct Uninterested;
    impl Plugin for Uninterested {
        fn name(&self) -> &'static str {
            "uninterested"
        }
        fn wants_entries(&self) -> bool {
            false
        }
        fn wants_rewards(&self) -> bool {
            false
        }
        fn on_entry<'a>(
            &'a self,
            _: usize,
            _: Option<Arc<Client>>,
            _: &'a EntryData,
        ) -> PluginFuture<'a> {
            panic!("entry dispatch should be disabled")
        }
        fn on_reward<'a>(
            &'a self,
            _: usize,
            _: Option<Arc<Client>>,
            _: &'a RewardsData,
        ) -> PluginFuture<'a> {
            panic!("reward dispatch should be disabled")
        }
    }

    #[test]
    fn builtins_and_empty_plugin_sets_do_not_register_optional_callbacks() {
        let plugins: Vec<Arc<dyn Plugin>> = vec![
            Arc::new(plugins::tx_metadata::TxMetadataPlugin::new()),
            Arc::new(plugins::program_tracking::ProgramTrackingPlugin::new()),
            Arc::new(plugins::instruction_tracking::InstructionTrackingPlugin::new()),
            Arc::new(plugins::pubkey_stats::PubkeyStatsPlugin::new()),
        ];
        let shutdown = Arc::new(AtomicBool::new(false));
        let handles: Vec<_> = plugins.into_iter().map(PluginHandle::from).collect();
        for set in [handles.as_slice(), &[]] {
            assert!(make_entry_handler(set, None, shutdown.clone()).is_none());
            assert!(make_reward_handler(set, None, shutdown.clone()).is_none());
        }
    }

    #[tokio::test]
    async fn custom_hooks_still_run_and_opted_out_hooks_are_skipped() {
        let observer = Arc::new(Observer::default());
        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(Uninterested), observer.clone()];
        let handles: Vec<_> = plugins.into_iter().map(PluginHandle::from).collect();
        let shutdown = Arc::new(AtomicBool::new(false));
        let on_entry = make_entry_handler(&handles, None, shutdown.clone()).unwrap();
        let on_reward = make_reward_handler(&handles, None, shutdown.clone()).unwrap();
        let entry = || EntryData {
            slot: 42,
            entry_index: 0,
            transaction_indexes: 0..1,
            num_hashes: 1,
            hash: solana_hash::Hash::default(),
        };
        on_entry(0, entry()).await.unwrap();
        on_reward(
            0,
            RewardsData {
                slot: 42,
                rewards: vec![],
            },
        )
        .await
        .unwrap();
        assert_eq!(observer.entries.load(Ordering::Relaxed), 1);
        assert_eq!(observer.rewards.load(Ordering::Relaxed), 1);
        shutdown.store(true, Ordering::SeqCst);
        on_entry(0, entry()).await.unwrap();
        on_reward(
            0,
            RewardsData {
                slot: 42,
                rewards: vec![],
            },
        )
        .await
        .unwrap();
        assert_eq!(observer.entries.load(Ordering::Relaxed), 1);
        assert_eq!(observer.rewards.load(Ordering::Relaxed), 1);
    }
}

#[derive(Row, Serialize)]
struct PluginRow<'a> {
    id: u32,
    name: &'a str,
    version: u32,
}

#[derive(Row, Serialize, Clone)]
struct PluginSlotRow {
    plugin_id: u32,
    slot: u64,
}

#[derive(Row, Serialize)]
struct SlotStatusRow {
    slot: u64,
    transaction_count: u32,
    vote_transaction_count: u32,
    non_vote_transaction_count: u32,
    thread_id: u8,
    block_time: u32,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct SlotTxTally {
    votes: u64,
    non_votes: u64,
}

/// Transaction callbacks for a worker are serial, but may run on different OS
/// threads. Keep counters under that worker's own lock instead of shared maps.
#[derive(Default)]
#[repr(align(128))]
struct WorkerMetrics {
    transactions: u64,
    slot: Option<u64>,
    tally: SlotTxTally,
}

#[cfg(test)]
mod worker_metrics_tests {
    use super::*;

    #[test]
    fn skipped_slots_do_not_consume_current_block_tallies() {
        let mut metrics = WorkerMetrics::default();
        metrics.record_transaction(100, true);
        metrics.record_transaction(100, false);
        metrics.record_transaction(100, false);
        assert_eq!(metrics.finish_block(99), (3, SlotTxTally::default()));
        assert_eq!(
            metrics.finish_block(100),
            (
                0,
                SlotTxTally {
                    votes: 1,
                    non_votes: 2
                }
            )
        );
        assert_eq!(metrics.finish_block(100), (0, SlotTxTally::default()));
    }

    #[test]
    fn partial_blocks_are_counted_at_shutdown_without_double_counting() {
        let mut metrics = WorkerMetrics::default();
        metrics.record_transaction(10, true);
        assert_eq!(metrics.finish_block(10).0, 1);
        metrics.record_transaction(11, false);
        assert_eq!(metrics.take_transactions(), 1);
        assert_eq!(metrics.take_transactions(), 0);
        let fresh_run = WorkerMetrics::default();
        assert_eq!(fresh_run.transactions, 0);
        assert_eq!(fresh_run.tally, SlotTxTally::default());
    }

    #[test]
    fn slot_transitions_discard_stale_tallies_but_preserve_processed_count() {
        let mut metrics = WorkerMetrics::default();
        metrics.record_transaction(200, true);
        metrics.record_transaction(100, false); // Reverse replay / new assignment.
        assert_eq!(
            metrics.finish_block(100),
            (
                2,
                SlotTxTally {
                    votes: 0,
                    non_votes: 1
                }
            )
        );
        assert_eq!(metrics.finish_block(200), (0, SlotTxTally::default()));
    }

    #[test]
    fn concurrent_workers_have_independent_counts_and_tallies() {
        let workers: Arc<Vec<Mutex<WorkerMetrics>>> = Arc::new(
            (0..8)
                .map(|_| Mutex::new(WorkerMetrics::default()))
                .collect(),
        );
        let threads = (0..8)
            .map(|id| {
                let workers = workers.clone();
                std::thread::spawn(move || {
                    for tx in 0..1000 {
                        workers[id]
                            .lock()
                            .unwrap()
                            .record_transaction(42, tx % 2 == 0);
                    }
                    workers[id].lock().unwrap().finish_block(42)
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            assert_eq!(
                thread.join().unwrap(),
                (
                    1000,
                    SlotTxTally {
                        votes: 500,
                        non_votes: 500
                    }
                )
            );
        }
    }
}

impl WorkerMetrics {
    fn record_transaction(&mut self, slot: u64, is_vote: bool) {
        self.transactions = self.transactions.saturating_add(1);
        if self.slot != Some(slot) {
            // A new slot supersedes a partial block left by a failed attempt.
            self.slot = Some(slot);
            self.tally = SlotTxTally::default();
        }
        if is_vote {
            self.tally.votes = self.tally.votes.saturating_add(1);
        } else {
            self.tally.non_votes = self.tally.non_votes.saturating_add(1);
        }
    }

    fn take_transactions(&mut self) -> u64 {
        std::mem::take(&mut self.transactions)
    }

    fn finish_block(&mut self, slot: u64) -> (u64, SlotTxTally) {
        let tally = if self.slot == Some(slot) {
            self.slot = None;
            std::mem::take(&mut self.tally)
        } else {
            // Skipped-slot callbacks can precede the current block's callback
            // after its transactions have already been delivered.
            SlotTxTally::default()
        };
        (self.take_transactions(), tally)
    }
}

async fn ensure_clickhouse_tables(db: &Client) -> Result<(), clickhouse::error::Error> {
    db.query(
        r#"CREATE TABLE IF NOT EXISTS jetstreamer_slot_status (
            slot UInt64,
            transaction_count UInt32 DEFAULT 0,
            vote_transaction_count UInt32 DEFAULT 0,
            non_vote_transaction_count UInt32 DEFAULT 0,
            thread_id UInt8 DEFAULT 0,
            block_time DateTime('UTC') DEFAULT toDateTime(0),
            indexed_at DateTime('UTC') DEFAULT now()
        ) ENGINE = ReplacingMergeTree(indexed_at)
        ORDER BY slot"#,
    )
    .execute()
    .await?;

    db.query(
        r#"CREATE TABLE IF NOT EXISTS jetstreamer_plugins (
            id UInt32,
            name String,
            version UInt32
        ) ENGINE = ReplacingMergeTree
        ORDER BY id"#,
    )
    .execute()
    .await?;

    db.query(
        r#"CREATE TABLE IF NOT EXISTS jetstreamer_plugin_slots (
            plugin_id UInt32,
            slot UInt64,
            indexed_at DateTime('UTC') DEFAULT now()
        ) ENGINE = ReplacingMergeTree
        ORDER BY (plugin_id, slot)"#,
    )
    .execute()
    .await?;

    Ok(())
}

async fn upsert_plugins(
    db: &Client,
    plugins: &[PluginHandle],
) -> Result<(), clickhouse::error::Error> {
    if plugins.is_empty() {
        return Ok(());
    }
    let mut insert = db.insert::<PluginRow>("jetstreamer_plugins").await?;
    for handle in plugins {
        insert
            .write(&PluginRow {
                id: handle.id as u32,
                name: handle.name,
                version: handle.version as u32,
            })
            .await?;
    }
    insert.end().await?;
    Ok(())
}

async fn record_plugin_slot(
    db: Arc<Client>,
    plugin_id: u16,
    slot: u64,
) -> Result<(), clickhouse::error::Error> {
    let mut insert = db
        .insert::<PluginSlotRow>("jetstreamer_plugin_slots")
        .await?;
    insert
        .write(&PluginSlotRow {
            plugin_id: plugin_id as u32,
            slot,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

/// Drains the shared slot buffer once and writes the drained rows with full retry
/// protection. The drain happens exactly once up front — retries replay the *drained* rows,
/// never re-drain the (now empty) buffer, so a failed attempt cannot lose them.
async fn flush_slot_buffer(
    db: Arc<Client>,
    buffer: Arc<DashMap<u16, Vec<PluginSlotRow>, ahash::RandomState>>,
) {
    let mut rows = Vec::new();
    buffer.iter_mut().for_each(|mut entry| {
        if !entry.value().is_empty() {
            rows.append(entry.value_mut());
        }
    });

    if rows.is_empty() {
        return;
    }

    retry_clickhouse_write("plugin slot flush", || {
        let db = Arc::clone(&db);
        let rows = rows.clone();
        async move {
            let mut insert = db
                .insert::<PluginSlotRow>("jetstreamer_plugin_slots")
                .await?;
            for row in &rows {
                insert.write(row).await?;
            }
            insert.end().await?;
            Ok(())
        }
    })
    .await;
}

/// Number of spawned ClickHouse write tasks still in flight. Drained at shutdown so runtime
/// teardown can never cancel a write mid-delivery (including retries parked in backoff).
static WRITES_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);

/// RAII guard for one in-flight write task; decrements on drop so even a panicking task
/// cannot leak the counter and wedge shutdown.
struct InFlightWrite;

impl InFlightWrite {
    fn begin() -> Self {
        WRITES_IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for InFlightWrite {
    fn drop(&mut self) {
        WRITES_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Spawns a fire-and-forget ClickHouse write task tracked by the in-flight counter. The
/// counter is incremented *before* spawning (in the caller's context), so by the time the
/// firehose finishes and shutdown reaches [`drain_outstanding_writes`], every write spawned
/// from a handler is guaranteed to be counted.
pub(crate) fn spawn_tracked_write<F>(write: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let guard = InFlightWrite::begin();
    tokio::spawn(async move {
        let _guard = guard;
        write.await;
    });
}

/// Waits until every tracked ClickHouse write task has completed. Called during shutdown
/// after ingestion stops and before the embedded ClickHouse helper is stopped and the tokio
/// runtime is dropped — otherwise in-flight batches would be silently cancelled. The wait is
/// bounded by the write tasks' own retry horizon: they either succeed or terminate the
/// process, so this cannot hang forever.
async fn drain_outstanding_writes() {
    let mut last_logged = std::time::Instant::now();
    let mut logged = false;
    loop {
        let in_flight = WRITES_IN_FLIGHT.load(Ordering::SeqCst);
        if in_flight == 0 {
            if logged {
                log::info!(target: LOG_MODULE, "all outstanding clickhouse writes finished");
            }
            return;
        }
        if !logged || last_logged.elapsed() >= Duration::from_secs(5) {
            log::info!(
                target: LOG_MODULE,
                "waiting for {in_flight} outstanding clickhouse write task(s) to finish before shutdown..."
            );
            last_logged = std::time::Instant::now();
            logged = true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Retries a ClickHouse write with exponential backoff (0.5s doubling to a 15s cap) for up
/// to 10 minutes. Every Jetstreamer table is a `ReplacingMergeTree` keyed on its logical
/// identity, so replaying a whole batch is idempotent — a rare double-commit collapses on
/// merge.
///
/// If the write is still failing after the full horizon, the process is terminated: silently
/// dropping ClickHouse data is never acceptable, and a database that has been unreachable
/// for 10 minutes means the run's output would be incomplete no matter what we do next.
pub(crate) async fn retry_clickhouse_write<F, Fut>(what: &'static str, mut write: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), clickhouse::error::Error>>,
{
    const RETRY_HORIZON: Duration = Duration::from_secs(600);
    let started = std::time::Instant::now();
    let mut delay = Duration::from_millis(500);
    let mut attempt: u32 = 1;
    loop {
        match write().await {
            Ok(()) => {
                if attempt > 1 {
                    log::info!("clickhouse write '{what}' succeeded on attempt {attempt}");
                }
                return;
            }
            Err(err) => {
                if started.elapsed() >= RETRY_HORIZON {
                    let resume_hint = match (
                        jetstreamer_firehose::firehose::resume_floor(),
                        metrics::run_slot_range(),
                    ) {
                        (Some(floor), Some((_, end))) => {
                            let range = format!("{floor}:{}", end.saturating_sub(1));
                            let command = metrics::resume_command_template()
                                .map(|template| template.replace("{range}", &range))
                                .unwrap_or_else(|| format!("jetstreamer {range} <your original flags>"));
                            format!(
                                "everything below slot {floor} is fully processed; resume with: {command} (overlapping rows deduplicate via ReplacingMergeTree)"
                            )
                        }
                        _ => "re-run the same range to resume (overlapping rows deduplicate via ReplacingMergeTree)".to_string(),
                    };
                    // Both sinks on purpose: the ring logger owns `log` in TUI mode, and
                    // stderr survives the process teardown.
                    log::error!(
                        "FATAL: clickhouse write '{what}' still failing after {:?} ({attempt} attempts); aborting run to avoid silent data loss: {err}. {resume_hint}",
                        started.elapsed()
                    );
                    eprintln!(
                        "FATAL: clickhouse write '{what}' still failing after {:?} ({attempt} attempts); aborting run to avoid silent data loss: {err}. {resume_hint}",
                        started.elapsed()
                    );
                    std::process::exit(1);
                }
                metrics::note_db_retry();
                log::warn!(
                    "clickhouse write '{what}' failed (attempt {attempt}); retrying in {delay:?}: {err}"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(15));
                attempt += 1;
            }
        }
    }
}

async fn record_slot_status(
    db: Arc<Client>,
    slot: u64,
    thread_id: usize,
    transaction_count: u64,
    vote_transaction_count: u64,
    non_vote_transaction_count: u64,
    block_time: Option<i64>,
) -> Result<(), clickhouse::error::Error> {
    let mut insert = db
        .insert::<SlotStatusRow>("jetstreamer_slot_status")
        .await?;
    insert
        .write(&SlotStatusRow {
            slot,
            transaction_count: transaction_count.min(u32::MAX as u64) as u32,
            vote_transaction_count: vote_transaction_count.min(u32::MAX as u64) as u32,
            non_vote_transaction_count: non_vote_transaction_count.min(u32::MAX as u64) as u32,
            thread_id: thread_id.try_into().unwrap_or(u8::MAX),
            block_time: clamp_block_time(block_time),
        })
        .await?;
    insert.end().await?;
    Ok(())
}

fn clamp_block_time(block_time: Option<i64>) -> u32 {
    match block_time {
        Some(ts) if ts > 0 && ts <= u32::MAX as i64 => ts as u32,
        Some(ts) if ts > u32::MAX as i64 => u32::MAX,
        Some(ts) if ts < 0 => 0,
        _ => 0,
    }
}

// Ensure PluginRunnerError is Send + Sync + 'static
trait _CanSend: Send + Sync + 'static {}
impl _CanSend for PluginRunnerError {}

#[inline]
fn human_readable_count(value: impl Into<u128>) -> String {
    let digits = value.into().to_string();
    let len = digits.len();
    let mut formatted = String::with_capacity(len + len / 3);
    for (idx, byte) in digits.bytes().enumerate() {
        if idx != 0 && (len - idx) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(char::from(byte));
    }
    formatted
}

fn human_readable_duration(seconds: f64) -> String {
    if !seconds.is_finite() {
        return "n/a".into();
    }
    if seconds <= 0.0 {
        return "0s".into();
    }
    if seconds < 60.0 {
        return format!("{:.1}s", seconds);
    }
    let duration = Duration::from_secs(seconds.round() as u64);
    let secs = duration.as_secs();
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3_600;
    let minutes = (secs % 3_600) / 60;
    let seconds_rem = secs % 60;
    if days > 0 {
        if hours > 0 {
            format!("{}d{}h", days, hours)
        } else {
            format!("{}d", days)
        }
    } else if hours > 0 {
        format!("{}h{}m", hours, minutes)
    } else {
        format!("{}m{}s", minutes, seconds_rem)
    }
}
