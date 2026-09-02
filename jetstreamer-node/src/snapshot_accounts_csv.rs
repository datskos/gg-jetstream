use {
    crossbeam_channel::{Receiver, Sender, bounded},
    log::info,
    rayon::{ThreadPoolBuilder, prelude::*},
    solana_account::ReadableAccount,
    solana_accounts_db::is_loadable::IsLoadable,
    solana_runtime::bank::Bank,
    std::{
        env, fs,
        io::{BufWriter, Write},
        path::Path,
        sync::atomic::{AtomicBool, Ordering},
        thread,
        time::Instant,
    },
};

const OUTPUT_BUFFER_BYTES: usize = 8 << 20;
const BATCH_INDEXED_RECORDS: u64 = 8_192;
const ESTIMATED_CSV_ROW_BYTES: usize = 100;
const LOG_EVERY_ACCOUNTS: u64 = 1_000_000;
const DEFAULT_MAX_WORKER_THREADS: usize = 32;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotAccountsCsvStats {
    pub accounts: u64,
    pub skipped_zero_lamport: u64,
    pub non_live_index_entries: u64,
    pub data_bytes: u128,
    pub lamports: u128,
    pub output_bytes: u64,
}

#[derive(Default)]
struct CsvBatch {
    bytes: Vec<u8>,
    indexed_records: u64,
    stats: SnapshotAccountsCsvStats,
}

impl CsvBatch {
    fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(BATCH_INDEXED_RECORDS as usize * ESTIMATED_CSV_ROW_BYTES),
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.indexed_records == 0
    }
}

fn output_directory(output_path: &Path) -> &Path {
    output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

pub fn prepare_snapshot_accounts_csv_output(output_path: &Path) -> Result<(), String> {
    let output_dir = output_directory(output_path);
    fs::create_dir_all(output_dir).map_err(|error| {
        format!(
            "failed to create CSV output directory {}: {error}",
            output_dir.display()
        )
    })?;
    match fs::symlink_metadata(output_path) {
        Ok(_) => Err(format!(
            "refusing to overwrite existing account CSV {}",
            output_path.display()
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "failed to inspect account CSV output {}: {error}",
            output_path.display()
        )),
    }
}

pub fn snapshot_accounts_csv_worker_threads() -> Result<usize, String> {
    let default = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(DEFAULT_MAX_WORKER_THREADS);
    match env::var("JETSTREAMER_SNAPSHOT_CSV_THREADS") {
        Ok(value) => value
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|threads| *threads > 0)
            .ok_or_else(|| {
                format!(
                    "invalid JETSTREAMER_SNAPSHOT_CSV_THREADS='{value}': expected a positive integer"
                )
            }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!(
            "failed to read JETSTREAMER_SNAPSHOT_CSV_THREADS: {error}"
        )),
    }
}

fn send_batch(sender: &Sender<CsvBatch>, batch: &mut CsvBatch) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }
    sender
        .send(std::mem::replace(batch, CsvBatch::new()))
        .map_err(|_| "snapshot account CSV writer stopped before the scan completed".to_string())
}

fn write_csv_batches(
    mut staged: tempfile::NamedTempFile,
    receiver: Receiver<CsvBatch>,
    started: Instant,
) -> Result<(tempfile::NamedTempFile, SnapshotAccountsCsvStats), String> {
    let mut stats = SnapshotAccountsCsvStats::default();
    let mut next_progress = LOG_EVERY_ACCOUNTS;
    {
        let mut output = BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, staged.as_file_mut());
        output
            .write_all(b"pubkey,owner,data_size,lamports\n")
            .map_err(|error| format!("failed to write CSV header: {error}"))?;

        for batch in receiver {
            output
                .write_all(&batch.bytes)
                .map_err(|error| format!("failed to write account CSV batch: {error}"))?;
            stats.accounts = stats.accounts.saturating_add(batch.stats.accounts);
            stats.skipped_zero_lamport = stats
                .skipped_zero_lamport
                .saturating_add(batch.stats.skipped_zero_lamport);
            stats.non_live_index_entries = stats
                .non_live_index_entries
                .saturating_add(batch.stats.non_live_index_entries);
            stats.data_bytes = stats.data_bytes.saturating_add(batch.stats.data_bytes);
            stats.lamports = stats.lamports.saturating_add(batch.stats.lamports);

            if stats.accounts >= next_progress {
                let elapsed = started.elapsed().as_secs_f64();
                info!(
                    "snapshot account CSV progress: accounts={} data_bytes={} lamports={} \
                     zero_lamport_skipped={} non_live_index_entries={} elapsed={:.1}s \
                     accounts_per_sec={:.0}",
                    stats.accounts,
                    stats.data_bytes,
                    stats.lamports,
                    stats.skipped_zero_lamport,
                    stats.non_live_index_entries,
                    elapsed,
                    stats.accounts as f64 / elapsed.max(f64::EPSILON),
                );
                next_progress =
                    (stats.accounts / LOG_EVERY_ACCOUNTS + 1).saturating_mul(LOG_EVERY_ACCOUNTS);
            }
        }
        output
            .flush()
            .map_err(|error| format!("failed to flush account CSV: {error}"))?;
    }

    staged
        .as_file()
        .sync_all()
        .map_err(|error| format!("failed to sync staged account CSV: {error}"))?;
    stats.output_bytes = staged
        .as_file()
        .metadata()
        .map_err(|error| format!("failed to inspect staged account CSV: {error}"))?
        .len();
    Ok((staged, stats))
}

/// Streams every live account visible to `bank` into a new CSV file.
///
/// AccountsIndex bins are scanned in parallel. Each worker resolves the newest
/// visible account version and sends bounded, preformatted batches to one
/// streaming writer. Zero-lamport records are tombstones rather than loadable
/// accounts, so they are counted but omitted. Row order is intentionally
/// unspecified.
pub fn export_snapshot_accounts_csv(
    bank: &Bank,
    output_path: &Path,
    worker_threads: usize,
    shutdown: &AtomicBool,
) -> Result<SnapshotAccountsCsvStats, String> {
    prepare_snapshot_accounts_csv_output(output_path)?;
    if worker_threads == 0 {
        return Err("snapshot account CSV worker count must be positive".to_string());
    }
    if shutdown.load(Ordering::Relaxed) {
        return Err("snapshot account CSV export cancelled before scan".to_string());
    }

    let output_dir = output_directory(output_path);
    let staged = tempfile::NamedTempFile::new_in(output_dir).map_err(|error| {
        format!(
            "failed to create staged CSV in {}: {error}",
            output_dir.display()
        )
    })?;

    // Normal snapshot restoration leaves account state in AppendVec storage,
    // but rooting and flushing here also covers cache-only records and gives
    // the parallel scan one complete, deduplicated primary index to shard.
    let accounts = bank.accounts();
    let _ = accounts.add_root(bank.slot());
    bank.force_flush_accounts_cache();
    let account_bins = &accounts.accounts_db.accounts_index.account_maps;
    let bin_count = account_bins.len();
    let pool = ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .thread_name(|index| format!("snapshotCsv{index:02}"))
        .build()
        .map_err(|error| format!("failed to create snapshot CSV worker pool: {error}"))?;
    let channel_capacity = worker_threads.saturating_mul(2).max(2);
    let (sender, receiver) = bounded::<CsvBatch>(channel_capacity);
    let started = Instant::now();

    info!(
        "snapshot account CSV parallel scan configured: bins={} workers={} batch_records={} \
         queued_batches={}",
        bin_count, worker_threads, BATCH_INDEXED_RECORDS, channel_capacity,
    );

    let writer = thread::Builder::new()
        .name("snapshotCsvWrite".to_string())
        .spawn(move || write_csv_batches(staged, receiver, started))
        .map_err(|error| format!("failed to start snapshot CSV writer: {error}"))?;

    let producer_result = pool.install(|| {
        account_bins.par_iter().try_for_each(|account_bin| {
            if shutdown.load(Ordering::Relaxed) {
                return Err("snapshot account CSV export cancelled".to_string());
            }

            // This allocation is bounded to one index bin per active worker.
            // keys() merges the in-memory and disk-backed index, deduplicating
            // pubkeys within the bin.
            let pubkeys = account_bin.keys();
            let mut batch = CsvBatch::new();
            for pubkey in pubkeys {
                if shutdown.load(Ordering::Relaxed) {
                    return Err("snapshot account CSV export cancelled".to_string());
                }
                batch.indexed_records = batch.indexed_records.saturating_add(1);
                match bank.get_account_modified_slot(&pubkey) {
                    Some((account, _last_modified_slot)) if account.is_loadable() => {
                        writeln!(
                            batch.bytes,
                            "{pubkey},{},{},{}",
                            account.owner(),
                            account.data().len(),
                            account.lamports()
                        )
                        .map_err(|error| format!("failed to format account CSV row: {error}"))?;
                        batch.stats.accounts = batch.stats.accounts.saturating_add(1);
                        batch.stats.data_bytes = batch
                            .stats
                            .data_bytes
                            .saturating_add(account.data().len() as u128);
                        batch.stats.lamports = batch
                            .stats
                            .lamports
                            .saturating_add(account.lamports() as u128);
                    }
                    Some((_account, _last_modified_slot)) => {
                        batch.stats.skipped_zero_lamport =
                            batch.stats.skipped_zero_lamport.saturating_add(1);
                    }
                    None => {
                        // The primary index can retain keys whose newest
                        // zero-lamport version has already been purged. They
                        // are not accounts visible at this Bank.
                        batch.stats.non_live_index_entries =
                            batch.stats.non_live_index_entries.saturating_add(1);
                    }
                }

                if batch.indexed_records >= BATCH_INDEXED_RECORDS {
                    send_batch(&sender, &mut batch)?;
                }
            }
            send_batch(&sender, &mut batch)
        })
    });
    drop(sender);

    let writer_result = writer
        .join()
        .map_err(|_| "snapshot account CSV writer thread panicked".to_string())?;
    if let Err(error) = writer_result.as_ref() {
        return Err(error.clone());
    }
    producer_result?;
    let (staged, stats) = writer_result?;

    if shutdown.load(Ordering::Relaxed) {
        return Err("snapshot account CSV export cancelled before publish".to_string());
    }
    staged.persist_noclobber(output_path).map_err(|error| {
        format!(
            "failed to publish account CSV {}: {}",
            output_path.display(),
            error.error
        )
    })?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_account::AccountSharedData, solana_pubkey::Pubkey,
        solana_runtime::genesis_utils::create_genesis_config,
    };

    #[test]
    fn exports_live_accounts_in_parallel_and_preserves_existing_output() {
        let genesis_config = create_genesis_config(1_000_000);
        let bank = Bank::new_for_tests(&genesis_config.genesis_config);
        let live_pubkey = Pubkey::from([41; 32]);
        let dead_pubkey = Pubkey::from([42; 32]);
        let owner = Pubkey::from([43; 32]);
        bank.store_account(&live_pubkey, &AccountSharedData::new(123, 17, &owner));
        bank.store_account(&dead_pubkey, &AccountSharedData::new(1, 9, &owner));
        bank.store_account(&dead_pubkey, &AccountSharedData::new(0, 9, &owner));
        bank.freeze();

        let output_dir = tempfile::tempdir().unwrap();
        let output_path = output_dir.path().join("accounts.csv");
        let shutdown = AtomicBool::new(false);
        let stats = export_snapshot_accounts_csv(&bank, &output_path, 4, &shutdown).unwrap();
        let csv = fs::read_to_string(&output_path).unwrap();

        assert!(csv.starts_with("pubkey,owner,data_size,lamports\n"));
        assert!(
            csv.contains(&format!("{live_pubkey},{owner},17,123\n")),
            "stats={stats:?} csv={csv:?}"
        );
        assert!(!csv.contains(&dead_pubkey.to_string()));
        assert!(stats.accounts > 0);
        assert!(stats.skipped_zero_lamport + stats.non_live_index_entries > 0);
        assert_eq!(stats.output_bytes, csv.len() as u64);

        let original = fs::read(&output_path).unwrap();
        let error = export_snapshot_accounts_csv(&bank, &output_path, 4, &shutdown).unwrap_err();
        assert!(error.contains("refusing to overwrite"));
        assert_eq!(fs::read(&output_path).unwrap(), original);
    }

    #[test]
    fn cancellation_does_not_publish_partial_output() {
        let genesis_config = create_genesis_config(1_000_000);
        let bank = Bank::new_for_tests(&genesis_config.genesis_config);
        let output_dir = tempfile::tempdir().unwrap();
        let output_path = output_dir.path().join("accounts.csv");
        let shutdown = AtomicBool::new(true);

        let error = export_snapshot_accounts_csv(&bank, &output_path, 2, &shutdown).unwrap_err();
        assert!(error.contains("cancelled"));
        assert!(!output_path.exists());
    }
}
