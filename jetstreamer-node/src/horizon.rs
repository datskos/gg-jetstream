//! Horizon archive recorder.
//!
//! Assembles the two data streams the node produces into per-slot horizon
//! archive frames and streams them to an
//! [`ArchiveWriter`](jetstreamer_horizon::archive::ArchiveWriter):
//!
//! - **Input side (firehose thread)**: the original chain's block metadata
//!   (rewards, blockhashes, block time/height) via
//!   [`record_block_meta`](HorizonRecorder::record_block_meta). Transactions
//!   and entries also originate here but travel through the
//!   `TransactionScheduler` so they reach the recorder on the replay thread
//!   in execution order, paired with their original `TransactionStatusMeta`.
//! - **Replay side (ready-entry thread)**: geyser account updates in true
//!   bank execution order via
//!   [`record_account_update`](HorizonRecorder::record_account_update)
//!   (transaction-owned when the store carried a `txn`, orphan otherwise),
//!   and committed entries with their transactions via
//!   [`record_committed_entry`](HorizonRecorder::record_committed_entry).
//!
//! # Slot lifecycle
//!
//! A slot's frame can only close once its bank froze (the freeze emits the
//! end-of-slot "post" orphan updates: fee distribution, incinerator, …),
//! and the bank for slot `S` freezes when the bank for the next replayed
//! slot is created. Both recorder entry points therefore double as the
//! advance signal: any activity tagged with slot `S` proves every earlier
//! slot is complete, so buffered assemblies `< S` are emitted. Stream
//! order also guarantees the input-side block metadata for those slots
//! arrived before any later slot's entries, so emission never has to wait.
//!
//! Frames are written in canonical wire order (epoch meta → pre-orphans →
//! transactions → post-orphans → block meta → entries); gap slots between
//! consecutive blocks are emitted as leader-skipped frames after checking
//! the slot-presence map (a gap that old-faithful says *should* have a
//! block means the archive would be silently incomplete — fatal).
//!
//! Integrity violations panic: the archive is the product of the run, and
//! the node's replay machinery already treats divergence as fatal.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

use jetstreamer_horizon::account_updates::AccountUpdateView;
use jetstreamer_horizon::archive::{
    ArchiveStats, ArchiveWriter, ArchiveWriterConfig, BlockMeta, EntryRecord, EpochMeta,
};
use jetstreamer_horizon::convert;
use jetstreamer_horizon::ggjet::{
    AccountManifest, CheckpointAccountView, GgjetStats, GgjetUpdateView, GgjetWriter,
    GgjetWriterConfig,
};
use jetstreamer_horizon::transactions::Transaction;
use log::{info, warn};
use solana_account::{AccountSharedData, ReadableAccount};
use solana_address::Address;
use solana_clock::Slot;
use solana_hash::Hash;
use solana_runtime::bank::Bank;
use solana_runtime::bank::KeyedRewardsAndNumPartitions;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::TransactionStatusMeta;

use crate::{SlotPresenceMap, SlotPresenceState};

/// The active archive recorder. Swappable so one process can record an entire
/// range of epochs back to back, writing an independent `.jet` per epoch: each
/// epoch installs a fresh recorder via [`init`] and tears it down via
/// [`finish`]. The holder is only written at epoch boundaries (when replay is
/// paused), so reads on the account-update hot path are effectively
/// uncontended.
static RECORDER: RwLock<Option<Arc<HorizonRecorder>>> = RwLock::new(None);

// Diagnostic counters for the per-account-update hot path. The recorder
// mutex is taken once per account write (~30k/s) from whatever thread is
// committing the batch — i.e. concurrently from the parallel execution
// workers — so if `held`/`wait` approach replay wall-time, the recorder is
// serializing execution at its own mutex rather than execution being the
// true floor.
static RECORDER_HELD_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static RECORDER_WAIT_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// (mutex-held µs, mutex-wait µs) accumulated across the account-update hot
/// path since process start. Read by the progress logger.
pub fn recorder_contention_us() -> (u64, u64) {
    use std::sync::atomic::Ordering;
    (
        RECORDER_HELD_US.load(Ordering::Relaxed),
        RECORDER_WAIT_US.load(Ordering::Relaxed),
    )
}

/// Lock-free mirror of the archive's running on-disk size, updated as
/// buckets flush. Zero until recording is enabled and the first bucket
/// flushes.
static ARCHIVE_BYTES_WRITTEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Current horizon archive size in bytes (0 if recording is disabled).
/// Lock-free — safe to call from the progress thread.
pub fn archive_bytes_written() -> u64 {
    ARCHIVE_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed)
}

/// Controls whether opening an archive may replace an existing file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchiveFileMode {
    /// Preserve the existing epoch writer behavior and truncate the path.
    Truncate,
    /// Atomically refuse to open the archive when the path already exists.
    CreateNew,
}

/// Optional selected-account archive paired with the full horizon archive.
pub struct GgjetOutput<'a> {
    pub path: &'a Path,
    pub manifest_path: &'a Path,
}

/// Installs the full `.jet` recorder and, when requested, a paired `.ggjet`
/// writer. The latter receives its checkpoint later through
/// [`initialize_ggjet_checkpoint`], exactly when replay reaches `slot_start`.
pub fn init_with_ggjet(
    path: &Path,
    epoch: u64,
    slot_start: Slot,
    slot_count: u64,
    presence: Arc<SlotPresenceMap>,
    file_mode: ArchiveFileMode,
    ggjet: Option<GgjetOutput<'_>>,
) -> Result<(), String> {
    let recorder = HorizonRecorder::create_with_ggjet(
        path, epoch, slot_start, slot_count, presence, file_mode, ggjet,
    )?;
    let mut slot = RECORDER
        .write()
        .map_err(|_| "horizon recorder lock poisoned".to_string())?;
    if slot.is_some() {
        return Err("horizon recorder already initialized".to_string());
    }
    *slot = Some(Arc::new(recorder));
    Ok(())
}

/// Writes the selected-account state at `slot_start - 1`. Idempotent so the
/// bank boundary hook can safely be reached again after a firehose restart.
pub fn initialize_ggjet_checkpoint(bank: &Bank) -> Result<Option<GgjetStats>, String> {
    let Some(recorder) = recorder() else {
        return Ok(None);
    };
    recorder.initialize_ggjet_checkpoint(bank)
}

/// Finalizes and removes the active recorder (writes the footer + bucket
/// index), returning its stats. No-op returning `Ok(None)` when recording is
/// disabled. Clears the slot so the next epoch's [`init`] can install a fresh
/// archive.
pub fn finish() -> Result<Option<ArchiveStats>, String> {
    let recorder = RECORDER
        .write()
        .map_err(|_| "horizon recorder lock poisoned".to_string())?
        .take();
    match recorder {
        Some(recorder) => recorder.finish().map(Some),
        None => Ok(None),
    }
}

/// Returns the active recorder when archive output is enabled. The returned
/// handle keeps the recorder alive for the duration of the call even if a
/// concurrent [`finish`] runs (which it never does during replay).
#[inline]
pub fn recorder() -> Option<Arc<HorizonRecorder>> {
    RECORDER.read().ok()?.clone()
}

/// Notifies the active recorder (if any) that the replay deliberately
/// force-skipped `slot` after exhausting fetch retries — its block data is
/// unavailable from old-faithful even though the index marks it present
/// (the block genuinely does not exist). The recorder then records it as
/// leader-skipped instead of treating the gap as a silent drop, which would
/// otherwise panic. No-op when recording is disabled.
pub fn note_force_skipped(slot: Slot) {
    if let Some(recorder) = recorder() {
        recorder.lock().force_skipped.insert(slot);
    }
}

/// One account update, owned (the geyser notification's buffers are only
/// borrowed for the duration of the callback).
struct OwnedAccountUpdate {
    pubkey: Address,
    lamports: u64,
    owner: Address,
    executable: bool,
    rent_epoch: u64,
    write_version: u64,
    data: Vec<u8>,
}

impl OwnedAccountUpdate {
    /// Copies a live geyser notification into an owned update (the only
    /// allocation/copy on the account-update path — the `data` memcpy).
    fn capture(
        pubkey: &solana_pubkey::Pubkey,
        account: &AccountSharedData,
        write_version: u64,
    ) -> Self {
        Self {
            pubkey: Address::new_from_array(pubkey.to_bytes()),
            lamports: account.lamports(),
            owner: Address::new_from_array(account.owner().to_bytes()),
            executable: account.executable(),
            rent_epoch: account.rent_epoch(),
            write_version,
            data: account.data().to_vec(),
        }
    }

    fn as_view(&self) -> AccountUpdateView<'_> {
        AccountUpdateView {
            pubkey: self.pubkey,
            lamports: self.lamports,
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            write_version: self.write_version,
            data: &self.data,
        }
    }
}

/// A captured account update awaiting merge into the recorder, tagged with
/// its slot and owning-transaction signature (`None` for orphan writes).
pub struct CapturedUpdate {
    slot: Slot,
    signature: Option<Signature>,
    update: OwnedAccountUpdate,
}

thread_local! {
    /// Per-thread capture buffer. When `Some`, account-update notifications
    /// land here instead of taking the recorder mutex; the same thread that
    /// enabled capture drains it (see [`begin_capture`] / [`take_captured`]).
    /// Each batch executes on a single thread, so the captured updates are
    /// perfectly ordered with no cross-thread merge.
    static CAPTURE: std::cell::RefCell<Option<Vec<CapturedUpdate>>> =
        const { std::cell::RefCell::new(None) };
}

/// Enables capture on the current thread (call before executing a batch).
pub fn begin_capture() {
    CAPTURE.with(|c| *c.borrow_mut() = Some(Vec::new()));
}

/// Disables capture on the current thread and returns what was collected.
pub fn take_captured() -> Vec<CapturedUpdate> {
    CAPTURE.with(|c| c.borrow_mut().take().unwrap_or_default())
}

/// Records one geyser account update. On a capturing thread (inside batch
/// execution) it pushes lock-free into the thread-local buffer; otherwise
/// (runtime-direct orphan writes on the coordinator) it falls back to the
/// recorder's locking path. No-op when recording is disabled.
pub fn note_account_update(
    slot: Slot,
    pubkey: &solana_pubkey::Pubkey,
    account: &AccountSharedData,
    signature: Option<Signature>,
    write_version: u64,
) {
    let Some(recorder) = recorder() else {
        return;
    };
    let update = OwnedAccountUpdate::capture(pubkey, account, write_version);
    let captured = CAPTURE.with(|c| {
        if let Some(buf) = c.borrow_mut().as_mut() {
            buf.push(CapturedUpdate {
                slot,
                signature,
                update,
            });
            None
        } else {
            Some(update)
        }
    });
    // Not on a capturing thread → orphan write; record directly.
    if let Some(update) = captured {
        recorder.record_owned_update(slot, signature, update);
    }
}

/// A committed transaction paired with its original chain metadata.
struct CommittedTx {
    tx: VersionedTransaction,
    meta: TransactionStatusMeta,
}

/// Everything buffered for one in-flight slot.
#[derive(Default)]
struct SlotAssembly {
    pre_orphans: Vec<OwnedAccountUpdate>,
    post_orphans: Vec<OwnedAccountUpdate>,
    /// Updates keyed by the owning transaction's first signature.
    tx_updates: HashMap<Signature, Vec<OwnedAccountUpdate>>,
    /// Committed transactions in slot-index order.
    txs: Vec<CommittedTx>,
    /// Entry records in entry-index order (ticks included).
    entries: Vec<EntryRecord>,
}

/// Original chain block metadata buffered from the input side.
struct BufferedBlockMeta {
    parent_slot: Slot,
    parent_blockhash: Hash,
    blockhash: Hash,
    rewards: Vec<(Address, solana_runtime::bank::RewardInfo)>,
    num_partitions: Option<u64>,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
}

struct RecorderState {
    writer: ArchiveWriter<BufWriter<File>>,
    ggjet_writer: Option<GgjetWriter<BufWriter<File>>>,
    epoch: u64,
    slot_start: Slot,
    slot_end_inclusive: Slot,
    assemblies: BTreeMap<Slot, SlotAssembly>,
    block_metas: BTreeMap<Slot, BufferedBlockMeta>,
    last_emitted: Option<Slot>,
    epoch_meta_written: bool,
    // Reusable encode scratches (large; allocated once on the heap).
    tx_scratch: Box<Transaction>,
    meta_scratch: Box<BlockMeta>,
    epoch_scratch: Box<EpochMeta>,
    presence: std::sync::Arc<SlotPresenceMap>,
    /// Slots the replay deliberately force-skipped after exhausting fetch
    /// retries. Their block data is genuinely unavailable (the old-faithful
    /// index marks them present, but the block does not exist), so the gap-fill
    /// records them as leader-skipped instead of treating them as silent drops.
    force_skipped: HashSet<Slot>,
    finished: bool,
}

pub struct HorizonRecorder {
    state: Mutex<RecorderState>,
}

impl HorizonRecorder {
    /// Opens the archive file and builds a recorder for `slot_start ..
    /// slot_start + slot_count`.
    #[cfg(test)]
    fn create_with_mode(
        path: &Path,
        epoch: u64,
        slot_start: Slot,
        slot_count: u64,
        presence: std::sync::Arc<SlotPresenceMap>,
        file_mode: ArchiveFileMode,
    ) -> Result<Self, String> {
        Self::create_with_ggjet(
            path, epoch, slot_start, slot_count, presence, file_mode, None,
        )
    }

    fn create_with_ggjet(
        path: &Path,
        epoch: u64,
        slot_start: Slot,
        slot_count: u64,
        presence: std::sync::Arc<SlotPresenceMap>,
        file_mode: ArchiveFileMode,
        ggjet: Option<GgjetOutput<'_>>,
    ) -> Result<Self, String> {
        let file = match file_mode {
            ArchiveFileMode::Truncate => File::create(path),
            ArchiveFileMode::CreateNew => {
                OpenOptions::new().write(true).create_new(true).open(path)
            }
        }
        .map_err(|err| {
            if file_mode == ArchiveFileMode::CreateNew
                && err.kind() == std::io::ErrorKind::AlreadyExists
            {
                format!(
                    "refusing to overwrite existing horizon archive {}",
                    path.display()
                )
            } else {
                format!("failed to create horizon archive {}: {err}", path.display())
            }
        })?;
        let writer = ArchiveWriter::new(
            BufWriter::with_capacity(8 << 20, file),
            epoch,
            slot_start,
            slot_count,
            ArchiveWriterConfig::default(),
        )
        .map_err(|err| format!("failed to initialize horizon archive writer: {err}"))?;
        let ggjet_writer = if let Some(output) = ggjet {
            let manifest = AccountManifest::load(output.manifest_path).map_err(|err| {
                format!(
                    "failed to load ggjet manifest {}: {err}",
                    output.manifest_path.display()
                )
            })?;
            let file = match file_mode {
                ArchiveFileMode::Truncate => File::create(output.path),
                ArchiveFileMode::CreateNew => OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(output.path),
            }
            .map_err(|err| {
                if file_mode == ArchiveFileMode::CreateNew
                    && err.kind() == std::io::ErrorKind::AlreadyExists
                {
                    format!(
                        "refusing to overwrite existing ggjet archive {}",
                        output.path.display()
                    )
                } else {
                    format!(
                        "failed to create ggjet archive {}: {err}",
                        output.path.display()
                    )
                }
            })?;
            let manifest_len = manifest.len();
            let digest = manifest.digest();
            let writer = GgjetWriter::new(
                BufWriter::with_capacity(8 << 20, file),
                manifest,
                slot_start.saturating_sub(1),
                slot_start,
                slot_count,
                GgjetWriterConfig::default(),
            )
            .map_err(|err| format!("failed to initialize ggjet writer: {err}"))?;
            info!(
                "ggjet archive initialized: path={} accounts={} digest={} checkpoint_slot={} updates={}..={}",
                output.path.display(),
                manifest_len,
                hex_digest(&digest),
                slot_start.saturating_sub(1),
                slot_start,
                slot_start.saturating_add(slot_count).saturating_sub(1),
            );
            Some(writer)
        } else {
            None
        };
        Ok(HorizonRecorder {
            state: Mutex::new(RecorderState {
                writer,
                ggjet_writer,
                epoch,
                slot_start,
                slot_end_inclusive: slot_start + slot_count - 1,
                assemblies: BTreeMap::new(),
                block_metas: BTreeMap::new(),
                last_emitted: None,
                epoch_meta_written: false,
                tx_scratch: Transaction::new_boxed(),
                meta_scratch: BlockMeta::new_boxed(),
                epoch_scratch: EpochMeta::new_boxed(),
                presence,
                force_skipped: HashSet::new(),
                finished: false,
            }),
        })
    }

    fn initialize_ggjet_checkpoint(&self, bank: &Bank) -> Result<Option<GgjetStats>, String> {
        const LOG_EVERY: u64 = 25_000;
        let start = std::time::Instant::now();
        let mut state = self.lock();
        let Some(writer) = state.ggjet_writer.as_mut() else {
            return Ok(None);
        };
        if writer.stats().checkpoint_accounts == writer.header().account_count {
            return Ok(Some(*writer.stats()));
        }
        if writer.stats().checkpoint_accounts != 0 {
            return Err(format!(
                "ggjet checkpoint is partially initialized ({}/{})",
                writer.stats().checkpoint_accounts,
                writer.header().account_count
            ));
        }

        let checkpoint_slot = writer.header().checkpoint_slot;
        let accounts = writer.manifest().accounts().to_vec();
        info!(
            "ggjet checkpoint capture starting: checkpoint_slot={} bank_slot={} accounts={}",
            checkpoint_slot,
            bank.slot(),
            accounts.len()
        );
        for (ordinal, address) in accounts.iter().enumerate() {
            let pubkey = solana_pubkey::Pubkey::new_from_array(address.to_bytes());
            match bank.get_account_modified_slot(&pubkey) {
                Some((account, last_modified_slot)) => writer
                    .write_checkpoint_account(Some(CheckpointAccountView {
                        last_modified_slot,
                        lamports: account.lamports(),
                        owner: Address::new_from_array(account.owner().to_bytes()),
                        executable: account.executable(),
                        rent_epoch: account.rent_epoch(),
                        data: account.data(),
                    }))
                    .map_err(|err| format!("ggjet checkpoint write failed: {err}"))?,
                None => writer
                    .write_checkpoint_account(None)
                    .map_err(|err| format!("ggjet checkpoint write failed: {err}"))?,
            }
            let done = ordinal as u64 + 1;
            if done.is_multiple_of(LOG_EVERY) || done == accounts.len() as u64 {
                info!(
                    "ggjet checkpoint progress: {}/{} ({:.1}%) present={} data={} elapsed={:.1}s",
                    done,
                    accounts.len(),
                    done as f64 * 100.0 / accounts.len().max(1) as f64,
                    writer.stats().checkpoint_present,
                    format_bytes(writer.stats().checkpoint_data_bytes),
                    start.elapsed().as_secs_f64(),
                );
            }
        }
        writer
            .finish_checkpoint()
            .map_err(|err| format!("ggjet checkpoint finalization failed: {err}"))?;
        let stats = *writer.stats();
        info!(
            "ggjet checkpoint complete: accounts={} present={} data={} elapsed={:.3}s",
            stats.checkpoint_accounts,
            stats.checkpoint_present,
            format_bytes(stats.checkpoint_data_bytes),
            start.elapsed().as_secs_f64(),
        );
        Ok(Some(stats))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RecorderState> {
        self.state.lock().expect("horizon recorder lock poisoned")
    }

    /// Logs writer progress every `LOG_PROGRESS_EVERY_N_SLOTS` slots.
    /// Called from the block-metadata notifier alongside the existing
    /// periodic replay log.
    pub fn maybe_log_progress(&self, slot: Slot) {
        const LOG_PROGRESS_EVERY_N_SLOTS: u64 = 100;
        if !slot.is_multiple_of(LOG_PROGRESS_EVERY_N_SLOTS) {
            return;
        }
        let state = self.lock();
        let stats = *state.writer.stats();
        let emitted = state.last_emitted;
        drop(state);
        let ratio = if stats.uncompressed_payload_bytes > 0 {
            format!(
                "{:.1}%",
                stats.bytes_written as f64 * 100.0 / stats.uncompressed_payload_bytes as f64
            )
        } else {
            "n/a".to_string()
        };
        info!(
            target: "jetstreamer_node_horizon",
            "horizon progress: emitted_slot={} slots={} blocks={} txs={} tx_updates={} orphan_updates={} written={} ({} of raw payload)",
            emitted.map(|s| s.to_string()).unwrap_or_else(|| "none".to_string()),
            stats.slots,
            stats.blocks,
            stats.transactions,
            stats.account_updates,
            stats.orphan_account_updates,
            stats.bytes_written,
            ratio,
        );
    }

    /// Buffers the original chain's block metadata (firehose thread).
    /// Idempotent per slot — firehose restarts may re-deliver.
    #[allow(clippy::too_many_arguments)] // mirrors the geyser notification's field set
    pub fn record_block_meta(
        &self,
        slot: Slot,
        parent_slot: Slot,
        parent_blockhash: &str,
        blockhash: &str,
        rewards: &KeyedRewardsAndNumPartitions,
        block_time: Option<i64>,
        block_height: Option<u64>,
        executed_transaction_count: u64,
        entry_count: u64,
    ) {
        let parent_blockhash = Hash::from_str(parent_blockhash).unwrap_or_else(|err| {
            panic!("horizon: unparseable parent blockhash for slot {slot}: {err}")
        });
        let blockhash = Hash::from_str(blockhash)
            .unwrap_or_else(|err| panic!("horizon: unparseable blockhash for slot {slot}: {err}"));
        let meta = BufferedBlockMeta {
            parent_slot,
            parent_blockhash,
            blockhash,
            rewards: rewards
                .keyed_rewards
                .iter()
                .map(|(pubkey, info)| (Address::new_from_array(pubkey.to_bytes()), *info))
                .collect(),
            num_partitions: rewards.num_partitions,
            block_time,
            block_height,
            executed_transaction_count,
            entry_count,
        };
        let mut state = self.lock();
        if state.finished {
            return;
        }
        // Re-delivery after a firehose restart replaces the buffered copy;
        // already-emitted slots are simply dropped.
        if state.last_emitted.is_some_and(|last| slot <= last) {
            return;
        }
        state.block_metas.insert(slot, meta);
    }

    /// Records one geyser account update (replay thread, bank execution
    /// order). `txn_signature` is the owning transaction's first signature
    /// for transaction stores, `None` for runtime-direct (orphan) writes.
    ///
    /// Direct (locking) record entry point. In production the orphan path
    /// reaches the recorder via [`note_account_update`] and transaction
    /// updates via the lock-free capture path, so this is exercised mainly
    /// by tests driving a recorder instance directly.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn record_account_update(
        &self,
        slot: Slot,
        pubkey: &solana_pubkey::Pubkey,
        account: &AccountSharedData,
        txn_signature: Option<Signature>,
        write_version: u64,
    ) {
        let update = OwnedAccountUpdate::capture(pubkey, account, write_version);
        self.record_owned_update(slot, txn_signature, update);
    }

    /// Records a pre-built owned update through the recorder mutex. Shared
    /// by the orphan direct path and the batched capture-merge path.
    fn record_owned_update(
        &self,
        slot: Slot,
        signature: Option<Signature>,
        update: OwnedAccountUpdate,
    ) {
        use std::sync::atomic::Ordering;
        let wait_start = std::time::Instant::now();
        let mut state = self.lock();
        let held_start = std::time::Instant::now();
        RECORDER_WAIT_US.fetch_add(
            held_start.duration_since(wait_start).as_micros() as u64,
            Ordering::Relaxed,
        );
        if !state.finished {
            state.route_update(slot, signature, update);
        }
        RECORDER_HELD_US.fetch_add(held_start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    /// Records a whole batch's worth of transaction-owned account updates,
    /// captured lock-free during execution (see [`begin_capture`]). Taken
    /// once per batch on the coordinator thread — off the hot path — so the
    /// recorder mutex sees per-entry frequency, not per-account-write.
    pub fn record_captured_updates(&self, captured: Vec<CapturedUpdate>) {
        if captured.is_empty() {
            return;
        }
        use std::sync::atomic::Ordering;
        let wait_start = std::time::Instant::now();
        let mut state = self.lock();
        let held_start = std::time::Instant::now();
        RECORDER_WAIT_US.fetch_add(
            held_start.duration_since(wait_start).as_micros() as u64,
            Ordering::Relaxed,
        );
        if !state.finished {
            for c in captured {
                state.route_update(c.slot, c.signature, c.update);
            }
        }
        RECORDER_HELD_US.fetch_add(held_start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    /// Records a verified, committed entry (replay thread, entry order).
    /// Ticks pass an empty `txs`.
    pub fn record_committed_entry(
        &self,
        slot: Slot,
        entry_index: usize,
        num_hashes: u64,
        txs: Vec<(VersionedTransaction, TransactionStatusMeta)>,
    ) {
        let mut state = self.lock();
        if state.finished {
            return;
        }
        state.emit_complete_below(slot);
        let assembly = state.assemblies.entry(slot).or_default();
        if assembly.entries.len() != entry_index {
            panic!(
                "horizon: entry discontinuity at slot {slot}: got entry {entry_index}, expected {} \
                 (mid-slot resume is not supported while recording an archive)",
                assembly.entries.len()
            );
        }
        assembly.entries.push(EntryRecord {
            num_hashes,
            tx_count: txs.len() as u32,
        });
        for (tx, meta) in txs {
            assembly.txs.push(CommittedTx { tx, meta });
        }
    }

    /// Emits everything still buffered (including trailing leader-skipped
    /// slots), finalizes the archive, and returns the writer stats.
    ///
    /// The caller must freeze the final replayed bank first so the last
    /// slot's post-transaction orphan updates have been recorded.
    pub fn finish(&self) -> Result<ArchiveStats, String> {
        let mut state = self.lock();
        if state.finished {
            return Err("horizon recorder already finished".to_string());
        }
        state.emit_complete_below(Slot::MAX);
        // Trailing leader-skipped slots through the end of the range.
        let next = state
            .last_emitted
            .map(|s| s + 1)
            .unwrap_or(state.slot_start);
        for slot in next..=state.slot_end_inclusive {
            state.check_gap_slot_skipped(slot);
            state.writer.write_skipped_slot(slot).map_err(|err| {
                format!("horizon: failed to write trailing skipped slot {slot}: {err}")
            })?;
            if let Some(writer) = state.ggjet_writer.as_mut() {
                writer
                    .write_slot_updates(slot, &mut [])
                    .map_err(|err| format!("ggjet: failed to write empty slot {slot}: {err}"))?;
            }
        }
        state.finished = true;
        if !state.block_metas.is_empty() {
            let leftover: Vec<Slot> = state.block_metas.keys().copied().collect();
            return Err(format!(
                "horizon: {} block metadata record(s) were never emitted (slots {:?}…)",
                leftover.len(),
                &leftover[..leftover.len().min(8)]
            ));
        }
        // `finish` consumes the writer; swap in a placeholder writer is not
        // possible, so rebuild via take. The writer is moved out by value.
        let state = &mut *state;
        let writer = std::mem::replace(
            &mut state.writer,
            ArchiveWriter::new(
                BufWriter::new(File::create("/dev/null").map_err(|err| err.to_string())?),
                state.epoch,
                state.slot_start,
                1,
                ArchiveWriterConfig::default(),
            )
            .map_err(|err| err.to_string())?,
        );
        let (sink, stats) = writer
            .finish()
            .map_err(|err| format!("horizon: failed to finalize archive: {err}"))?;
        sink.into_inner()
            .map_err(|err| format!("horizon: failed to flush archive: {err}"))?;
        if let Some(ggjet_writer) = state.ggjet_writer.take() {
            let (sink, ggstats) = ggjet_writer
                .finish()
                .map_err(|err| format!("ggjet: failed to finalize archive: {err}"))?;
            sink.into_inner()
                .map_err(|err| format!("ggjet: failed to flush archive: {err}"))?;
            info!(
                "ggjet archive complete: checkpoint_accounts={} checkpoint_present={} slots={} updates={} buckets={} bytes={} checkpoint_data={} update_data={}",
                ggstats.checkpoint_accounts,
                ggstats.checkpoint_present,
                ggstats.slots,
                ggstats.updates,
                ggstats.buckets,
                ggstats.bytes_written,
                ggstats.checkpoint_data_bytes,
                ggstats.update_data_bytes,
            );
        }
        info!(
            "horizon archive complete: slots={} blocks={} txs={} tx_updates={} orphan_updates={} \
             epochs={} buckets={} bytes={} (payload {} uncompressed)",
            stats.slots,
            stats.blocks,
            stats.transactions,
            stats.account_updates,
            stats.orphan_account_updates,
            stats.epochs,
            stats.buckets,
            stats.bytes_written,
            stats.uncompressed_payload_bytes,
        );
        Ok(stats)
    }
}

impl RecorderState {
    /// Files one update into the right slot assembly: keyed by signature for
    /// transaction-owned writes, or pre/post-transaction orphan groups for
    /// runtime-direct writes (phase decided by whether the slot has seen its
    /// first committed transaction yet — matching the writer's wire order).
    fn route_update(
        &mut self,
        slot: Slot,
        signature: Option<Signature>,
        update: OwnedAccountUpdate,
    ) {
        self.emit_complete_below(slot);
        let assembly = self.assemblies.entry(slot).or_default();
        match signature {
            Some(sig) => assembly.tx_updates.entry(sig).or_default().push(update),
            None if assembly.txs.is_empty() => assembly.pre_orphans.push(update),
            None => assembly.post_orphans.push(update),
        }
    }

    /// Emits every buffered assembly with slot < `boundary`. Activity for
    /// `boundary` proves those slots' banks froze (their post-orphans are
    /// in), and stream order guarantees their input-side block metadata
    /// already arrived.
    fn emit_complete_below(&mut self, boundary: Slot) {
        while let Some((&slot, _)) = self.assemblies.first_key_value() {
            if slot >= boundary {
                break;
            }
            let assembly = self.assemblies.remove(&slot).expect("assembly present");
            self.emit_slot(slot, assembly);
        }
    }

    /// Verifies a gap slot really was leader-skipped before recording it as
    /// such. A slot the old-faithful index marks present must either have been
    /// replayed (not a gap) or deliberately force-skipped by the replay after
    /// exhausting fetch retries (its block data genuinely does not exist — the
    /// index is wrong). A present gap slot that was *not* force-skipped is a
    /// silent drop and would corrupt the archive, so it still panics.
    fn check_gap_slot_skipped(&self, slot: Slot) {
        if self.presence.state(slot) != Some(SlotPresenceState::Present) {
            return; // genuinely leader-skipped per the index
        }
        if self.force_skipped.contains(&slot) {
            warn!(
                target: "jetstreamer_node_horizon",
                "horizon: recording slot {slot} as leader-skipped — the old-faithful index marks \
                 it present, but its block data was unavailable after exhausting fetch retries \
                 (block does not exist)"
            );
            return;
        }
        panic!(
            "horizon: slot {slot} has block data in the old-faithful index but was never \
             replayed; refusing to record it as leader-skipped"
        );
    }

    fn emit_slot(&mut self, slot: Slot, mut assembly: SlotAssembly) {
        // Leader-skipped frames for the gap since the previous block.
        let next = self.last_emitted.map(|s| s + 1).unwrap_or(self.slot_start);
        for gap_slot in next..slot {
            self.check_gap_slot_skipped(gap_slot);
            self.writer
                .write_skipped_slot(gap_slot)
                .unwrap_or_else(|err| {
                    panic!("horizon: failed to write skipped slot {gap_slot}: {err}")
                });
            if let Some(writer) = self.ggjet_writer.as_mut() {
                writer
                    .write_slot_updates(gap_slot, &mut [])
                    .unwrap_or_else(|err| {
                        panic!("ggjet: failed to write empty slot {gap_slot}: {err}")
                    });
            }
        }

        let block_meta = self.block_metas.remove(&slot).unwrap_or_else(|| {
            panic!("horizon: no block metadata buffered for replayed slot {slot}")
        });

        // The selected-account stream is independent of transaction grouping.
        // Gather references in true replay order, then let the writer impose
        // the authoritative `(slot, write_version)` ordering.
        if let Some(writer) = self.ggjet_writer.as_mut() {
            let mut selected = Vec::new();
            selected.extend(
                assembly
                    .pre_orphans
                    .iter()
                    .filter(|update| writer.manifest().contains(&update.pubkey)),
            );
            for committed in &assembly.txs {
                if let Some(signature) = committed.tx.signatures.first()
                    && let Some(updates) = assembly.tx_updates.get(signature)
                {
                    selected.extend(
                        updates
                            .iter()
                            .filter(|update| writer.manifest().contains(&update.pubkey)),
                    );
                }
            }
            selected.extend(
                assembly
                    .post_orphans
                    .iter()
                    .filter(|update| writer.manifest().contains(&update.pubkey)),
            );
            let mut views = selected
                .into_iter()
                .map(|update| GgjetUpdateView {
                    pubkey: update.pubkey,
                    write_version: update.write_version,
                    lamports: update.lamports,
                    owner: update.owner,
                    executable: update.executable,
                    rent_epoch: update.rent_epoch,
                    data: &update.data,
                })
                .collect::<Vec<_>>();
            writer
                .write_slot_updates(slot, &mut views)
                .unwrap_or_else(|err| panic!("ggjet: failed to write slot {slot}: {err}"));
        }

        self.writer
            .begin_slot(slot)
            .unwrap_or_else(|err| panic!("horizon: begin_slot({slot}) failed: {err}"));

        // The epoch notification rides the epoch's first block frame.
        if !self.epoch_meta_written {
            let em = &mut self.epoch_scratch;
            em.clear();
            em.epoch = self.epoch;
            em.start_slot = self.slot_start;
            em.slot_count = self.slot_end_inclusive - self.slot_start + 1;
            em.first_block_slot = slot;
            em.num_reward_partitions = block_meta.num_partitions;
            self.writer
                .write_epoch_meta(em)
                .unwrap_or_else(|err| panic!("horizon: write_epoch_meta failed: {err}"));
            self.epoch_meta_written = true;
        }

        for update in &assembly.pre_orphans {
            self.writer
                .write_orphan_update(&update.as_view())
                .unwrap_or_else(|err| {
                    panic!("horizon: pre-orphan update failed at slot {slot}: {err}")
                });
        }

        for (tx_index, committed) in assembly.txs.iter().enumerate() {
            let scratch = &mut self.tx_scratch;
            convert::populate_transaction(scratch, &committed.tx, &committed.meta).unwrap_or_else(
                |err| {
                    panic!(
                        "horizon: transaction conversion failed at slot {slot} index {tx_index} \
                         sig {:?}: {err}",
                        committed.tx.signatures.first()
                    )
                },
            );
            let sig = committed.tx.signatures.first().unwrap_or_else(|| {
                panic!("horizon: transaction without signature at slot {slot} index {tx_index}")
            });
            if let Some(updates) = assembly.tx_updates.remove(sig) {
                for update in &updates {
                    scratch
                        .push_account_update(&update.as_view())
                        .unwrap_or_else(|err| {
                            panic!(
                                "horizon: account update overflow at slot {slot} sig {sig}: {err}"
                            )
                        });
                }
            }
            self.writer
                .write_transaction(scratch)
                .unwrap_or_else(|err| {
                    panic!(
                        "horizon: write_transaction failed at slot {slot} index {tx_index}: {err}"
                    )
                });
        }

        if !assembly.tx_updates.is_empty() {
            let stray: Vec<String> = assembly
                .tx_updates
                .keys()
                .take(4)
                .map(|sig| sig.to_string())
                .collect();
            panic!(
                "horizon: {} account update group(s) at slot {slot} reference transactions that \
                 were never committed (e.g. {stray:?})",
                assembly.tx_updates.len()
            );
        }

        for update in &assembly.post_orphans {
            self.writer
                .write_orphan_update(&update.as_view())
                .unwrap_or_else(|err| {
                    panic!("horizon: post-orphan update failed at slot {slot}: {err}")
                });
        }

        let meta = &mut self.meta_scratch;
        meta.clear();
        meta.slot = slot;
        meta.parent_slot = block_meta.parent_slot;
        meta.parent_blockhash = block_meta.parent_blockhash;
        meta.blockhash = block_meta.blockhash;
        meta.block_time = block_meta.block_time;
        meta.block_height = block_meta.block_height;
        meta.executed_transaction_count = block_meta.executed_transaction_count;
        meta.entry_count = block_meta.entry_count;
        meta.num_partitions = block_meta.num_partitions;
        for (pubkey, info) in &block_meta.rewards {
            meta.rewards
                .try_push(convert::reward_from_info(
                    *pubkey,
                    info.reward_type,
                    info.lamports,
                    info.post_balance,
                    info.commission_bps,
                ))
                .unwrap_or_else(|_| {
                    panic!(
                        "horizon: block rewards overflow at slot {slot} ({} rewards)",
                        block_meta.rewards.len()
                    )
                });
        }

        self.writer
            .end_slot(meta, &assembly.entries)
            .unwrap_or_else(|err| panic!("horizon: end_slot({slot}) failed: {err}"));
        // Mirror the running file size into a lock-free atomic so the
        // progress thread can read it without contending the recorder mutex.
        // `bytes_written` steps up as buckets flush (every ~128 slots).
        ARCHIVE_BYTES_WRITTEN.store(
            self.writer.stats().bytes_written,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.last_emitted = Some(slot);
    }
}

fn hex_digest(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, SlotVisitor};
    use solana_account::Account;
    use solana_pubkey::Pubkey;

    fn presence_map(start: Slot, states: &[SlotPresenceState]) -> std::sync::Arc<SlotPresenceMap> {
        let end_inclusive = start + states.len() as u64 - 1;
        let mut next_present_after = vec![None; states.len()];
        let mut next: Option<Slot> = None;
        for (i, state) in states.iter().enumerate().rev() {
            next_present_after[i] = next;
            if *state == SlotPresenceState::Present {
                next = Some(start + i as u64);
            }
        }
        std::sync::Arc::new(SlotPresenceMap {
            start,
            end_inclusive,
            states: states.to_vec(),
            next_present_after,
        })
    }

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn account(lamports: u64, data: &[u8], owner: u8) -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports,
            data: data.to_vec(),
            owner: pk(owner),
            executable: false,
            rent_epoch: u64::MAX,
        })
    }

    fn legacy_tx(sig_byte: u8, key: u8) -> (VersionedTransaction, TransactionStatusMeta) {
        let tx = VersionedTransaction {
            signatures: vec![Signature::from([sig_byte; 64])],
            message: solana_message::VersionedMessage::Legacy(solana_message::legacy::Message {
                header: solana_message::MessageHeader {
                    num_required_signatures: 1,
                    ..Default::default()
                },
                account_keys: vec![pk(key)],
                recent_blockhash: solana_hash::Hash::new_unique(),
                instructions: vec![],
            }),
        };
        let meta = TransactionStatusMeta {
            fee: 5_000 + sig_byte as u64,
            pre_balances: vec![100],
            post_balances: vec![90],
            ..Default::default()
        };
        (tx, meta)
    }

    /// (slot, blockhash, n_rewards, pre write_versions, post
    /// write_versions, entries as (num_hashes, tx_count)).
    type BlockSnapshot = (Slot, Hash, usize, Vec<u64>, Vec<u64>, Vec<(u64, u32)>);
    /// (slot, tx_index, sig, fee, updates as (pubkey, write_version, data)).
    type TxSnapshot = (Slot, u32, Signature, u64, Vec<(Address, u64, Vec<u8>)>);

    #[derive(Default)]
    struct Collected {
        epochs: Vec<(u64, u64, u64, Option<u64>)>, // (epoch, first_block_slot, slot_count, partitions)
        skipped: Vec<Slot>,
        blocks: Vec<BlockSnapshot>,
        txs: Vec<TxSnapshot>,
    }

    impl SlotVisitor for Collected {
        fn on_epoch(&mut self, meta: &EpochMeta) {
            self.epochs.push((
                meta.epoch,
                meta.first_block_slot,
                meta.slot_count,
                meta.num_reward_partitions,
            ));
        }

        fn on_transaction(&mut self, slot: Slot, tx_index: u32, tx: &Transaction) {
            let updates = tx
                .iter_account_updates()
                .map(|(m, d)| (m.pubkey, m.write_version, d.to_vec()))
                .collect();
            self.txs
                .push((slot, tx_index, tx.signatures[0], tx.fee, updates));
        }

        fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
            match notification {
                BlockNotification::Skipped(s) => self.skipped.push(s.slot),
                BlockNotification::Block(meta) => self.blocks.push((
                    meta.slot,
                    meta.blockhash,
                    meta.rewards.len(),
                    meta.pre_updates
                        .iter()
                        .map(|(m, _)| m.write_version)
                        .collect(),
                    meta.post_updates
                        .iter()
                        .map(|(m, _)| m.write_version)
                        .collect(),
                    entries.iter().map(|e| (e.num_hashes, e.tx_count)).collect(),
                )),
            }
        }
    }

    /// Drives the recorder through a realistic two-block scenario (in true
    /// replay event order) and verifies the archive reads back exactly.
    #[test]
    fn recorder_end_to_end() {
        std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(recorder_end_to_end_body)
            .expect("spawn test thread")
            .join()
            .expect("join test thread");
    }

    #[test]
    fn create_new_preserves_existing_archive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("existing.jet");
        let original = b"existing archive bytes";
        std::fs::write(&path, original).expect("write existing archive");
        let presence = presence_map(100, &[SlotPresenceState::Missing]);

        let result = HorizonRecorder::create_with_mode(
            &path,
            42,
            100,
            1,
            presence,
            ArchiveFileMode::CreateNew,
        );
        let err = match result {
            Ok(_) => panic!("create-new unexpectedly replaced an existing archive"),
            Err(err) => err,
        };

        assert!(err.contains("refusing to overwrite existing horizon archive"));
        assert_eq!(std::fs::read(&path).expect("read archive"), original);
    }

    fn recorder_end_to_end_body() {
        // Slots 100..=109: blocks at 100 and 105, everything else
        // leader-skipped.
        let mut states = vec![SlotPresenceState::Missing; 10];
        states[0] = SlotPresenceState::Present;
        states[5] = SlotPresenceState::Present;
        let presence = presence_map(100, &states);

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.jet");
        let ggjet_path = dir.path().join("test.ggjet");
        let manifest_path = dir.path().join("accounts.json");
        let mut manifest_entries = [pk(1).to_string(), pk(10).to_string()];
        manifest_entries.sort();
        let manifest_addresses = manifest_entries
            .iter()
            .map(|text| Address::from_str(text).unwrap())
            .collect::<Vec<_>>();
        let digest = AccountManifest::from_accounts(manifest_addresses)
            .unwrap()
            .digest();
        std::fs::write(
            &manifest_path,
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "accountCount": manifest_entries.len(),
                "accountSetSha256": hex_digest(&digest),
                "accounts": manifest_entries,
            }))
            .unwrap(),
        )
        .unwrap();
        let recorder = HorizonRecorder::create_with_ggjet(
            &path,
            42,
            100,
            10,
            presence,
            ArchiveFileMode::Truncate,
            Some(GgjetOutput {
                path: &ggjet_path,
                manifest_path: &manifest_path,
            }),
        )
        .expect("create recorder");
        {
            // The production boundary hook fills these from the Bank. This
            // recorder test uses explicit absence to isolate update routing.
            let mut state = recorder.lock();
            let writer = state.ggjet_writer.as_mut().unwrap();
            writer.write_checkpoint_account(None).unwrap();
            writer.write_checkpoint_account(None).unwrap();
            writer.finish_checkpoint().unwrap();
        }

        let bh_100 = solana_hash::Hash::new_unique();
        let bh_105 = solana_hash::Hash::new_unique();
        let (tx1, meta1) = legacy_tx(0xA1, 1);
        let (tx2, meta2) = legacy_tx(0xA2, 2);
        let (tx3, meta3) = legacy_tx(0xB1, 3);
        let sig1 = tx1.signatures[0];
        let sig2 = tx2.signatures[0];
        let sig3 = tx3.signatures[0];

        // --- input side: block metadata arrives ahead of replay ---
        recorder.record_block_meta(
            100,
            99,
            &Hash::default().to_string(),
            &bh_100.to_string(),
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![(
                    pk(9),
                    solana_runtime::bank::RewardInfo {
                        reward_type: solana_reward_info::RewardType::Voting,
                        lamports: 10,
                        post_balance: 100,
                        commission_bps: Some(500),
                    },
                )],
                num_partitions: Some(7),
            },
            Some(111),
            Some(50),
            2,
            2,
        );

        // --- replay side, slot 100 ---
        // Bank creation: slot-start orphan (sysvar rewrite).
        recorder.record_account_update(100, &pk(10), &account(1, b"sysvar", 20), None, 1);
        // Entry 0: tick.
        recorder.record_committed_entry(100, 0, 8, Vec::new());
        // Entry 1: two transactions; their stores arrive during execution.
        recorder.record_account_update(100, &pk(1), &account(90, b"alpha", 21), Some(sig1), 2);
        recorder.record_account_update(100, &pk(2), &account(80, b"beta", 21), Some(sig2), 3);
        recorder.record_committed_entry(100, 1, 4, vec![(tx1, meta1), (tx2, meta2)]);
        // Freeze of slot 100 (during slot 105's bank creation): slot-end orphan.
        recorder.record_account_update(100, &pk(11), &account(7, b"fees", 20), None, 4);

        // --- input side: slot 105 block metadata ---
        recorder.record_block_meta(
            105,
            100,
            &bh_100.to_string(),
            &bh_105.to_string(),
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![],
                num_partitions: None,
            },
            Some(222),
            Some(51),
            1,
            1,
        );

        // --- replay side, slot 105 (pre-orphan triggers emit of slot 100) ---
        recorder.record_account_update(105, &pk(10), &account(2, b"sysvar2", 20), None, 5);
        recorder.record_account_update(105, &pk(3), &account(70, b"gamma", 21), Some(sig3), 6);
        recorder.record_committed_entry(105, 0, 9, vec![(tx3, meta3)]);
        // Final freeze (driven by `freeze_latest_bank` in the real flow).
        recorder.record_account_update(105, &pk(11), &account(8, b"fees2", 20), None, 7);

        let stats = recorder.finish().expect("finish");
        assert_eq!(stats.slots, 10);
        assert_eq!(stats.blocks, 2);
        assert_eq!(stats.transactions, 3);
        assert_eq!(stats.account_updates, 3);
        assert_eq!(stats.orphan_account_updates, 4);
        assert_eq!(stats.epochs, 1);

        // --- read back and verify ---
        let bytes = std::fs::read(&path).expect("read archive");
        let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).expect("open archive");
        let mut collected = Collected::default();
        let visited = reader.read_slots(100, 64, &mut collected).expect("read");
        assert_eq!(visited, 10);

        assert_eq!(collected.epochs, vec![(42, 100, 10, Some(7))]);
        assert_eq!(
            collected.skipped,
            vec![101, 102, 103, 104, 106, 107, 108, 109]
        );

        assert_eq!(collected.blocks.len(), 2);
        let (slot, blockhash, n_rewards, pre, post, entries) = &collected.blocks[0];
        assert_eq!(*slot, 100);
        assert_eq!(*blockhash, bh_100);
        assert_eq!(*n_rewards, 1);
        assert_eq!(pre, &[1]);
        assert_eq!(post, &[4]);
        assert_eq!(entries, &[(8, 0), (4, 2)]);
        let (slot, blockhash, n_rewards, pre, post, entries) = &collected.blocks[1];
        assert_eq!(*slot, 105);
        assert_eq!(*blockhash, bh_105);
        assert_eq!(*n_rewards, 0);
        assert_eq!(pre, &[5]);
        assert_eq!(post, &[7]);
        assert_eq!(entries, &[(9, 1)]);

        assert_eq!(collected.txs.len(), 3);
        let (slot, tx_index, sig, fee, updates) = &collected.txs[0];
        assert_eq!((*slot, *tx_index, *sig, *fee), (100, 0, sig1, 5_000 + 0xA1));
        assert_eq!(
            updates,
            &[(Address::new_from_array([1; 32]), 2, b"alpha".to_vec())]
        );
        let (slot, tx_index, sig, _, updates) = &collected.txs[1];
        assert_eq!((*slot, *tx_index, *sig), (100, 1, sig2));
        assert_eq!(updates[0].2, b"beta".to_vec());
        let (slot, tx_index, sig, _, updates) = &collected.txs[2];
        assert_eq!((*slot, *tx_index, *sig), (105, 0, sig3));
        assert_eq!(updates[0].2, b"gamma".to_vec());

        #[derive(Default)]
        struct SelectedUpdates(Vec<(Slot, Address, u64, Vec<u8>)>);
        impl jetstreamer_horizon::ggjet::GgjetVisitor for SelectedUpdates {
            fn on_update(
                &mut self,
                slot: u64,
                update: jetstreamer_horizon::ggjet::GgjetUpdateView<'_>,
            ) {
                self.0.push((
                    slot,
                    update.pubkey,
                    update.write_version,
                    update.data.to_vec(),
                ));
            }
        }
        let ggjet_bytes = std::fs::read(&ggjet_path).expect("read ggjet");
        let mut ggjet =
            jetstreamer_horizon::ggjet::GgjetReader::open(std::io::Cursor::new(ggjet_bytes))
                .expect("open ggjet");
        let mut selected = SelectedUpdates::default();
        assert_eq!(ggjet.visit_slots(100, 10, &mut selected).unwrap(), 10);
        assert_eq!(
            selected.0,
            vec![
                (
                    100,
                    Address::new_from_array([10; 32]),
                    1,
                    b"sysvar".to_vec()
                ),
                (100, Address::new_from_array([1; 32]), 2, b"alpha".to_vec()),
                (
                    105,
                    Address::new_from_array([10; 32]),
                    5,
                    b"sysvar2".to_vec()
                ),
            ]
        );
    }

    /// A gap slot that old-faithful says has a block must abort the run
    /// rather than silently recording it as leader-skipped.
    #[test]
    #[should_panic(expected = "refusing to record it as leader-skipped")]
    fn refuses_to_skip_present_slot() {
        let states = vec![SlotPresenceState::Present; 3];
        let presence = presence_map(200, &states);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.jet");
        let recorder = HorizonRecorder::create_with_mode(
            &path,
            1,
            200,
            3,
            presence,
            ArchiveFileMode::Truncate,
        )
        .expect("create recorder");
        // Only slot 202 gets data; 200-201 are gaps the index says exist.
        recorder.record_block_meta(
            202,
            199,
            &Hash::default().to_string(),
            &Hash::default().to_string(),
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![],
                num_partitions: None,
            },
            None,
            None,
            0,
            1,
        );
        recorder.record_committed_entry(202, 0, 1, Vec::new());
        let _ = recorder.finish();
    }

    /// A present gap slot the replay *deliberately* force-skipped (block data
    /// genuinely unavailable) is recorded as leader-skipped, not panicked.
    #[test]
    fn force_skipped_present_slot_records_as_skipped() {
        let states = vec![SlotPresenceState::Present; 3];
        let presence = presence_map(200, &states);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("forced.jet");
        let recorder = HorizonRecorder::create_with_mode(
            &path,
            1,
            200,
            3,
            presence,
            ArchiveFileMode::Truncate,
        )
        .expect("create recorder");
        // 200-201 have no fetchable block (their blocks don't exist despite the
        // index); the replay force-skipped them.
        {
            let mut state = recorder.lock();
            state.force_skipped.insert(200);
            state.force_skipped.insert(201);
        }
        recorder.record_block_meta(
            202,
            199,
            &Hash::default().to_string(),
            &Hash::default().to_string(),
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![],
                num_partitions: None,
            },
            None,
            None,
            0,
            1,
        );
        recorder.record_committed_entry(202, 0, 1, Vec::new());
        let stats = recorder.finish().expect("finish should not panic");
        // 3 slots: 200 + 201 recorded skipped, 202 a block.
        assert_eq!(stats.slots, 3);
        assert_eq!(stats.blocks, 1);
    }
}
