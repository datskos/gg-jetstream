//! Compact per-transaction execution metadata, parsed directly from the
//! firehose's native Solana transaction types.
//!
//! The output retains `gg-mev-hub`'s execution-metadata semantics while
//! intentionally omitting signatures and signature prefixes. Vote
//! transactions retain the compact representation but use their observed
//! compute-unit consumption. No `GhostTransaction` or other intermediate
//! model is constructed.

mod scheduler_metrics;

use std::sync::Arc;

use ahash::RandomState;
use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{BlockData, TransactionData};
use serde::{Deserialize, Serialize};
use solana_address::Address;

use crate::{Plugin, PluginFuture};

const BASE_SIGNATURE_FEE_LAMPORTS: u64 = 5_000;
const TX_METADATA_TABLE: &str = "tx_meta_v2";

/// Number of slots between flushes for the horizon-native per-transaction
/// writer. This keeps each batch bounded even though the general horizon
/// plugin default is tuned for much smaller aggregate outputs.
pub(crate) const HORIZON_FLUSH_INTERVAL_SLOTS: u32 = 32;

#[derive(Row, Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TxMetadataRow {
    pub(crate) slot: u64,
    pub(crate) tx_idx: u32,
    pub(crate) cu: u64,
    pub(crate) cu_limit: Option<u64>,
    pub(crate) cu_price: Option<u64>,
    pub(crate) prio_fee: u64,
    pub(crate) txn_fee: u64,
    pub(crate) scheduler_cost_units: Option<u64>,
    pub(crate) scheduler_priority: Option<u64>,
    pub(crate) is_vote: bool,
    pub(crate) is_success: bool,
    pub(crate) num_ixs: u16,
    pub(crate) num_ixs_builtin: u16,
}

/// Borrowed message-header values used by the scheduler cost model.
#[derive(Clone, Copy)]
pub(crate) struct MessageHeaderView {
    pub(crate) num_required_signatures: u8,
    pub(crate) num_readonly_signed_accounts: u8,
    pub(crate) num_readonly_unsigned_accounts: u8,
}

/// Borrowed resolved-account view in Solana's canonical order: static keys,
/// loaded writable keys, then loaded readonly keys.
#[derive(Clone, Copy)]
pub(crate) struct ResolvedAccounts<'a> {
    static_keys: &'a [Address],
    loaded_writable: &'a [Address],
    loaded_readonly: &'a [Address],
}

impl<'a> ResolvedAccounts<'a> {
    pub(crate) const fn new(
        static_keys: &'a [Address],
        loaded_writable: &'a [Address],
        loaded_readonly: &'a [Address],
    ) -> Self {
        Self {
            static_keys,
            loaded_writable,
            loaded_readonly,
        }
    }

    #[inline]
    fn get(self, index: usize) -> Option<&'a Address> {
        if let Some(key) = self.static_keys.get(index) {
            return Some(key);
        }
        let index = index.checked_sub(self.static_keys.len())?;
        if let Some(key) = self.loaded_writable.get(index) {
            return Some(key);
        }
        self.loaded_readonly
            .get(index.checked_sub(self.loaded_writable.len())?)
    }

    #[inline]
    fn get_static(self, index: usize) -> Option<&'a Address> {
        self.static_keys.get(index)
    }
}

/// Borrowed instruction fields needed by metadata and scheduler calculations.
#[derive(Clone, Copy)]
pub(crate) struct InstructionView<'a> {
    pub(crate) program_id_index: u8,
    pub(crate) accounts: &'a [u8],
    pub(crate) data: &'a [u8],
}

/// Scalar transaction fields plus the borrowed account view needed by the
/// shared firehose/horizon parser.
pub(crate) struct TxMetadataInput<'a> {
    pub(crate) slot: u64,
    pub(crate) tx_idx: u32,
    pub(crate) num_signatures: usize,
    pub(crate) header: MessageHeaderView,
    pub(crate) is_legacy: bool,
    pub(crate) fee: u64,
    pub(crate) compute_units_consumed: Option<u64>,
    pub(crate) is_success: bool,
    pub(crate) accounts: ResolvedAccounts<'a>,
}

/// Builds one ClickHouse row without allocating a resolved-account vector or
/// converting the transaction into another representation.
pub(crate) fn parse_tx_metadata<'transaction, I>(
    input: TxMetadataInput<'transaction>,
    instructions: I,
) -> TxMetadataRow
where
    I: IntoIterator<Item = InstructionView<'transaction>>,
    I::IntoIter: Clone,
{
    let TxMetadataInput {
        slot,
        tx_idx,
        num_signatures,
        header,
        is_legacy,
        fee,
        compute_units_consumed,
        is_success,
        accounts,
    } = input;
    let instructions = instructions.into_iter();
    let scheduler_metrics = scheduler_metrics::calculate(
        num_signatures,
        header,
        is_legacy,
        accounts,
        instructions.clone(),
    );
    let scheduler_cost_units = scheduler_metrics.map(|metrics| metrics.cost_units);
    let scheduler_priority = scheduler_metrics.map(|metrics| metrics.priority);
    let mut instructions = instructions.peekable();
    // Match gg-mev-hub's vote classification: the first top-level instruction
    // targets the vote program through a static account key.
    let is_vote = instructions
        .peek()
        .and_then(|ix| accounts.get_static(ix.program_id_index as usize))
        .is_some_and(|program| program == &solana_sdk_ids::vote::id());

    if is_vote {
        return TxMetadataRow {
            slot,
            tx_idx,
            cu: compute_units_consumed.unwrap_or_default(),
            txn_fee: BASE_SIGNATURE_FEE_LAMPORTS,
            scheduler_cost_units,
            scheduler_priority,
            is_vote: true,
            is_success,
            num_ixs: 1,
            num_ixs_builtin: 1,
            ..TxMetadataRow::default()
        };
    }

    let mut cu_limit = None;
    let mut cu_price = None;
    let mut num_ixs = 0u16;
    let mut num_ixs_builtin = 0u16;

    for ix in instructions {
        num_ixs = num_ixs.saturating_add(1);
        let Some(program) = accounts.get(ix.program_id_index as usize) else {
            continue;
        };

        if is_tx_metadata_builtin(program) {
            num_ixs_builtin = num_ixs_builtin.saturating_add(1);
        }

        if program == &solana_sdk_ids::compute_budget::id() {
            match ix.data {
                [2, a, b, c, d] => {
                    cu_limit = Some(u32::from_le_bytes([*a, *b, *c, *d]) as u64);
                }
                [3, a, b, c, d, e, f, g, h] => {
                    cu_price = Some(u64::from_le_bytes([*a, *b, *c, *d, *e, *f, *g, *h]));
                }
                _ => {}
            }
        }
    }

    let base_fee = (num_signatures as u64).saturating_mul(BASE_SIGNATURE_FEE_LAMPORTS);
    TxMetadataRow {
        slot,
        tx_idx,
        cu: compute_units_consumed.unwrap_or_default(),
        cu_limit,
        cu_price,
        prio_fee: fee.saturating_sub(base_fee),
        txn_fee: fee,
        scheduler_cost_units,
        scheduler_priority,
        is_vote: false,
        is_success,
        num_ixs,
        num_ixs_builtin,
    }
}

#[inline]
fn is_tx_metadata_builtin(program: &Address) -> bool {
    program == &solana_sdk_ids::vote::id()
        || program == &solana_sdk_ids::system_program::id()
        || program == &solana_sdk_ids::compute_budget::id()
        || program == &solana_sdk_ids::bpf_loader_upgradeable::id()
        || program == &solana_sdk_ids::bpf_loader_deprecated::id()
        || program == &solana_sdk_ids::bpf_loader::id()
        || program == &solana_sdk_ids::loader_v4::id()
        || program == &solana_sdk_ids::secp256k1_program::id()
        || program == &solana_sdk_ids::ed25519_program::id()
}

fn row_from_firehose(transaction: &TransactionData) -> TxMetadataRow {
    let message = &transaction.transaction.message;
    let header = message.header();
    let meta = &transaction.transaction_status_meta;
    let accounts = ResolvedAccounts::new(
        message.static_account_keys(),
        &meta.loaded_addresses.writable,
        &meta.loaded_addresses.readonly,
    );

    parse_tx_metadata(
        TxMetadataInput {
            slot: transaction.slot,
            tx_idx: transaction.transaction_slot_index as u32,
            num_signatures: transaction.transaction.signatures.len(),
            header: MessageHeaderView {
                num_required_signatures: header.num_required_signatures,
                num_readonly_signed_accounts: header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
            },
            is_legacy: matches!(message, solana_message::VersionedMessage::Legacy(_)),
            fee: meta.fee,
            compute_units_consumed: meta.compute_units_consumed,
            is_success: meta.status.is_ok(),
            accounts,
        },
        message.instructions().iter().map(|ix| InstructionView {
            program_id_index: ix.program_id_index,
            accounts: &ix.accounts,
            data: &ix.data,
        }),
    )
}

/// Stores one compact execution-metadata row in `tx_meta_v2` for every
/// firehose transaction, using `gg-mev-hub` semantics without its signature
/// columns.
#[derive(Clone)]
pub struct TxMetadataPlugin {
    pending: Arc<DashMap<u64, Vec<TxMetadataRow>, RandomState>>,
}

impl TxMetadataPlugin {
    /// Creates an empty transaction metadata plugin.
    pub fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::with_hasher(RandomState::new())),
        }
    }

    fn take_slot_rows(&self, slot: u64) -> Vec<TxMetadataRow> {
        self.pending
            .remove(&slot)
            .map(|(_, rows)| rows)
            .unwrap_or_default()
    }

    fn drain_all_rows(&self) -> Vec<TxMetadataRow> {
        let slots: Vec<_> = self.pending.iter().map(|entry| *entry.key()).collect();
        let mut rows = Vec::new();
        for slot in slots {
            rows.extend(self.take_slot_rows(slot));
        }
        rows
    }
}

impl Default for TxMetadataPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for TxMetadataPlugin {
    fn name(&self) -> &'static str {
        "Transaction Metadata"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            self.pending
                .entry(transaction.slot)
                .or_default()
                .push(row_from_firehose(transaction));
            Ok(())
        }
        .boxed()
    }

    fn on_block(
        &self,
        _thread_id: usize,
        db: Option<Arc<Client>>,
        block: &BlockData,
    ) -> PluginFuture<'_> {
        let rows = self.take_slot_rows(block.slot());
        async move {
            if let Some(db) = db
                && !rows.is_empty()
            {
                crate::spawn_tracked_write(async move {
                    crate::retry_clickhouse_write("transaction metadata", || {
                        write_tx_metadata_rows(Arc::clone(&db), rows.clone())
                    })
                    .await;
                });
            }
            Ok(())
        }
        .boxed()
    }

    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            if let Some(db) = db {
                ensure_tx_metadata_table(db.as_ref()).await?;
            } else {
                log::warn!(
                    "Transaction Metadata Plugin running without ClickHouse; data will not be persisted."
                );
            }
            Ok(())
        }
        .boxed()
    }

    fn on_exit(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        let rows = self.drain_all_rows();
        async move {
            if let Some(db) = db
                && !rows.is_empty()
            {
                crate::retry_clickhouse_write("transaction metadata (exit flush)", || {
                    write_tx_metadata_rows(Arc::clone(&db), rows.clone())
                })
                .await;
            }
            Ok(())
        }
        .boxed()
    }
}

pub(crate) async fn ensure_tx_metadata_table(db: &Client) -> Result<(), clickhouse::error::Error> {
    db.query(
        r#"
        CREATE TABLE IF NOT EXISTS tx_meta_v2 (
            slot             UInt64,
            tx_idx           UInt32,
            cu               UInt64,
            cu_limit         Nullable(UInt64),
            cu_price         Nullable(UInt64),
            prio_fee         UInt64,
            txn_fee          UInt64,
            scheduler_cost_units Nullable(UInt64),
            scheduler_priority   Nullable(UInt64),
            is_vote          Bool,
            is_success       Bool,
            num_ixs          UInt16,
            num_ixs_builtin  UInt16
        )
        ENGINE = ReplacingMergeTree
        PARTITION BY intDiv(slot, 1000000)
        ORDER BY (slot, tx_idx)
        "#,
    )
    .execute()
    .await?;

    // `CREATE TABLE IF NOT EXISTS` does not evolve an already-created table.
    // Keep startup migration-safe for deployments that already have
    // `tx_meta_v2` from an earlier Jetstreamer build.
    db.query(
        "ALTER TABLE tx_meta_v2 ADD COLUMN IF NOT EXISTS scheduler_cost_units Nullable(UInt64) AFTER txn_fee",
    )
    .execute()
    .await?;
    db.query(
        "ALTER TABLE tx_meta_v2 ADD COLUMN IF NOT EXISTS scheduler_priority Nullable(UInt64) AFTER scheduler_cost_units",
    )
    .execute()
    .await?;

    Ok(())
}

pub(crate) async fn write_tx_metadata_rows(
    db: Arc<Client>,
    rows: Vec<TxMetadataRow>,
) -> Result<(), clickhouse::error::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = db.insert::<TxMetadataRow>(TX_METADATA_TABLE).await?;
    for row in &rows {
        insert.write(row).await?;
    }
    insert.end().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse::Row;

    fn ix(program_id_index: u8, data: &[u8]) -> InstructionView<'_> {
        InstructionView {
            program_id_index,
            accounts: &[],
            data,
        }
    }

    #[test]
    fn clickhouse_columns_match_compact_schema_order() {
        assert_eq!(
            TxMetadataRow::COLUMN_NAMES,
            &[
                "slot",
                "tx_idx",
                "cu",
                "cu_limit",
                "cu_price",
                "prio_fee",
                "txn_fee",
                "scheduler_cost_units",
                "scheduler_priority",
                "is_vote",
                "is_success",
                "num_ixs",
                "num_ixs_builtin",
            ]
        );
    }

    #[test]
    fn writes_to_the_v2_table() {
        assert_eq!(TX_METADATA_TABLE, "tx_meta_v2");
    }

    #[test]
    fn builtin_program_set_matches_gg_mev_hub() {
        let builtin_programs = [
            solana_sdk_ids::vote::id(),
            solana_sdk_ids::system_program::id(),
            solana_sdk_ids::compute_budget::id(),
            solana_sdk_ids::bpf_loader_upgradeable::id(),
            solana_sdk_ids::bpf_loader_deprecated::id(),
            solana_sdk_ids::bpf_loader::id(),
            solana_sdk_ids::loader_v4::id(),
            solana_sdk_ids::secp256k1_program::id(),
            solana_sdk_ids::ed25519_program::id(),
        ];

        assert!(builtin_programs.iter().all(is_tx_metadata_builtin));
        assert!(!is_tx_metadata_builtin(&Address::from_str_const(
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
        )));
    }

    #[test]
    fn parses_non_vote_metadata_without_an_intermediate_transaction() {
        let custom = Address::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");
        let keys = [
            solana_sdk_ids::system_program::id(),
            solana_sdk_ids::compute_budget::id(),
            custom,
        ];
        let cu_limit = [2, 0x40, 0x0d, 0x03, 0x00]; // 200_000
        let cu_price = [3, 42, 0, 0, 0, 0, 0, 0, 0];
        let instructions = [ix(0, &[]), ix(1, &cu_limit), ix(1, &cu_price), ix(2, &[])];

        let row = parse_tx_metadata(
            TxMetadataInput {
                slot: 42,
                tx_idx: 7,
                num_signatures: 2,
                header: MessageHeaderView {
                    num_required_signatures: 2,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                is_legacy: true,
                fee: 15_123,
                compute_units_consumed: Some(321),
                is_success: true,
                accounts: ResolvedAccounts::new(&keys, &[], &[]),
            },
            instructions,
        );

        assert_eq!(row.slot, 42);
        assert_eq!(row.tx_idx, 7);
        assert_eq!(row.cu, 321);
        assert_eq!(row.cu_limit, Some(200_000));
        assert_eq!(row.cu_price, Some(42));
        assert_eq!(row.prio_fee, 5_123);
        assert_eq!(row.txn_fee, 15_123);
        assert!(row.scheduler_cost_units.is_some());
        assert!(row.scheduler_priority.is_some());
        assert!(!row.is_vote);
        assert!(row.is_success);
        assert_eq!(row.num_ixs, 4);
        assert_eq!(row.num_ixs_builtin, 3);
    }

    #[test]
    fn scheduler_metrics_match_agave_cost_model_and_priority_formula() {
        use solana_hash::Hash;
        use solana_message::{Message, MessageHeader, compiled_instruction::CompiledInstruction};

        let payer = Address::new_from_array([1; 32]);
        let writable_account = Address::new_from_array([2; 32]);
        let custom_program = Address::new_from_array([3; 32]);
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: vec![
                payer,
                writable_account,
                solana_sdk_ids::compute_budget::id(),
                custom_program,
            ],
            recent_blockhash: Hash::default(),
            instructions: vec![
                CompiledInstruction {
                    program_id_index: 2,
                    accounts: Vec::new(),
                    data: [vec![2], 100_000_u32.to_le_bytes().to_vec()].concat(),
                },
                CompiledInstruction {
                    program_id_index: 2,
                    accounts: Vec::new(),
                    data: [vec![3], 1_000_000_u64.to_le_bytes().to_vec()].concat(),
                },
                CompiledInstruction {
                    program_id_index: 3,
                    accounts: vec![0, 1],
                    data: vec![1, 2, 3],
                },
            ],
        };
        let row = parse_tx_metadata(
            TxMetadataInput {
                slot: 1,
                tx_idx: 0,
                num_signatures: 1,
                header: MessageHeaderView {
                    num_required_signatures: message.header.num_required_signatures,
                    num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                    num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
                },
                is_legacy: true,
                fee: 105_000,
                compute_units_consumed: Some(50_000),
                is_success: true,
                accounts: ResolvedAccounts::new(&message.account_keys, &[], &[]),
            },
            message.instructions.iter().map(|ix| InstructionView {
                program_id_index: ix.program_id_index,
                accounts: &ix.accounts,
                data: &ix.data,
            }),
        );

        // Agave 4.2 components: 720 signature + 600 write locks +
        // floor(17 / 4) instruction bytes + 100k requested execution +
        // 2,048 loaded-account pages * 8 CU.
        let expected_cost = 117_708;
        let scheduler_reward = 100_000_u64 + 2_500;
        assert_eq!(row.scheduler_cost_units, Some(expected_cost));
        assert_eq!(
            row.scheduler_priority,
            Some(scheduler_reward.saturating_mul(1_000_000) / (expected_cost + 1))
        );
    }

    #[test]
    fn resolves_loaded_program_addresses() {
        let static_keys = [Address::default()];
        let loaded_writable = [solana_sdk_ids::compute_budget::id()];
        let cu_limit = [2, 1, 0, 0, 0];

        let row = parse_tx_metadata(
            TxMetadataInput {
                slot: 1,
                tx_idx: 0,
                num_signatures: 1,
                header: MessageHeaderView {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                is_legacy: false,
                fee: 5_000,
                compute_units_consumed: None,
                is_success: true,
                accounts: ResolvedAccounts::new(&static_keys, &loaded_writable, &[]),
            },
            [ix(1, &cu_limit)],
        );

        assert_eq!(row.cu_limit, Some(1));
        assert_eq!(row.num_ixs_builtin, 1);
    }

    #[test]
    fn invalid_scheduler_configuration_is_stored_as_null() {
        let keys = [Address::default(), solana_sdk_ids::compute_budget::id()];
        let cu_limit = [2, 1, 0, 0, 0];
        let row = parse_tx_metadata(
            TxMetadataInput {
                slot: 1,
                tx_idx: 0,
                num_signatures: 1,
                header: MessageHeaderView {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                is_legacy: true,
                fee: 5_000,
                compute_units_consumed: None,
                is_success: false,
                accounts: ResolvedAccounts::new(&keys, &[], &[]),
            },
            [ix(1, &cu_limit), ix(1, &cu_limit)],
        );

        assert_eq!(row.scheduler_cost_units, None);
        assert_eq!(row.scheduler_priority, None);
    }

    #[test]
    fn vote_rows_use_observed_compute_units() {
        let keys = [Address::default(), solana_sdk_ids::vote::id()];
        let row = parse_tx_metadata(
            TxMetadataInput {
                slot: 42,
                tx_idx: 7,
                num_signatures: 1,
                header: MessageHeaderView {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                is_legacy: true,
                fee: 99_999,
                compute_units_consumed: Some(999_999),
                is_success: false,
                accounts: ResolvedAccounts::new(&keys, &[], &[]),
            },
            [ix(1, &[])],
        );

        assert_eq!(row.cu, 999_999);
        assert_eq!(row.txn_fee, 5_000);
        assert_eq!(row.prio_fee, 0);
        // Agave 4.2 no longer applies the old fixed 3,428-CU simple-vote
        // shortcut. With all features active, vote is treated as migrated and
        // receives the 200k default instruction allocation.
        let scheduler_cost_units = 217_404;
        assert_eq!(row.scheduler_cost_units, Some(scheduler_cost_units));
        assert_eq!(
            row.scheduler_priority,
            Some(2_500_u64.saturating_mul(1_000_000) / scheduler_cost_units.saturating_add(1))
        );
        assert!(row.is_vote);
        assert!(!row.is_success);
        assert_eq!(row.num_ixs, 1);
        assert_eq!(row.num_ixs_builtin, 1);
    }
}
