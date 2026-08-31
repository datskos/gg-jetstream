//! Horizon-native `tx_meta_v2` persistence.
//!
//! Rows are derived directly from horizon's borrowed, reusable
//! [`Transaction`] scratch. Only the compact row is copied out; transaction
//! bodies and account-update data are never cloned or retained.

use std::sync::Arc;

use clickhouse::Client;
use futures_util::FutureExt;
use jetstreamer_horizon::transactions::{Transaction, VersionedMessage};

use crate::PluginFuture;
use crate::horizon::{Consumption, HorizonPlugin, Output, PluginWorker};

use super::tx_metadata::{
    HORIZON_FLUSH_INTERVAL_SLOTS, InstructionView, MessageHeaderView, ResolvedAccounts,
    TxMetadataInput, TxMetadataRow, V1TransactionConfigView, ensure_tx_metadata_table,
    parse_tx_metadata, write_tx_metadata_rows,
};

#[derive(Default)]
struct TxMetadataWorker {
    rows: Vec<TxMetadataRow>,
}

/// Builds the compact metadata projection for one decoded Horizon
/// transaction. Lossless transaction sinks reuse this helper so their
/// compute-budget columns have exactly the same semantics as `tx_meta_v2`.
pub(crate) fn row_from_horizon(slot: u64, tx_index: u32, tx: &Transaction) -> TxMetadataRow {
    let (header, static_keys, instructions, is_legacy, v1_config) = match &tx.message {
        VersionedMessage::Legacy(message) => (
            message.header,
            message.account_keys.as_slice(),
            message.instructions.as_slice(),
            true,
            None,
        ),
        VersionedMessage::V0(message) => (
            message.header,
            message.account_keys.as_slice(),
            message.instructions.as_slice(),
            false,
            None,
        ),
        VersionedMessage::V1(message) => (
            message.header,
            message.account_keys.as_slice(),
            message.instructions.as_slice(),
            false,
            Some(V1TransactionConfigView {
                priority_fee: message.config.priority_fee,
                compute_unit_limit: message.config.compute_unit_limit,
                loaded_accounts_data_size_limit: message.config.loaded_accounts_data_size_limit,
                heap_size: message.config.heap_size,
            }),
        ),
    };
    let accounts = ResolvedAccounts::new(
        static_keys,
        tx.loaded_writable_addresses.as_slice(),
        tx.loaded_readonly_addresses.as_slice(),
    );
    parse_tx_metadata(
        TxMetadataInput {
            slot,
            tx_idx: tx_index,
            num_signatures: tx.signatures.len(),
            header: MessageHeaderView {
                num_required_signatures: header.num_required_signatures,
                num_readonly_signed_accounts: header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
            },
            is_legacy,
            v1_config,
            fee: tx.fee,
            compute_units_consumed: tx.compute_units_consumed,
            is_success: tx.status.is_ok(),
            accounts,
        },
        instructions.iter().map(|ix| InstructionView {
            program_id_index: ix.program_id_index,
            accounts: ix.accounts.as_slice(),
            data: ix.data.as_slice(),
        }),
    )
}

impl PluginWorker for TxMetadataWorker {
    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        self.rows.push(row_from_horizon(slot, tx_index, tx));
    }

    fn flush(&mut self, out: &Output) {
        if self.rows.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.rows);
        let db = out.db();
        out.submit(write_tx_metadata_rows(db, rows));
    }

    fn flush_interval_slots(&self) -> u32 {
        HORIZON_FLUSH_INTERVAL_SLOTS
    }
}

/// Stores compact transaction execution metadata directly from `.jet`
/// transaction records, using `gg-mev-hub` semantics without signatures.
#[derive(Debug, Clone, Default)]
pub struct TxMetadataHorizonPlugin;

impl TxMetadataHorizonPlugin {
    /// Creates a new horizon transaction metadata plugin.
    pub const fn new() -> Self {
        Self
    }
}

impl HorizonPlugin for TxMetadataHorizonPlugin {
    fn name(&self) -> &'static str {
        "Transaction Metadata (horizon)"
    }

    fn spawn_worker(&self, _thread_id: usize) -> Box<dyn PluginWorker> {
        Box::<TxMetadataWorker>::default()
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_account_update_data()
    }

    fn on_start(&self, db: Arc<Client>, _epoch: u64) -> PluginFuture<'_> {
        async move {
            ensure_tx_metadata_table(db.as_ref()).await?;
            Ok(())
        }
        .boxed()
    }
}
