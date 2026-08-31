//! Lossless per-transaction persistence for Horizon `.jet` archives.
//!
//! Each ClickHouse row carries a small searchable projection plus the full
//! lencode-encoded [`Transaction`] record. The record includes the signed
//! message, execution metadata, rewards, logs, token balances, and account
//! updates. Keeping the original Horizon shape avoids a second lossy mapping
//! and lets future consumers recover fields that were not promoted to
//! dedicated ClickHouse columns in this schema version.

use std::sync::Arc;

use clickhouse::{Client, Row};
use futures_util::FutureExt;
use jetstreamer_horizon::transactions::{Transaction, VersionedMessage};
use lencode::prelude::Encode;
use serde::{Deserialize, Serialize};

use crate::PluginFuture;
use crate::horizon::{HorizonPlugin, Output, PluginWorker};

use super::tx_metadata_horizon::row_from_horizon;

const TABLE: &str = "transactions_raw_v1";
const RECORD_FORMAT_VERSION: u16 = 1;
const FLUSH_INTERVAL_SLOTS: u32 = 8;

#[derive(Row, Deserialize, Serialize, Clone, Debug)]
struct RawTransactionRow {
    slot: u64,
    tx_idx: u32,
    /// Base58 primary transaction signature. Empty only for malformed input
    /// that contains no signatures; the complete signature list remains in
    /// `transaction_record`.
    signature: String,
    /// 0 = legacy, 1 = v0, 2 = v1.
    version: u8,
    is_success: bool,
    fee: u64,
    /// Explicit requested limit. Null means the message did not contain a
    /// SetComputeUnitLimit instruction/config value; it does not mean zero.
    compute_units_requested: Option<u64>,
    compute_units_consumed: Option<u64>,
    compute_unit_price_microlamports: Option<u64>,
    /// Version of the lencode `Transaction` payload stored below.
    record_format_version: u16,
    #[serde(with = "serde_bytes")]
    transaction_record: Vec<u8>,
}

fn build_row(slot: u64, tx_idx: u32, tx: &Transaction) -> RawTransactionRow {
    let compact = row_from_horizon(slot, tx_idx, tx);
    let version = match &tx.message {
        VersionedMessage::Legacy(_) => 0,
        VersionedMessage::V0(_) => 1,
        VersionedMessage::V1(_) => 2,
    };
    let signature = tx
        .signatures
        .first()
        .map(ToString::to_string)
        .unwrap_or_default();

    // The callback's Transaction is reusable decoder scratch. Encode the
    // complete record before returning instead of retaining a reference.
    let mut transaction_record = Vec::new();
    tx.encode_ext(&mut transaction_record, None)
        .expect("encoding a decoded Horizon transaction into Vec cannot fail");

    RawTransactionRow {
        slot,
        tx_idx,
        signature,
        version,
        is_success: tx.status.is_ok(),
        fee: tx.fee,
        compute_units_requested: compact.cu_limit,
        compute_units_consumed: tx.compute_units_consumed,
        compute_unit_price_microlamports: compact.cu_price,
        record_format_version: RECORD_FORMAT_VERSION,
        transaction_record,
    }
}

#[derive(Default)]
struct TransactionsRawWorker {
    rows: Vec<RawTransactionRow>,
}

impl PluginWorker for TransactionsRawWorker {
    fn on_transaction(&mut self, slot: u64, tx_idx: u32, tx: &Transaction) {
        self.rows.push(build_row(slot, tx_idx, tx));
    }

    fn flush(&mut self, out: &Output) {
        if self.rows.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.rows);
        let db = out.db();
        out.submit(async move {
            let mut insert = db.insert::<RawTransactionRow>(TABLE).await?;
            for row in &rows {
                insert.write(row).await?;
            }
            insert.end().await
        });
    }

    fn flush_interval_slots(&self) -> u32 {
        FLUSH_INTERVAL_SLOTS
    }
}

/// Stores one lossless Horizon transaction record per ClickHouse row.
#[derive(Debug, Clone, Default)]
pub struct TransactionsRawHorizonPlugin;

impl TransactionsRawHorizonPlugin {
    /// Creates a lossless Horizon transaction plugin.
    pub const fn new() -> Self {
        Self
    }
}

impl HorizonPlugin for TransactionsRawHorizonPlugin {
    fn name(&self) -> &'static str {
        "Raw Transactions (horizon)"
    }

    fn spawn_worker(&self, _thread_id: usize) -> Box<dyn PluginWorker> {
        Box::<TransactionsRawWorker>::default()
    }

    // Keep the default all-fields consumption. The stored record is only
    // lossless when account-update data is materialized by the decoder.

    fn on_start(&self, db: Arc<Client>, _epoch: u64) -> PluginFuture<'_> {
        async move {
            db.query(
                r#"
                CREATE TABLE IF NOT EXISTS transactions_raw_v1 (
                    slot                              UInt64,
                    tx_idx                            UInt32,
                    signature                         String,
                    version                           UInt8,
                    is_success                        Bool,
                    fee                               UInt64,
                    compute_units_requested           Nullable(UInt64),
                    compute_units_consumed            Nullable(UInt64),
                    compute_unit_price_microlamports  Nullable(UInt64),
                    record_format_version              UInt16,
                    transaction_record                 String CODEC(ZSTD(3))
                )
                ENGINE = ReplacingMergeTree
                PARTITION BY intDiv(slot, 1000000)
                ORDER BY (slot, tx_idx)
                "#,
            )
            .execute()
            .await?;
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn row_preserves_full_record_and_requested_compute_units() {
        let mut tx = Transaction::new_boxed();
        tx.fee = 12_345;
        tx.compute_units_consumed = Some(55_000);
        tx.message.force_v1_mut().config.compute_unit_limit = Some(80_000);

        let row = build_row(42, 7, &tx);
        assert_eq!(row.slot, 42);
        assert_eq!(row.tx_idx, 7);
        assert_eq!(row.version, 2);
        assert_eq!(row.compute_units_requested, Some(80_000));
        assert_eq!(row.compute_units_consumed, Some(55_000));
        assert!(!row.transaction_record.is_empty());

        let mut decoded = Transaction::new_boxed();
        decoded
            .decode_into(&mut Cursor::new(&row.transaction_record), None)
            .unwrap();
        assert_eq!(decoded.fee, 12_345);
        assert_eq!(decoded.compute_units_consumed, Some(55_000));
        let VersionedMessage::V1(message) = &decoded.message else {
            panic!("expected v1 message");
        };
        assert_eq!(message.config.compute_unit_limit, Some(80_000));
    }
}
