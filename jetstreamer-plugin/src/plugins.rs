/// Per-slot account-write statistics from horizon's account-update stream.
pub mod account_writes;
/// Plugin that records total instructions per slot.
pub mod instruction_tracking;
/// Default plugin that aggregates program invocation statistics.
pub mod program_tracking;
/// Plugin that tracks per-slot pubkey mention counts for popularity analysis.
pub mod pubkey_stats;
/// Horizon-native port of the pubkey mention tracker.
pub mod pubkey_stats_horizon;
/// Lossless Horizon transaction records for ClickHouse.
pub mod transactions_raw_horizon;
/// Plugin that stores compact per-transaction execution metadata.
pub mod tx_metadata;
/// Horizon-native port of the transaction metadata plugin.
pub mod tx_metadata_horizon;
