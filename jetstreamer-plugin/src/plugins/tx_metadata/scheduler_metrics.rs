//! Agave 4.2 scheduler cost and priority over borrowed transaction data.
//!
//! Jetstreamer currently links Agave 3, while the reference Geyser plugin
//! links Agave 4.2. The two dependency graphs cannot coexist because several
//! Solana crates pin mutually exclusive versions. For legacy and v0 messages,
//! Agave 4.2's summed scheduler cost is the five scalar components reproduced
//! below. Account-allocation cost is tracked separately by Agave but is not
//! included in `TransactionCost::sum()`.

use super::{InstructionView, MessageHeaderView, ResolvedAccounts};

const BASE_FEE_BURN_PERCENT: u64 = 50;
const LAMPORTS_PER_SIGNATURE: u64 = 5_000;
const PRIORITY_MULTIPLIER: u64 = 1_000_000;

const SIGNATURE_COST: u64 = 720;
const SECP256K1_VERIFY_COST: u64 = 6_690;
const ED25519_VERIFY_STRICT_COST: u64 = 2_400;
const SECP256R1_VERIFY_COST: u64 = 4_800;
const WRITE_LOCK_UNITS: u64 = 300;
const INSTRUCTION_DATA_BYTES_COST: u16 = 4;

const DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT: u32 = 200_000;
const MAX_BUILTIN_ALLOCATION_COMPUTE_UNIT_LIMIT: u32 = 3_000;
const MAX_COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const MIN_HEAP_FRAME_BYTES: u32 = 32 * 1024;
const MAX_HEAP_FRAME_BYTES: u32 = 256 * 1024;
const MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES: u32 = 64 * 1024 * 1024;
const ACCOUNT_DATA_COST_PAGE_SIZE: u64 = 32 * 1024;
const DEFAULT_HEAP_COST: u64 = 8;
const MICRO_LAMPORTS_PER_LAMPORT: u128 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SchedulerMetrics {
    pub(super) priority: u64,
    pub(super) cost_units: u64,
}

#[derive(Clone, Copy)]
struct SchedulerConfiguration {
    compute_unit_limit: u32,
    priority_fee_lamports: u64,
    loaded_accounts_data_size_limit: u32,
}

#[derive(Default)]
struct SignatureDetails {
    transaction: u64,
    secp256k1: u64,
    ed25519: u64,
    secp256r1: u64,
}

impl SignatureDetails {
    fn total(&self) -> u64 {
        self.transaction
            .saturating_add(self.secp256k1)
            .saturating_add(self.ed25519)
            .saturating_add(self.secp256r1)
    }

    fn cost(&self) -> u64 {
        self.transaction
            .saturating_mul(SIGNATURE_COST)
            .saturating_add(self.secp256k1.saturating_mul(SECP256K1_VERIFY_COST))
            .saturating_add(self.ed25519.saturating_mul(ED25519_VERIFY_STRICT_COST))
            .saturating_add(self.secp256r1.saturating_mul(SECP256R1_VERIFY_COST))
    }
}

/// Mirrors the referenced Agave 4.2 scheduler metrics for legacy/v0 messages.
pub(super) fn calculate<'transaction, I>(
    num_signatures: usize,
    header: MessageHeaderView,
    is_legacy: bool,
    accounts: ResolvedAccounts<'transaction>,
    instructions: I,
) -> Option<SchedulerMetrics>
where
    I: Iterator<Item = InstructionView<'transaction>> + Clone,
{
    let required_signatures = usize::from(header.num_required_signatures);
    let readonly_signed = usize::from(header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(header.num_readonly_unsigned_accounts);
    let unsigned_static = accounts
        .static_keys
        .len()
        .checked_sub(required_signatures)?;
    if num_signatures != required_signatures
        || readonly_signed > required_signatures
        || readonly_unsigned > unsigned_static
        || (is_legacy
            && (!accounts.loaded_writable.is_empty() || !accounts.loaded_readonly.is_empty()))
    {
        return None;
    }

    let account_count = accounts
        .static_keys
        .len()
        .saturating_add(accounts.loaded_writable.len())
        .saturating_add(accounts.loaded_readonly.len());
    if instructions.clone().any(|ix| {
        accounts.get(usize::from(ix.program_id_index)).is_none()
            || ix
                .accounts
                .iter()
                .any(|index| usize::from(*index) >= account_count)
    }) {
        return None;
    }

    let configuration = scheduler_configuration(accounts, instructions.clone())?;
    let signature_details = signature_details(
        u64::from(header.num_required_signatures),
        accounts,
        instructions.clone(),
    );
    let instruction_data_len = instructions
        .clone()
        .try_fold(0usize, |total, ix| total.checked_add(ix.data.len()))?;
    let instruction_data_len = u16::try_from(instruction_data_len).ok()?;
    let readonly_accounts = readonly_signed
        .saturating_add(readonly_unsigned)
        .saturating_add(accounts.loaded_readonly.len());
    let num_write_locks = account_count.checked_sub(readonly_accounts)? as u64;

    let loaded_account_pages = u64::from(configuration.loaded_accounts_data_size_limit)
        .saturating_add(ACCOUNT_DATA_COST_PAGE_SIZE.saturating_sub(1))
        .saturating_div(ACCOUNT_DATA_COST_PAGE_SIZE);
    let cost_units = signature_details
        .cost()
        .saturating_add(num_write_locks.saturating_mul(WRITE_LOCK_UNITS))
        .saturating_add(u64::from(
            instruction_data_len / INSTRUCTION_DATA_BYTES_COST,
        ))
        .saturating_add(u64::from(configuration.compute_unit_limit))
        .saturating_add(loaded_account_pages.saturating_mul(DEFAULT_HEAP_COST));

    let base_fee = signature_details
        .total()
        .saturating_mul(LAMPORTS_PER_SIGNATURE);
    let burned_base_fee = base_fee
        .saturating_mul(BASE_FEE_BURN_PERCENT)
        .saturating_div(100);
    let scheduler_reward = configuration
        .priority_fee_lamports
        .saturating_add(base_fee.saturating_sub(burned_base_fee));
    let priority = scheduler_reward
        .saturating_mul(PRIORITY_MULTIPLIER)
        .saturating_div(cost_units.saturating_add(1));

    Some(SchedulerMetrics {
        priority,
        cost_units,
    })
}

fn scheduler_configuration<'transaction, I>(
    accounts: ResolvedAccounts<'transaction>,
    instructions: I,
) -> Option<SchedulerConfiguration>
where
    I: Iterator<Item = InstructionView<'transaction>>,
{
    let mut requested_heap_size = None;
    let mut requested_compute_unit_limit = None;
    let mut requested_compute_unit_price = None;
    let mut requested_loaded_accounts_data_size_limit = None;
    let mut builtin_instructions = 0u32;
    let mut non_builtin_instructions = 0u32;

    for ix in instructions {
        let program_id = accounts.get(usize::from(ix.program_id_index))?;
        if program_id == &solana_sdk_ids::compute_budget::ID {
            match ix.data.first().copied()? {
                1 => set_once(&mut requested_heap_size, read_u32(ix.data)?)?,
                2 => set_once(&mut requested_compute_unit_limit, read_u32(ix.data)?)?,
                3 => set_once(&mut requested_compute_unit_price, read_u64(ix.data)?)?,
                4 => set_once(
                    &mut requested_loaded_accounts_data_size_limit,
                    read_u32(ix.data)?,
                )?,
                _ => return None,
            }
        } else if is_agave_4_2_builtin(program_id) {
            builtin_instructions = builtin_instructions.saturating_add(1);
        } else {
            // With all features enabled, Agave 4.2 treats the vote program as
            // migrated for default-CU allocation, so votes take this branch.
            non_builtin_instructions = non_builtin_instructions.saturating_add(1);
        }
    }

    if requested_heap_size.is_some_and(|bytes| {
        !(MIN_HEAP_FRAME_BYTES..=MAX_HEAP_FRAME_BYTES).contains(&bytes)
            || !bytes.is_multiple_of(1024)
    }) {
        return None;
    }

    let compute_unit_limit = requested_compute_unit_limit.unwrap_or_else(|| {
        builtin_instructions
            .saturating_mul(MAX_BUILTIN_ALLOCATION_COMPUTE_UNIT_LIMIT)
            .saturating_add(
                non_builtin_instructions.saturating_mul(DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT),
            )
    });
    let compute_unit_limit = compute_unit_limit.min(MAX_COMPUTE_UNIT_LIMIT);
    let compute_unit_price = requested_compute_unit_price.unwrap_or_default();
    let priority_fee_lamports = (compute_unit_price as u128)
        .saturating_mul(compute_unit_limit as u128)
        .saturating_add(MICRO_LAMPORTS_PER_LAMPORT.saturating_sub(1))
        .checked_div(MICRO_LAMPORTS_PER_LAMPORT)
        .and_then(|fee| u64::try_from(fee).ok())
        .unwrap_or(u64::MAX);
    let loaded_accounts_data_size_limit = match requested_loaded_accounts_data_size_limit {
        Some(0) => return None,
        Some(bytes) => bytes.min(MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES),
        None => MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES,
    };

    Some(SchedulerConfiguration {
        compute_unit_limit,
        priority_fee_lamports,
        loaded_accounts_data_size_limit,
    })
}

fn signature_details<'transaction, I>(
    transaction_signatures: u64,
    accounts: ResolvedAccounts<'transaction>,
    instructions: I,
) -> SignatureDetails
where
    I: Iterator<Item = InstructionView<'transaction>>,
{
    let mut details = SignatureDetails {
        transaction: transaction_signatures,
        ..SignatureDetails::default()
    };
    for ix in instructions {
        let Some(program_id) = accounts.get(usize::from(ix.program_id_index)) else {
            continue;
        };
        let count = u64::from(ix.data.first().copied().unwrap_or_default());
        if program_id == &solana_sdk_ids::secp256k1_program::ID {
            details.secp256k1 = details.secp256k1.wrapping_add(count);
        } else if program_id == &solana_sdk_ids::ed25519_program::ID {
            details.ed25519 = details.ed25519.wrapping_add(count);
        } else if program_id == &solana_sdk_ids::secp256r1_program::ID {
            details.secp256r1 = details.secp256r1.wrapping_add(count);
        }
    }
    details
}

fn set_once<T>(target: &mut Option<T>, value: T) -> Option<()> {
    if target.is_some() {
        return None;
    }
    *target = Some(value);
    Some(())
}

fn read_u32(data: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(data.get(1..5)?.try_into().ok()?))
}

fn read_u64(data: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(data.get(1..9)?.try_into().ok()?))
}

fn is_agave_4_2_builtin(program_id: &solana_address::Address) -> bool {
    program_id == &solana_sdk_ids::system_program::ID
        || program_id == &solana_sdk_ids::compute_budget::ID
        || program_id == &solana_sdk_ids::bpf_loader_upgradeable::ID
        || program_id == &solana_sdk_ids::bpf_loader_deprecated::ID
        || program_id == &solana_sdk_ids::bpf_loader::ID
        || program_id == &solana_sdk_ids::loader_v4::ID
        || program_id == &solana_sdk_ids::secp256k1_program::ID
        || program_id == &solana_sdk_ids::ed25519_program::ID
}
