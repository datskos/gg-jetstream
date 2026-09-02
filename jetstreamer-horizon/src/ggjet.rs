//! Selected-account state archives (`.ggjet`).
//!
//! A `.ggjet` contains a deterministic account manifest, a complete state
//! checkpoint immediately before its update range, and every matching account
//! write in `(slot, write_version)` order.  Checkpoints are chunked by account
//! ordinal and updates are bucketed by slot, so both large writes and random
//! reads stay bounded in memory.

use std::{collections::HashMap, fs, io::SeekFrom, path::Path, str::FromStr};

use lencode::{
    context::{DecoderContext, EncoderContext},
    diff::{DiffDecoder, DiffEncoder},
    prelude::*,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use solana_address::Address;
use xxhash_rust::xxh64::xxh64;

use crate::{
    account_updates::AccountUpdateView,
    archive::{
        ArchiveFormatError, ArchiveReader, BlockNotification, Compression, Consumption,
        EntryRecord, SlotVisitor, account_diff_key,
    },
    dedupe::{new_decoder_context, new_encoder_context, reset_decoder, reset_encoder},
    epochs::EpochMeta,
    transactions::Transaction,
};

/// Opening magic (`GGJET`, version 1, two NUL bytes).
pub const MAGIC: [u8; 8] = *b"GGJET1\0\0";
/// Closing magic in every finalized archive.
pub const MAGIC_END: [u8; 8] = *b"\0\0TEJGG1";
pub const FORMAT_VERSION: u16 = 1;
pub const FOOTER_LEN: usize = 56;
pub const DEFAULT_BUCKET_SLOTS: u16 = 128;
pub const DEFAULT_CHECKPOINT_BUCKET_ACCOUNTS: u32 = 4096;
pub const DEFAULT_CHECKPOINT_BUCKET_BYTES: usize = 64 << 20;

#[derive(Debug, thiserror::Error)]
pub enum GgjetError {
    #[error("bad magic bytes (not a finalized .ggjet archive)")]
    BadMagic,
    #[error("unsupported .ggjet format version {0}")]
    UnsupportedVersion(u16),
    #[error("manifest is invalid: {0}")]
    Manifest(String),
    #[error("manifest digest mismatch: declared {declared}, calculated {calculated}")]
    ManifestDigest {
        declared: String,
        calculated: String,
    },
    #[error("section checksum mismatch at ordinal/slot {0}")]
    Checksum(u64),
    #[error("index checksum mismatch")]
    IndexChecksum,
    #[error("checkpoint has {got} records; expected {expected}")]
    CheckpointCount { got: u64, expected: u64 },
    #[error("slot {got} is out of sequence; expected {expected}")]
    SlotSequence { got: u64, expected: u64 },
    #[error("account {0} is not in the archive manifest")]
    AccountNotInManifest(Address),
    #[error("write versions are not strictly increasing in slot {slot}: {previous} then {got}")]
    NonMonotonicWriteVersion { slot: u64, previous: u64, got: u64 },
    #[error(
        "source .jet range {source_start}..={source_end} does not match .ggjet range {ggjet_start}..={ggjet_end}"
    )]
    SourceRangeMismatch {
        source_start: u64,
        source_end: u64,
        ggjet_start: u64,
        ggjet_end: u64,
    },
    #[error("integer overflow while constructing archive range")]
    RangeOverflow,
    #[error("decode error: {0}")]
    Decode(#[from] lencode::io::Error),
    #[error(".jet decode error: {0}")]
    Jet(#[from] ArchiveFormatError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestJson {
    version: u32,
    source: Option<String>,
    source_version: Option<u64>,
    source_generated_at_unix: Option<u64>,
    account_count: u64,
    account_set_sha256: String,
    accounts: Vec<String>,
}

/// Validated deterministic account set used by both replay and conversion.
#[derive(Debug, Clone)]
pub struct AccountManifest {
    accounts: Vec<Address>,
    index: HashMap<Address, u32>,
    digest: [u8; 32],
    pub source: Option<String>,
    pub source_version: Option<u64>,
    pub source_generated_at_unix: Option<u64>,
}

impl AccountManifest {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, GgjetError> {
        let bytes = fs::read(path)?;
        let json: ManifestJson = serde_json::from_slice(&bytes)?;
        if json.version != 1 {
            return Err(GgjetError::Manifest(format!(
                "unsupported manifest version {}",
                json.version
            )));
        }
        if json.account_count != json.accounts.len() as u64 {
            return Err(GgjetError::Manifest(format!(
                "accountCount={} but accounts has {} entries",
                json.account_count,
                json.accounts.len()
            )));
        }
        if json.accounts.len() > u32::MAX as usize {
            return Err(GgjetError::Manifest(
                "more than u32::MAX accounts are not supported".into(),
            ));
        }

        let mut accounts = Vec::with_capacity(json.accounts.len());
        let mut previous: Option<&str> = None;
        for (ordinal, text) in json.accounts.iter().enumerate() {
            if let Some(prev) = previous
                && prev >= text.as_str()
            {
                return Err(GgjetError::Manifest(format!(
                    "accounts are not strictly sorted at ordinal {ordinal}: {prev:?} then {text:?}"
                )));
            }
            let address = Address::from_str(text).map_err(|err| {
                GgjetError::Manifest(format!(
                    "invalid account at ordinal {ordinal} ({text:?}): {err}"
                ))
            })?;
            accounts.push(address);
            previous = Some(text);
        }

        let digest = manifest_digest(&accounts);
        let declared = decode_sha256(&json.account_set_sha256)?;
        if digest != declared {
            return Err(GgjetError::ManifestDigest {
                declared: json.account_set_sha256,
                calculated: hex(&digest),
            });
        }
        let index = accounts
            .iter()
            .copied()
            .enumerate()
            .map(|(i, address)| (address, i as u32))
            .collect();
        Ok(Self {
            accounts,
            index,
            digest,
            source: json.source,
            source_version: json.source_version,
            source_generated_at_unix: json.source_generated_at_unix,
        })
    }

    /// Builds a manifest from an already ordered account set (primarily for
    /// embedding/test callers). Order is part of the digest and is preserved.
    pub fn from_accounts(accounts: Vec<Address>) -> Result<Self, GgjetError> {
        if accounts.len() > u32::MAX as usize {
            return Err(GgjetError::Manifest(
                "more than u32::MAX accounts are not supported".into(),
            ));
        }
        let mut index = HashMap::with_capacity(accounts.len());
        for (i, address) in accounts.iter().copied().enumerate() {
            if index.insert(address, i as u32).is_some() {
                return Err(GgjetError::Manifest(format!(
                    "duplicate address {address} at ordinal {i}"
                )));
            }
        }
        let digest = manifest_digest(&accounts);
        Ok(Self {
            accounts,
            index,
            digest,
            source: None,
            source_version: None,
            source_generated_at_unix: None,
        })
    }

    pub fn accounts(&self) -> &[Address] {
        &self.accounts
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn index_of(&self, address: &Address) -> Option<u32> {
        self.index.get(address).copied()
    }

    pub fn contains(&self, address: &Address) -> bool {
        self.index.contains_key(address)
    }
}

fn manifest_digest(accounts: &[Address]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for address in accounts {
        hasher.update(address.to_bytes());
    }
    hasher.finalize().into()
}

fn decode_sha256(value: &str) -> Result<[u8; 32], GgjetError> {
    if value.len() != 64 {
        return Err(GgjetError::Manifest(format!(
            "accountSetSha256 must contain 64 hex characters, got {}",
            value.len()
        )));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16).map_err(|_| {
            GgjetError::Manifest("accountSetSha256 contains non-hex characters".into())
        })?;
    }
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    const CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(CHARS[(byte >> 4) as usize] as char);
        out.push(CHARS[(byte & 0xf) as usize] as char);
    }
    out
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct GgjetHeader {
    pub format_version: u16,
    pub bucket_slots: u16,
    pub checkpoint_bucket_accounts: u32,
    pub checkpoint_slot: u64,
    pub update_start_slot: u64,
    pub update_slot_count: u64,
    pub account_count: u64,
    pub account_set_sha256: [u8; 32],
    pub flags: u64,
    pub created_unix_ms: u64,
    pub writer_version: Vec<u8>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
struct BlobHeader {
    first_ordinal: u64,
    item_count: u32,
    compression: Compression,
    uncompressed_len: u64,
    stored_len: u64,
    xxh64: u64,
}

#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct GgjetBucketIndexEntry {
    pub first_slot: u64,
    pub slot_count: u32,
    pub update_count: u64,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Footer {
    index_offset: u64,
    index_len: u64,
    bucket_count: u64,
    index_xxh64: u64,
}

impl Footer {
    fn to_bytes(self) -> [u8; FOOTER_LEN] {
        let mut out = [0u8; FOOTER_LEN];
        out[0..8].copy_from_slice(&self.index_offset.to_le_bytes());
        out[8..16].copy_from_slice(&self.index_len.to_le_bytes());
        out[16..24].copy_from_slice(&self.bucket_count.to_le_bytes());
        out[24..32].copy_from_slice(&self.index_xxh64.to_le_bytes());
        out[48..56].copy_from_slice(&MAGIC_END);
        out
    }

    fn from_bytes(bytes: &[u8; FOOTER_LEN]) -> Result<Self, GgjetError> {
        if bytes[48..56] != MAGIC_END {
            return Err(GgjetError::BadMagic);
        }
        Ok(Self {
            index_offset: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            index_len: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            bucket_count: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            index_xxh64: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GgjetWriterConfig {
    pub bucket_slots: u16,
    pub checkpoint_bucket_accounts: u32,
    /// Soft uncompressed byte ceiling; a single maximum-sized Solana account
    /// may take a bucket slightly above it.
    pub checkpoint_bucket_bytes: usize,
    pub compression: Compression,
    pub zstd_level: i32,
}

impl Default for GgjetWriterConfig {
    fn default() -> Self {
        Self {
            bucket_slots: DEFAULT_BUCKET_SLOTS,
            checkpoint_bucket_accounts: DEFAULT_CHECKPOINT_BUCKET_ACCOUNTS,
            checkpoint_bucket_bytes: DEFAULT_CHECKPOINT_BUCKET_BYTES,
            compression: Compression::Zstd,
            zstd_level: 3,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GgjetStats {
    pub bytes_written: u64,
    pub checkpoint_accounts: u64,
    pub checkpoint_present: u64,
    pub checkpoint_data_bytes: u64,
    pub slots: u64,
    pub updates: u64,
    pub update_data_bytes: u64,
    pub buckets: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckpointAccountView<'a> {
    pub last_modified_slot: u64,
    pub lamports: u64,
    pub owner: Address,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub struct GgjetUpdateView<'a> {
    pub pubkey: Address,
    pub write_version: u64,
    pub lamports: u64,
    pub owner: Address,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: &'a [u8],
}

impl<'a> From<&'a AccountUpdateView<'a>> for GgjetUpdateView<'a> {
    fn from(update: &'a AccountUpdateView<'a>) -> Self {
        Self {
            pubkey: update.pubkey,
            write_version: update.write_version,
            lamports: update.lamports,
            owner: update.owner,
            executable: update.executable,
            rent_epoch: update.rent_epoch,
            data: update.data,
        }
    }
}

pub struct GgjetWriter<W: std::io::Write> {
    sink: W,
    header: GgjetHeader,
    manifest: AccountManifest,
    config: GgjetWriterConfig,
    file_offset: u64,
    stats: GgjetStats,
    checkpoint_buf: Vec<u8>,
    checkpoint_bucket_first: u64,
    checkpoint_bucket_count: u32,
    checkpoint_finished: bool,
    bucket_buf: Vec<u8>,
    bucket_first_slot: Option<u64>,
    bucket_slot_count: u32,
    bucket_update_count: u64,
    index: Vec<GgjetBucketIndexEntry>,
    enc_ctx: EncoderContext,
    diff: DiffEncoder,
}

impl<W: std::io::Write> GgjetWriter<W> {
    pub fn new(
        mut sink: W,
        manifest: AccountManifest,
        checkpoint_slot: u64,
        update_start_slot: u64,
        update_slot_count: u64,
        config: GgjetWriterConfig,
    ) -> Result<Self, GgjetError> {
        if config.bucket_slots == 0
            || config.checkpoint_bucket_accounts == 0
            || config.checkpoint_bucket_bytes == 0
        {
            return Err(GgjetError::Manifest(
                "bucket sizes must be greater than zero".into(),
            ));
        }
        update_start_slot
            .checked_add(update_slot_count)
            .ok_or(GgjetError::RangeOverflow)?;
        let header = GgjetHeader {
            format_version: FORMAT_VERSION,
            bucket_slots: config.bucket_slots,
            checkpoint_bucket_accounts: config.checkpoint_bucket_accounts,
            checkpoint_slot,
            update_start_slot,
            update_slot_count,
            account_count: manifest.len() as u64,
            account_set_sha256: manifest.digest(),
            flags: 0,
            created_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            writer_version: env!("CARGO_PKG_VERSION").as_bytes().to_vec(),
        };

        sink.write_all(&MAGIC)?;
        let header_len = write_framed(&mut sink, &header)?;
        let mut file_offset = MAGIC.len() as u64 + header_len;

        // Manifest bytes are canonical raw pubkeys in manifest order.  Their
        // SHA-256 is exactly account_set_sha256.
        let mut manifest_raw = Vec::with_capacity(manifest.len() * 32);
        for address in manifest.accounts() {
            manifest_raw.extend_from_slice(&address.to_bytes());
        }
        file_offset += write_blob(
            &mut sink,
            0,
            manifest.len() as u32,
            &manifest_raw,
            config.compression,
            config.zstd_level,
        )?;

        Ok(Self {
            sink,
            header,
            manifest,
            config,
            file_offset,
            stats: GgjetStats {
                bytes_written: file_offset,
                ..Default::default()
            },
            checkpoint_buf: Vec::with_capacity(8 << 20),
            checkpoint_bucket_first: 0,
            checkpoint_bucket_count: 0,
            checkpoint_finished: false,
            bucket_buf: Vec::with_capacity(8 << 20),
            bucket_first_slot: None,
            bucket_slot_count: 0,
            bucket_update_count: 0,
            index: Vec::new(),
            enc_ctx: new_encoder_context(),
            diff: DiffEncoder::with_capacity(64 * 1024),
        })
    }

    pub fn header(&self) -> &GgjetHeader {
        &self.header
    }

    pub fn manifest(&self) -> &AccountManifest {
        &self.manifest
    }

    pub fn stats(&self) -> &GgjetStats {
        &self.stats
    }

    /// Appends the next checkpoint record. Its pubkey is implicit from the
    /// matching manifest ordinal; `None` explicitly records absence.
    pub fn write_checkpoint_account(
        &mut self,
        account: Option<CheckpointAccountView<'_>>,
    ) -> Result<(), GgjetError> {
        if self.checkpoint_finished {
            return Err(GgjetError::CheckpointCount {
                got: self.stats.checkpoint_accounts + 1,
                expected: self.header.account_count,
            });
        }
        if self.stats.checkpoint_accounts >= self.header.account_count {
            return Err(GgjetError::CheckpointCount {
                got: self.stats.checkpoint_accounts + 1,
                expected: self.header.account_count,
            });
        }
        self.checkpoint_buf.push(account.is_some() as u8);
        if let Some(account) = account {
            account
                .last_modified_slot
                .encode_ext(&mut self.checkpoint_buf, None)?;
            account
                .lamports
                .encode_ext(&mut self.checkpoint_buf, None)?;
            self.checkpoint_buf
                .extend_from_slice(&account.owner.to_bytes());
            self.checkpoint_buf.push(account.executable as u8);
            account
                .rent_epoch
                .encode_ext(&mut self.checkpoint_buf, None)?;
            (account.data.len() as u64).encode_ext(&mut self.checkpoint_buf, None)?;
            self.checkpoint_buf.extend_from_slice(account.data);
            self.stats.checkpoint_present += 1;
            self.stats.checkpoint_data_bytes += account.data.len() as u64;
        }
        self.checkpoint_bucket_count += 1;
        self.stats.checkpoint_accounts += 1;
        if self.checkpoint_bucket_count == self.config.checkpoint_bucket_accounts
            || self.checkpoint_buf.len() >= self.config.checkpoint_bucket_bytes
        {
            self.flush_checkpoint_bucket()?;
        }
        Ok(())
    }

    pub fn finish_checkpoint(&mut self) -> Result<(), GgjetError> {
        if self.checkpoint_finished {
            return Ok(());
        }
        if self.stats.checkpoint_accounts != self.header.account_count {
            return Err(GgjetError::CheckpointCount {
                got: self.stats.checkpoint_accounts,
                expected: self.header.account_count,
            });
        }
        self.flush_checkpoint_bucket()?;
        // Self-delimiting checkpoint section. Byte-based flushing means the
        // exact number of checkpoint buckets is intentionally not derivable
        // from account_count alone.
        let written = write_blob(
            &mut self.sink,
            self.header.account_count,
            0,
            &[],
            Compression::None,
            self.config.zstd_level,
        )?;
        self.file_offset += written;
        self.stats.bytes_written += written;
        self.checkpoint_finished = true;
        Ok(())
    }

    fn flush_checkpoint_bucket(&mut self) -> Result<(), GgjetError> {
        if self.checkpoint_bucket_count == 0 {
            return Ok(());
        }
        let written = write_blob(
            &mut self.sink,
            self.checkpoint_bucket_first,
            self.checkpoint_bucket_count,
            &self.checkpoint_buf,
            self.config.compression,
            self.config.zstd_level,
        )?;
        self.file_offset += written;
        self.stats.bytes_written += written;
        self.checkpoint_bucket_first += self.checkpoint_bucket_count as u64;
        self.checkpoint_bucket_count = 0;
        self.checkpoint_buf.clear();
        Ok(())
    }

    /// Writes exactly one slot frame, including empty/skipped slots. Matching
    /// updates are sorted and validated by `write_version` before encoding.
    pub fn write_slot_updates(
        &mut self,
        slot: u64,
        updates: &mut [GgjetUpdateView<'_>],
    ) -> Result<(), GgjetError> {
        self.finish_checkpoint()?;
        let expected = self
            .header
            .update_start_slot
            .checked_add(self.stats.slots)
            .ok_or(GgjetError::RangeOverflow)?;
        if slot != expected {
            return Err(GgjetError::SlotSequence {
                got: slot,
                expected,
            });
        }
        if self.stats.slots >= self.header.update_slot_count {
            return Err(GgjetError::SlotSequence {
                got: slot,
                expected,
            });
        }
        if let Some(first) = self.bucket_first_slot
            && (slot - self.header.update_start_slot) / self.config.bucket_slots as u64
                != (first - self.header.update_start_slot) / self.config.bucket_slots as u64
        {
            self.flush_update_bucket()?;
        }
        self.bucket_first_slot.get_or_insert(slot);

        updates.sort_by_key(|update| update.write_version);
        for pair in updates.windows(2) {
            if pair[0].write_version >= pair[1].write_version {
                return Err(GgjetError::NonMonotonicWriteVersion {
                    slot,
                    previous: pair[0].write_version,
                    got: pair[1].write_version,
                });
            }
        }

        slot.encode_ext(&mut self.bucket_buf, None)?;
        (updates.len() as u64).encode_ext(&mut self.bucket_buf, None)?;
        for update in updates.iter() {
            let ordinal = self
                .manifest
                .index_of(&update.pubkey)
                .ok_or(GgjetError::AccountNotInManifest(update.pubkey))?;
            ordinal.encode_ext(&mut self.bucket_buf, None)?;
            update
                .write_version
                .encode_ext(&mut self.bucket_buf, None)?;
            update.lamports.encode_ext(&mut self.bucket_buf, None)?;
            update
                .owner
                .encode_ext(&mut self.bucket_buf, Some(&mut self.enc_ctx))?;
            update.executable.encode_ext(&mut self.bucket_buf, None)?;
            update.rent_epoch.encode_ext(&mut self.bucket_buf, None)?;
            self.diff.set_key(account_diff_key(&update.pubkey));
            self.diff.encode_blob(update.data, &mut self.bucket_buf)?;
            self.stats.update_data_bytes += update.data.len() as u64;
        }
        self.bucket_slot_count += 1;
        self.bucket_update_count += updates.len() as u64;
        self.stats.slots += 1;
        self.stats.updates += updates.len() as u64;
        Ok(())
    }

    fn flush_update_bucket(&mut self) -> Result<(), GgjetError> {
        let Some(first_slot) = self.bucket_first_slot.take() else {
            return Ok(());
        };
        let uncompressed_len = self.bucket_buf.len() as u64;
        let stored = compress(
            &self.bucket_buf,
            self.config.compression,
            self.config.zstd_level,
        )?;
        let header = BlobHeader {
            first_ordinal: first_slot,
            item_count: self.bucket_slot_count,
            compression: self.config.compression,
            uncompressed_len,
            stored_len: stored.len() as u64,
            xxh64: xxh64(&stored, 0),
        };
        let offset = self.file_offset;
        let header_len = write_framed(&mut self.sink, &header)?;
        self.sink.write_all(&stored)?;
        let len = header_len + stored.len() as u64;
        self.index.push(GgjetBucketIndexEntry {
            first_slot,
            slot_count: self.bucket_slot_count,
            update_count: self.bucket_update_count,
            offset,
            len,
        });
        self.file_offset += len;
        self.stats.bytes_written += len;
        self.stats.buckets += 1;
        self.bucket_buf.clear();
        self.bucket_slot_count = 0;
        self.bucket_update_count = 0;
        reset_encoder(&mut self.enc_ctx);
        self.diff.clear();
        Ok(())
    }

    pub fn finish(mut self) -> Result<(W, GgjetStats), GgjetError> {
        self.finish_checkpoint()?;
        if self.stats.slots != self.header.update_slot_count {
            let expected = self
                .header
                .update_start_slot
                .checked_add(self.stats.slots)
                .ok_or(GgjetError::RangeOverflow)?;
            return Err(GgjetError::SlotSequence {
                got: expected,
                expected: self
                    .header
                    .update_start_slot
                    .checked_add(self.header.update_slot_count)
                    .ok_or(GgjetError::RangeOverflow)?,
            });
        }
        self.flush_update_bucket()?;

        let index_offset = self.file_offset;
        let mut index_bytes = Vec::with_capacity(self.index.len() * 40 + 8);
        (self.index.len() as u64).encode_ext(&mut index_bytes, None)?;
        for entry in &self.index {
            entry.encode_ext(&mut index_bytes, None)?;
        }
        self.sink.write_all(&index_bytes)?;
        let footer = Footer {
            index_offset,
            index_len: index_bytes.len() as u64,
            bucket_count: self.index.len() as u64,
            index_xxh64: xxh64(&index_bytes, 0),
        };
        self.sink.write_all(&footer.to_bytes())?;
        self.sink.flush()?;
        self.stats.bytes_written += index_bytes.len() as u64 + FOOTER_LEN as u64;
        Ok((self.sink, self.stats))
    }
}

fn compress(raw: &[u8], compression: Compression, level: i32) -> Result<Vec<u8>, GgjetError> {
    match compression {
        Compression::None => Ok(raw.to_vec()),
        Compression::Zstd => Ok(zstd::bulk::compress(raw, level)?),
    }
}

fn decompress(header: &BlobHeader, stored: &[u8]) -> Result<Vec<u8>, GgjetError> {
    if stored.len() as u64 != header.stored_len || xxh64(stored, 0) != header.xxh64 {
        return Err(GgjetError::Checksum(header.first_ordinal));
    }
    let raw = match header.compression {
        Compression::None => stored.to_vec(),
        Compression::Zstd => zstd::bulk::decompress(stored, header.uncompressed_len as usize)?,
    };
    if raw.len() as u64 != header.uncompressed_len {
        return Err(GgjetError::Checksum(header.first_ordinal));
    }
    Ok(raw)
}

fn write_blob(
    sink: &mut impl std::io::Write,
    first_ordinal: u64,
    item_count: u32,
    raw: &[u8],
    compression: Compression,
    level: i32,
) -> Result<u64, GgjetError> {
    let stored = compress(raw, compression, level)?;
    let header = BlobHeader {
        first_ordinal,
        item_count,
        compression,
        uncompressed_len: raw.len() as u64,
        stored_len: stored.len() as u64,
        xxh64: xxh64(&stored, 0),
    };
    let header_len = write_framed(sink, &header)?;
    sink.write_all(&stored)?;
    Ok(header_len + stored.len() as u64)
}

fn write_framed<T: Encode>(sink: &mut impl std::io::Write, value: &T) -> Result<u64, GgjetError> {
    let mut bytes = Vec::with_capacity(128);
    value.encode_ext(&mut bytes, None)?;
    let mut prefix = Vec::with_capacity(8);
    (bytes.len() as u64).encode_ext(&mut prefix, None)?;
    sink.write_all(&prefix)?;
    sink.write_all(&bytes)?;
    Ok((prefix.len() + bytes.len()) as u64)
}

fn read_varint(source: &mut impl std::io::Read) -> Result<u64, GgjetError> {
    let mut first = [0u8; 1];
    source.read_exact(&mut first)?;
    if first[0] & 0x80 == 0 {
        return Ok(first[0] as u64);
    }
    let n = (first[0] & 0x7f) as usize;
    if n > 8 {
        return Err(lencode::io::Error::InvalidData.into());
    }
    let mut bytes = [0u8; 8];
    source.read_exact(&mut bytes[..n])?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_framed<T: Decode>(source: &mut impl std::io::Read) -> Result<(T, u64), GgjetError> {
    let len = read_varint(source)? as usize;
    let mut bytes = vec![0u8; len];
    source.read_exact(&mut bytes)?;
    let mut cursor = lencode::io::Cursor::new(bytes.as_slice());
    let value = T::decode_ext(&mut cursor, None)?;
    Ok((value, len as u64 + encoded_varint_len(len as u64) as u64))
}

fn read_framed_cursor<T: Decode>(cursor: &mut lencode::io::Cursor<&[u8]>) -> Result<T, GgjetError> {
    let len = u64::decode_ext(cursor, None)? as usize;
    let Some(bytes) = cursor.buf().and_then(|buf| buf.get(..len)) else {
        return Err(lencode::io::Error::ReaderOutOfData.into());
    };
    let mut nested = lencode::io::Cursor::new(bytes);
    let value = T::decode_ext(&mut nested, None)?;
    cursor.advance(len);
    Ok(value)
}

fn encoded_varint_len(value: u64) -> usize {
    if value < 0x80 {
        1
    } else {
        1 + (64 - value.leading_zeros() as usize).div_ceil(8)
    }
}

pub trait GgjetVisitor {
    fn on_checkpoint(&mut self, _pubkey: Address, _account: Option<CheckpointAccountView<'_>>) {}
    fn on_update(&mut self, _slot: u64, _update: GgjetUpdateView<'_>) {}
}

pub struct GgjetReader<R: std::io::Read + std::io::Seek> {
    source: R,
    header: GgjetHeader,
    manifest: AccountManifest,
    checkpoint_offset: u64,
    index: Vec<GgjetBucketIndexEntry>,
}

impl<R: std::io::Read + std::io::Seek> GgjetReader<R> {
    pub fn open(mut source: R) -> Result<Self, GgjetError> {
        source.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        source.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(GgjetError::BadMagic);
        }
        let (header, _) = read_framed::<GgjetHeader>(&mut source)?;
        if header.format_version != FORMAT_VERSION {
            return Err(GgjetError::UnsupportedVersion(header.format_version));
        }

        let (manifest_header, _) = read_framed::<BlobHeader>(&mut source)?;
        let mut stored = vec![0u8; manifest_header.stored_len as usize];
        source.read_exact(&mut stored)?;
        let raw = decompress(&manifest_header, &stored)?;
        let expected_bytes = header
            .account_count
            .checked_mul(32)
            .ok_or(GgjetError::RangeOverflow)? as usize;
        if manifest_header.first_ordinal != 0
            || manifest_header.item_count as u64 != header.account_count
            || raw.len() != expected_bytes
        {
            return Err(GgjetError::Manifest(
                "embedded manifest section has inconsistent dimensions".into(),
            ));
        }
        let accounts = raw
            .chunks_exact(32)
            .map(|chunk| Address::new_from_array(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        let manifest = AccountManifest::from_accounts(accounts)?;
        if manifest.digest() != header.account_set_sha256 {
            return Err(GgjetError::ManifestDigest {
                declared: hex(&header.account_set_sha256),
                calculated: hex(&manifest.digest()),
            });
        }
        let checkpoint_offset = source.stream_position()?;

        source.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer_bytes = [0u8; FOOTER_LEN];
        source.read_exact(&mut footer_bytes)?;
        let footer = Footer::from_bytes(&footer_bytes)?;
        source.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_bytes = vec![0u8; footer.index_len as usize];
        source.read_exact(&mut index_bytes)?;
        if xxh64(&index_bytes, 0) != footer.index_xxh64 {
            return Err(GgjetError::IndexChecksum);
        }
        let mut cursor = lencode::io::Cursor::new(index_bytes.as_slice());
        let count = u64::decode_ext(&mut cursor, None)? as usize;
        if count as u64 != footer.bucket_count {
            return Err(GgjetError::IndexChecksum);
        }
        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            index.push(GgjetBucketIndexEntry::decode_ext(&mut cursor, None)?);
        }
        let mut expected_slot = header.update_start_slot;
        for entry in &index {
            if entry.first_slot != expected_slot || entry.slot_count == 0 {
                return Err(GgjetError::SlotSequence {
                    got: entry.first_slot,
                    expected: expected_slot,
                });
            }
            expected_slot = expected_slot
                .checked_add(entry.slot_count as u64)
                .ok_or(GgjetError::RangeOverflow)?;
        }
        let expected_end = header
            .update_start_slot
            .checked_add(header.update_slot_count)
            .ok_or(GgjetError::RangeOverflow)?;
        if expected_slot != expected_end {
            return Err(GgjetError::SlotSequence {
                got: expected_slot,
                expected: expected_end,
            });
        }
        Ok(Self {
            source,
            header,
            manifest,
            checkpoint_offset,
            index,
        })
    }

    pub fn header(&self) -> &GgjetHeader {
        &self.header
    }

    pub fn manifest(&self) -> &AccountManifest {
        &self.manifest
    }

    pub fn bucket_index(&self) -> &[GgjetBucketIndexEntry] {
        &self.index
    }

    pub fn visit_checkpoint<V: GgjetVisitor>(
        &mut self,
        visitor: &mut V,
    ) -> Result<u64, GgjetError> {
        self.source.seek(SeekFrom::Start(self.checkpoint_offset))?;
        let mut ordinal = 0u64;
        loop {
            let (bucket, _) = read_framed::<BlobHeader>(&mut self.source)?;
            if bucket.first_ordinal != ordinal {
                return Err(GgjetError::CheckpointCount {
                    got: bucket.first_ordinal,
                    expected: ordinal,
                });
            }
            if bucket.item_count == 0 {
                if bucket.uncompressed_len != 0
                    || bucket.stored_len != 0
                    || bucket.xxh64 != xxh64(&[], 0)
                {
                    return Err(GgjetError::Checksum(bucket.first_ordinal));
                }
                break;
            }
            if bucket.item_count > self.header.checkpoint_bucket_accounts {
                return Err(GgjetError::CheckpointCount {
                    got: bucket.item_count as u64,
                    expected: self.header.checkpoint_bucket_accounts as u64,
                });
            }
            let mut stored = vec![0u8; bucket.stored_len as usize];
            self.source.read_exact(&mut stored)?;
            let raw = decompress(&bucket, &stored)?;
            let mut cursor = lencode::io::Cursor::new(raw.as_slice());
            for _ in 0..bucket.item_count {
                let mut flag = [0u8; 1];
                cursor.read(&mut flag)?;
                let address = *self.manifest.accounts.get(ordinal as usize).ok_or(
                    GgjetError::CheckpointCount {
                        got: ordinal + 1,
                        expected: self.header.account_count,
                    },
                )?;
                if flag[0] == 0 {
                    visitor.on_checkpoint(address, None);
                } else if flag[0] == 1 {
                    let last_modified_slot = u64::decode_ext(&mut cursor, None)?;
                    let lamports = u64::decode_ext(&mut cursor, None)?;
                    let mut owner = [0u8; 32];
                    cursor.read(&mut owner)?;
                    let mut executable = [0u8; 1];
                    cursor.read(&mut executable)?;
                    let rent_epoch = u64::decode_ext(&mut cursor, None)?;
                    let data_len = u64::decode_ext(&mut cursor, None)? as usize;
                    let Some(data) = cursor.buf().and_then(|buf| buf.get(..data_len)) else {
                        return Err(lencode::io::Error::ReaderOutOfData.into());
                    };
                    if executable[0] > 1 {
                        return Err(lencode::io::Error::InvalidData.into());
                    }
                    visitor.on_checkpoint(
                        address,
                        Some(CheckpointAccountView {
                            last_modified_slot,
                            lamports,
                            owner: Address::new_from_array(owner),
                            executable: executable[0] != 0,
                            rent_epoch,
                            data,
                        }),
                    );
                    cursor.advance(data_len);
                } else {
                    return Err(lencode::io::Error::InvalidData.into());
                }
                ordinal += 1;
            }
            if cursor.buf().is_some_and(|remaining| !remaining.is_empty()) {
                return Err(lencode::io::Error::InvalidData.into());
            }
        }
        if ordinal != self.header.account_count {
            return Err(GgjetError::CheckpointCount {
                got: ordinal,
                expected: self.header.account_count,
            });
        }
        Ok(ordinal)
    }

    pub fn visit_slots<V: GgjetVisitor>(
        &mut self,
        start_slot: u64,
        max_slots: u64,
        visitor: &mut V,
    ) -> Result<u64, GgjetError> {
        if max_slots == 0 || self.index.is_empty() {
            return Ok(0);
        }
        let end_exclusive = start_slot
            .checked_add(max_slots)
            .ok_or(GgjetError::RangeOverflow)?;
        let mut visited = 0u64;
        let mut start_index = self.index.partition_point(|entry| {
            entry.first_slot.saturating_add(entry.slot_count as u64) <= start_slot
        });
        while start_index < self.index.len() {
            let entry = self.index[start_index];
            if entry.first_slot >= end_exclusive {
                break;
            }
            self.source.seek(SeekFrom::Start(entry.offset))?;
            let mut raw_bucket = vec![0u8; entry.len as usize];
            self.source.read_exact(&mut raw_bucket)?;
            let mut framed = lencode::io::Cursor::new(raw_bucket.as_slice());
            let header = read_framed_cursor::<BlobHeader>(&mut framed)?;
            if header.first_ordinal != entry.first_slot
                || header.item_count != entry.slot_count
                || header.stored_len as usize != framed.buf().map_or(0, |buf| buf.len())
            {
                return Err(GgjetError::Checksum(entry.first_slot));
            }
            let Some(stored) = framed
                .buf()
                .and_then(|buf| buf.get(..header.stored_len as usize))
            else {
                return Err(lencode::io::Error::ReaderOutOfData.into());
            };
            let payload = decompress(&header, stored)?;
            let mut cursor = lencode::io::Cursor::new(payload.as_slice());
            let mut dec_ctx: DecoderContext = new_decoder_context();
            let mut diff = DiffDecoder::with_capacity(64 * 1024);
            let mut bucket_updates = 0u64;
            for frame_index in 0..header.item_count {
                let slot = u64::decode_ext(&mut cursor, None)?;
                let expected_slot = header
                    .first_ordinal
                    .checked_add(frame_index as u64)
                    .ok_or(GgjetError::RangeOverflow)?;
                if slot != expected_slot {
                    return Err(GgjetError::SlotSequence {
                        got: slot,
                        expected: expected_slot,
                    });
                }
                let update_count = u64::decode_ext(&mut cursor, None)?;
                bucket_updates = bucket_updates
                    .checked_add(update_count)
                    .ok_or(GgjetError::RangeOverflow)?;
                let emit = slot >= start_slot && slot < end_exclusive;
                let mut previous_write_version = None;
                for _ in 0..update_count {
                    let ordinal = u32::decode_ext(&mut cursor, None)?;
                    let pubkey =
                        *self
                            .manifest
                            .accounts
                            .get(ordinal as usize)
                            .ok_or_else(|| {
                                GgjetError::Manifest(format!(
                                    "update references out-of-range account ordinal {ordinal}"
                                ))
                            })?;
                    let write_version = u64::decode_ext(&mut cursor, None)?;
                    if let Some(previous) = previous_write_version
                        && previous >= write_version
                    {
                        return Err(GgjetError::NonMonotonicWriteVersion {
                            slot,
                            previous,
                            got: write_version,
                        });
                    }
                    previous_write_version = Some(write_version);
                    let lamports = u64::decode_ext(&mut cursor, None)?;
                    let owner = Address::decode_ext(&mut cursor, Some(&mut dec_ctx))?;
                    let executable = bool::decode_ext(&mut cursor, None)?;
                    let rent_epoch = u64::decode_ext(&mut cursor, None)?;
                    diff.set_key(account_diff_key(&pubkey));
                    let data = diff.decode_blob_ref(&mut cursor)?;
                    if emit {
                        visitor.on_update(
                            slot,
                            GgjetUpdateView {
                                pubkey,
                                write_version,
                                lamports,
                                owner,
                                executable,
                                rent_epoch,
                                data,
                            },
                        );
                    }
                }
                if emit {
                    visited += 1;
                }
            }
            if bucket_updates != entry.update_count
                || cursor.buf().is_some_and(|remaining| !remaining.is_empty())
            {
                return Err(GgjetError::Checksum(entry.first_slot));
            }
            reset_decoder(&mut dec_ctx);
            start_index += 1;
        }
        Ok(visited)
    }
}

struct OwnedSelectedUpdate {
    pubkey: Address,
    write_version: u64,
    lamports: u64,
    owner: Address,
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
}

impl OwnedSelectedUpdate {
    fn capture(view: AccountUpdateView<'_>) -> Self {
        Self {
            pubkey: view.pubkey,
            write_version: view.write_version,
            lamports: view.lamports,
            owner: view.owner,
            executable: view.executable,
            rent_epoch: view.rent_epoch,
            data: view.data.to_vec(),
        }
    }

    fn as_view(&self) -> GgjetUpdateView<'_> {
        GgjetUpdateView {
            pubkey: self.pubkey,
            write_version: self.write_version,
            lamports: self.lamports,
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data: &self.data,
        }
    }
}

struct JetUpdateCopier<'a, W: std::io::Write> {
    writer: &'a mut GgjetWriter<W>,
    pending: Vec<OwnedSelectedUpdate>,
    error: Option<GgjetError>,
}

impl<W: std::io::Write> JetUpdateCopier<'_, W> {
    fn capture_arena<'a>(
        &mut self,
        updates: impl Iterator<Item = (&'a crate::account_updates::AccountUpdateMeta, &'a [u8])>,
    ) {
        if self.error.is_some() {
            return;
        }
        for (meta, data) in updates {
            if self.writer.manifest.contains(&meta.pubkey) {
                self.pending
                    .push(OwnedSelectedUpdate::capture(AccountUpdateView {
                        pubkey: meta.pubkey,
                        lamports: meta.lamports,
                        owner: meta.owner,
                        executable: meta.executable,
                        rent_epoch: meta.rent_epoch,
                        write_version: meta.write_version,
                        data,
                    }));
            }
        }
    }

    fn finish_slot(&mut self, slot: u64) {
        if self.error.is_some() {
            return;
        }
        let mut views = self
            .pending
            .iter()
            .map(OwnedSelectedUpdate::as_view)
            .collect::<Vec<_>>();
        if let Err(err) = self.writer.write_slot_updates(slot, &mut views) {
            self.error = Some(err);
        }
        self.pending.clear();
    }
}

impl<W: std::io::Write> SlotVisitor for JetUpdateCopier<'_, W> {
    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.capture_arena(meta.updates.iter());
    }

    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, tx: &Transaction) {
        self.capture_arena(tx.iter_account_updates());
    }

    fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
        if let BlockNotification::Block(meta) = notification {
            self.capture_arena(meta.pre_updates.iter());
            self.capture_arena(meta.post_updates.iter());
        }
        self.finish_slot(notification.slot());
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
    }
}

/// Copies every matching update from a complete `.jet` into a writer whose
/// checkpoint has already been populated. No transaction replay is involved.
pub fn copy_updates_from_jet<R, W>(
    source: R,
    writer: &mut GgjetWriter<W>,
) -> Result<u64, GgjetError>
where
    R: std::io::Read + std::io::Seek,
    W: std::io::Write,
{
    writer.finish_checkpoint()?;
    let mut reader = ArchiveReader::open(source)?;
    let source_start = reader.header().slot_start;
    let source_count = reader.header().slot_count;
    let source_end = source_start
        .checked_add(source_count.saturating_sub(1))
        .ok_or(GgjetError::RangeOverflow)?;
    let ggjet_start = writer.header.update_start_slot;
    let ggjet_end = ggjet_start
        .checked_add(writer.header.update_slot_count.saturating_sub(1))
        .ok_or(GgjetError::RangeOverflow)?;
    if source_start != ggjet_start || source_count != writer.header.update_slot_count {
        return Err(GgjetError::SourceRangeMismatch {
            source_start,
            source_end,
            ggjet_start,
            ggjet_end,
        });
    }
    let mut copier = JetUpdateCopier {
        writer,
        pending: Vec::new(),
        error: None,
    };
    let slots = reader.read_slots(source_start, source_count, &mut copier)?;
    if let Some(err) = copier.error {
        return Err(err);
    }
    Ok(slots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn address(byte: u8) -> Address {
        Address::new_from_array([byte; 32])
    }

    #[derive(Default)]
    struct Collect {
        checkpoint: Vec<(Address, bool, Vec<u8>)>,
        updates: Vec<(u64, Address, u64, u64, Vec<u8>)>,
    }

    impl GgjetVisitor for Collect {
        fn on_checkpoint(&mut self, pubkey: Address, account: Option<CheckpointAccountView<'_>>) {
            self.checkpoint.push((
                pubkey,
                account.is_some(),
                account.map(|a| a.data.to_vec()).unwrap_or_default(),
            ));
        }

        fn on_update(&mut self, slot: u64, update: GgjetUpdateView<'_>) {
            self.updates.push((
                slot,
                update.pubkey,
                update.write_version,
                update.lamports,
                update.data.to_vec(),
            ));
        }
    }

    #[test]
    fn manifest_digest_is_binary_pubkey_concatenation() {
        let manifest = AccountManifest::from_accounts(vec![address(1), address(2)]).unwrap();
        let mut expected = Sha256::new();
        expected.update([1u8; 32]);
        expected.update([2u8; 32]);
        assert_eq!(manifest.digest(), <[u8; 32]>::from(expected.finalize()));
    }

    #[test]
    fn writer_reader_roundtrip_preserves_absence_zero_lamports_and_versions() {
        let accounts = vec![address(1), address(2), address(3)];
        let manifest = AccountManifest::from_accounts(accounts.clone()).unwrap();
        let mut writer = GgjetWriter::new(
            Vec::new(),
            manifest,
            99,
            100,
            3,
            GgjetWriterConfig {
                bucket_slots: 2,
                checkpoint_bucket_accounts: 2,
                ..Default::default()
            },
        )
        .unwrap();
        writer
            .write_checkpoint_account(Some(CheckpointAccountView {
                last_modified_slot: 90,
                lamports: 10,
                owner: address(9),
                executable: false,
                rent_epoch: u64::MAX,
                data: b"before-a",
            }))
            .unwrap();
        writer.write_checkpoint_account(None).unwrap();
        writer
            .write_checkpoint_account(Some(CheckpointAccountView {
                last_modified_slot: 98,
                lamports: 1,
                owner: address(8),
                executable: true,
                rent_epoch: 7,
                data: b"before-c",
            }))
            .unwrap();

        let mut slot100 = [
            GgjetUpdateView {
                pubkey: accounts[0],
                write_version: 12,
                lamports: 0,
                owner: address(9),
                executable: false,
                rent_epoch: u64::MAX,
                data: b"after-a-2",
            },
            GgjetUpdateView {
                pubkey: accounts[0],
                write_version: 11,
                lamports: 5,
                owner: address(9),
                executable: false,
                rent_epoch: u64::MAX,
                data: b"after-a-1",
            },
        ];
        writer.write_slot_updates(100, &mut slot100).unwrap();
        writer.write_slot_updates(101, &mut []).unwrap();
        let mut slot102 = [GgjetUpdateView {
            pubkey: accounts[2],
            write_version: 14,
            lamports: 2,
            owner: address(8),
            executable: true,
            rent_epoch: 7,
            data: b"after-c",
        }];
        writer.write_slot_updates(102, &mut slot102).unwrap();
        let (bytes, stats) = writer.finish().unwrap();

        assert_eq!(stats.checkpoint_accounts, 3);
        assert_eq!(stats.checkpoint_present, 2);
        assert_eq!(stats.slots, 3);
        assert_eq!(stats.updates, 3);

        let mut reader = GgjetReader::open(Cursor::new(bytes)).unwrap();
        assert_eq!(reader.header().checkpoint_slot, 99);
        assert_eq!(
            reader.header().account_set_sha256,
            reader.manifest().digest()
        );
        let mut collect = Collect::default();
        assert_eq!(reader.visit_checkpoint(&mut collect).unwrap(), 3);
        assert_eq!(reader.visit_slots(100, 3, &mut collect).unwrap(), 3);
        assert_eq!(
            collect.checkpoint,
            vec![
                (accounts[0], true, b"before-a".to_vec()),
                (accounts[1], false, Vec::new()),
                (accounts[2], true, b"before-c".to_vec()),
            ]
        );
        assert_eq!(collect.updates[0].2, 11);
        assert_eq!(collect.updates[1].2, 12);
        assert_eq!(collect.updates[1].3, 0);
        assert_eq!(collect.updates[2].2, 14);
    }

    #[test]
    fn writer_rejects_duplicate_write_version() {
        let manifest = AccountManifest::from_accounts(vec![address(1)]).unwrap();
        let mut writer =
            GgjetWriter::new(Vec::new(), manifest, 9, 10, 1, Default::default()).unwrap();
        writer.write_checkpoint_account(None).unwrap();
        let mut updates = [
            GgjetUpdateView {
                pubkey: address(1),
                write_version: 4,
                lamports: 1,
                owner: address(2),
                executable: false,
                rent_epoch: 0,
                data: b"a",
            },
            GgjetUpdateView {
                pubkey: address(1),
                write_version: 4,
                lamports: 2,
                owner: address(2),
                executable: false,
                rent_epoch: 0,
                data: b"b",
            },
        ];
        assert!(matches!(
            writer.write_slot_updates(10, &mut updates),
            Err(GgjetError::NonMonotonicWriteVersion { .. })
        ));
    }

    #[test]
    fn copies_filtered_updates_from_jet_without_transactions() {
        use crate::archive::{ArchiveWriter, ArchiveWriterConfig, BlockMeta};

        let selected = address(7);
        let ignored = address(8);
        let mut jet = ArchiveWriter::new(
            Vec::new(),
            1,
            10,
            2,
            ArchiveWriterConfig {
                bucket_slots: 1,
                ..Default::default()
            },
        )
        .unwrap();
        jet.begin_slot(10).unwrap();
        for (pubkey, write_version, data) in [
            (ignored, 1, b"ignored".as_slice()),
            (selected, 2, b"kept".as_slice()),
        ] {
            jet.write_orphan_update(&AccountUpdateView {
                pubkey,
                lamports: 55,
                owner: address(9),
                executable: false,
                rent_epoch: 3,
                write_version,
                data,
            })
            .unwrap();
        }
        let mut meta = BlockMeta::new_boxed();
        meta.slot = 10;
        jet.end_slot(&meta, &[]).unwrap();
        jet.write_skipped_slot(11).unwrap();
        let (jet_bytes, _) = jet.finish().unwrap();

        let manifest = AccountManifest::from_accounts(vec![selected]).unwrap();
        let mut ggjet =
            GgjetWriter::new(Vec::new(), manifest, 9, 10, 2, Default::default()).unwrap();
        ggjet.write_checkpoint_account(None).unwrap();
        assert_eq!(
            copy_updates_from_jet(Cursor::new(jet_bytes), &mut ggjet).unwrap(),
            2
        );
        let (ggjet_bytes, stats) = ggjet.finish().unwrap();
        assert_eq!(stats.updates, 1);

        let mut reader = GgjetReader::open(Cursor::new(ggjet_bytes)).unwrap();
        let mut collect = Collect::default();
        assert_eq!(reader.visit_slots(10, 2, &mut collect).unwrap(), 2);
        assert_eq!(
            collect.updates,
            vec![(10, selected, 2, 55, b"kept".to_vec())]
        );
    }
}
