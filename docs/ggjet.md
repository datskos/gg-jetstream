# `.ggjet` v1 selected-account archive

A `.ggjet` reconstructs the state of one deterministic account set without transactions, entries,
logs, or block metadata. For a range `START..=END`, it contains the complete selected-account state
at `START - 1` followed by every selected-account write through `END`.

```text
header
manifest blob
checkpoint bucket 0
checkpoint bucket 1
...
update bucket 0
update bucket 1
...
update bucket index
footer
```

The manifest is an ordered list of raw 32-byte pubkeys. `account_set_sha256` is SHA-256 over their
binary concatenation in manifest order. Writers validate the JSON manifest's count, strict sort
order, pubkeys, and digest before creating an archive. Updates refer to manifest ordinals.

Checkpoint buckets contain one record per manifest account in the same order. An absent account is
encoded explicitly. A present record contains `last_modified_slot`, lamports, owner, executable,
rent epoch, and data. Buckets flush at either 4,096 accounts or 64 MiB of uncompressed payload and
the section ends with an empty bucket marker, keeping memory bounded regardless of total account
data.

Each update slot frame contains zero or more records. Every slot in the declared range has a frame,
including skipped slots and slots with no selected writes. Records contain manifest ordinal,
`write_version`, lamports, owner, executable, rent epoch, and data. The writer sorts each frame by
`write_version` and rejects duplicates/non-monotonic versions. Zero-lamport updates are retained.

Account data uses the same keyed `lencode::diff::DiffEncoder` scheme as `.jet`. Diff state, pubkey
deduplication state, and whole-payload zstd compression reset at update bucket boundaries. The
footer index maps slot ranges to byte offsets, so readers can seek to a bucket without decoding
earlier buckets. Manifest, checkpoint, update buckets, and the bucket index are checksummed.

The reconstruction rule is:

```text
state(checkpoint_slot) + ordered updates through slot N = selected account state at N
```

An offline `.jet` conversion is lossless only with the exact checkpoint Bank. The converter
therefore requires a full snapshot at `START - 1`. If the available snapshot is older, use the
replay command with paired `.jet`/`.ggjet` outputs so warmup reaches the checkpoint before capture.
