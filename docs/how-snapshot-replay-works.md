# How Snapshot Replay Works in gg-jetstream

The core model is:

```text
Snapshot state at slot S
        +
Historical slot data for S+1..E
        |
        v
Execute transactions against successive Solana Banks
        |
        v
Final account state and Bank hash at slot E
```

The snapshot supplies state, not future transactions. Jetstreamer obtains the
incremental transactions from Old Faithful.

## Example Command

```bash
jetstreamer-node replay-slots \
  441851484:441851493 \
  /path/to/replay-cache \
  --no-verify
```

## Log File

Jetstreamer mirrors its timestamped console logs to this file by default:

```text
/path/to/replay-cache/jetstreamer-node.log
```

The file is opened in append mode, so logs from a later invocation do not
erase an earlier run. The beginning of each invocation contains a `file
logging enabled` record. Set `JETSTREAMER_LOG_FILE` to use a different path:

```bash
JETSTREAMER_LOG_FILE=/path/to/replay-441851484-441851489.log \
  jetstreamer-node replay-slots \
    441851484:441851489 \
    /path/to/replay-cache \
    --no-verify
```

The final `replay complete` summary is written to the file as well as stdout.

## 1. Parse the Requested Range

`replay-slots` validates that:

- `START <= END`
- `START > 0`
- The requested range stays within one epoch

The command is handled in `jetstreamer-node/src/main.rs` by the
`replay-slots` branch in `main()`.

## 2. Prepare the Replay Cache

The current code clears mutable AccountsDB state before replaying.

Important: in the current checkout, cleanup removes these paths under
`CACHE_DIR`:

```text
accounts/
accounts-index/
snapshots/
accounts-run/snapshot/
accounts-run/run/
version
```

The snapshot archive itself is expected to be directly inside `CACHE_DIR`,
for example:

```text
replay-cache/
|-- genesis.bin
`-- snapshot-441851483-....tar.zst
```

A snapshot stored inside `replay-cache/snapshots/` is unsafe with the current
implementation because that directory is removed by cleanup.

Cleanup is implemented by `clear_ledger_accounts_state()` in
`jetstreamer-node/src/main.rs`.

## 3. Select an Existing Snapshot Archive

Jetstreamer scans only the top level of `CACHE_DIR` for files named like:

```text
snapshot-<slot>-<hash>.tar.zst
```

It selects the newest nonempty snapshot satisfying:

```text
snapshot_slot <= START - 1
```

For the example, it selects snapshot slot `441851483`.

If no suitable local archive exists, the current code attempts to download
one using GCS.

This selection is implemented by `find_existing_snapshot_archive()`.

## 4. Load Genesis

Jetstreamer requires one of:

```text
CACHE_DIR/genesis.bin
CACHE_DIR/genesis.tar.bz2
```

Genesis supplies the cluster configuration required to deserialize and
operate the Bank. If neither file exists, the current code attempts a GCS
download.

## 5. Extract the Snapshot

The archive is unpacked into a reusable extraction directory:

```text
CACHE_DIR/.snapshot-extract-snapshot-441851483-....tar.zst/
```

AppendVec account files are placed under:

```text
CACHE_DIR/accounts-run/
```

On later runs, extraction is skipped when its completion marker and account
files remain present.

When extraction is required, progress reports the extracted file count and
rate without first counting every archive entry. This avoids an additional
full decompression pass. Set `JETSTREAMER_SNAPSHOT_UNPACK_PERCENT=1` only when
percentage progress is worth that startup cost. The legacy
`JETSTREAMER_SNAPSHOT_UNPACK_NO_PERCENT=1` setting remains accepted but is no
longer necessary.

Archive loading is implemented by `load_bank_from_snapshot_archive()`.

## 6. Reconstruct AccountsDB and the Snapshot Bank

Jetstreamer:

1. Repairs snapshot metadata if necessary.
2. Rebuilds the account hardlink farm.
3. Creates clean writable account run directories.
4. Opens the compatible genesis configuration.
5. Calls Agave's `bank_from_snapshot_dir()`.
6. Scans the AppendVec files and constructs the in-memory AccountsIndex.
7. Verifies that the reconstructed Bank hash matches the archive hash.

This is the expensive startup phase that produces messages such as:

```text
Building accounts index...
generating index: processed ... slots
```

The result is a Solana `Bank` representing the complete account state at
snapshot slot `441851483`.

## 7. Determine the Incremental Replay Window

If the snapshot slot is `S`, replay begins at `S + 1`.

For the example:

```text
snapshot Bank: 441851483
replay starts: 441851484
target ends:   441851493
```

If the requested start were later, such as `441851490`, Jetstreamer would
first execute slots `441851484..441851489` as warmup. This derives the correct
Bank state at slot `441851490`.

The main replay orchestration is implemented by `run_geyser_replay()`.

## 8. Determine Which Slots Exist

Jetstreamer downloads or reuses Old Faithful compact indexes:

```text
slot-to-cid.index
cid-to-offset-and-size.index
```

It builds a presence map marking each requested slot as present or missing.
This prevents the scheduler from waiting forever for a slot that did not
produce a block.

This step is implemented by `build_slot_presence_map()`.

## 9. Fetch Incremental Block Data

The firehose reads Old Faithful historical CAR data for the requested slots.
For each decoded slot, it emits three kinds of records:

- Transactions, including their historical execution status
- PoH entries, including hashes, tick counts, and transaction counts
- Block metadata, including the parent slot and expected totals

The firehose entry point is `firehose_geyser_with_notifiers()` in
`jetstreamer-firehose/src/firehose.rs`.

## 10. Assemble Complete Replay Entries

`TransactionScheduler` correlates the three streams. It waits until it has
enough information to prove that an entry is complete:

```text
entry metadata
+ expected transaction count
+ all corresponding transactions
+ block metadata
```

It then emits a `ReadyEntry` for execution.

## 11. Create a Bank for Each Slot

When replay advances to a new slot, Jetstreamer:

1. Freezes the preceding Bank.
2. Determines the slot leader.
3. Creates `Bank::new_from_parent()`.
4. Inserts the Bank into `BankForks`.
5. Uses that Bank for every entry in the slot.

Conceptually:

```text
Bank(441851483)
    |
    v new_from_parent
Bank(441851484)
    |
    v
Bank(441851485)
    |
    v
...
    |
    v
Bank(441851493)
```

This is handled by `BankReplay::bank_for_slot()`.

## 12. Execute and Commit Transactions

For each ready entry, Jetstreamer:

1. Sanitizes its transactions.
2. Resolves account locks and address lookup tables.
3. Groups nonconflicting entries for parallel execution.
4. Calls Agave's `load_execute_and_commit_transactions()`.
5. Commits account changes into the Bank's AccountsDB.
6. Registers ticks for tick-only entries.

The execution call is made by `execute_entry_batch()`.

## 13. Compare Replay Results With History

Each historical transaction includes its expected status. After execution,
Jetstreamer compares:

```text
actual execution result == historical expected result
```

A mismatch is treated as a replay failure because it means the reconstructed
state or runtime behavior diverged from mainnet.

This verification occurs in `BankReplay::post_process_entry()`.

## 14. Finish the Range

After the firehose ends, Jetstreamer:

- Drains remaining ready entries.
- Verifies that every present slot is complete.
- Waits for the execution worker.
- Freezes the final Bank.
- Prints the final slot and Bank hash.

Example output:

```text
replay complete:
requested=441851484..=441851493
snapshot_slot=441851483
final_bank_slot=441851493
final_bank_hash=...
```

## What Survives After the Command Exits?

The snapshot archive and extraction cache can survive, but:

- AccountsIndex is currently in memory and must be rebuilt for every new
  process.
- The final Bank is not written as a new reusable snapshot.
- Account changes may exist in AccountsDB run files, but those files are not a
  formal replay checkpoint.
- `replay-slots` is currently a one-shot process, not a persistent replay
  service.

## Summary

For snapshot slot `S` and requested end slot `E`, gg-jetstream does this:

```text
1. Restore Bank(S) from snapshot + genesis.
2. Fetch historical block data for S+1..E from Old Faithful.
3. Reconstruct entries and their original transaction ordering.
4. Create child Banks one slot at a time.
5. Execute and commit each transaction into AccountsDB.
6. Compare execution results with historical results.
7. Finish with Bank(E) and its resulting account state/hash.
```
