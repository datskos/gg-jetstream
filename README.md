# Jetstreamer

[![Crates.io](https://img.shields.io/crates/v/jetstreamer.svg)](https://crates.io/crates/jetstreamer)
[![Docs.rs](https://docs.rs/jetstreamer/badge.svg)](https://docs.rs/jetstreamer)
[![CI](https://github.com/anza-xyz/jetstreamer/actions/workflows/rust.yaml/badge.svg)](https://github.com/anza-xyz/jetstreamer/actions/workflows/rust.yaml)

## Overview

Jetstreamer is a high-throughput Solana backfilling and research toolkit designed to stream
historical chain data live over the network from Project Yellowstone's [Old
Faithful](https://old-faithful.net/) archive, which is a comprehensive open source archive of
all Solana blocks and transactions from genesis to the current tip of the chain. Given the
right hardware and network connection, Jetstreamer can stream data at over 2.7M TPS to a local
Jetstreamer plugin or geyser plugin. Higher speeds are possible with better hardware (in our
case 64 core CPU, 30 Gbps+ network for the 2.7M TPS record).

Jetstreamer is split across four crates:

- `jetstreamer` – the primary facade that wires firehose ingestion into your plugins through
  `JetstreamerRunner`.
- `jetstreamer-firehose` – async helpers for downloading, compacting, and replaying Old
  Faithful CAR archives at scale.
- `jetstreamer-plugin` – a trait-based framework for building structured observers with
  ClickHouse-friendly batching and runtime metrics.
- `jetstreamer-utils` - utils used by the Jetstreamer ecosystem.

Every crate ships with rich module-level documentation and runnable examples. Visit
[docs.rs/jetstreamer](https://docs.rs/jetstreamer) to explore the API surface in detail.

All 3 sub-crates are provided as re-exports within the main `jetstreamer` crate via the
following re-exports:
- `jetstreamer::firehose`
- `jetstreamer::plugin`
- `jetstreamer::utils`

## Limitations

While Jetstreamer is able to play back all blocks, transactions, epochs, and rewards in the
history of Solana mainnet, it is limited by what is in Old Faithful. Old Faithful does not
contain account updates, so Jetstreamer at the moment also does not have them. Transaction logs
are available in `transaction_status_meta.log_messages` for all epochs.

It is worth noting that the way Old Faithful and thus Jetstreamer stores transactions, they are
stored in their "already-executed" state as they originally appeared to Geyser when they were
first executed. Thus while Jetstreamer can replay ledger data, it is not executing transactions
directly, and when we say 2.7M TPS, we mean "2.7M transactions processed by a Jetstreamer or
Geyser plugin locally, streamed over the internet from the Old Faithful archive."

## Quick Start

To get an idea of what Jetstreamer is capable of, you can try out the demo CLI that runs
Jetstreamer Runner with the Program Tracking plugin enabled. The built-in plugins are
`program-tracking`, `instruction-tracking`, `pubkey-stats`, and `tx-metadata`; pass
`--with-plugin <name>` to select one (repeat the flag to run multiple at once), or
`--no-plugins` to disable the default. Run `--list-plugins` to print the names of every bundled
plugin and exit.

### Snapshot-backed transaction replay (`jetstreamer-node`)

`jetstreamer-node replay-slots` restores a Solana Bank from a full snapshot, downloads the
incremental slot data from Old Faithful, and executes the requested transactions against that
state. This is separate from the firehose-only Runner CLI documented below.

#### Required open-file limit

A mainnet snapshot can contain hundreds of thousands of AccountsDB files. The usual soft limit
of 1,024 file descriptors is too low and causes snapshot restoration to fail with `Too many open
files (os error 24)` or a secondary snapshot-storage-rebuilder panic.

In the same shell that will launch `jetstreamer-node`, confirm that the hard limit is at least
524,288, raise the soft limit, and verify it:

```bash
ulimit -Hn
ulimit -S -n 524288
ulimit -Sn
```

The last command must print `524288`. `ulimit` affects only the current shell and processes
started from it, so opening another terminal or running the binary from a service does not reuse
this setting. For a systemd service, configure `LimitNOFILE=524288` in the service unit instead.
If the hard limit is below 524,288 or the shell reports `Operation not permitted`, raise the
login/service hard limit first and start a new shell; running only the binary with `sudo` does not
reliably preserve the caller's limit.

#### Prepare the replay cache

Build the node explicitly:

```bash
cargo build --release -p jetstreamer-node
```

The replay cache must contain a compatible mainnet genesis and a full snapshot from before the
first requested slot. Keep the snapshot archive directly in the cache directory, not in its
`snapshots/` subdirectory:

```text
/home/ubuntu/jetstreamer_replay_scratch/
|-- genesis.bin
`-- snapshot-441851483-<hash>.tar.zst
```

`genesis.tar.bz2` can be used instead of `genesis.bin`. The genesis and snapshot must be compatible
with the Agave version used to build `jetstreamer-node`.

#### Run replay

This example replays 1,000 inclusive slots using the binary installed under
`/home/ubuntu/gg-jetstream`:

```bash
ulimit -S -n 524288

JETSTREAMER_CLEAR_ACCOUNTS_ON_START=true \
JETSTREAMER_ROOT_INTERVAL=1024 \
  /home/ubuntu/gg-jetstream/jetstreamer-node \
    replay-slots 441851484:441852483 \
    /home/ubuntu/jetstreamer_replay_scratch \
    --no-verify
```

`JETSTREAMER_CLEAR_ACCOUNTS_ON_START=true` is the default and is the safe choice for a fresh run.
It removes mutable AccountsDB paths left by an earlier completed or interrupted replay, while
preserving the top-level snapshot archive, genesis, compact indexes, completed archives, and the
pristine snapshot extraction cache. Set it to `false` only when deliberately continuing with an
AccountsDB workspace known to be clean and compatible.

`JETSTREAMER_ROOT_INTERVAL=1024` bounds the number of live replay Banks and avoids accumulating
program-cache assignment work across a long range.

`--no-verify` avoids the GCS checkpoint lookup and therefore works without Google Cloud
credentials when the snapshot and genesis have already been staged locally. It skips comparison
with canonical snapshot checkpoint hashes, but transaction execution results are still compared
with their historical expected statuses. Use `--verify` only when `gcloud` is installed and can
list the configured snapshot bucket.

#### Write a Horizon `.jet` archive

To record a requested slot range as a compressed Horizon `.jet` archive, add an explicit output
path to `replay-slots`:

```bash
ulimit -S -n 524288

JETSTREAMER_CLEAR_ACCOUNTS_ON_START=true \
JETSTREAMER_ROOT_INTERVAL=1024 \
  /home/ubuntu/gg-jetstream/jetstreamer-node \
    replay-slots 441851484:441852483 \
    /home/ubuntu/jetstreamer_replay_scratch \
    --no-verify \
    --horizon-output=/home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441852483.jet
```

The archive contains only incremental data for `START..=END`: block and entry metadata, full
transactions and statuses, skipped-slot markers, rewards, and account updates in application
order. It does not contain the starting snapshot state, so reconstructing complete state still
requires the snapshot (or an equivalent state store) at `START - 1`.

Jetstreamer refuses to overwrite an existing path. A replay interrupted after archive creation
can leave an incomplete file without a valid footer; preserve it for diagnosis or move/remove it
explicitly before reusing that output name.

A successful run prints a summary resembling:

```text
replay complete: requested=441851484..=441852483 ... final_bank_slot=441852483 final_bank_hash=...
```

Console logs are also appended automatically to:

```text
/home/ubuntu/jetstreamer_replay_scratch/jetstreamer-node.log
```

Follow them from another shell with:

```bash
tail -f /home/ubuntu/jetstreamer_replay_scratch/jetstreamer-node.log
```

For a full epoch, pass the epoch and destination directory instead. This form automatically uses
the importer-compatible filename `epoch-<EPOCH>.jet`:

```bash
/home/ubuntu/gg-jetstream/jetstreamer-node \
  1022 /data/jets --no-verify
# Output: /data/jets/epoch-1022.jet
```

Before moving or importing an archive, verify that its footer, bucket checksums, decoded tallies,
and blockhash chain are internally consistent:

```bash
cargo run --release -p jetstreamer-horizon --example verify_archive -- \
  /home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441852483.jet
```

The first line includes the archive header's `epoch`, `slot_start`, and `slot_count`. Use that
header epoch when importing; a ten-slot archive is still associated with its containing epoch.

#### Write a selected-account `.ggjet` archive

`.ggjet` requires a normalized, deterministic account manifest. It does not accept the original
arb catalog whose top-level `entries` object contains `requiredAccounts`. The normalized manifest
has top-level `accountCount`, `accountSetSha256`, and a unique, strictly sorted `accounts` array.

Create it once from the source catalog. The builder takes the strict union of only
`entries.*.requiredAccounts`, validates every value as a 32-byte base58 pubkey, and refuses to
overwrite an existing output:

```bash
python3 /home/ubuntu/gg-jetstream/scripts/build_ggjet_manifest.py \
  /home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.json \
  /home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.manifest.json
```

Before starting a long replay, confirm that the normalized manifest—not the source catalog—is at
the path that will be passed to the node:

```bash
wc -c /home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.manifest.json
sha256sum /home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.manifest.json
grep -m1 '"accountCount"' \
  /home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.manifest.json
grep -m1 '"accountSetSha256"' \
  /home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.manifest.json
```

For the current arb account set, the expected values are:

```text
20294297 bytes
file SHA-256:        39909dfc2f3fc21bc4407b8dc6ebe24f1d0d2f60460376368f1c7e746ce0063f
accountCount:        390696
accountSetSha256:    07f5c5e50de7a2d32b488824effab2d86b9120cc3d9b451904a24556d8f67f17
```

The file SHA-256 covers the JSON file; `accountSetSha256` covers the concatenated raw 32-byte
pubkeys. Once the preflight values match, the following clean 10,000-slot replay produces both
archives in one execution:

```bash
ulimit -S -n 524288

JETSTREAMER_CLEAR_ACCOUNTS_ON_START=true \
JETSTREAMER_ROOT_INTERVAL=1024 \
JETSTREAMER_LOG_FILE=/home/ubuntu/jetstreamer_replay_scratch/replay-both.log \
  /home/ubuntu/gg-jetstream/jetstreamer-node \
    replay-slots 441851484:441861483 \
    /home/ubuntu/jetstreamer_replay_scratch \
    --no-verify \
    --horizon-output=/home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441861483-both.jet \
    --ggjet-output=/home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441861483-both.ggjet \
    --ggjet-manifest=/home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.manifest.json
```

Both output paths must be absent before launch; Jetstreamer never overwrites an archive. Manifest
validation and output-path checks happen before the expensive snapshot load. The checkpoint is
captured from the frozen Bank at slot `441851483`, immediately before the requested range. Every
manifest account is represented, including accounts absent at the checkpoint. All matching
updates—including zero-lamport writes and repeated writes to one account—are stored in
`(slot, write_version)` order. On success, the `.jet` and `.ggjet` both cover
`441851484..=441861483`.

Follow the dedicated run log from another shell:

```bash
tail -f /home/ubuntu/jetstreamer_replay_scratch/replay-both.log
```

After the replay succeeds, validate the selected-account archive:

```bash
/home/ubuntu/gg-jetstream/jetstreamer-node verify-ggjet \
  /home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441861483-both.ggjet
```

An already-completed `.jet` can be converted without executing its transactions again when an
exact `START - 1` snapshot is available:

```bash
/home/ubuntu/gg-jetstream/jetstreamer-node ggjet-from-jet \
  /home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441861483.jet \
  /home/ubuntu/jetstreamer_replay_scratch/snapshot-441851483-9Wfwu7Qtw9vLQDUmbzE7LsQP7B1qP7a4bKMCp63S8nFd.tar.zst \
  /home/ubuntu/jetstreamer_replay_scratch \
  /home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441861483.ggjet \
  /home/ubuntu/gg-jetstream/data/cache/arb_required_accounts.manifest.json
```

The converter rejects an older or newer snapshot rather than producing a checkpoint for the
wrong slot. It validates the manifest before loading the Bank or creating the output file. It then
loads the snapshot Bank, writes the checkpoint, and decodes/filter-copies account updates from
`.jet`; it does not replay transactions. Validate every checksum and fully decode the checkpoint
and update stream afterward:

```bash
/home/ubuntu/gg-jetstream/jetstreamer-node verify-ggjet \
  /home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441861483.ggjet
```

The v1 wire format and reconstruction invariants are documented in
[`docs/ggjet.md`](docs/ggjet.md).

#### Load a `.jet` archive into ClickHouse

`horizon_pipeline` reads local archives from `<JET_DIR>/epoch-<EPOCH>.jet`. A full-epoch archive
already has that name. For a partial archive, create a separate staging directory and link it
under the expected name. For example, the range above belongs to epoch 1022:

```bash
mkdir -p /data/jets-partial-1022
ln -s \
  /home/ubuntu/jetstreamer_replay_scratch/replay-441851484-441852483.jet \
  /data/jets-partial-1022/epoch-1022.jet
```

The first positional argument below is the archive's epoch, not the number of slots in the file.
Although the progress log prints the epoch's full slot range, a partial archive emits only its
stored slots:

```bash
cargo run --release --bin horizon_pipeline -- \
  1022 /data/jets-partial-1022 \
  --threads 16
```

With the default DSN (`http://localhost:8123`), the pipeline starts and supervises its bundled
ClickHouse server. Do not use this mode while another ClickHouse server already owns the local
data directory or port.

> **ClickHouse safety:** Treat every ClickHouse instance, including one reached through
> `localhost`, as persistent production data. A local endpoint or data directory may later be
> mounted into, or moved to, a server. Do not automatically `DROP` or `TRUNCATE` tables, run
> `ALTER TABLE ... DELETE`, remove ClickHouse data directories, or delete its `access/`,
> `metadata/`, `store/`, `data/`, or `uuid` files. A hostname that looks local is not permission
> to destroy its data. Any destructive maintenance must be a separate, explicit operator action
> with the exact server, database, tables, slot range, and recovery plan reviewed first.

To write to an already-running ClickHouse server, first verify its HTTP endpoint, then pass an
external DSN. The current pipeline treats the literal hosts `localhost` and `127.0.0.1` as a
request to start the bundled server. On Linux, `127.1` reaches the same loopback interface while
selecting external-server mode:

```bash
curl --fail --show-error http://127.1:8123/ping

env NO_PROXY=127.1,127.0.0.1,localhost \
    no_proxy=127.1,127.0.0.1,localhost \
  cargo run --release --bin horizon_pipeline -- \
    1022 /data/jets-partial-1022 \
    --threads 16 \
    --clickhouse-dsn http://127.1:8123
```

For a remote server, replace `127.1` with a hostname or IP on which ClickHouse is listening. The
default Horizon plugins create these ClickHouse objects if they do not already exist:

| Object | Contents |
| --- | --- |
| `tx_meta_v2` | One execution-metadata row per transaction, keyed by `(slot, tx_idx)` |
| `transactions_raw_v1` | One complete lencode transaction record per transaction, plus searchable signature, status, fee, and requested/consumed compute units |
| `pubkey_mentions` | Per-slot transaction-message account-key mention counts |
| `pubkeys` | Deduplicated pubkey-to-`sipHash64` ID mapping |
| `pubkeys_mv` | Materialized view that populates `pubkeys` from `pubkey_mentions` |
| `account_write_stats` | Per-block account-write count, distinct accounts, and total data bytes |

The default pipeline aggregates account writes into `account_write_stats`; it does not persist one
raw ClickHouse row for every account update contained in the `.jet` archive.

After a clean, single import, ordinary queries do not need `FINAL`. The tables use
`ReplacingMergeTree`, so importing the same slots again can expose duplicate physical rows until
ClickHouse merges them. Do not delete existing rows or tables as part of an import or retry. Use
`FINAL` when a query must hide transient duplicates from repeated imports that have not yet
merged. If an isolated rebuild is required, write it to a separately provisioned database or
server and validate it before an operator deliberately changes production ownership or routing.

See [How Snapshot Replay Works](docs/how-snapshot-replay-works.md) for the cache layout, cleanup
scope, verification behavior, and replay pipeline in detail.

### Jetstreamer Runner CLI

```bash
# Replay all transactions in epoch 800, using the default number of multiplexing threads based on your system
cargo run --release -- 800

# The same as above, but tuning network capacity for 10 Gbps, resulting in a higher number of multiplexing threads
JETSTREAMER_NETWORK_CAPACITY_MB=10000 cargo run --release -- 800

# Do the same but for slots 358560000 through 367631999, which is epoch 830-850 (slot ranges can be cross-epoch!)
# and using 8 threads explicitly instead of using automatic thread count
JETSTREAMER_THREADS=8 cargo run --release -- 358560000:367631999

# Replay epochs 900 through 950 inclusive using epoch-range syntax
cargo run --release -- 900-950

# Replay with the live terminal dashboard (TPS graph, per-thread health, system stats)
cargo run --release -- 950 --tui

# Replay epoch 800 with the instruction tracking plugin instead of the default
cargo run --release -- 800 --with-plugin instruction-tracking

# Store one compact execution-metadata row per transaction
cargo run --release -- 800 --with-plugin tx-metadata

# Point the runner at an external ClickHouse instance (overrides JETSTREAMER_CLICKHOUSE_DSN)
cargo run --release -- 800 --clickhouse-dsn http://clickhouse.example.com:8123
```

If `JETSTREAMER_THREADS` is omitted, Jetstreamer auto-sizes the worker pool using the same
hardware-aware heuristic exposed by
`jetstreamer_firehose::system::optimal_firehose_thread_count`.

For sequential replay mode, enable `--sequential` (or `JETSTREAMER_SEQUENTIAL=1`). In this mode
Jetstreamer uses a single firehose worker and reuses `JETSTREAMER_THREADS` as ripget parallel
download concurrency:

```bash
# Sequential mode with CLI flag
JETSTREAMER_THREADS=4 cargo run --release -- 800 --sequential

# Sequential mode with explicit ripget window override (either the env var or the
# --buffer-window CLI flag works)
JETSTREAMER_SEQUENTIAL=1 cargo run --release -- 800 --buffer-window 4GiB
```

`JETSTREAMER_BUFFER_WINDOW` (or `--buffer-window`) defaults to `min(4 GiB, 15% of available
RAM)` when unset.

To backfill from newest to oldest, add `--reverse` (or `JETSTREAMER_REVERSE=1`). Reverse mode
implies sequential mode and processes epochs in the slot range from highest to lowest. Within
each epoch, slots still come out in ascending order because Old Faithful CAR archives are
forward-only streams.

```bash
# Backfill epochs 800..=802 starting with 802, then 801, then 800
cargo run --release -- 345600000:1296000000 --reverse
```

The built-in `program-tracking` and `instruction-tracking` plugins record vote and non-vote
activity separately: `program_invocations` includes an `is_vote` flag per row, while
`slot_instructions` stores separate vote/non-vote instruction and transaction counts. The
`pubkey-stats` plugin aggregates per-slot account-key mention counts into a `pubkey_mentions`
table (with a companion `pubkeys` lookup table populated via materialised view). The
`tx-metadata` plugin writes one compact row per transaction to `tx_meta_v2`, matching
`gg-mev-hub`'s execution-metadata semantics without constructing its intermediate
`GhostTransaction` or storing transaction signatures. It also stores Agave's estimated
`scheduler_cost_units` and reward-per-cost `scheduler_priority`; both are nullable when a
transaction's scheduler configuration cannot be derived.

The CLI accepts a single epoch (`950`), an inclusive `<start>-<end>` epoch range (`900-950`),
or an inclusive `<start>:<end>` slot range on the command line. See
[`JetstreamerRunner::parse_cli_args`](https://docs.rs/jetstreamer/latest/jetstreamer/fn.parse_cli_args.html)
for the precise rules.

### TUI dashboard

Add `--tui` to render a live terminal dashboard instead of plain log output:

- **TPS graph** with clickable time ranges (`5m`–`12h` trailing windows, or `all` for the
  entire run), a 5s rolling rate, and `0 / current / peak` axis labels.
- **Progress bar** with slots, ETA, and elapsed time.
- **Thread grid**: one dot per firehose thread, colored by data freshness (green → red as a
  thread approaches the stall timeout; cyan ✓ = finished its work).
- **System chart**: CPU, memory, and NIC download rate on one axis.
- **Stats panel**: live and average TPS, per-thread TPS spread, block/transaction totals,
  connection recycles, timeouts, work steals, ClickHouse write retries, and wire vs payload
  data rates.
- **Scrollable log pane** with a scrollbar and a click-to-resume `live` button.

Pane dividers are mouse-draggable. Press `q`, `Esc`, or `Ctrl-C` for the same graceful
shutdown as SIGINT; the final log lines are replayed to the terminal on exit.

### Throughput management

The threaded firehose actively manages its connection fleet to cope with CDN throttling:
threads launch through a health gate (pausing the ramp while any thread is stalled),
failed threads restart with exponential backoff, persistently-slow connections are recycled
(♻️) for fresh ones, and threads that finish their slot range steal work (🥷) — via a
message handshake in which the least-progressed thread hands over half of its remaining
slots — so every connection stays busy to the end of the run. Tune with
`JETSTREAMER_SPAWN_PENDING`, `JETSTREAMER_SPAWN_GRACE_SECS`, and `JETSTREAMER_RECYCLE_PCT`
(see the crate docs for details).

### ClickHouse Integration

Jetstreamer Runner has a built-in ClickHouse integration (by default a clickhouse server is
spawned running out of the `bin` directory in the repo)

To manage the ClickHouse integration with ease, the following bundled Cargo aliases are
provided when within the `jetstreamer` workspace:

```bash
cargo clickhouse-server
cargo clickhouse-client
```

`cargo clickhouse-server` launches the same ClickHouse binary that Jetstreamer Runner spawns in
`bin/`, while `cargo clickhouse-client` connects to the local instance so you can inspect
tables populated by the runner or plugin runner.

While Jetstreamer is running, you can use `cargo clickhouse-client` to connect directly to the
ClickHouse instance that Jetstreamer has spawned. If you want to access data after a run has
finished, you can run `cargo clickhouse-server` to bring up that server again using the data
that is currently in the `bin` directory. It is also possible to copy a `bin` directory from
one system to another as a way of migrating data.

#### Write durability

ClickHouse writes are never silently dropped. Inserts use `async_insert` with
`wait_for_async_insert=1`, so an acknowledgment means the data was durably flushed; any
failure is retried with exponential backoff (0.5s doubling to a 15s cap) for up to 10
minutes, visible live as the `db retries` stat in the TUI. In-flight write tasks are tracked
and drained at shutdown, so finishing a run (or Ctrl-C) never cancels a batch mid-delivery.
If a write is still failing after the full horizon, the run aborts loudly and prints the
exact command to resume from the lowest unprocessed slot. Retries give at-least-once delivery: every table is a
`ReplacingMergeTree` keyed on its logical identity, so a replayed batch deduplicates on
merge — consumers should query with `FINAL` (or tolerate transient duplicates) when reading
while ingestion is active.

### Writing Jetstreamer Plugins

Jetstreamer Plugins are plugins that can be run by the Jetstreamer Runner.

Implement the `Plugin` trait to observe epoch/block/transaction/reward/entry events. The
example below mirrors the crate-level documentation and demonstrates how to react to both
transactions and blocks.

Note that Jetstreamer's firehose and underlying interface emits `BlockData::PossibleLeaderSkipped`
events whenever it observes a slot gap. These represent either leader-skipped slots or blocks
that have not arrived yet; when the real block eventually shows up, `BlockData::Block` will be
emitted for it just like normal geyser streams.

Also note that because Jetstreamer spawns parallel threads that process different subranges of
the overall slot range at the same time, while each thread sees a purely sequential view of
transactions, downstream services such as databases that consume this data will see writes in a
fairly arbitrary order, so you should design your database tables and shared data structures
accordingly.

```rust
use std::sync::Arc;

use clickhouse::Client;
use jetstreamer::{
    JetstreamerRunner,
    firehose::{BlockData, TransactionData},
    firehose::epochs,
    plugin::{Plugin, PluginFuture},
};

struct LoggingPlugin;

impl Plugin for LoggingPlugin {
    fn name(&self) -> &'static str {
        "logging"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        tx: &'a TransactionData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            println!("tx {} landed in slot {}", tx.signature, tx.slot);
            Ok(())
        })
    }

    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            if block.was_skipped() {
                println!("slot {} was skipped", block.slot());
            } else {
                println!("processed block at slot {}", block.slot());
            }
            Ok(())
        })
    }
}

let (start_slot, end_inclusive) = epochs::epoch_to_slot_range(800);

JetstreamerRunner::new()
    .with_plugin(Box::new(LoggingPlugin))
    .with_threads(4)
    .with_slot_range_bounds(start_slot, end_inclusive + 1)
    .with_clickhouse_dsn("https://clickhouse.example.com")
    .run()
    .expect("runner completed");
```

If you prefer to configure Jetstreamer via the command line, keep using
`JetstreamerRunner::parse_cli_args` to hydrate the runner from process arguments and
environment variables.

When `JETSTREAMER_CLICKHOUSE_MODE` is `auto` (the default), Jetstreamer inspects the DSN to
decide whether to launch the bundled ClickHouse helper or connect to an external cluster.

### Alternate Archive Mirrors

Jetstreamer defaults to the public Old Faithful mirror (`https://files.old-faithful.net`), but
the firehose can also stream CARs and compact indexes directly from authenticated
S3-compatible storage. Configure the backend via the following environment variables:

- `JETSTREAMER_ARCHIVE_BACKEND` (default `http`): set to `s3` to force the S3 client.
- `JETSTREAMER_HTTP_BASE_URL`: base URL or `s3://bucket/prefix` for CAR files.
- `JETSTREAMER_COMPACT_INDEX_BASE_URL`: optional override for slot indexes (also accepts `s3://` URIs). Jetstreamer resolves slot offsets from the per-epoch `epoch-{N}-slot-ranges.raw` files (~5 MB each) and falls back to the deprecated legacy compactindex pair when a mirror does not serve them (`JETSTREAMER_FORCE_LEGACY_INDEX=1` forces the legacy path).
- `JETSTREAMER_ARCHIVE_BASE`: single knob that applies to both cars and indexes when the more specific variables are unset.
- `JETSTREAMER_S3_BUCKET`, `JETSTREAMER_S3_PREFIX`, `JETSTREAMER_S3_INDEX_PREFIX`: bucket/prefix overrides when not encoded in the `s3://` URL.
- `JETSTREAMER_S3_REGION` and `JETSTREAMER_S3_ENDPOINT`: region plus optional custom endpoint (e.g. `https://s3.eu-central-003.backblazeb2.com`).
- `JETSTREAMER_S3_ACCESS_KEY`, `JETSTREAMER_S3_SECRET_KEY`, `JETSTREAMER_S3_SESSION_TOKEN`: credentials used for signing requests (falls back to AWS standard env vars).

S3 support is compiled behind the `s3-backend` Cargo feature. Enable it when running or
depending on `jetstreamer` if you plan to consume `s3://` archives:

```bash
cargo run --features s3-backend -- 800
```

#### Batching ClickHouse Writes

ClickHouse (and anything you do in your callbacks) applies backpressure that will slow down
Jetstreamer if not kept in check.

When implementing a Jetstreamer plugin, prefer buffering records locally and flushing them in
periodic batches rather than writing on every hook invocation. The runner's built-in stats
pulses and per-plugin flush cadence are both driven by `db_update_interval_slots` (100 slots
by default, defined in `jetstreamer-plugin/src/lib.rs`), which strikes a balance between
timely metrics and avoiding tight write loops. The bundled plugins follow this model: they
accumulate per-slot events in a shared `DashMap` (see
`jetstreamer-plugin/src/plugins/program_tracking.rs`) and the runner batch-inserts the drained
rows on the shared flush cadence. Structuring custom plugins with a similar cadence keeps
ClickHouse responsive during high throughput replays.

### Firehose

For direct access to the stream of transactions/blocks/rewards etc, you can use the `firehose`
interface, which allows you to specify a number of async function callbacks that will receive
transaction/block/reward/etc data on multiple threads in parallel.

## Epoch Feature Availability

Old Faithful ledger snapshots vary in what metadata is available, because Solana as a
blockchain has evolved significantly over time. Use the table below to decide which epochs fit
your needs. In particular, note that early versions of the chain are no longer compatible with
modern geyser but _do_ work with the current `firehose` interface and `JetstreamerRunner`.
Furthermore, CU tracking was not always available historically so it is not available once you
go back far enough.

| Epoch | Slot        | Comment |
|-------|-------------|--------------------------------------------------|
| 0-156 | 0-?         | Incompatible with modern Geyser plugins |
| 157+  | ?           | Compatible with modern Geyser plugins |
| 0-449 | 0-194184610 | CU tracking not available (reported as 0)        |
| 450+  | 194184611+  | CU tracking available                            |

Epochs at or above 157 are compatible with the current Geyser plugin interface, while compute
unit accounting first appears at epoch 450. Plan replay windows accordingly.

## Installation and Setup

### Nix (Recommended)

For the most reliable setup, use Nix:

```bash
nix-shell
cargo build --release
```

### Non-Nix Setup

Jetstreamer requires **Clang 16** (not 17) due to RocksDB dependencies. Install dependencies and set environment variables:

#### Linux (Ubuntu/Debian)

```bash
# Install Clang 16
wget -qO- https://apt.llvm.org/llvm.sh | sudo bash -s -- 16
sudo apt update && sudo apt install -y gcc-13 g++-13 zlib1g-dev libssl-dev libtool

# Set as default
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-16 100
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-16 100

# Environment variables (add to ~/.bashrc)
export CC=clang
export CXX=clang++
export LIBCLANG_PATH=/usr/lib/llvm16/lib/libclang.so
```

#### Linux (Arch)

```bash
sudo pacman -S clang16 llvm16 zlib openssl libtool
yay -S gcc13  # or use system gcc

# Environment variables (add to ~/.bashrc)
export CC=clang-16
export CXX=clang++-16
export LIBCLANG_PATH=/usr/lib/llvm16/lib/libclang.so
export LD_LIBRARY_PATH=/usr/lib/llvm16/lib:$LD_LIBRARY_PATH
```

#### macOS

```bash
brew install llvm@16 zlib openssl libtool

# Environment variables (add to ~/.zshrc)
export CC=/opt/homebrew/opt/llvm@16/bin/clang
export CXX=/opt/homebrew/opt/llvm@16/bin/clang++
export LIBCLANG_PATH=/opt/homebrew/opt/llvm@16/lib/libclang.dylib
export LDFLAGS="-L/opt/homebrew/opt/llvm@16/lib"
export CPPFLAGS="-I/opt/homebrew/opt/llvm@16/include"
```

**Troubleshooting**: If you get RocksDB compilation errors, ensure you're using Clang 16 (not 17) and `LIBCLANG_PATH` is correctly set.

## Developing Locally

- Format and lint: `cargo fmt --all` and `cargo clippy --workspace`.
- Run tests: `cargo test --workspace`.
- Regenerate docs: `cargo doc --workspace --open`.

## Community

Questions, issues, and contributions are welcome! Open a discussion or pull request on
[GitHub](https://github.com/anza-xyz/jetstreamer) and join the effort to build faster Solana
analytics pipelines.

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
