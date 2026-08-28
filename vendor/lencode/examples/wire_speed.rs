//! Codec speed comparison on real mainnet message bodies: wincode (the wire
//! serializer agave uses) vs serde-bincode vs lencode, plain and with the
//! frozen-dictionary dedupe context.
//!
//! Input is produced by jetstreamer's `shred_compression --dump-msgs PATH N`:
//! a bincode `Vec` of message bodies (static account keys, blockhash,
//! instructions) plus `PATH.prime`, the 65,535-entry frequency table as raw
//! 32-byte keys. Message bodies are where all compressible structure lives;
//! signatures are a raw memcpy for every codec and are excluded equally.
//!
//! Usage:
//!   cargo run --release --features solana --example wire_speed -- <dump.bin>

#![cfg_attr(not(all(feature = "solana", feature = "std")), allow(dead_code))]

#[cfg(all(feature = "solana", feature = "std"))]
mod real {
    use lencode::context::{DecoderContext, EncoderContext};
    use lencode::dedupe::{
        DedupeDecodeable, DedupeDecoder, DedupeEncodeable, DedupeEncoder, DefaultDedupeHasher,
    };
    use lencode::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::io::Cursor;
    use std::sync::Arc;
    use std::time::Instant;
    use wincode::io::Cursor as WincodeCursor;
    use wincode::{SchemaRead, SchemaWrite};

    // Transport schema (matches the dumper in jetstreamer).
    #[derive(Deserialize)]
    struct DumpIx {
        program_id_index: u8,
        accounts: Vec<u8>,
        data: Vec<u8>,
    }
    #[derive(Deserialize)]
    struct DumpMsg {
        account_keys: Vec<[u8; 32]>,
        recent_blockhash: [u8; 32],
        instructions: Vec<DumpIx>,
    }

    // Wire-shaped mirrors (same layout choices as benches/solana_bench.rs:
    // short-vec lengths where the solana wire format uses them).
    #[derive(Clone, PartialEq, Eq, Hash, Pack, Serialize, Deserialize, SchemaWrite, SchemaRead)]
    #[repr(transparent)]
    struct BenchPubkey([u8; 32]);
    impl DedupeEncodeable for BenchPubkey {
        type Hasher = DefaultDedupeHasher;
    }
    impl DedupeDecodeable for BenchPubkey {
        type Hasher = DefaultDedupeHasher;
    }

    #[derive(Clone, PartialEq, Serialize, Deserialize, SchemaWrite, SchemaRead, Encode, Decode)]
    struct BenchCompiledInstruction {
        program_id_index: u8,
        #[serde(with = "solana_short_vec")]
        #[wincode(with = "wincode::containers::Vec<_, wincode::len::ShortU16Len>")]
        accounts: Vec<u8>,
        #[serde(with = "solana_short_vec")]
        #[wincode(with = "wincode::containers::Vec<_, wincode::len::ShortU16Len>")]
        data: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Serialize, Deserialize, SchemaWrite, SchemaRead, Encode, Decode)]
    struct BenchMessage {
        #[serde(with = "solana_short_vec")]
        #[wincode(with = "wincode::containers::Vec<_, wincode::len::ShortU16Len>")]
        account_keys: Vec<BenchPubkey>,
        recent_blockhash: [u8; 32],
        #[serde(with = "solana_short_vec")]
        #[wincode(with = "wincode::containers::Vec<_, wincode::len::ShortU16Len>")]
        instructions: Vec<BenchCompiledInstruction>,
    }

    struct Timing {
        name: &'static str,
        enc_ns: u128,
        dec_ns: u128,
        bytes: u64,
    }

    fn report(t: &Timing, base: &Timing, msgs: usize) {
        let enc_rate = msgs as f64 / (t.enc_ns as f64 / 1e9);
        let dec_rate = msgs as f64 / (t.dec_ns as f64 / 1e9);
        let mibs_out = t.bytes as f64 / (t.enc_ns as f64 / 1e9) / (1 << 20) as f64;
        println!(
            "  {:<18} enc {:>10.0} msg/s ({:>7.1} MiB/s out)   dec {:>10.0} msg/s   {:>5.1} B/msg   \
             enc {:>5.2}x, dec {:>5.2}x vs wincode",
            t.name,
            enc_rate,
            mibs_out,
            dec_rate,
            t.bytes as f64 / msgs as f64,
            base.enc_ns as f64 / t.enc_ns as f64,
            base.dec_ns as f64 / t.dec_ns as f64,
        );
    }

    pub fn run() {
        let mut args = std::env::args().skip(1);
        let path = args.next().unwrap_or_else(|| {
            eprintln!("usage: wire_speed <dump.bin>   (produced by shred_compression --dump-msgs)");
            std::process::exit(2);
        });

        let raw = std::fs::read(&path).expect("read dump");
        let dump: Vec<DumpMsg> = bincode::serde::decode_from_slice(&raw, bincode::config::legacy())
            .expect("parse dump")
            .0;
        let msgs: Vec<BenchMessage> = dump
            .into_iter()
            .map(|m| BenchMessage {
                account_keys: m.account_keys.into_iter().map(BenchPubkey).collect(),
                recent_blockhash: m.recent_blockhash,
                instructions: m
                    .instructions
                    .into_iter()
                    .map(|ix| BenchCompiledInstruction {
                        program_id_index: ix.program_id_index,
                        accounts: ix.accounts,
                        data: ix.data,
                    })
                    .collect(),
            })
            .collect();
        let n = msgs.len();

        let prime_raw = std::fs::read(format!("{path}.prime")).expect("read prime table");
        assert_eq!(prime_raw.len() % 32, 0);
        let mut enc_primer = DedupeEncoder::new();
        let mut dec_primer = DedupeDecoder::new();
        for chunk in prime_raw.chunks_exact(32) {
            let pk = BenchPubkey(<[u8; 32]>::try_from(chunk).unwrap());
            enc_primer.prime::<BenchPubkey, <BenchPubkey as DedupeEncodeable>::Hasher>(&pk);
            dec_primer.prime::<BenchPubkey>(pk);
        }
        let frozen_enc = Arc::new(enc_primer.freeze());
        let frozen_dec = Arc::new(dec_primer.freeze());

        println!(
            "\n=== {path} | {} real mainnet message bodies | prime table {} entries ===\n",
            n,
            prime_raw.len() / 32
        );

        // ---- wincode (the deployed wire serializer) ----
        let win = {
            let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(n);
            let t0 = Instant::now();
            for m in &msgs {
                let mut cur = WincodeCursor::new(Vec::with_capacity(512));
                wincode::serialize_into(&mut cur, m).unwrap();
                encoded.push(cur.into_inner());
            }
            let enc_ns = t0.elapsed().as_nanos();
            let bytes: u64 = encoded.iter().map(|b| b.len() as u64).sum();
            let t1 = Instant::now();
            for b in &encoded {
                let m: BenchMessage = wincode::deserialize(b).unwrap();
                std::hint::black_box(&m);
            }
            let dec_ns = t1.elapsed().as_nanos();
            // Wire-compat sanity: wincode bytes must equal serde-bincode bytes.
            for (m, b) in msgs.iter().zip(&encoded).take(1000) {
                assert_eq!(
                    &bincode::serde::encode_to_vec(m, bincode::config::legacy()).unwrap(),
                    b,
                    "wincode/bincode divergence"
                );
            }
            Timing { name: "wincode", enc_ns, dec_ns, bytes }
        };

        // ---- serde bincode ----
        let bin = {
            let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(n);
            let t0 = Instant::now();
            for m in &msgs {
                encoded.push(bincode::serde::encode_to_vec(m, bincode::config::legacy()).unwrap());
            }
            let enc_ns = t0.elapsed().as_nanos();
            let bytes: u64 = encoded.iter().map(|b| b.len() as u64).sum();
            let t1 = Instant::now();
            for b in &encoded {
                let m: BenchMessage =
                    bincode::serde::decode_from_slice(b, bincode::config::legacy())
                        .unwrap()
                        .0;
                std::hint::black_box(&m);
            }
            Timing { name: "bincode (serde)", enc_ns, dec_ns: t1.elapsed().as_nanos(), bytes }
        };

        // ---- lencode, no context ----
        let len_plain = {
            let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(n);
            let t0 = Instant::now();
            for m in &msgs {
                let mut w = lencode::io::VecWriter::new();
                m.encode_ext(&mut w, None).unwrap();
                encoded.push(w.into_inner());
            }
            let enc_ns = t0.elapsed().as_nanos();
            let bytes: u64 = encoded.iter().map(|b| b.len() as u64).sum();
            let t1 = Instant::now();
            for b in &encoded {
                let m = BenchMessage::decode_ext(&mut Cursor::new(b), None).unwrap();
                std::hint::black_box(&m);
            }
            Timing { name: "lencode", enc_ns, dec_ns: t1.elapsed().as_nanos(), bytes }
        };

        // ---- lencode + frozen dictionary, reset per message (SIMD-0385 model) ----
        let len_dict = {
            let mut ctx = EncoderContext {
                dedupe: Some(DedupeEncoder::with_frozen(Arc::clone(&frozen_enc))),
                diff: None,
            };
            let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(n);
            let t0 = Instant::now();
            for m in &msgs {
                if let Some(d) = ctx.dedupe.as_mut() {
                    d.clear();
                }
                let mut w = lencode::io::VecWriter::new();
                m.encode_ext(&mut w, Some(&mut ctx)).unwrap();
                encoded.push(w.into_inner());
            }
            let enc_ns = t0.elapsed().as_nanos();
            let bytes: u64 = encoded.iter().map(|b| b.len() as u64).sum();
            let mut dctx = DecoderContext {
                dedupe: Some(DedupeDecoder::with_frozen(Arc::clone(&frozen_dec))),
                diff: None,
            };
            let t1 = Instant::now();
            for b in &encoded {
                if let Some(d) = dctx.dedupe.as_mut() {
                    d.clear();
                }
                let m = BenchMessage::decode_ext(&mut Cursor::new(b), Some(&mut dctx)).unwrap();
                std::hint::black_box(&m);
            }
            let dec_ns = t1.elapsed().as_nanos();
            // Correctness: dictionary round-trip must reproduce the message.
            let mut vctx = DecoderContext {
                dedupe: Some(DedupeDecoder::with_frozen(Arc::clone(&frozen_dec))),
                diff: None,
            };
            for (m, b) in msgs.iter().zip(&encoded).take(1000) {
                if let Some(d) = vctx.dedupe.as_mut() {
                    d.clear();
                }
                let back = BenchMessage::decode_ext(&mut Cursor::new(b), Some(&mut vctx)).unwrap();
                assert!(back == *m, "dictionary round-trip mismatch");
            }
            Timing { name: "lencode + dict", enc_ns, dec_ns, bytes }
        };

        println!("throughput (single thread), higher is better; size per message body:");
        for t in [&win, &bin, &len_plain, &len_dict] {
            report(t, &win, n);
        }
        println!(
            "\nwire sanity: wincode output byte-identical to serde-bincode on sampled messages ✓"
        );
    }
}

#[cfg(all(feature = "solana", feature = "std"))]
fn main() {
    real::run();
}

#[cfg(not(all(feature = "solana", feature = "std")))]
fn main() {
    eprintln!("build with --features solana (std default) to run wire_speed");
}
