use {
    crate::SharedError,
    base64::engine::{Engine, general_purpose::STANDARD},
    std::{
        io::{self, Read},
        vec::Vec,
    },
};

const MAX_VARINT_LEN_64: usize = 10;

/// Reads an unsigned LEB128-encoded integer from the provided reader.
pub fn read_uvarint<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut x = 0u64;
    let mut s = 0u32;
    let mut buffer = [0u8; 1];
    for i in 0..MAX_VARINT_LEN_64 {
        reader.read_exact(&mut buffer)?;
        let b = buffer[0];
        if b < 0x80 {
            if i == MAX_VARINT_LEN_64 - 1 && b > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "uvarint overflow",
                ));
            }
            return Ok(x | ((b as u64) << s));
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;

        if s > 63 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "uvarint too long",
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "uvarint overflow",
    ))
}

/// Owner type for 32-byte hashes that renders them as lowercase hex.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Hash(#[doc = "Underlying bytes comprising the hash."] pub Vec<u8>);

// debug converts the hash to hex
impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut hex = String::new();
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        write!(f, "{}", hex)
    }
}

// implement stringer for hash
impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut hex = String::new();
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        write!(f, "{}", hex)
    }
}

// implement serde serialization for hash
impl serde::Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut hex = String::new();
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        serializer.serialize_str(&hex)
    }
}

// implement serde deserialization for hash
impl<'de> serde::Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Hash, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        let mut bytes = vec![];
        for i in 0..hex.len() / 2 {
            bytes.push(u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap());
        }
        Ok(Hash(bytes))
    }
}

impl Hash {
    /// Returns the hash bytes as a `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }

    /// Constructs a [`struct@Hash`] from owned bytes.
    pub const fn from_vec(data: Vec<u8>) -> Hash {
        Hash(data)
    }

    /// Returns the hash as a 32-byte array.
    ///
    /// # Panics
    ///
    /// Panics if the underlying byte slice is shorter than 32 bytes.
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[..32].copy_from_slice(&self.0[..32]);
        bytes
    }
}

/// Growable binary buffer with base64 formatting helpers.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct Buffer(#[doc = "Owned bytes stored in the buffer."] Vec<u8>);

impl Buffer {
    /// Creates an empty buffer.
    pub const fn new() -> Buffer {
        Buffer(vec![])
    }

    /// Appends `data` to the buffer.
    pub fn write(&mut self, data: Vec<u8>) {
        self.0.extend(data);
    }

    /// Removes and returns `len` bytes from the front of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the available bytes.
    pub fn read(&mut self, len: usize) -> Vec<u8> {
        let mut data = vec![];
        for _ in 0..len {
            data.push(self.0.remove(0));
        }
        data
    }

    /// Returns the buffer length in bytes.
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the buffer is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the buffer contents as a borrowed slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the buffer contents as a `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }

    /// Creates a buffer from owned bytes.
    pub const fn from_vec(data: Vec<u8>) -> Buffer {
        Buffer(data)
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer").field("data", &self.0).finish()
    }
}

// base64
impl std::fmt::Display for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        STANDARD.encode(&self.0).fmt(f)
    }
}

impl serde::Serialize for Buffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        STANDARD.encode(&self.0).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Buffer {
    fn deserialize<D>(deserializer: D) -> Result<Buffer, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let base64 = String::deserialize(deserializer)?;
        Ok(Buffer(STANDARD.decode(base64).unwrap()))
    }
}

/// Maximum Old Faithful CAR section size permitted while parsing (32 MiB).
pub const MAX_ALLOWED_SECTION_SIZE: usize = 32 << 20; // 32MiB

/// Decompresses a Zstandard byte stream.
pub fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>, SharedError> {
    let mut decoder = ZstdDecoder::default();
    decoder.decompress(data)?;
    Ok(decoder.output)
}

/// Scratch space owned by one firehose worker; no locks or thread-local state.
/// The output is borrowed until the next frame so decoding need not copy it.
#[derive(Default)]
pub(crate) struct ZstdDecoder {
    context: zstd::zstd_safe::DCtx<'static>,
    output: Vec<u8>,
}

impl ZstdDecoder {
    pub(crate) fn decompress(&mut self, data: &[u8]) -> Result<&[u8], SharedError> {
        // Reset even after an invalid/truncated frame before reusing the context.
        self.context
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|code| io::Error::other(zstd::zstd_safe::get_error_name(code)))?;
        self.output.clear();
        // A slice already implements BufRead: do not allocate a BufReader.
        zstd::Decoder::with_context(data, &mut self.context).read_to_end(&mut self.output)?;
        Ok(&self.output)
    }
}

#[cfg(test)]
mod zstd_reuse_tests {
    use super::*;

    #[test]
    fn reuses_output_allocation_and_clears_previous_frame() {
        let mut decoder = ZstdDecoder::default();
        let first = vec![42; 65536];
        let compressed = zstd::encode_all(first.as_slice(), 1).unwrap();
        let output = decoder.decompress(&compressed).unwrap();
        assert_eq!(output, first);
        let ptr = output.as_ptr();
        let compressed = zstd::encode_all(&b"short"[..], 1).unwrap();
        let output = decoder.decompress(&compressed).unwrap();
        assert_eq!(output, b"short");
        assert_eq!(output.as_ptr(), ptr);
    }

    #[test]
    fn recovers_after_corrupt_and_truncated_frames() {
        let compressed = zstd::encode_all(&b"hello world"[..], 1).unwrap();
        let mut decoder = ZstdDecoder::default();
        for bad in [&b"not zstd"[..], &compressed[..compressed.len() - 1]] {
            assert!(decoder.decompress(bad).is_err());
            assert_eq!(decoder.decompress(&compressed).unwrap(), b"hello world");
        }
    }

    #[test]
    fn supports_concatenated_frames_without_content_size() {
        let mut compressed = zstd::encode_all(&b"hello"[..], 1).unwrap();
        compressed.extend(zstd::encode_all(&b" world"[..], 1).unwrap());
        assert_eq!(
            ZstdDecoder::default().decompress(&compressed).unwrap(),
            b"hello world"
        );
    }
}
