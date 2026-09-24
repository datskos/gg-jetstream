//! Bounded HTTP range prefetching for parallel CAR readers.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesOrdered};
use once_cell::sync::Lazy;
use reqwest::{Client, StatusCode, header};
use tokio::{
    io::{AsyncRead, AsyncSeek, ReadBuf, SeekFrom},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};

use crate::node_reader::Len;

pub(crate) struct Config {
    concurrency: usize,
    chunk_bytes: u32,
    budget: Arc<Semaphore>,
}

impl Config {
    fn new(concurrency: usize, chunk_bytes: u64, budget_bytes: u64) -> Result<Self, String> {
        if concurrency == 0 || concurrency > 1024 {
            return Err(
                "JETSTREAMER_DOWNLOAD_CONCURRENCY must be between 1 and 1024 (or 0 to disable)"
                    .into(),
            );
        }
        let chunk_bytes = u32::try_from(chunk_bytes)
            .ok()
            .filter(|size| *size > 0)
            .ok_or("JETSTREAMER_DOWNLOAD_CHUNK must be between 1 byte and 4 GiB minus 1 byte")?;
        let budget_bytes = usize::try_from(budget_bytes).ok()
            .filter(|size| *size >= chunk_bytes as usize && *size <= Semaphore::MAX_PERMITS)
            .ok_or("JETSTREAMER_DOWNLOAD_BUFFER must fit in memory addressing and hold at least one download chunk")?;
        Ok(Self {
            concurrency,
            chunk_bytes,
            budget: Arc::new(Semaphore::new(budget_bytes)),
        })
    }
}

static CONFIG: Lazy<Result<Option<Arc<Config>>, String>> = Lazy::new(|| {
    let concurrency = std::env::var("JETSTREAMER_DOWNLOAD_CONCURRENCY")
        .unwrap_or_else(|_| "0".into())
        .parse::<usize>()
        .map_err(|_| "JETSTREAMER_DOWNLOAD_CONCURRENCY must be an integer")?;
    if concurrency == 0 {
        return Ok(None);
    }
    let bytes = |key: &str, default: &str| {
        let value = std::env::var(key).unwrap_or_else(|_| default.into());
        crate::system::parse_buffer_window_bytes(&value)
            .ok_or_else(|| format!("invalid {key}: {value}"))
    };
    let chunk = bytes("JETSTREAMER_DOWNLOAD_CHUNK", "8MiB")?;
    let budget = bytes("JETSTREAMER_DOWNLOAD_BUFFER", "8GiB")?;
    let config = Config::new(concurrency, chunk, budget)?;
    log::info!(
        "HTTP prefetch enabled: {concurrency} requests per reader, {chunk} bytes per chunk, {budget} bytes shared payload budget"
    );
    Ok(Some(Arc::new(config)))
});

pub(crate) fn settings() -> Result<Option<Arc<Config>>, String> {
    CONFIG.clone()
}

struct Chunk {
    bytes: Vec<u8>,
    // Held while downloading, queued, or being consumed. Drop returns capacity.
    _permit: OwnedSemaphorePermit,
}

struct FetchTask(JoinHandle<io::Result<Chunk>>);
impl Future for FetchTask {
    type Output = io::Result<Chunk>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|result| result.map_err(io::Error::other).and_then(|result| result))
    }
}
impl Drop for FetchTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn content_range(response: &reqwest::Response) -> io::Result<(u64, u64, u64)> {
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(io::Error::other(format!(
            "range request returned {}, expected 206",
            response.status()
        )));
    }
    let parsed = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes "))
        .and_then(|value| value.split_once('/'))
        .and_then(|(range, total)| {
            range
                .split_once('-')
                .map(|(start, end)| (start, end, total))
        })
        .and_then(|(start, end, total)| {
            Some((
                start.parse::<u64>().ok()?,
                end.parse::<u64>().ok()?,
                total.parse::<u64>().ok()?,
            ))
        });
    parsed
        .filter(|(start, end, total)| start <= end && end < total)
        .ok_or_else(|| io::Error::other("invalid or missing Content-Range"))
}

async fn request(
    client: &Client,
    url: &str,
    start: u64,
    end: u64,
    total: Option<u64>,
) -> io::Result<(u64, Vec<u8>)> {
    let mut response = client
        .get(url)
        .header(header::RANGE, format!("bytes={start}-{end}"))
        .header(header::ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(io::Error::other)?;
    let (actual_start, actual_end, actual_total) = content_range(&response)?;
    if actual_start != start
        || actual_end != end
        || total.is_some_and(|total| total != actual_total)
    {
        return Err(io::Error::other(
            "server returned a different byte range or object length",
        ));
    }
    let expected = usize::try_from(end - start + 1).map_err(io::Error::other)?;
    let mut bytes = Vec::with_capacity(expected);
    while let Some(chunk) = response.chunk().await.map_err(io::Error::other)? {
        if chunk.len() > expected - bytes.len() {
            return Err(io::Error::other("range response exceeded requested length"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != expected {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated range response",
        ));
    }
    Ok((actual_total, bytes))
}

async fn fetch(
    client: Client,
    url: String,
    start: u64,
    end: u64,
    total: u64,
    permit: OwnedSemaphorePermit,
) -> io::Result<Chunk> {
    for attempt in 0..3 {
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            request(&client, &url, start, end, Some(total)),
        )
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "range download timed out",
            ))
        });
        match result {
            Ok((_, bytes)) => {
                return Ok(Chunk {
                    bytes,
                    _permit: permit,
                });
            }
            Err(err) if attempt == 2 => return Err(err),
            Err(_) => tokio::time::sleep(Duration::from_millis(100 << attempt)).await,
        }
    }
    unreachable!()
}

async fn produce(
    client: Client,
    url: String,
    mut next: u64,
    total: u64,
    config: Arc<Config>,
    tx: mpsc::Sender<io::Result<Chunk>>,
) {
    let mut pending = FuturesOrdered::new();
    loop {
        while next < total && pending.len() < config.concurrency {
            let size = (total - next).min(u64::from(config.chunk_bytes)) as u32;
            // Reserve in stream order BEFORE launching requests. Never wait for
            // memory while holding completed chunks that this reader needs first.
            let permit = match config.budget.clone().try_acquire_many_owned(size) {
                Ok(permit) => permit,
                Err(_) if !pending.is_empty() => break,
                Err(_) => match config.budget.clone().acquire_many_owned(size).await {
                    Ok(permit) => permit,
                    Err(_) => return,
                },
            };
            let end = next + u64::from(size) - 1;
            pending.push_back(FetchTask(tokio::spawn(fetch(
                client.clone(),
                url.clone(),
                next,
                end,
                total,
                permit,
            ))));
            next = end + 1;
        }
        let Some(result) = pending.next().await else {
            return;
        };
        let failed = result.is_err();
        if tx.send(result).await.is_err() || failed {
            return;
        }
    }
}

/// Each reader has its own ordered pipeline; all readers share payload permits.
pub(crate) struct RangeReader {
    client: Client,
    url: String,
    config: Arc<Config>,
    len: u64,
    position: u64,
    producer: Option<JoinHandle<()>>,
    receiver: Option<mpsc::Receiver<io::Result<Chunk>>>,
    current: Option<Chunk>,
    offset: usize,
}

impl RangeReader {
    pub(crate) async fn open(client: Client, url: String, config: Arc<Config>) -> io::Result<Self> {
        let (len, _) =
            tokio::time::timeout(Duration::from_secs(30), request(&client, &url, 0, 0, None))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "range length probe timed out")
                })??;
        Ok(Self {
            client,
            url,
            config,
            len,
            position: 0,
            producer: None,
            receiver: None,
            current: None,
            offset: 0,
        })
    }

    fn cancel(&mut self) {
        if let Some(producer) = self.producer.take() {
            producer.abort();
        }
        self.receiver = None;
        self.current = None;
        self.offset = 0;
    }
}

impl Drop for RangeReader {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl Len for RangeReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl AsyncRead for RangeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 || self.position >= self.len {
            return Poll::Ready(Ok(()));
        }
        if self.receiver.is_none() {
            let (tx, rx) = mpsc::channel(self.config.concurrency);
            self.producer = Some(tokio::spawn(produce(
                self.client.clone(),
                self.url.clone(),
                self.position,
                self.len,
                self.config.clone(),
                tx,
            )));
            self.receiver = Some(rx);
        }
        if self.current.is_none() {
            match std::task::ready!(self.receiver.as_mut().unwrap().poll_recv(cx)) {
                Some(Ok(chunk)) => {
                    self.current = Some(chunk);
                    self.offset = 0;
                }
                Some(Err(err)) => {
                    self.cancel();
                    return Poll::Ready(Err(err));
                }
                None => {
                    self.cancel();
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "prefetch ended before object EOF",
                    )));
                }
            }
        }
        let chunk = self.current.as_ref().unwrap();
        let count = buf.remaining().min(chunk.bytes.len() - self.offset);
        buf.put_slice(&chunk.bytes[self.offset..self.offset + count]);
        self.offset += count;
        self.position += count as u64;
        if self.offset == self.current.as_ref().unwrap().bytes.len() {
            self.current = None;
            self.offset = 0;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for RangeReader {
    fn start_seek(mut self: Pin<&mut Self>, from: SeekFrom) -> io::Result<()> {
        let position = match from {
            SeekFrom::Start(position) => i128::from(position),
            SeekFrom::Current(delta) => i128::from(self.position) + i128::from(delta),
            SeekFrom::End(delta) => i128::from(self.len) + i128::from(delta),
        };
        let position = u64::try_from(position)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid seek offset"))?;
        if position != self.position {
            self.cancel();
            self.position = position;
        }
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    struct Server {
        url: String,
        task: JoinHandle<()>,
        data: Arc<Vec<u8>>,
        peak: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    // mode 1 ignores ranges; 2 lies about offsets; 3 truncates the first data response.
    async fn server(mode: u8) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/archive.car", listener.local_addr().unwrap());
        let data = Arc::new((0..4099).map(|n| (n % 251) as u8).collect::<Vec<_>>());
        let peak = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let (shared_data, shared_peak, shared_requests) =
            (data.clone(), peak.clone(), requests.clone());
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let (data, peak, active, requests) = (shared_data.clone(), shared_peak.clone(), active.clone(), shared_requests.clone());
                        connections.spawn(async move {
                            let mut header = Vec::new();
                            while !header.ends_with(b"\r\n\r\n") {
                                match socket.read_u8().await { Ok(byte) => header.push(byte), Err(_) => return }
                                assert!(header.len() < 16384);
                            }
                            let header = String::from_utf8(header).unwrap().to_ascii_lowercase();
                            let range = header.lines().find_map(|line| line.strip_prefix("range: bytes=")).unwrap();
                            let (start, end) = range.split_once('-').unwrap();
                            let (start, end): (usize, usize) = (start.parse().unwrap(), end.parse().unwrap());
                            if start == 0 && end == 0 {
                                let response = format!("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-0/{}\r\nContent-Length: 1\r\nConnection: close\r\n\r\n", data.len());
                                let _ = socket.write_all(response.as_bytes()).await;
                                let _ = socket.write_all(&data[..1]).await;
                                return;
                            }
                            let number = requests.fetch_add(1, Ordering::Relaxed);
                            let current = active.fetch_add(1, Ordering::Relaxed) + 1;
                            peak.fetch_max(current, Ordering::Relaxed);
                            // Complete later byte ranges before the first one.
                            tokio::time::sleep(Duration::from_millis(if start == 0 { 40 } else { 5 })).await;
                            let status = if mode == 1 { "200 OK" } else { "206 Partial Content" };
                            let reported_start = if mode == 2 { start + 1 } else { start };
                            let response = format!("HTTP/1.1 {status}\r\nContent-Range: bytes {reported_start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", data.len(), end - start + 1);
                            let _ = socket.write_all(response.as_bytes()).await;
                            let actual_end = if mode == 3 && number == 0 { end - 1 } else { end };
                            let _ = socket.write_all(&data[start..=actual_end]).await;
                            active.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Server {
            url,
            task,
            data,
            peak,
            requests,
        }
    }

    async fn reader(server: &Server, config: Arc<Config>) -> RangeReader {
        RangeReader::open(Client::new(), server.url.clone(), config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn parallel_requests_are_delivered_in_order_with_exact_eof() {
        let server = server(0).await;
        let mut reader = reader(&server, Arc::new(Config::new(4, 256, 2048).unwrap())).await;
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), reader.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes, *server.data);
        assert!(server.peak.load(Ordering::Relaxed) >= 2);
        assert_eq!(
            reader.stream_position().await.unwrap(),
            server.data.len() as u64
        );
        assert_eq!(reader.read(&mut [0; 1]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn downloads_continue_while_consumer_is_idle_and_share_a_bounded_budget() {
        let server = server(0).await;
        let config = Arc::new(Config::new(4, 256, 1024).unwrap());
        let mut first = reader(&server, config.clone()).await;
        let mut second = reader(&server, config.clone()).await;
        let mut prefix = [0; 1];
        first.read_exact(&mut prefix).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            server.requests.load(Ordering::Relaxed) > 1,
            "must fetch ahead without consumer polls"
        );
        assert_eq!(config.budget.available_permits(), 0);
        let mut a = Vec::new();
        let mut b = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::try_join!(first.read_to_end(&mut a), second.read_to_end(&mut b))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(a, server.data[1..]);
        assert_eq!(b, *server.data);
        drop((first, second));
        tokio::task::yield_now().await;
        assert_eq!(config.budget.available_permits(), 1024);
    }

    #[tokio::test]
    async fn seek_discards_prefetch_and_drop_releases_all_permits() {
        let server = server(0).await;
        let config = Arc::new(Config::new(4, 256, 1024).unwrap());
        let mut reader = reader(&server, config.clone()).await;
        reader.read_exact(&mut [0; 3]).await.unwrap();
        reader.seek(SeekFrom::Start(1000)).await.unwrap();
        let mut bytes = [0; 21];
        tokio::time::timeout(Duration::from_secs(5), reader.read_exact(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes, server.data[1000..1021]);
        reader.seek(SeekFrom::Current(-10)).await.unwrap();
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, server.data[1011..1032]);
        assert!(reader.seek(SeekFrom::Current(-99999)).await.is_err());
        reader.seek(SeekFrom::End(0)).await.unwrap();
        assert_eq!(reader.read(&mut bytes).await.unwrap(), 0);
        drop(reader);
        tokio::time::timeout(Duration::from_secs(2), async {
            while config.budget.available_permits() != 1024 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn retries_truncation_without_duplicate_bytes() {
        let server = server(3).await;
        let mut reader = reader(&server, Arc::new(Config::new(2, 256, 1024).unwrap())).await;
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), reader.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes, *server.data);
        assert!(server.requests.load(Ordering::Relaxed) > server.data.len().div_ceil(256));
    }

    #[tokio::test]
    async fn refuses_ignored_ranges_and_wrong_offsets() {
        for mode in [1, 2] {
            let server = server(mode).await;
            let mut reader = reader(&server, Arc::new(Config::new(2, 256, 512).unwrap())).await;
            let mut bytes = Vec::new();
            assert!(
                tokio::time::timeout(Duration::from_secs(5), reader.read_to_end(&mut bytes))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(bytes.is_empty());
        }
    }

    #[test]
    fn rejects_invalid_resource_limits() {
        assert!(Config::new(0, 256, 1024).is_err());
        assert!(Config::new(2, 0, 1024).is_err());
        assert!(Config::new(2, 2048, 1024).is_err());
        assert!(Config::new(2, u64::from(u32::MAX) + 1, u64::MAX).is_err());
        assert!(Config::new(4, 256, 256).is_ok());
    }
}
