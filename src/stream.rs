use streamstore::{
    batching::AppendRecordsBatchingStream,
    client::StreamClient,
    types::{AppendOutput, ReadLimit, ReadOutput, ReadSessionRequest},
    Streaming,
};
use tokio::io::AsyncBufRead;

use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use streamstore::types::AppendRecord;
use tokio::io::Lines;
use tokio_stream::Stream;

use crate::error::s2_status;

pin_project! {
    #[derive(Debug)]
    pub struct RecordStream<R> {
        #[pin]
        inner: Lines<R>,
    }
}

impl<R> RecordStream<R> {
    pub fn new(lines: Lines<R>) -> Self {
        Self { inner: lines }
    }
}

impl<R: AsyncBufRead> Stream for RecordStream<R> {
    type Item = AppendRecord;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.inner.as_mut().poll_next_line(cx) {
            Poll::Ready(Ok(Some(line))) => Poll::Ready(Some(AppendRecord::new(line))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(e)) => {
                eprintln!("Error reading line: {}", e);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct StreamService {
    client: StreamClient,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamServiceError {
    #[error("Failed to get next sequence number: {0}")]
    CheckTail(String),

    #[error("Failed to append records: {0}")]
    AppendSession(String),

    #[error("Failed to read records: {0}")]
    ReadSession(String),
}

impl StreamService {
    pub fn new(client: StreamClient) -> Self {
        Self { client }
    }

    pub async fn check_tail(&self) -> Result<u64, StreamServiceError> {
        self.client
            .check_tail()
            .await
            .map_err(|e| StreamServiceError::CheckTail(s2_status(&e)))
    }

    pub async fn append_session(
        &self,
        append_input_stream: RecordStream<Box<dyn AsyncBufRead + Send + Unpin>>,
    ) -> Result<Streaming<AppendOutput>, StreamServiceError> {
        let append_record_stream =
            AppendRecordsBatchingStream::new(append_input_stream, Default::default());

        self.client
            .append_session(append_record_stream)
            .await
            .map_err(|e| StreamServiceError::AppendSession(s2_status(&e)))
    }

    pub async fn read_session(
        &self,
        start: u64,
        limit_count: Option<u64>,
        limit_bytes: Option<u64>,
    ) -> Result<Streaming<ReadOutput>, StreamServiceError> {
        let read_limit = match (limit_count, limit_bytes) {
            (Some(count), Some(bytes)) => Some(ReadLimit { count, bytes }),
            (Some(count), None) => Some(ReadLimit {
                count,
                bytes: 1024 * 1024,
            }),

            (None, Some(bytes)) => Some(ReadLimit { count: 1000, bytes }),
            _ => None,
        };

        let mut read_session_req = ReadSessionRequest::new()            
            .with_start_seq_num(start);

        if let Some(read_limit) = read_limit {
            read_session_req = read_session_req.with_limit(read_limit);
        }

        self.client
            .read_session(read_session_req)
            .await
            .map_err(|e| StreamServiceError::ReadSession(s2_status(&e)))
    }
}
