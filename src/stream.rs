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

use crate::error::ServiceError;
use crate::ByteSize;

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

impl StreamService {
    const ENTITY: &'static str = "stream";
    pub fn new(client: StreamClient) -> Self {
        Self { client }
    }

    pub async fn check_tail(&self) -> Result<u64, ServiceError> {
        const OPERATION: &str = "check tail";
        self.client
            .check_tail()
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }

    pub async fn append_session(
        &self,
        append_input_stream: RecordStream<Box<dyn AsyncBufRead + Send + Unpin>>,
    ) -> Result<Streaming<AppendOutput>, ServiceError> {
        const OPERATION: &str = "append to";
        let append_record_stream =
            AppendRecordsBatchingStream::new(append_input_stream, Default::default());

        self.client
            .append_session(append_record_stream)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }

    pub async fn read_session(
        &self,
        start_seq_num: u64,
        limit_count: Option<u64>,
        limit_bytes: Option<ByteSize>,
    ) -> Result<Streaming<ReadOutput>, ServiceError> {
        const OPERATION: &str = "read from";
        let read_session_req = ReadSessionRequest {
            start_seq_num: Some(start_seq_num),
            limit: match (limit_count, limit_bytes.map(|b| b.as_u64())) {
                (Some(count), Some(bytes)) => Some(ReadLimit { count, bytes }),
                (Some(count), None) => Some(ReadLimit { count, bytes: 0 }),
                (None, Some(bytes)) => Some(ReadLimit { count: 0, bytes }),
                (None, None) => None,
            },
        };

        self.client
            .read_session(read_session_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }
}
