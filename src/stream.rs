use streamstore::{
    batching::{AppendRecordsBatchingOpts, AppendRecordsBatchingStream},
    client::StreamClient,
    types::{
        AppendInput, AppendOutput, AppendRecordBatch, CommandRecord, FencingToken, ReadLimit,
        ReadOutput, ReadSessionRequest,
    },
    Streaming,
};
use tokio::io::AsyncBufRead;

use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use streamstore::types::AppendRecord;
use tokio::io::Lines;
use tokio_stream::Stream;

use crate::error::{ServiceError, ServiceErrorContext};

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
            Poll::Ready(Ok(Some(line))) => match AppendRecord::new(line) {
                Ok(record) => Poll::Ready(Some(record)),
                Err(e) => {
                    eprintln!("Error parsing line: {}", e);
                    Poll::Ready(None)
                }
            },
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
    pub fn new(client: StreamClient) -> Self {
        Self { client }
    }

    pub async fn check_tail(&self) -> Result<u64, ServiceError> {
        self.client
            .check_tail()
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::CheckTail, e))
    }

    pub async fn append_command_record(
        &self,
        cmd: CommandRecord,
        fencing_token: Option<FencingToken>,
        match_seq_num: Option<u64>,
    ) -> Result<AppendOutput, ServiceError> {
        let context = match &cmd {
            CommandRecord::Fence { .. } => ServiceErrorContext::Fence,
            CommandRecord::Trim { .. } => ServiceErrorContext::Trim,
        };
        let records = AppendRecordBatch::try_from_iter([cmd]).expect("single valid append record");
        let append_input = AppendInput {
            records,
            fencing_token,
            match_seq_num,
        };
        self.client
            .append(append_input)
            .await
            .map_err(|e| ServiceError::new(context, e))
    }

    pub async fn append_session(
        &self,
        stream: impl 'static + Send + Stream<Item = AppendRecord> + Unpin,
        fencing_token: Option<FencingToken>,
        match_seq_num: Option<u64>,
        max_batch_records: Option<usize>,
    ) -> Result<Streaming<AppendOutput>, ServiceError> {
        let mut opts = AppendRecordsBatchingOpts::default()
            .with_fencing_token(fencing_token)
            .with_match_seq_num(match_seq_num);

        if let Some(n) = max_batch_records {
            opts = opts.with_max_batch_records(n);
        }

        let append_record_stream = AppendRecordsBatchingStream::new(stream, opts);

        self.client
            .append_session(append_record_stream)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::AppendSession, e))
    }

    pub async fn read_session(
        &self,
        start_seq_num: u64,
        limit_count: Option<u64>,
        limit_bytes: Option<u64>,
    ) -> Result<Streaming<ReadOutput>, ServiceError> {
        let read_session_req = ReadSessionRequest {
            start_seq_num,
            limit: match (limit_count, limit_bytes) {
                (Some(count), Some(bytes)) => Some(ReadLimit { count, bytes }),
                (Some(count), None) => Some(ReadLimit { count, bytes: 0 }),
                (None, Some(bytes)) => Some(ReadLimit { count: 0, bytes }),
                (None, None) => None,
            },
        };

        self.client
            .read_session(read_session_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ReadSession, e))
    }
}
