use colored::Colorize;
use s2::{
    Streaming,
    batching::{AppendRecordsBatchingOpts, AppendRecordsBatchingStream},
    client::StreamClient,
    types::{
        AppendAck, AppendInput, AppendRecordBatch, Command, CommandRecord, FencingToken, ReadLimit,
        ReadOutput, ReadSessionRequest, ReadStart, StreamPosition,
    },
};
use std::ops::RangeTo;

use futures::{Stream, StreamExt};
use s2::types::AppendRecord;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{
    error::{ServiceError, ServiceErrorContext},
    formats::RecordParser,
};

#[derive(Debug)]
pub struct RecordStream<S, P>(P::RecordStream)
where
    S: Stream<Item = std::io::Result<String>> + Send + Unpin,
    P: RecordParser<S>;

impl<S, P> RecordStream<S, P>
where
    S: Stream<Item = std::io::Result<String>> + Send + Unpin,
    P: RecordParser<S>,
{
    pub fn new(s: S) -> Self {
        Self(P::parse_records(s))
    }
}

impl<S, P> Stream for RecordStream<S, P>
where
    S: Stream<Item = std::io::Result<String>> + Send + Unpin,
    P: RecordParser<S>,
{
    type Item = AppendRecord;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.0.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(record))) => Poll::Ready(Some(record)),
            Poll::Ready(Some(Err(e))) => {
                eprintln!("{}", e.to_string().red());
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
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

    pub async fn check_tail(&self) -> Result<StreamPosition, ServiceError> {
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
    ) -> Result<AppendAck, ServiceError> {
        let context = match &cmd.command {
            Command::Fence { .. } => ServiceErrorContext::Fence,
            Command::Trim { .. } => ServiceErrorContext::Trim,
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
        opts: AppendRecordsBatchingOpts,
    ) -> Result<Streaming<AppendAck>, ServiceError> {
        let append_record_stream = AppendRecordsBatchingStream::new(stream, opts);

        self.client
            .append_session(append_record_stream)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::AppendSession, e))
    }

    pub async fn read_session(
        &self,
        start: ReadStart,
        limit: ReadLimit,
        until: Option<RangeTo<u64>>,
    ) -> Result<Streaming<ReadOutput>, ServiceError> {
        self.client
            .read_session(ReadSessionRequest {
                start,
                limit,
                until,
            })
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ReadSession, e))
    }
}
