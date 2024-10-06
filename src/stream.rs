use streamstore::{
    client::StreamClient,
    service_error::{AppendSessionError, GetNextSeqNumError, ServiceError},
    types::AppendOutput,
    Streaming,
};
use tokio::io::AsyncBufRead;

use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use streamstore::types::{AppendInput, AppendRecord};
use tokio::io::Lines;
use tokio_stream::Stream;

pin_project! {
    #[derive(Debug)]
    pub struct RecordStream<R> {
        #[pin]
        inner: Lines<R>,
        peeked_record: Option<String>,
    }
}

impl<R> RecordStream<R> {
    pub fn new(lines: Lines<R>) -> Self {
        Self {
            inner: lines,
            peeked_record: None,
        }
    }
}

impl<R: AsyncBufRead> Stream for RecordStream<R> {
    type Item = AppendInput;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut num_records = 0;
        let mut batch_size = 0;
        let mut append_input = AppendInput::new(Vec::with_capacity(1000));

        if let Some(record) = &this.peeked_record.take() {
            append_input.records.push(AppendRecord::new(record.clone()));
            num_records += 1;
            batch_size += record.len() + 8;
        }

        while num_records < 1000 {
            let line = match this.inner.as_mut().poll_next_line(cx) {
                Poll::Ready(Ok(Some(line))) => {
                    if batch_size + line.len() + 8 > 1024 * 1024 {
                        *this.peeked_record = Some(line.clone());
                        break;
                    }
                    batch_size += line.len() + 8;
                    line
                }
                Poll::Ready(Err(e)) => {
                    eprintln!("Error reading line: {}", e);
                    return Poll::Ready(None);
                }
                Poll::Ready(Ok(None)) | Poll::Pending => {
                    if num_records == 0 {
                        return Poll::Pending;
                    } else {
                        break;
                    }
                }
            };
            num_records += 1;
            append_input.records.push(AppendRecord::new(line));
        }

        eprintln!(
            "[SENT]: read [{} records], assumed batch size [{} bytes]",
            num_records, batch_size
        );
        Poll::Ready(Some(append_input))
    }
}

pub struct StreamService {
    client: StreamClient,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamServiceError {
    #[error("Failed to get next sequence number")]
    GetNextSeqNumError(#[from] ServiceError<GetNextSeqNumError>),

    #[error("Failed to append records")]
    AppendSessionError(#[from] ServiceError<AppendSessionError>),
}

impl StreamService {
    pub fn new(client: StreamClient) -> Self {
        Self { client }
    }

    pub async fn get_next_seq_num(&self) -> Result<u64, StreamServiceError> {
        Ok(self.client.get_next_seq_num().await?)
    }

    pub async fn append_session(
        &self,
        stream: RecordStream<Box<dyn AsyncBufRead + Send + Unpin>>,
    ) -> Result<Streaming<AppendOutput, AppendSessionError>, StreamServiceError> {
        Ok(self.client.append_session(stream).await?)
    }
}
