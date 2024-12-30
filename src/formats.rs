use s2::types::{AppendRecord, ConvertError, SequencedRecord};
use std::io;
use tokio::io::AsyncWrite;

use futures::Stream;

#[derive(Debug, thiserror::Error)]
pub enum RecordParseError {
    #[error("Error reading: {0}")]
    Io(#[from] io::Error),
    #[error("Error parsing: {0}")]
    Convert(#[from] ConvertError),
}

pub trait RecordParser<I>
where
    I: Stream<Item = io::Result<String>> + Send + Unpin,
{
    type RecordStream: Stream<Item = Result<AppendRecord, RecordParseError>> + Send + Unpin;

    fn parse_records(lines: I) -> Self::RecordStream;
}

pub trait RecordWriter {
    async fn write_record(
        record: &SequencedRecord,
        writer: &mut (impl AsyncWrite + Unpin),
    ) -> io::Result<()>;
}

pub mod text {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{Stream, StreamExt};
    use s2::types::{AppendRecord, SequencedRecord};
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    use super::{RecordParseError, RecordParser, RecordWriter};

    pub struct Formatter;

    impl RecordWriter for Formatter {
        async fn write_record(
            record: &SequencedRecord,
            writer: &mut (impl AsyncWrite + Unpin),
        ) -> io::Result<()> {
            let s = String::from_utf8_lossy(&record.body);
            writer.write_all(s.as_ref().as_bytes()).await
        }
    }

    impl<I> RecordParser<I> for Formatter
    where
        I: Stream<Item = io::Result<String>> + Send + Unpin,
    {
        type RecordStream = RecordStream<I>;

        fn parse_records(lines: I) -> Self::RecordStream {
            RecordStream(lines)
        }
    }

    pub struct RecordStream<S>(S);

    impl<S> Stream for RecordStream<S>
    where
        S: Stream<Item = io::Result<String>> + Send + Unpin,
    {
        type Item = Result<AppendRecord, RecordParseError>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.0.poll_next_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Some(Ok(s))) => Poll::Ready(Some(Ok(AppendRecord::new(s)?))),
            }
        }
    }
}

pub mod json {
    use std::{
        borrow::Cow,
        collections::HashMap,
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{Stream, StreamExt};
    use s2::types::{AppendRecord, AppendRecordParts, ConvertError, Header, SequencedRecord};
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    use super::{RecordParseError, RecordParser, RecordWriter};

    pub struct Formatter;

    #[derive(Debug, Clone, Serialize)]
    struct SerializableSequencedRecord<'a> {
        seq_num: u64,
        headers: HashMap<Cow<'a, str>, Cow<'a, str>>,
        body: Cow<'a, str>,
    }

    impl<'a> From<&'a SequencedRecord> for SerializableSequencedRecord<'a> {
        fn from(value: &'a SequencedRecord) -> Self {
            let SequencedRecord {
                seq_num,
                headers,
                body,
            } = value;

            let headers = headers
                .iter()
                .map(|Header { name, value }| {
                    (
                        String::from_utf8_lossy(name),
                        String::from_utf8_lossy(value),
                    )
                })
                .collect::<HashMap<_, _>>();

            let body = String::from_utf8_lossy(body);

            SerializableSequencedRecord {
                seq_num: *seq_num,
                headers,
                body,
            }
        }
    }

    impl RecordWriter for Formatter {
        async fn write_record(
            record: &SequencedRecord,
            writer: &mut (impl AsyncWrite + Unpin),
        ) -> io::Result<()> {
            let record: SerializableSequencedRecord = record.into();
            let s = serde_json::to_string(&record).map_err(io::Error::other)?;
            writer.write_all(s.as_bytes()).await
        }
    }

    impl<I> RecordParser<I> for Formatter
    where
        I: Stream<Item = io::Result<String>> + Send + Unpin,
    {
        type RecordStream = RecordStream<I>;

        fn parse_records(lines: I) -> Self::RecordStream {
            RecordStream(lines)
        }
    }

    #[derive(Debug, Clone, Deserialize)]
    struct DeserializableAppendRecord {
        #[serde(default)]
        headers: HashMap<String, String>,
        #[serde(default)]
        body: String,
    }

    impl TryFrom<DeserializableAppendRecord> for AppendRecord {
        type Error = ConvertError;

        fn try_from(value: DeserializableAppendRecord) -> Result<Self, Self::Error> {
            let DeserializableAppendRecord { headers, body } = value;

            let parts = AppendRecordParts {
                headers: headers
                    .into_iter()
                    .map(|(k, v)| Header::new(k, v))
                    .collect(),
                body: body.into(),
            };

            parts.try_into()
        }
    }

    pub struct RecordStream<S>(S);

    impl<S> Stream for RecordStream<S>
    where
        S: Stream<Item = io::Result<String>> + Send + Unpin,
    {
        type Item = Result<AppendRecord, RecordParseError>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            fn parse_record(s: String) -> Result<AppendRecord, RecordParseError> {
                let append_record: DeserializableAppendRecord = serde_json::from_str(&s)
                    .map_err(|e| RecordParseError::Convert(e.to_string().into()))?;

                Ok(append_record.try_into()?)
            }

            match self.0.poll_next_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Some(Ok(s))) => Poll::Ready(Some(parse_record(s))),
            }
        }
    }
}
