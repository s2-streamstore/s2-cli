use s2::types::{AppendRecord, ConvertError};
use std::io;

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

pub mod text {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{Stream, StreamExt};
    use s2::types::AppendRecord;

    use super::RecordParseError;

    pub struct Formatter;

    impl<I> super::RecordParser<I> for Formatter
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
        collections::HashMap,
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{Stream, StreamExt};
    use s2::types::{AppendRecord, AppendRecordParts, ConvertError, Header};
    use serde::Deserialize;

    use super::RecordParseError;

    pub struct Formatter;

    impl<I> super::RecordParser<I> for Formatter
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
