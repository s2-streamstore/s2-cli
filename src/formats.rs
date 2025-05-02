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

pub use text::Formatter as TextFormatter;
pub type JsonFormatter = json::Formatter<false>;
pub type JsonBinsafeFormatter = json::Formatter<true>;

mod text {
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

mod json {
    use std::{
        borrow::Cow,
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use base64ct::{Base64, Encoding};
    use bytes::Bytes;
    use futures::{Stream, StreamExt};
    use s2::types::{AppendRecord, AppendRecordParts, ConvertError, Header, SequencedRecord};
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    use super::{RecordParseError, RecordParser, RecordWriter};

    #[derive(Debug, Clone, Default)]
    struct CowStr<'a, const BIN_SAFE: bool>(Cow<'a, str>);

    impl<const BIN_SAFE: bool> CowStr<'_, BIN_SAFE> {
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    type OwnedCowStr<const BIN_SAFE: bool> = CowStr<'static, BIN_SAFE>;

    impl<'a, const BIN_SAFE: bool> From<&'a [u8]> for CowStr<'a, BIN_SAFE> {
        fn from(value: &'a [u8]) -> Self {
            Self(if BIN_SAFE {
                Base64::encode_string(value).into()
            } else {
                String::from_utf8_lossy(value)
            })
        }
    }

    impl<const BIN_SAFE: bool> TryFrom<OwnedCowStr<BIN_SAFE>> for Bytes {
        type Error = ConvertError;

        fn try_from(value: OwnedCowStr<BIN_SAFE>) -> Result<Self, Self::Error> {
            let CowStr(s) = value;

            Ok(if BIN_SAFE {
                Base64::decode_vec(&s).map_err(|_| format!("invalid base64: {s}"))?
            } else {
                s.into_owned().into_bytes()
            }
            .into())
        }
    }

    impl<const BIN_SAFE: bool> Serialize for CowStr<'_, BIN_SAFE> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.0.serialize(serializer)
        }
    }

    impl<'de, const BIN_SAFE: bool> Deserialize<'de> for OwnedCowStr<BIN_SAFE> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            String::deserialize(deserializer).map(|s| CowStr(s.into()))
        }
    }

    pub struct Formatter<const BIN_SAFE: bool>;

    #[derive(Debug, Clone, Serialize)]
    struct SerializableSequencedRecord<'a, const BIN_SAFE: bool> {
        seq_num: u64,
        timestamp: u64,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        headers: Vec<(CowStr<'a, BIN_SAFE>, CowStr<'a, BIN_SAFE>)>,
        #[serde(skip_serializing_if = "CowStr::is_empty")]
        body: CowStr<'a, BIN_SAFE>,
    }

    impl<'a, const BIN_SAFE: bool> From<&'a SequencedRecord>
        for SerializableSequencedRecord<'a, BIN_SAFE>
    {
        fn from(value: &'a SequencedRecord) -> Self {
            let SequencedRecord {
                timestamp,
                seq_num,
                headers,
                body,
            } = value;

            let headers: Vec<(CowStr<BIN_SAFE>, CowStr<BIN_SAFE>)> = headers
                .iter()
                .map(|Header { name, value }| (name.as_ref().into(), value.as_ref().into()))
                .collect();

            let body: CowStr<BIN_SAFE> = body.as_ref().into();

            SerializableSequencedRecord {
                timestamp: *timestamp,
                seq_num: *seq_num,
                headers,
                body,
            }
        }
    }

    impl<const BIN_SAFE: bool> RecordWriter for Formatter<BIN_SAFE> {
        async fn write_record(
            record: &SequencedRecord,
            writer: &mut (impl AsyncWrite + Unpin),
        ) -> io::Result<()> {
            let record: SerializableSequencedRecord<BIN_SAFE> = record.into();
            let s = serde_json::to_string(&record).map_err(io::Error::other)?;
            writer.write_all(s.as_bytes()).await
        }
    }

    impl<const BIN_SAFE: bool, I> RecordParser<I> for Formatter<BIN_SAFE>
    where
        I: Stream<Item = io::Result<String>> + Send + Unpin,
    {
        type RecordStream = RecordStream<BIN_SAFE, I>;

        fn parse_records(lines: I) -> Self::RecordStream {
            RecordStream(lines)
        }
    }

    #[derive(Debug, Clone, Deserialize)]
    struct DeserializableAppendRecord<const BIN_SAFE: bool> {
        timestamp: Option<u64>,
        #[serde(default)]
        headers: Vec<(OwnedCowStr<BIN_SAFE>, OwnedCowStr<BIN_SAFE>)>,
        #[serde(default)]
        body: OwnedCowStr<BIN_SAFE>,
    }

    impl<const BIN_SAFE: bool> TryFrom<DeserializableAppendRecord<BIN_SAFE>> for AppendRecord {
        type Error = ConvertError;

        fn try_from(value: DeserializableAppendRecord<BIN_SAFE>) -> Result<Self, Self::Error> {
            let DeserializableAppendRecord {
                timestamp,
                headers,
                body,
            } = value;

            let parts = AppendRecordParts {
                timestamp,
                headers: headers
                    .into_iter()
                    .map(|(name, value)| {
                        Ok(Header {
                            name: name.try_into()?,
                            value: value.try_into()?,
                        })
                    })
                    .collect::<Result<Vec<_>, ConvertError>>()?,
                body: body.try_into()?,
            };

            parts.try_into()
        }
    }

    pub struct RecordStream<const BIN_SAFE: bool, S>(S);

    impl<const BIN_SAFE: bool, S> Stream for RecordStream<BIN_SAFE, S>
    where
        S: Stream<Item = io::Result<String>> + Send + Unpin,
    {
        type Item = Result<AppendRecord, RecordParseError>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            fn parse_record<const BIN_SAFE: bool>(
                s: String,
            ) -> Result<AppendRecord, RecordParseError> {
                let append_record: DeserializableAppendRecord<BIN_SAFE> = serde_json::from_str(&s)
                    .map_err(|e| RecordParseError::Convert(e.to_string().into()))?;

                Ok(append_record.try_into()?)
            }

            match self.0.poll_next_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Some(Ok(s))) => Poll::Ready(Some(parse_record::<BIN_SAFE>(s))),
            }
        }
    }
}
