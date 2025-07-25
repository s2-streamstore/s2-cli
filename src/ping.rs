use std::time::Duration;

use rand::Rng;
use s2::{
    batching::AppendRecordsBatchingOpts,
    types::{
        AppendAck, AppendRecord, ReadLimit, ReadOutput, ReadStart, SequencedRecord,
        SequencedRecordBatch,
    },
};
use tokio::{join, select, signal, sync::mpsc, task::JoinHandle, time::Instant};
use tokio_stream::StreamExt;

use crate::{
    error::{S2CliError, ServiceError, ServiceErrorContext},
    stream::StreamService,
};

pub struct PingResult {
    pub bytes: u64,
    pub ack: Duration,
    pub e2e: Duration,
}

pub struct Pinger {
    records_tx: mpsc::UnboundedSender<AppendRecord>,
    appends_handle: JoinHandle<()>,
    reads_handle: JoinHandle<()>,
    appends_rx: mpsc::UnboundedReceiver<Result<Instant, S2CliError>>,
    reads_rx: mpsc::UnboundedReceiver<Result<(Instant, SequencedRecord), S2CliError>>,
}

impl Pinger {
    pub async fn init(stream_client: &StreamService) -> Result<Self, S2CliError> {
        let tail = stream_client.check_tail().await?;

        let mut read_stream = stream_client
            .read_session(
                ReadStart::SeqNum(tail.seq_num),
                ReadLimit::default(),
                None,
                false,
            )
            .await?;

        let (records_tx, records_rx) = mpsc::unbounded_channel();
        let mut append_stream = stream_client
            .append_session(
                tokio_stream::wrappers::UnboundedReceiverStream::new(records_rx),
                AppendRecordsBatchingOpts::new()
                    .with_max_batch_records(1)
                    .with_match_seq_num(Some(tail.seq_num)),
            )
            .await?;

        let warmup_record = AppendRecord::new("warmup").expect("valid record");
        records_tx
            .send(warmup_record.clone())
            .expect("stream channel open");

        match append_stream.next().await.expect("warmup batch ack") {
            Ok(AppendAck { start, .. }) if start.seq_num == tail.seq_num => (),
            Ok(_) => return Err(S2CliError::PingStreamMutated),
            Err(e) => return Err(ServiceError::new(ServiceErrorContext::AppendSession, e).into()),
        };

        match read_stream.next().await.expect("warmup batch e2e") {
            Ok(ReadOutput::Batch(SequencedRecordBatch { records }))
                if records.len() == 1
                    && records[0].headers.is_empty()
                    && records[0].body.as_ref() == warmup_record.body() => {}
            Ok(_) => return Err(S2CliError::PingStreamMutated),
            Err(e) => return Err(ServiceError::new(ServiceErrorContext::ReadSession, e).into()),
        };

        let (reads_tx, reads_rx) = mpsc::unbounded_channel();
        let reads_handle = tokio::spawn(async move {
            loop {
                select! {
                    next = read_stream.next() => match next {
                        Some(Err(e)) => {
                            reads_tx.send(Err(
                                ServiceError::new(ServiceErrorContext::ReadSession, e).into()
                            )).expect("open reads channel");
                            return;
                        }
                        Some(Ok(output)) => {
                            if let ReadOutput::Batch(SequencedRecordBatch { mut records }) = output {
                                let read = Instant::now();
                                if records.len() != 1 {
                                    reads_tx.send(Err(
                                        S2CliError::PingStreamMutated
                                    )).expect("reads channel open");
                                    return;
                                }
                                let record = records.pop().expect("pre validated length");
                                reads_tx.send(Ok((read, record))).expect("reads channel open");
                            } else {
                                reads_tx.send(Err(
                                    S2CliError::PingStreamMutated
                                )).expect("reads channel open");
                                return;
                            }
                        }
                        None => break,
                    },
                    _ = signal::ctrl_c() => break,
                };
            }
        });

        let (appends_tx, appends_rx) = mpsc::unbounded_channel();
        let appends_handle = tokio::spawn(async move {
            while let Some(next) = append_stream.next().await {
                match next {
                    Ok(AppendAck { start, end, .. }) => {
                        let append = Instant::now();
                        let records = end.seq_num - start.seq_num;
                        if records != 1 {
                            appends_tx
                                .send(Err(S2CliError::PingStreamMutated))
                                .expect("appends channel open");
                            return;
                        }
                        appends_tx.send(Ok(append)).expect("appends channel open");
                    }
                    Err(e) => {
                        appends_tx
                            .send(Err(S2CliError::from(ServiceError::new(
                                ServiceErrorContext::AppendSession,
                                e,
                            ))))
                            .expect("appends channel open");
                    }
                }
            }
        });

        Ok(Self {
            records_tx,
            appends_handle,
            reads_handle,
            appends_rx,
            reads_rx,
        })
    }

    pub async fn ping(&mut self, bytes: u64) -> Result<Option<PingResult>, S2CliError> {
        let body = rand::rng()
            .sample_iter(
                rand::distr::Uniform::new_inclusive(0, u8::MAX).expect("valid distribution"),
            )
            .take(bytes as usize)
            .collect::<Vec<_>>();

        let record = AppendRecord::new(body.clone()).expect("pre validated append record bytes");

        self.records_tx.send(record).expect("stream channel open");

        let send = Instant::now();

        let (append, read, record) = match join!(self.appends_rx.recv(), self.reads_rx.recv()) {
            (None, _) | (_, None) => return Ok(None),
            (Some(Err(e)), _) | (_, Some(Err(e))) => return Err(e),
            (Some(Ok(append)), Some(Ok((read, record)))) => (append, read, record),
        };

        // Validate the received record
        if body != record.body || !record.headers.is_empty() {
            return Err(S2CliError::PingStreamMutated);
        }

        Ok(Some(PingResult {
            bytes,
            ack: append - send,
            e2e: read - send,
        }))
    }
}

impl Drop for Pinger {
    fn drop(&mut self) {
        self.appends_handle.abort();
        self.reads_handle.abort();
    }
}

pub struct LatencyStats {
    pub min: Duration,
    pub median: Duration,
    pub p90: Duration,
    pub p99: Duration,
    pub max: Duration,
}

impl LatencyStats {
    pub fn generate(mut data: Vec<Duration>) -> Self {
        data.sort_unstable();

        let n = data.len();

        if n == 0 {
            return Self {
                min: Duration::ZERO,
                median: Duration::ZERO,
                p90: Duration::ZERO,
                p99: Duration::ZERO,
                max: Duration::ZERO,
            };
        }

        let median = if n % 2 == 0 {
            (data[n / 2 - 1] + data[n / 2]) / 2
        } else {
            data[n / 2]
        };

        let p_idx = |p: f64| ((n as f64) * p).ceil() as usize - 1;

        Self {
            min: data[0],
            median,
            p90: data[p_idx(0.90)],
            p99: data[p_idx(0.99)],
            max: data[n - 1],
        }
    }

    pub fn into_vec(self) -> Vec<(String, Duration)> {
        vec![
            ("min".to_owned(), self.min),
            ("median".to_owned(), self.median),
            ("p90".to_owned(), self.p90),
            ("p99".to_owned(), self.p99),
            ("max".to_owned(), self.max),
        ]
    }
}
