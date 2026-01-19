use bytes::Bytes;
use rand::{RngCore, SeedableRng};
use s2_sdk::types::{AppendRecord, Header, RECORD_BATCH_MAX, SequencedRecord, ValidationError};

pub const TPUT_SEQ_HEADER_NAME: &[u8] = b"tput-seq";
const TPUT_SEQ_HEADER_VALUE_LEN: usize = 8;
const TPUT_HEADER_COUNT: usize = 1;
const TPUT_RECORD_BASE_OVERHEAD: usize = 8;
const TPUT_RECORD_OVERHEAD_BYTES: usize = TPUT_RECORD_BASE_OVERHEAD
    + 2 * TPUT_HEADER_COUNT
    + TPUT_SEQ_HEADER_NAME.len()
    + TPUT_SEQ_HEADER_VALUE_LEN;
pub const TPUT_MAX_RECORD_BYTES: u32 = (RECORD_BATCH_MAX.bytes - TPUT_RECORD_OVERHEAD_BYTES) as u32;

pub fn body(record_bytes: u32) -> Bytes {
    // Deterministic payload so the reader can verify data integrity.
    let mut body = vec![0u8; record_bytes as usize];
    let mut rng = rand::rngs::StdRng::seed_from_u64(u64::from(record_bytes));
    rng.fill_bytes(&mut body);
    Bytes::from(body)
}

pub fn record(body: &Bytes, seq: u64) -> Result<AppendRecord, ValidationError> {
    let header = Header::new(TPUT_SEQ_HEADER_NAME, seq.to_be_bytes().to_vec());
    AppendRecord::new(body.clone()).and_then(|record| record.with_headers([header]))
}

pub fn record_seq(record: &SequencedRecord) -> Result<u64, String> {
    let header = record
        .headers
        .iter()
        .find(|h| h.name.as_ref() == TPUT_SEQ_HEADER_NAME)
        .ok_or_else(|| "missing tput sequence header".to_string())?;
    let value = header.value.as_ref();
    if value.len() != TPUT_SEQ_HEADER_VALUE_LEN {
        return Err(format!(
            "invalid tput sequence header length: {}",
            value.len()
        ));
    }
    Ok(u64::from_be_bytes(
        value.try_into().expect("length checked"),
    ))
}
