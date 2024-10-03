use s2::{
    client::StreamClient,
    service_error::{AppendError, GetNextSeqNumError, ServiceError},
    types::{AppendInput, AppendRecord},
};

pub struct StreamService {
    client: StreamClient,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamServiceError {
    #[error("Failed to get next sequence number")]
    GetNextSeqNumError(#[from] ServiceError<GetNextSeqNumError>),

    #[error("Failed to append records")]
    AppendError(#[from] ServiceError<AppendError>),
}

impl StreamService {
    pub fn new(client: StreamClient) -> Self {
        Self { client }
    }

    pub async fn get_next_seq_num(&self) -> Result<u64, StreamServiceError> {
        Ok(self.client.get_next_seq_num().await?)
    }

    pub async fn append(
        &self,
        records: Vec<AppendRecord>,
        match_seq_num: Option<u64>,
        fencing_token: Option<String>,
    ) -> Result<(), StreamServiceError> {    
        let mut append_req = AppendInput::new(records);            

        if let Some(seq_num) = match_seq_num {
            append_req = append_req.with_match_seq_num(seq_num);
        }

        if let Some(token) = fencing_token {
            append_req = append_req.with_fencing_token(token);
        }

        self.client.append(append_req).await?;

        Ok(())
    }
}