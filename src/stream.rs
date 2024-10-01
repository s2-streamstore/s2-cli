use s2::{
    client::StreamClient,
    service_error::{GetNextSeqNumError, ServiceError},
};

pub struct StreamService {
    client: StreamClient,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamServiceError {
    #[error("Failed to get next sequence number")]
    GetNextSeqNumError(#[from] ServiceError<GetNextSeqNumError>),
}

impl StreamService {
    pub fn new(client: StreamClient) -> Self {
        Self { client }
    }

    pub async fn get_next_seq_num(&self) -> Result<u64, StreamServiceError> {
        Ok(self.client.get_next_seq_num().await?)
    }
}
