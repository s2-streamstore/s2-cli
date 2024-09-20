use s2::{
    client::BasinClient,
    service_error::{ListStreamsError, ServiceError},
    types::ListStreamsResponse,
};

pub struct BasinService {
    client: BasinClient,
}

#[derive(Debug, thiserror::Error)]
pub enum BasinServiceError {
    #[error("Failed to list streams: {0}")]
    ListStreams(#[from] ServiceError<ListStreamsError>),
}

impl BasinService {
    pub fn new(client: BasinClient) -> Self {
        Self { client }
    }

    pub async fn list_streams(
        &self,
        prefix: String,
        start_after: String,
        limit: usize,
    ) -> Result<ListStreamsResponse, BasinServiceError> {
        let list_streams_req = s2::types::ListStreamsRequest::builder()
            .prefix(prefix)
            .start_after(start_after)
            .limit(limit)
            .build();

        self.client
            .list_streams(list_streams_req)
            .await
            .map_err(BasinServiceError::ListStreams)
    }
}
