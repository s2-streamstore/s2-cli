use streamstore::{
    client::BasinClient,
    types::{
        CreateStreamRequest, DeleteStreamRequest, ListStreamsRequest, ListStreamsResponse,
        ReconfigureStreamRequest, StreamConfig,
    },
};

use crate::error::s2_status;

pub struct BasinService {
    client: BasinClient,
}

#[derive(Debug, thiserror::Error)]
pub enum BasinServiceError {
    #[error("Failed to list streams: {0}")]
    ListStreams(String),

    #[error("Failed to create stream: {0}")]
    CreateStream(String),

    #[error("Failed to delete stream: {0}")]
    DeleteStream(String),

    #[error("Failed to get stream config: {0}")]
    GetStreamConfig(String),

    #[error("Failed to reconfigure stream: {0}")]
    ReconfigureStream(String),
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
    ) -> Result<Vec<String>, BasinServiceError> {
        let list_streams_req = ListStreamsRequest::new()
            .with_prefix(prefix)
            .with_start_after(start_after)
            .with_limit(limit);

        let ListStreamsResponse { streams, .. } = self
            .client
            .list_streams(list_streams_req)
            .await
            .map_err(|e| BasinServiceError::ListStreams(s2_status(&e)))?;

        Ok(streams)
    }

    pub async fn create_stream(
        &self,
        stream: String,
        config: Option<StreamConfig>,
    ) -> Result<(), BasinServiceError> {
        let mut create_stream_req = CreateStreamRequest::new(stream);

        if let Some(config) = config {
            create_stream_req = create_stream_req.with_config(config);
        };

        self.client
            .create_stream(create_stream_req)
            .await
            .map_err(|e| BasinServiceError::CreateStream(s2_status(&e)))?;
        Ok(())
    }

    pub async fn delete_stream(&self, stream: String) -> Result<(), BasinServiceError> {
        self.client
            .delete_stream(DeleteStreamRequest::new(stream))
            .await
            .map_err(|e| BasinServiceError::DeleteStream(s2_status(&e)))?;
        Ok(())
    }

    pub async fn get_stream_config(
        &self,
        stream: String,
    ) -> Result<StreamConfig, BasinServiceError> {
        Ok(self
            .client
            .get_stream_config(stream)
            .await
            .map_err(|e| BasinServiceError::GetStreamConfig(s2_status(&e)))?)
    }

    pub async fn reconfigure_stream(
        &self,
        stream: String,
        config: StreamConfig,
        mask: Vec<String>,
    ) -> Result<(), BasinServiceError> {
        let reconfigure_stream_req = ReconfigureStreamRequest::new(stream)
            .with_config(config)
            .with_mask(mask);

        self.client
            .reconfigure_stream(reconfigure_stream_req)
            .await
            .map_err(|e| BasinServiceError::ReconfigureStream(s2_status(&e)))?;
        Ok(())
    }
}
