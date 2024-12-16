use streamstore::{
    client::BasinClient,
    types::{
        CreateStreamRequest, DeleteStreamRequest, ListStreamsRequest, ListStreamsResponse,
        ReconfigureStreamRequest, StreamConfig, StreamInfo,
    },
};

use crate::error::{ServiceError, ServiceErrorContext};

pub struct BasinService {
    client: BasinClient,
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
    ) -> Result<Vec<StreamInfo>, ServiceError> {
        let list_streams_req = ListStreamsRequest::new()
            .with_prefix(prefix)
            .with_start_after(start_after)
            .with_limit(limit);

        let ListStreamsResponse { streams, .. } = self
            .client
            .list_streams(list_streams_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ListStreams, e))?;

        Ok(streams)
    }

    pub async fn create_stream(
        &self,
        stream: String,
        config: Option<StreamConfig>,
    ) -> Result<StreamInfo, ServiceError> {
        let mut create_stream_req = CreateStreamRequest::new(stream);

        if let Some(config) = config {
            create_stream_req = create_stream_req.with_config(config);
        };

        self.client
            .create_stream(create_stream_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::CreateStream, e))
    }

    pub async fn delete_stream(&self, stream: String) -> Result<(), ServiceError> {
        self.client
            .delete_stream(DeleteStreamRequest::new(stream))
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::DeleteStream, e))
    }

    pub async fn get_stream_config(&self, stream: String) -> Result<StreamConfig, ServiceError> {
        self.client
            .get_stream_config(stream)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::GetStreamConfig, e))
    }

    pub async fn reconfigure_stream(
        &self,
        stream: String,
        config: StreamConfig,
        mask: Vec<String>,
    ) -> Result<StreamConfig, ServiceError> {
        let reconfigure_stream_req = ReconfigureStreamRequest::new(stream)
            .with_config(config)
            .with_mask(mask);

        self.client
            .reconfigure_stream(reconfigure_stream_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ReconfigureStream, e))
    }
}
