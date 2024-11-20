use streamstore::{
    client::BasinClient,
    types::{
        CreateStreamRequest, DeleteStreamRequest, ListStreamsRequest, ListStreamsResponse,
        ReconfigureStreamRequest, StreamConfig,
    },
};

use crate::error::ServiceError;

pub struct BasinService {
    client: BasinClient,
}

impl BasinService {
    const ENTITY: &'static str = "stream";
    pub fn new(client: BasinClient) -> Self {
        Self { client }
    }

    pub async fn list_streams(
        &self,
        prefix: String,
        start_after: String,
        limit: usize,
    ) -> Result<Vec<String>, ServiceError> {
        const OPERATION: &str = "list";
        let list_streams_req = ListStreamsRequest::new()
            .with_prefix(prefix)
            .with_start_after(start_after)
            .with_limit(limit);

        let ListStreamsResponse { streams, .. } = self
            .client
            .list_streams(list_streams_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e).with_plural())?;

        Ok(streams)
    }

    pub async fn create_stream(
        &self,
        stream: String,
        config: Option<StreamConfig>,
    ) -> Result<(), ServiceError> {
        const OPERATION: &str = "create";
        let mut create_stream_req = CreateStreamRequest::new(stream);

        if let Some(config) = config {
            create_stream_req = create_stream_req.with_config(config);
        };

        self.client
            .create_stream(create_stream_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }

    pub async fn delete_stream(&self, stream: String) -> Result<(), ServiceError> {
        const OPERATION: &str = "delete";
        self.client
            .delete_stream(DeleteStreamRequest::new(stream))
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }

    pub async fn get_stream_config(&self, stream: String) -> Result<StreamConfig, ServiceError> {
        const OPERATION: &str = "get";
        self.client
            .get_stream_config(stream)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e).with_context("config"))
    }

    pub async fn reconfigure_stream(
        &self,
        stream: String,
        config: StreamConfig,
        mask: Vec<String>,
    ) -> Result<(), ServiceError> {
        const OPERATION: &str = "reconfigure";
        let reconfigure_stream_req = ReconfigureStreamRequest::new(stream)
            .with_config(config)
            .with_mask(mask);

        self.client
            .reconfigure_stream(reconfigure_stream_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }
}
