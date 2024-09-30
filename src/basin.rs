use s2::{
    client::BasinClient,
    service_error::{
        CreateStreamError, DeleteStreamError, GetStreamConfigError, ListStreamsError,
        ReconfigureStreamError, ServiceError,
    },
    types::{GetStreamConfigResponse, ListStreamsResponse, StreamConfig},
};

pub struct BasinService {
    client: BasinClient,
}

#[derive(Debug, thiserror::Error)]
pub enum BasinServiceError {
    #[error("Failed to list streams: {0}")]
    ListStreams(#[from] ServiceError<ListStreamsError>),

    #[error("Failed to create stream")]
    CreateStream(#[from] ServiceError<CreateStreamError>),

    #[error("Failed to delete stream")]
    DeleteStream(#[from] ServiceError<DeleteStreamError>),

    #[error("Failed to get stream config")]
    GetStreamConfig(#[from] ServiceError<GetStreamConfigError>),

    #[error("Failed to reconfigure stream")]
    ReconfigureStream(#[from] ServiceError<ReconfigureStreamError>),
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
        let list_streams_req = s2::types::ListStreamsRequest::builder()
            .prefix(prefix)
            .start_after(start_after)
            .limit(limit)
            .build();

        let ListStreamsResponse { streams, .. } =
            self.client.list_streams(list_streams_req).await?;

        Ok(streams)
    }

    pub async fn create_stream(
        &self,
        stream_name: String,
        config: Option<StreamConfig>,
    ) -> Result<(), BasinServiceError> {
        let create_stream_req = s2::types::CreateStreamRequest::builder()
            .stream(stream_name)
            .config(config)
            .build();

        self.client.create_stream(create_stream_req).await?;

        Ok(())
    }

    pub async fn delete_stream(&self, stream_name: String) -> Result<(), BasinServiceError> {
        let delete_stream_req = s2::types::DeleteStreamRequest::builder()
            .stream(stream_name)
            .build();

        self.client.delete_stream(delete_stream_req).await?;

        Ok(())
    }

    pub async fn get_stream_config(
        &self,
        stream: String,
    ) -> Result<StreamConfig, BasinServiceError> {
        let get_stream_config_req = s2::types::GetStreamConfigRequest::builder()
            .stream(stream)
            .build();

        let GetStreamConfigResponse { config } =
            self.client.get_stream_config(get_stream_config_req).await?;
        Ok(config)
    }

    pub async fn reconfigure_stream(
        &self,
        stream: String,
        config: StreamConfig,
        mask: Vec<String>,
    ) -> Result<(), BasinServiceError> {
        let reconfigure_stream_req = s2::types::ReconfigureStreamRequest::builder()
            .stream(stream)
            .config(config)
            .mask(mask)
            .build();

        self.client
            .reconfigure_stream(reconfigure_stream_req)
            .await?;

        Ok(())
    }
}
