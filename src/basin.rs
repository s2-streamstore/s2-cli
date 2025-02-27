use async_stream::stream;
use futures::Stream;
use s2::{
    client::BasinClient,
    types::{
        CreateStreamRequest, DeleteStreamRequest, ListStreamsRequest, ListStreamsResponse,
        ReconfigureStreamRequest, StreamConfig, StreamInfo,
    },
};

use crate::error::{ServiceError, ServiceErrorContext, ServiceStatus};

pub struct BasinService {
    client: BasinClient,
}

impl BasinService {
    pub fn new(client: BasinClient) -> Self {
        Self { client }
    }

    pub fn list_streams(
        &self,
        prefix: String,
        mut start_after: String,
        mut limit: Option<usize>,
        no_auto_paginate: bool,
    ) -> impl Stream<Item = Result<ListStreamsResponse, ServiceError>> + '_ {
        stream! {
            loop {
                let resp = self
                    .list_streams_internal(prefix.to_owned(), start_after.to_string(), limit.map(|l| l.min(1000)))
                    .await;

                match resp.as_ref() {
                    Ok(ListStreamsResponse { streams, has_more}) if *has_more && !no_auto_paginate => {
                            start_after = streams
                                .last()
                                .map(|s| s.name.clone())
                                .ok_or(ServiceError::new(ServiceErrorContext::ListStreams, ServiceStatus::default()))?;
                            if let Some(l) = limit {
                                if l > streams.len() {
                                    limit = Some(l - streams.len());
                                } else {
                                    // Limit has been exhausted.
                                    return yield resp;
                                }
                            }
                            yield resp;
                    },
                    _ => {
                       return yield resp;
                    }

                }
            }
        }
    }

    async fn list_streams_internal(
        &self,
        prefix: String,
        start_after: String,
        limit: Option<usize>,
    ) -> Result<ListStreamsResponse, ServiceError> {
        self.client
            .list_streams(
                ListStreamsRequest::new()
                    .with_prefix(prefix)
                    .with_start_after(start_after)
                    .with_limit(limit),
            )
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ListStreams, e))
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
