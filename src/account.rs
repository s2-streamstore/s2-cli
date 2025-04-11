use crate::{
    error::{ServiceError, ServiceErrorContext, ServiceStatus},
    types::{PermittedOperationGroups, ResourceSet},
};
use async_stream::stream;
use futures::Stream;
use s2::{
    client::Client,
    types::{
        AccessTokenId, AccessTokenInfo, BasinConfig, BasinInfo, BasinName, CreateBasinRequest,
        DeleteBasinRequest, ListAccessTokensRequest, ListAccessTokensResponse, ListBasinsRequest,
        ListBasinsResponse, Operation, ReconfigureBasinRequest, StreamConfig,
    },
};

pub struct AccountService {
    client: Client,
}

impl AccountService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn list_basins(
        &self,
        prefix: String,
        mut start_after: String,
        mut limit: Option<usize>,
        no_auto_paginate: bool,
    ) -> impl Stream<Item = Result<ListBasinsResponse, ServiceError>> + '_ {
        stream! {
            loop {
                let resp = self
                    .list_basins_internal(prefix.to_owned(), start_after.to_string(), limit.map(|l| l.min(1000)))
                    .await;

                match resp.as_ref() {
                    Ok(ListBasinsResponse { basins, has_more }) if *has_more && !no_auto_paginate => {
                            start_after = basins
                                .last()
                                .map(|s| s.name.clone())
                                .ok_or(ServiceError::new(ServiceErrorContext::ListBasins, ServiceStatus::default()))?;
                            if let Some(l) = limit {
                                if l > basins.len() {
                                    limit = Some(l - basins.len());
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
    async fn list_basins_internal(
        &self,
        prefix: String,
        start_after: String,
        limit: Option<usize>,
    ) -> Result<ListBasinsResponse, ServiceError> {
        let list_basins_req = ListBasinsRequest::new()
            .with_prefix(prefix)
            .with_start_after(start_after)
            .with_limit(limit);

        self.client
            .list_basins(list_basins_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ListBasins, e))
    }

    pub async fn create_basin(
        &self,
        basin: BasinName,
        storage_class: Option<crate::types::StorageClass>,
        retention_policy: Option<crate::types::RetentionPolicy>,
        create_stream_on_append: bool,
    ) -> Result<BasinInfo, ServiceError> {
        let mut stream_config = StreamConfig::new();

        if let Some(storage_class) = storage_class {
            stream_config = stream_config.with_storage_class(storage_class);
        }

        if let Some(retention_policy) = retention_policy {
            stream_config = stream_config.with_retention_policy(retention_policy.into());
        }

        let basin_config = BasinConfig {
            default_stream_config: Some(stream_config),
            create_stream_on_append,
        };

        let create_basin_req = CreateBasinRequest::new(basin).with_config(basin_config);

        self.client
            .create_basin(create_basin_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::CreateBasin, e))
    }

    pub async fn delete_basin(&self, basin: BasinName) -> Result<(), ServiceError> {
        let delete_basin_req = DeleteBasinRequest::new(basin);
        self.client
            .delete_basin(delete_basin_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::DeleteBasin, e))
    }

    pub async fn get_basin_config(&self, basin: BasinName) -> Result<BasinConfig, ServiceError> {
        self.client
            .get_basin_config(basin)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::GetBasinConfig, e))
    }

    pub async fn reconfigure_basin(
        &self,
        basin: BasinName,
        basin_config: BasinConfig,
        mask: Vec<String>,
    ) -> Result<BasinConfig, ServiceError> {
        let reconfigure_basin_req = ReconfigureBasinRequest::new(basin)
            .with_config(basin_config)
            .with_mask(mask);
        self.client
            .reconfigure_basin(reconfigure_basin_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ReconfigureBasin, e))
    }

    pub async fn issue_access_token(
        &self,
        id: AccessTokenId,
        expires_at: Option<u32>,
        auto_prefix_streams: bool,
        basins: Option<ResourceSet<8, 48>>,
        streams: Option<ResourceSet<1, 512>>,
        tokens: Option<ResourceSet<1, 50>>,
        op_groups: Option<PermittedOperationGroups>,
        ops: Vec<Operation>,
    ) -> Result<String, ServiceError> {
        let mut access_token_scope = s2::types::AccessTokenScope::new().with_ops(ops);
        if let Some(basins) = basins {
            access_token_scope = access_token_scope.with_basins(basins.into());
        }
        if let Some(streams) = streams {
            access_token_scope = access_token_scope.with_streams(streams.into());
        }
        if let Some(tokens) = tokens {
            access_token_scope = access_token_scope.with_tokens(tokens.into());
        }
        if let Some(op_groups) = op_groups {
            access_token_scope = access_token_scope.with_op_groups(op_groups.into());
        }
        let mut access_token_info = s2::types::AccessTokenInfo::new(id)
            .with_auto_prefix_streams(auto_prefix_streams)
            .with_scope(access_token_scope);

        if let Some(expires_at) = expires_at {
            access_token_info = access_token_info.with_expires_at(expires_at);
        }

        self.client
            .issue_access_token(access_token_info)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::IssueAccessToken, e))
    }

    pub async fn revoke_access_token(
        &self,
        id: AccessTokenId,
    ) -> Result<AccessTokenInfo, ServiceError> {
        self.client
            .revoke_access_token(id)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::RevokeAccessToken, e))
    }

    pub fn list_access_tokens(
        &self,
        prefix: String,
        mut start_after: String,
        mut limit: Option<usize>,
        no_auto_paginate: bool,
    ) -> impl Stream<Item = Result<ListAccessTokensResponse, ServiceError>> + '_ {
        stream! {
            loop {
                let resp = self
                    .list_access_tokens_internal(prefix.to_owned(), start_after.to_string(), limit.map(|l| l.min(1000)))
                    .await;

                match resp.as_ref() {
                    Ok(ListAccessTokensResponse { tokens, has_more }) if *has_more && !no_auto_paginate => {
                            start_after = tokens
                                .last()
                                .map(|s| s.id.clone().into())
                                .ok_or(ServiceError::new(ServiceErrorContext::ListAccessTokens, ServiceStatus::default()))?;
                            if let Some(l) = limit {
                                if l > tokens.len() {
                                    limit = Some(l - tokens.len());
                                } else {
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

    async fn list_access_tokens_internal(
        &self,
        prefix: String,
        start_after: String,
        limit: Option<usize>,
    ) -> Result<ListAccessTokensResponse, ServiceError> {
        let list_access_tokens_req = ListAccessTokensRequest::new()
            .with_prefix(prefix)
            .with_start_after(start_after)
            .with_limit(limit);

        self.client
            .list_access_tokens(list_access_tokens_req)
            .await
            .map_err(|e| ServiceError::new(ServiceErrorContext::ListAccessTokens, e))
    }
}
