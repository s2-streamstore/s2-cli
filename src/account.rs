use streamstore::{
    client::Client,
    types::{
        BasinConfig, BasinMetadata, BasinName, CreateBasinRequest, DeleteBasinRequest,
        ListBasinsRequest, ListBasinsResponse, ReconfigureBasinRequest, StreamConfig,
    },
};

use crate::error::ServiceError;

pub struct AccountService {
    client: Client,
}

impl AccountService {
    const ENTITY: &'static str = "basin";

    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn list_basins(
        &self,
        prefix: String,
        start_after: String,
        limit: usize,
    ) -> Result<ListBasinsResponse, ServiceError> {
        const OPERATION: &str = "list";

        let list_basins_req = ListBasinsRequest::new()
            .with_prefix(prefix)
            .with_start_after(start_after)
            .with_limit(limit);

        self.client
            .list_basins(list_basins_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e).with_plural())
    }

    pub async fn create_basin(
        &self,
        basin: BasinName,
        storage_class: Option<crate::types::StorageClass>,
        retention_policy: Option<crate::types::RetentionPolicy>,
    ) -> Result<BasinMetadata, ServiceError> {
        const OPERATION: &str = "create";

        let mut stream_config = StreamConfig::new();

        if let Some(storage_class) = storage_class {
            stream_config = stream_config.with_storage_class(storage_class);
        }

        if let Some(retention_policy) = retention_policy {
            stream_config = stream_config.with_retention_policy(retention_policy.into());
        }

        let create_basin_req = CreateBasinRequest::new(basin)
            .with_config(BasinConfig::with_default_stream_config(stream_config));

        self.client
            .create_basin(create_basin_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }

    pub async fn delete_basin(&self, basin: BasinName) -> Result<(), ServiceError> {
        const OPERATION: &str = "delete";

        let delete_basin_req = DeleteBasinRequest::new(basin);
        self.client
            .delete_basin(delete_basin_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }

    pub async fn get_basin_config(&self, basin: BasinName) -> Result<BasinConfig, ServiceError> {
        const OPERATION: &str = "get";

        self.client
            .get_basin_config(basin)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e).with_context("config"))
    }

    pub async fn reconfigure_basin(
        &self,
        basin: BasinName,
        basin_config: BasinConfig,
        mask: Vec<String>,
    ) -> Result<(), ServiceError> {
        const OPERATION: &str = "reconfigure";
        let reconfigure_basin_req = ReconfigureBasinRequest::new(basin)
            .with_config(basin_config)
            .with_mask(mask);
        self.client
            .reconfigure_basin(reconfigure_basin_req)
            .await
            .map_err(|e| ServiceError::new(Self::ENTITY, OPERATION, e))
    }
}
