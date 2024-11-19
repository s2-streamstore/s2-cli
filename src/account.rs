use streamstore::{
    client::Client,
    types::{
        BasinConfig, BasinMetadata, BasinName, CreateBasinRequest, DeleteBasinRequest,
        ListBasinsRequest, ListBasinsResponse, ReconfigureBasinRequest, StreamConfig,
    },
};

use crate::error::s2_status;

pub struct AccountService {
    client: Client,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountServiceError {
    #[error("Failed to list basins: {0}")]
    ListBasins(String),

    #[error("Failed to create basin: {0}")]
    CreateBasin(String),

    #[error("Failed to delete basin: {0}")]
    DeleteBasin(String),

    #[error("Failed to get basin config: {0}")]
    GetBasinConfig(String),

    #[error("Failed to reconfigure basin: {0}")]
    ReconfigureBasin(String),
}

impl AccountService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn list_basins(
        &self,
        prefix: String,
        start_after: String,
        limit: usize,
    ) -> Result<ListBasinsResponse, AccountServiceError> {
        let list_basins_req = ListBasinsRequest::new()
            .with_prefix(prefix)
            .with_start_after(start_after)
            .with_limit(limit);

        self.client
            .list_basins(list_basins_req)
            .await
            .map_err(|e| AccountServiceError::ListBasins(s2_status(&e)))
    }

    pub async fn create_basin(
        &self,
        basin: BasinName,
        storage_class: Option<crate::types::StorageClass>,
        retention_policy: Option<crate::types::RetentionPolicy>,
    ) -> Result<BasinMetadata, AccountServiceError> {
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
            .map_err(|e| AccountServiceError::CreateBasin(s2_status(&e)))
    }

    pub async fn delete_basin(&self, basin: BasinName) -> Result<(), AccountServiceError> {
        let delete_basin_req = DeleteBasinRequest::new(basin);
        self.client
            .delete_basin(delete_basin_req)
            .await
            .map_err(|e| AccountServiceError::DeleteBasin(s2_status(&e)))?;
        Ok(())
    }

    pub async fn get_basin_config(
        &self,
        basin: BasinName,
    ) -> Result<BasinConfig, AccountServiceError> {
        self.client
            .get_basin_config(basin)
            .await
            .map_err(|e| AccountServiceError::GetBasinConfig(s2_status(&e)))
    }

    pub async fn reconfigure_basin(
        &self,
        basin: BasinName,
        basin_config: BasinConfig,
        mask: Vec<String>,
    ) -> Result<(), AccountServiceError> {
        let reconfigure_basin_req = ReconfigureBasinRequest::new(basin)
            .with_config(basin_config)
            .with_mask(mask);
        self.client
            .reconfigure_basin(reconfigure_basin_req)
            .await
            .map_err(|e| AccountServiceError::ReconfigureBasin(s2_status(&e)))?;
        Ok(())
    }
}
