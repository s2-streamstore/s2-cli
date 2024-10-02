use s2::{
    client::Client,
    service_error::{
        CreateBasinError, DeleteBasinError, GetBasinConfigError, ReconfigureBasinError,
        ServiceError,
    },
    types::{
        BasinConfig, BasinMetadata, CreateBasinRequest, DeleteBasinRequest, ListBasinsRequest,
        ListBasinsResponse, ReconfigureBasinRequest, RetentionPolicy, StorageClass, StreamConfig,
    },
};

pub struct AccountService {
    client: Client,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountServiceError {
    #[error("Failed to list basins: {0}")]
    ListBasins(String),

    #[error("Failed to create basin")]
    CreateBasin(#[from] ServiceError<CreateBasinError>),

    #[error("Failed to delete basin")]
    DeleteBasin(#[from] ServiceError<DeleteBasinError>),

    #[error("Failed to get basin config")]
    GetBasinConfig(#[from] ServiceError<GetBasinConfigError>),

    #[error("Failed to reconfigure basin")]
    ReconfigureBasin(#[from] ServiceError<ReconfigureBasinError>),
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
            .map_err(|e| AccountServiceError::ListBasins(e.to_string()))
    }

    pub async fn create_basin(
        &self,
        basin: String,
        storage_class: Option<StorageClass>,
        retention_policy: Option<humantime::Duration>,
    ) -> Result<BasinMetadata, AccountServiceError> {
        let basin_config = match (&storage_class, retention_policy) {
            (Some(storage_class), Some(retention_policy)) => {
                let stream_config = StreamConfig::new()
                    .with_storage_class(*storage_class)
                    .with_retention_policy(RetentionPolicy::Age(*retention_policy));

                let basin_config = BasinConfig::with_default_stream_config(stream_config);

                Some(basin_config)
            }
            _ => None,
        };

        let mut create_basin_req = CreateBasinRequest::new(basin);

        if let Some(basin_config) = basin_config {
            create_basin_req = create_basin_req.with_config(basin_config)
        };

        self.client
            .create_basin(create_basin_req)
            .await
            .map_err(AccountServiceError::CreateBasin)
    }

    pub async fn delete_basin(&self, basin: String) -> Result<(), AccountServiceError> {
        let delete_basin_req = DeleteBasinRequest::new(basin);
        self.client.delete_basin(delete_basin_req).await?;
        Ok(())
    }

    pub async fn get_basin_config(
        &self,
        basin: String,
    ) -> Result<BasinConfig, AccountServiceError> {
        Ok(self.client.get_basin_config(basin).await?)
    }

    pub async fn reconfigure_basin(
        &self,
        basin: String,
        basin_config: BasinConfig,
        mask: Vec<String>,
    ) -> Result<(), AccountServiceError> {
        let reconfigure_basin_req = ReconfigureBasinRequest::new(basin)
            .with_config(basin_config)
            .with_mask(mask);
        self.client.reconfigure_basin(reconfigure_basin_req).await?;
        Ok(())
    }
}
