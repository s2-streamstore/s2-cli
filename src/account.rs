use s2::{
    client::Client,
    service_error::{CreateBasinError, ServiceError},
    types::{
        BasinConfig, CreateBasinResponse, ListBasinsResponse, RetentionPolicy, StorageClass,
        StreamConfig,
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
}

impl AccountService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn list_basins(
        &self,
        prefix: String,
        start_after: String,
        limit: u32,
    ) -> Result<ListBasinsResponse, AccountServiceError> {
        let list_basins_req = s2::types::ListBasinsRequest {
            prefix,
            start_after,
            limit,
        };

        self.client
            .list_basins(list_basins_req)
            .await
            .map_err(|e| AccountServiceError::ListBasins(e.to_string()))
    }

    pub async fn create_basin(
        &self,
        name: String,
        storage_class: Option<StorageClass>,
        retention_policy: Option<humantime::Duration>,
    ) -> Result<CreateBasinResponse, AccountServiceError> {
        let basin_config = match (&storage_class, retention_policy) {
            (Some(storage_class), Some(retention_policy)) => {
                let stream_config = StreamConfig::builder()
                    .storage_class(*storage_class)
                    .retention_policy(RetentionPolicy::Age(*retention_policy))
                    .build();

                let basin_config = BasinConfig::builder()
                    .default_stream_config(Some(stream_config))
                    .build();

                Some(basin_config)
            }
            _ => None,
        };

        let create_basin_req = s2::types::CreateBasinRequest::builder()
            .basin(name)
            .config(basin_config)
            .build();

        self.client
            .create_basin(create_basin_req)
            .await
            .map_err(AccountServiceError::CreateBasin)
    }
}
