
use std::collections::HashMap;

use crate::db::errors::Result;

#[async_trait::async_trait]
pub trait Repository {
    type CreateRequest;

    type UpdateRequest;

    type Response;

    type Created;

    type Id: Send + Sync;

    type Filter: Send + Sync;

    async fn create(&mut self, request: &Self::CreateRequest) -> Result<Self::Created>;

    async fn get_by_id(&mut self, id: Self::Id) -> Result<Option<Self::Response>>;

    async fn get_bulk(&mut self, ids: Vec<Self::Id>) -> Result<HashMap<Self::Id, Self::Response>>;

    async fn list(&mut self, filter: &Self::Filter) -> Result<Vec<Self::Response>>;

    async fn delete(&mut self, id: Self::Id) -> Result<bool>;

    async fn update(&mut self, id: Self::Id, request: &Self::UpdateRequest) -> Result<Self::Response>;
}
