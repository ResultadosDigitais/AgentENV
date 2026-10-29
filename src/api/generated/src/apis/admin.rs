use async_trait::async_trait;
use axum::extract::*;
use axum_extra::extract::CookieJar;
use bytes::Bytes;
use headers::Host;
use http::Method;
use serde::{Deserialize, Serialize};

use crate::{models, types::*};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum AdminManagedLayersCollectPostResponse {
    /// Successfully ran one collection pass
    Status200_SuccessfullyRanOneCollectionPass(models::ManagedLayerCollectReport),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Forbidden
    Status403_Forbidden(models::Error),
    /// The pass is disabled on this node, another pass holds the lock, or enforce was requested before a report ran in this process.
    Status409_ThePassIsDisabledOnThisNode(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum NodesGetResponse {
    /// Successfully returned all nodes
    Status200_SuccessfullyReturnedAllNodes(Vec<models::Node>),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum NodesNodeIdGetResponse {
    /// Successfully returned the node
    Status200_SuccessfullyReturnedTheNode(models::NodeDetail),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

/// Admin
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Admin<E: std::fmt::Debug + Send + Sync + 'static = ()>: super::ErrorHandler<E> {
    type Claims;

    /// Collect managed layers no record cites.
    ///
    /// AdminManagedLayersCollectPost - POST /admin/managed-layers/collect
    async fn admin_managed_layers_collect_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::ManagedLayerCollectRequest,
    ) -> Result<AdminManagedLayersCollectPostResponse, E>;

    /// List nodes.
    ///
    /// NodesGet - GET /nodes
    async fn nodes_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, E>;

    /// Node info.
    ///
    /// NodesNodeIdGet - GET /nodes/{nodeID}
    async fn nodes_node_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, E>;
}
