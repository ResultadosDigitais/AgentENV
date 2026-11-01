//! The cluster's sandboxes, read from the gateway's aggregated list.
//!
//! The collector runs on one node, but a sandbox paused on another node still
//! reads its source snapshot's layers. The gateway fans `GET /v2/sandboxes` out
//! to every node in the scheduler and merges the answers
//! (`services/gateway/internal/cluster_list.go`), so its list is the only view
//! that covers the cluster.
//!
//! # Fail closed
//!
//! A partial list is worse than no list: with one node missing, every sandbox
//! that node holds looks gone, and their layers look uncited. The gateway
//! answers `502` when any node in the fan-out fails, and this reader turns every
//! such answer, every transport error, every unparsable page and every repeated
//! page token into an error. The collector then downgrades the whole pass to a
//! report, which deletes nothing.

use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;
use serde::Deserialize;

use super::live_sandboxes::ClusterSandboxList;
use crate::api_key::{ApiKey, API_KEY_HEADER};

/// Where the aggregated list lives. The path is the gateway's paginated
/// endpoint, which fans out to every node.
const CLUSTER_LIST_PATH: &str = "/v2/sandboxes";

/// Page size asked of the gateway. Large enough that a normal cluster answers
/// in one page, small enough that a huge one still streams.
const PAGE_LIMIT: u32 = 500;

/// A pass a runaway cursor cannot make endless. Hitting it is an error, not a
/// truncated list: the caller must never read a short list as complete.
const MAX_PAGES: usize = 1_000;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One sandbox as the aggregated list reports it.
///
/// Only the source snapshot is read. `templateID` carries the node's
/// `metadata.snapshot_id` for a sandbox launched from a snapshot, which is the
/// same field the node's own list reports; the rest of the object is ignored.
#[derive(Deserialize)]
struct ListedSandbox {
    #[serde(rename = "templateID")]
    template_id: String,
}

/// Reads every sandbox in the cluster through the gateway.
pub struct GatewaySandboxList {
    endpoint: String,
    client: reqwest::Client,
    api_key: ApiKey,
}

impl GatewaySandboxList {
    /// Builds a reader over `base_url`, authenticating with the API key this
    /// node already holds: the aggregated list is the same authenticated node
    /// API, fanned out.
    pub fn new(base_url: &str, api_key: ApiKey) -> anyhow::Result<Self> {
        let endpoint = base_url.trim().trim_end_matches('/');
        if endpoint.is_empty() {
            bail!("the cluster gateway base URL is empty");
        }
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            bail!("the cluster gateway base URL must start with http:// or https://: {endpoint}");
        }
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("build the cluster gateway client")?;
        Ok(Self {
            endpoint: endpoint.to_owned(),
            client,
            api_key,
        })
    }

    /// One page of the aggregated list, and the token for the next one.
    async fn page(&self, next_token: Option<&str>) -> anyhow::Result<(Vec<ListedSandbox>, Option<String>)> {
        let mut request = self
            .client
            .get(format!("{}{CLUSTER_LIST_PATH}", self.endpoint))
            .query(&[("limit", PAGE_LIMIT.to_string())])
            .header(API_KEY_HEADER, self.api_key.expose());
        if let Some(token) = next_token {
            request = request.query(&[("nextToken", token)]);
        }
        let response = request
            .send()
            .await
            .context("the cluster gateway is unreachable")?;
        let status = response.status();
        let next = response
            .headers()
            .get("x-next-token")
            .and_then(|value| value.to_str().ok())
            .filter(|token| !token.is_empty())
            .map(str::to_owned);
        if !status.is_success() {
            // The gateway answers 502 when any node in the fan-out failed. A
            // client error means the list is equally unusable.
            let body = response.text().await.unwrap_or_default();
            bail!(
                "the cluster gateway answered {status} for the aggregated sandbox list: {}",
                body.trim()
            );
        }
        let page: Vec<ListedSandbox> = response
            .json()
            .await
            .context("the aggregated sandbox list is not a sandbox array")?;
        Ok((page, next))
    }
}

#[async_trait]
impl ClusterSandboxList for GatewaySandboxList {
    async fn list_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        let mut origins = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let (page, next) = self.page(token.as_deref()).await?;
            origins.extend(page.into_iter().map(|sandbox| sandbox.template_id));
            match next {
                Some(next) if token.as_deref() == Some(next.as_str()) => {
                    bail!("the cluster gateway repeated the page token '{next}'")
                }
                Some(next) => token = Some(next),
                None => return Ok(origins),
            }
        }
        bail!("the cluster gateway list did not end after {MAX_PAGES} pages")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{Request, Response, StatusCode};
    use axum::Router;

    use super::*;

    const KEY: &str = "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// A gateway that answers with the scripted pages, in order.
    struct FakeGateway {
        endpoint: String,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl FakeGateway {
        async fn start(pages: Vec<Response<Body>>) -> Self {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let state = (Arc::new(Mutex::new(pages.into_iter())), Arc::clone(&requests));
            let app = Router::new()
                .fallback(handle)
                .with_state(state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind fake gateway");
            let address = listener.local_addr().expect("address");
            tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve fake gateway");
            });
            Self {
                endpoint: format!("http://{address}"),
                requests,
            }
        }
    }

    type ServeState = (
        Arc<Mutex<std::vec::IntoIter<Response<Body>>>>,
        Arc<Mutex<Vec<String>>>,
    );

    async fn handle(State((pages, requests)): State<ServeState>, request: Request<Body>) -> Response<Body> {
        let uri = request.uri().to_string();
        assert_eq!(
            request.headers().get("x-api-key").and_then(|v| v.to_str().ok()),
            Some(KEY),
            "the aggregated list is called with this node's API key"
        );
        requests.lock().expect("requests").push(uri);
        pages
            .lock()
            .expect("pages")
            .next()
            .unwrap_or_else(|| Response::builder().status(500).body(Body::empty()).expect("response"))
    }

    fn page(body: &str, next: Option<&str>) -> Response<Body> {
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json");
        if let Some(next) = next {
            builder = builder.header("x-next-token", next);
        }
        builder.body(Body::from(body.to_owned())).expect("page")
    }

    fn reader(endpoint: &str) -> GatewaySandboxList {
        GatewaySandboxList::new(endpoint, ApiKey::new(KEY).expect("api key")).expect("reader")
    }

    #[tokio::test]
    async fn follows_every_page_of_the_aggregated_list() {
        let gateway = FakeGateway::start(vec![
            page(r#"[{"templateID":"snap-a"},{"templateID":"snap-b"}]"#, Some("t1")),
            page(r#"[{"templateID":"snap-c"}]"#, None),
        ])
        .await;

        let origins = reader(&gateway.endpoint)
            .list_snapshot_origins()
            .await
            .expect("list");

        assert_eq!(origins, vec!["snap-a", "snap-b", "snap-c"]);
        let requests = gateway.requests.lock().expect("requests").clone();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].contains("nextToken=t1"));
    }

    #[tokio::test]
    async fn a_failed_node_in_the_fan_out_is_an_error() {
        // The gateway reports one node's failure as 502 for the whole answer.
        let gateway = FakeGateway::start(vec![Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::from("cluster list unavailable"))
            .expect("page")])
        .await;

        let error = reader(&gateway.endpoint)
            .list_snapshot_origins()
            .await
            .expect_err("a partial fan-out must not be reported as a list");

        assert!(error.to_string().contains("502"), "{error}");
    }

    #[tokio::test]
    async fn a_page_that_is_not_a_sandbox_array_is_an_error() {
        let gateway = FakeGateway::start(vec![page(r#"{"items":[]}"#, None)]).await;

        assert!(reader(&gateway.endpoint)
            .list_snapshot_origins()
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_repeated_page_token_ends_the_list_with_an_error() {
        // A cursor that never advances would loop forever; stopping early would
        // silently report a truncated list.
        let gateway = FakeGateway::start(vec![
            page(r#"[{"templateID":"snap-a"}]"#, Some("t1")),
            page(r#"[{"templateID":"snap-b"}]"#, Some("t1")),
        ])
        .await;

        let error = reader(&gateway.endpoint)
            .list_snapshot_origins()
            .await
            .expect_err("a repeated cursor must not be read as the end");

        assert!(error.to_string().contains("repeated the page token"), "{error}");
    }
}
