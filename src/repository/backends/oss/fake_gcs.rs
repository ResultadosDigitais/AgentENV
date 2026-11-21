//! An in-process stand-in for the Google Cloud Storage endpoints the OpenDAL
//! `gcs` service calls, so the repository client is tested over its real wire
//! protocol without a bucket or a container.
//!
//! It implements only what the repository uses: JSON object insert with
//! `ifGenerationMatch`, metadata, ranged and `If-Match` reads, delete, paged
//! listing, and the XML multipart upload that large files go through. It is a
//! model of the documented behavior, not a replacement for a bucket.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, Request, StatusCode};
use axum::response::Response;
use axum::Router;
use bytes::Bytes;
use serde_json::json;

#[derive(Clone)]
struct Object {
    data: Bytes,
    generation: u64,
    /// When the object was last written, as the store reports it in RFC 3339.
    /// It is real time, not a fixed stamp: a maintenance pass decides whether an
    /// object is old enough to collect from this value alone.
    updated: String,
}

struct Upload {
    key: String,
    parts: BTreeMap<usize, Bytes>,
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, Object>,
    uploads: HashMap<String, Upload>,
    generation: u64,
    requests: Vec<String>,
}

/// A running fake. Dropping it stops nothing: the server lives for the test
/// process, on its own port.
#[derive(Clone)]
pub(crate) struct FakeGcs {
    pub(crate) endpoint: String,
    inner: Arc<Mutex<Inner>>,
}

impl FakeGcs {
    pub(crate) async fn start() -> Self {
        let inner = Arc::new(Mutex::new(Inner::default()));
        let app = Router::new()
            .fallback(handle)
            .with_state(Arc::clone(&inner));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake gcs");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fake gcs");
        });
        Self { endpoint, inner }
    }

    /// The object at `name` in the bucket, if any.
    pub(crate) fn object(&self, name: &str) -> Option<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        inner.objects.get(name).map(|object| object.data.to_vec())
    }

    pub(crate) fn object_names(&self) -> Vec<String> {
        self.inner.lock().unwrap().objects.keys().cloned().collect()
    }

    /// `METHOD path?query` of every request so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<String> {
        self.inner.lock().unwrap().requests.clone()
    }

    pub(crate) fn requests_matching(&self, needle: &str) -> usize {
        self.requests()
            .iter()
            .filter(|request| request.contains(needle))
            .count()
    }
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(value) = hex.and_then(|hex| u8::from_str_radix(hex, 16).ok()) {
                out.push(value);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn resource(bucket: &str, name: &str, object: &Object) -> serde_json::Value {
    json!({
        "kind": "storage#object",
        "name": name,
        "bucket": bucket,
        "size": object.data.len().to_string(),
        "etag": etag(object),
        "generation": object.generation.to_string(),
        "md5Hash": "",
        "contentType": "application/octet-stream",
        "updated": object.updated,
    })
}

/// The current instant in the format the store reports `updated` in.
fn now_rfc3339() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = now.as_secs();
    let millis = now.subsec_millis();
    let days = seconds / 86_400;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(i64::try_from(days).unwrap_or_default());
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// The civil date of `days` since the Unix epoch, by Howard Hinnant's
/// `civil_from_days`, so the fake needs no date crate.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = u32::try_from(day_of_year - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let month = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn etag(object: &Object) -> String {
    format!("etag-{}", object.generation)
}

fn reply(status: StatusCode, body: impl Into<Body>) -> Response {
    Response::builder()
        .status(status)
        .body(body.into())
        .expect("build response")
}

fn json_reply(status: StatusCode, value: serde_json::Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(value.to_string()))
        .expect("build response")
}

fn error(status: StatusCode, message: &str) -> Response {
    json_reply(
        status,
        json!({"error": {"code": status.as_u16(), "message": message}}),
    )
}

async fn handle(State(inner): State<Arc<Mutex<Inner>>>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, usize::MAX).await.expect("read body");
    let path = parts.uri.path().to_owned();
    let query: HashMap<String, String> =
        url::form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    let mut inner = inner.lock().unwrap();
    inner.requests.push(format!(
        "{} {}?{}",
        parts.method,
        percent_decode(&path),
        parts.uri.query().unwrap_or("")
    ));

    let json_api = path.strip_prefix("/storage/v1/b/");
    let upload_api = path.strip_prefix("/upload/storage/v1/b/");
    match (parts.method.as_str(), json_api, upload_api) {
        ("POST", _, Some(rest)) => {
            let bucket = rest.trim_end_matches("/o");
            let Some(name) = query.get("name") else {
                return error(StatusCode::BAD_REQUEST, "name is required");
            };
            if query.get("ifGenerationMatch").map(String::as_str) == Some("0")
                && inner.objects.contains_key(name)
            {
                return error(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
            }
            inner.generation += 1;
            let object = Object {
                data: body,
                generation: inner.generation,
                updated: now_rfc3339(),
            };
            let created = resource(bucket, name, &object);
            inner.objects.insert(name.clone(), object);
            json_reply(StatusCode::OK, created)
        }
        ("GET", Some(rest), _) => match rest.split_once("/o/") {
            Some((bucket, name)) => {
                let name = percent_decode(name);
                let Some(object) = inner.objects.get(&name).cloned() else {
                    return error(StatusCode::NOT_FOUND, "no such object");
                };
                if query.get("alt").map(String::as_str) != Some("media") {
                    return json_reply(StatusCode::OK, resource(bucket, &name, &object));
                }
                if let Some(wanted) = parts.headers.get(header::IF_MATCH) {
                    if wanted.to_str().unwrap_or("") != etag(&object) {
                        return error(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
                    }
                }
                read(&object, parts.headers.get(header::RANGE))
            }
            None => list(&inner, rest.trim_end_matches("/o"), &query),
        },
        ("DELETE", Some(rest), _) => {
            let name = rest.split_once("/o/").map(|(_, name)| percent_decode(name));
            match name.and_then(|name| inner.objects.remove(&name)) {
                Some(_) => reply(StatusCode::NO_CONTENT, Body::empty()),
                None => error(StatusCode::NOT_FOUND, "no such object"),
            }
        }
        (method, None, None) => multipart(&mut inner, method, &path, &query, body),
        _ => error(StatusCode::BAD_REQUEST, "unsupported request"),
    }
}

fn read(object: &Object, range: Option<&header::HeaderValue>) -> Response {
    let total = object.data.len();
    let Some(range) = range.and_then(|value| value.to_str().ok()) else {
        return reply(StatusCode::OK, Body::from(object.data.clone()));
    };
    let spec = range.trim_start_matches("bytes=");
    let (start, end) = spec.split_once('-').unwrap_or((spec, ""));
    let start: usize = start.parse().unwrap_or(0);
    if start >= total {
        return error(StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable");
    }
    let end = end
        .parse::<usize>()
        .map_or(total - 1, |end| end.min(total - 1));
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}"),
        )
        .body(Body::from(object.data.slice(start..=end)))
        .expect("build response")
}

const PAGE: usize = 1000;

fn list(inner: &Inner, bucket: &str, query: &HashMap<String, String>) -> Response {
    let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
    let start_offset = query.get("startOffset").map(String::as_str);
    let token = query.get("pageToken").map(String::as_str);
    let page = query
        .get("maxResults")
        .and_then(|value| value.parse().ok())
        .unwrap_or(PAGE)
        .min(PAGE);
    let mut matching = inner
        .objects
        .iter()
        .filter(|(name, _)| name.starts_with(prefix))
        // `startOffset` is inclusive; a page token resumes after its key.
        .filter(|(name, _)| start_offset.is_none_or(|offset| name.as_str() >= offset))
        .filter(|(name, _)| token.is_none_or(|token| name.as_str() > token))
        .map(|(name, object)| (name.clone(), resource(bucket, name, object)))
        .collect::<Vec<_>>();
    let next = (matching.len() > page).then(|| matching[page - 1].0.clone());
    matching.truncate(page);
    let items: Vec<_> = matching.into_iter().map(|(_, item)| item).collect();
    let mut body = json!({"kind": "storage#objects", "items": items});
    if let Some(next) = next {
        body["nextPageToken"] = json!(next);
    }
    json_reply(StatusCode::OK, body)
}

/// The XML API: `/<bucket>/<object>?uploads`, `?partNumber&uploadId`, `?uploadId`.
fn multipart(
    inner: &mut Inner,
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
    body: Bytes,
) -> Response {
    let Some((_bucket, name)) = path.trim_start_matches('/').split_once('/') else {
        return error(StatusCode::BAD_REQUEST, "unsupported request");
    };
    let name = percent_decode(name);
    match method {
        "POST" if query.contains_key("uploads") => {
            inner.generation += 1;
            let upload_id = format!("upload-{}", inner.generation);
            inner.uploads.insert(
                upload_id.clone(),
                Upload {
                    key: name,
                    parts: BTreeMap::new(),
                },
            );
            reply(
                StatusCode::OK,
                format!(
                    "<InitiateMultipartUploadResult><UploadId>{upload_id}</UploadId>\
                     </InitiateMultipartUploadResult>"
                ),
            )
        }
        "PUT" => {
            let (Some(upload_id), Some(part)) = (query.get("uploadId"), query.get("partNumber"))
            else {
                return error(StatusCode::BAD_REQUEST, "uploadId and partNumber");
            };
            let Some(upload) = inner.uploads.get_mut(upload_id) else {
                return error(StatusCode::NOT_FOUND, "no such upload");
            };
            let part: usize = part.parse().unwrap_or(0);
            upload.parts.insert(part, body);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::ETAG, format!("\"part-{part}\""))
                .body(Body::empty())
                .expect("build response")
        }
        "POST" => {
            let Some(upload) = query
                .get("uploadId")
                .and_then(|id| inner.uploads.remove(id))
            else {
                return error(StatusCode::NOT_FOUND, "no such upload");
            };
            let data: Vec<u8> = upload
                .parts
                .values()
                .flat_map(|part| part.to_vec())
                .collect();
            inner.generation += 1;
            let generation = inner.generation;
            inner.objects.insert(
                upload.key,
                Object {
                    data: Bytes::from(data),
                    generation,
                    updated: now_rfc3339(),
                },
            );
            reply(
                StatusCode::OK,
                "<CompleteMultipartUploadResult></CompleteMultipartUploadResult>",
            )
        }
        "DELETE" => {
            if let Some(id) = query.get("uploadId") {
                inner.uploads.remove(id);
            }
            reply(StatusCode::NO_CONTENT, Body::empty())
        }
        _ => error(StatusCode::BAD_REQUEST, "unsupported request"),
    }
}
