use std::{
    collections::HashMap, fmt::Display, num::NonZeroUsize, string::FromUtf8Error, time::Duration,
};

use bytes::Bytes;
use iox_query_params::StatementParam;
use reqwest::{Body, IntoUrl, Method, StatusCode};
use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use url::Url;

/// Primary error type for the [`Client`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("base URL error: {0}")]
    BaseUrl(#[source] reqwest::Error),

    #[error("request URL error: {0}")]
    RequestUrl(#[from] url::ParseError),

    #[error("failed to read the API response bytes: {0}")]
    Bytes(#[source] reqwest::Error),

    #[error(
        "provided parameter ('{name}') could not be converted \
        to a statment parameter"
    )]
    ConvertQueryParam {
        name: String,
        #[source]
        source: iox_query_params::Error,
    },

    #[error("invalid UTF8 in response: {0}")]
    InvalidUtf8(#[from] FromUtf8Error),

    #[error("failed to parse JSON response: {0}")]
    Json(#[source] reqwest::Error),

    #[error("failed to parse plaintext response: {0}")]
    Text(#[source] reqwest::Error),

    #[error("server responded with error [{code}]: {message}")]
    ApiError { code: StatusCode, message: String },

    #[error("failed to send {method} {url} request: {source}")]
    RequestSend {
        method: Method,
        url: String,
        #[source]
        source: reqwest::Error,
    },
}

impl Error {
    fn request_send(method: Method, url: impl Into<String>, source: reqwest::Error) -> Self {
        Self::RequestSend {
            method,
            url: url.into(),
            source,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// The InfluxDB 3.0 Client
///
/// For programmatic access to the HTTP API of InfluxDB 3.0
#[derive(Debug, Clone)]
pub struct Client {
    /// The base URL for making requests to a running InfluxDB 3.0 server
    base_url: Url,
    /// The `Bearer` token to use for authenticating on each request to the server
    auth_token: Option<Secret<String>>,
    /// A [`reqwest::Client`] for handling HTTP requests
    http_client: reqwest::Client,
}

impl Client {
    /// Create a new [`Client`]
    pub fn new<U: IntoUrl>(base_url: U) -> Result<Self> {
        Ok(Self {
            base_url: base_url.into_url().map_err(Error::BaseUrl)?,
            auth_token: None,
            http_client: reqwest::Client::new(),
        })
    }

    /// Set the `Bearer` token that will be sent with each request to the server
    ///
    /// # Example
    /// ```
    /// # use influxdb3_client::Client;
    /// # use influxdb3_client::Precision;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let token = "secret-token-string";
    /// let client = Client::new("http://localhost:8181")?
    ///     .with_auth_token(token);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_auth_token<S: Into<String>>(mut self, auth_token: S) -> Self {
        self.auth_token = Some(Secret::new(auth_token.into()));
        self
    }

    /// Compose a request to the `/api/v3/write_lp` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # use influxdb3_client::Precision;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// client
    ///     .api_v3_write_lp("db_name")
    ///     .precision(Precision::Millisecond)
    ///     .accept_partial(true)
    ///     .body("cpu,host=s1 usage=0.5")
    ///     .send()
    ///     .await
    ///     .expect("send write_lp request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_write_lp<S: Into<String>>(&self, db: S) -> WriteRequestBuilder<'_, NoBody> {
        WriteRequestBuilder {
            client: self,
            db: db.into(),
            precision: None,
            accept_partial: None,
            body: NoBody,
        }
    }

    /// Compose a request to the `/api/v3/query_sql` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo")
    ///     .send()
    ///     .await
    ///     .expect("send query_sql request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_query_sql<D: Into<String>, Q: Into<String>>(
        &self,
        db: D,
        query: Q,
    ) -> QueryRequestBuilder<'_> {
        QueryRequestBuilder {
            client: self,
            kind: QueryKind::Sql,
            db: db.into(),
            query: query.into(),
            format: None,
            params: None,
        }
    }

    /// Compose a request to the `/api/v3/query_influxql` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_influxql("db_name", "SELECT * FROM foo")
    ///     .send()
    ///     .await
    ///     .expect("send query_influxql request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_query_influxql<D: Into<String>, Q: Into<String>>(
        &self,
        db: D,
        query: Q,
    ) -> QueryRequestBuilder<'_> {
        QueryRequestBuilder {
            client: self,
            kind: QueryKind::InfluxQl,
            db: db.into(),
            query: query.into(),
            format: None,
            params: None,
        }
    }

    /// Compose a request to the `POST /api/v3/configure/last_cache` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let resp = client
    ///     .api_v3_configure_last_cache_create("db_name", "table_name")
    ///     .ttl(120)
    ///     .name("cache_name")
    ///     .count(5)
    ///     .key_columns(["col1", "col2"])
    ///     .send()
    ///     .await
    ///     .expect("send create last cache request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_configure_last_cache_create(
        &self,
        db: impl Into<String>,
        table: impl Into<String>,
    ) -> CreateLastCacheRequestBuilder<'_> {
        CreateLastCacheRequestBuilder::new(self, db, table)
    }

    /// Make a request to the `DELETE /api/v3/configure/last_cache` API
    pub async fn api_v3_configure_last_cache_delete(
        &self,
        db: impl Into<String> + Send,
        table: impl Into<String> + Send,
        name: impl Into<String> + Send,
    ) -> Result<()> {
        let url = self.base_url.join("/api/v3/configure/last_cache")?;
        #[derive(Serialize)]
        struct Req {
            db: String,
            table: String,
            name: String,
        }
        let mut req = self.http_client.delete(url).json(&Req {
            db: db.into(),
            table: table.into(),
            name: name.into(),
        });
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req.send().await.map_err(|src| {
            Error::request_send(Method::DELETE, "/api/v3/configure/last_cache", src)
        })?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }

    /// Compose a request to the `POST /api/v3/configure/meta_cache` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # use std::num::NonZeroUsize;
    /// # use std::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let resp = client
    ///     .api_v3_configure_meta_cache_create("db_name", "table_name", ["col1", "col2"])
    ///     .name("cache_name")
    ///     .max_cardinality(NonZeroUsize::new(1_000).unwrap())
    ///     .max_age(Duration::from_secs(3_600))
    ///     .send()
    ///     .await
    ///     .expect("send create meta cache request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_configure_meta_cache_create(
        &self,
        db: impl Into<String>,
        table: impl Into<String>,
        columns: impl IntoIterator<Item: Into<String>>,
    ) -> CreateMetaCacheRequestBuilder<'_> {
        CreateMetaCacheRequestBuilder::new(self, db, table, columns)
    }

    /// Make a request to the `DELETE /api/v3/configure/meta_cache` API
    pub async fn api_v3_configure_meta_cache_delete(
        &self,
        db: impl Into<String> + Send,
        table: impl Into<String> + Send,
        name: impl Into<String> + Send,
    ) -> Result<()> {
        let url = self.base_url.join("/api/v3/configure/meta_cache")?;
        #[derive(Serialize)]
        struct Req {
            db: String,
            table: String,
            name: String,
        }
        let mut req = self.http_client.delete(url).json(&Req {
            db: db.into(),
            table: table.into(),
            name: name.into(),
        });
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req.send().await.map_err(|src| {
            Error::request_send(Method::DELETE, "/api/v3/configure/meta_cache", src)
        })?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }

    /// Make a request to the `DELETE /api/v3/configure/database?db=foo` API
    pub async fn api_v3_configure_db_delete(&self, db: impl AsRef<str> + Send) -> Result<()> {
        let api_path = "/api/v3/configure/database";

        let url = self.base_url.join(api_path)?;

        let mut req = self.http_client.delete(url).query(&[("db", db.as_ref())]);
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(Method::DELETE, api_path, src))?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }

    /// Make a request to the `DELETE /api/v3/configure/table?db=foo&table=bar` API
    pub async fn api_v3_configure_table_delete<T: AsRef<str> + Send>(
        &self,
        db: T,
        table: T,
    ) -> Result<()> {
        let api_path = "/api/v3/configure/table";

        let url = self.base_url.join(api_path)?;

        let mut req = self
            .http_client
            .delete(url)
            .query(&[("db", db.as_ref()), ("table", table.as_ref())]);
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(Method::DELETE, api_path, src))?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }

    /// Send a `/ping` request to the target `influxdb3` server to check its
    /// status and gather `version` and `revision` information
    pub async fn ping(&self) -> Result<PingResponse> {
        let url = self.base_url.join("/ping")?;
        let mut req = self.http_client.get(url);
        if let Some(t) = &self.auth_token {
            req = req.bearer_auth(t.expose_secret());
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(Method::GET, "/ping", src))?;
        if resp.status().is_success() {
            resp.json().await.map_err(Error::Json)
        } else {
            Err(Error::ApiError {
                code: resp.status(),
                message: resp.text().await.map_err(Error::Text)?,
            })
        }
    }
}

/// The response of the `/ping` API on `influxdb3`
#[derive(Debug, Serialize, Deserialize)]
pub struct PingResponse {
    version: String,
    revision: String,
}

impl PingResponse {
    /// Get the `version` from the response
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get the `revision` from the response
    pub fn revision(&self) -> &str {
        &self.revision
    }
}

/// The URL parameters of the request to the `/api/v3/write_lp` API
// TODO - this should re-use a type defined in the server code, or a separate crate,
//        central to both.
#[derive(Debug, Serialize)]
struct WriteParams<'a> {
    db: &'a str,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
}

impl<'a, B> From<&'a WriteRequestBuilder<'a, B>> for WriteParams<'a> {
    fn from(builder: &'a WriteRequestBuilder<'a, B>) -> Self {
        Self {
            db: &builder.db,
            precision: builder.precision,
            accept_partial: builder.accept_partial,
        }
    }
}

/// Time series precision
// TODO - this should re-use a type defined in the server code, or a separate crate,
//        central to both.
#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// Builder type for composing a request to `/api/v3/write_lp`
///
/// Produced by [`Client::api_v3_write_lp`]
#[derive(Debug)]
pub struct WriteRequestBuilder<'c, B> {
    client: &'c Client,
    db: String,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
    body: B,
}

impl<'c, B> WriteRequestBuilder<'c, B> {
    /// Set the precision
    pub fn precision(mut self, set_to: Precision) -> Self {
        self.precision = Some(set_to);
        self
    }

    /// Set the `accept_partial` parameter
    pub fn accept_partial(mut self, set_to: bool) -> Self {
        self.accept_partial = Some(set_to);
        self
    }
}

impl<'c> WriteRequestBuilder<'c, NoBody> {
    /// Set the body of the request to the `/api/v3/write_lp` API
    ///
    /// This essentially wraps `reqwest`'s [`body`][reqwest::RequestBuilder::body]
    /// method, and puts the responsibility on the caller for now.
    pub fn body<T: Into<Body>>(self, body: T) -> WriteRequestBuilder<'c, Body> {
        WriteRequestBuilder {
            client: self.client,
            db: self.db,
            precision: self.precision,
            accept_partial: self.accept_partial,
            body: body.into(),
        }
    }
}

impl<'c> WriteRequestBuilder<'c, Body> {
    /// Send the request to the server
    pub async fn send(self) -> Result<()> {
        let url = self.client.base_url.join("/api/v3/write_lp")?;
        let params = WriteParams::from(&self);
        let mut req = self.client.http_client.post(url).query(&params);
        if let Some(token) = &self.client.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req
            .body(self.body)
            .send()
            .await
            .map_err(|src| Error::request_send(Method::POST, "/api/v3/write_lp", src))?;
        let status = resp.status();
        let content = resp.bytes().await.map_err(Error::Bytes)?;
        match status {
            // TODO - handle the OK response content, return to caller, etc.
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: String::from_utf8(content.to_vec())?,
            }),
        }
    }
}

#[doc(hidden)]
/// Typestate type for [`WriteRequestBuilder`]
#[derive(Debug, Copy, Clone)]
pub struct NoBody;

/// Used to compose a request to the `/api/v3/query_sql` API
///
/// Produced by [`Client::api_v3_query_sql`] method.
#[derive(Debug)]
pub struct QueryRequestBuilder<'c> {
    client: &'c Client,
    kind: QueryKind,
    db: String,
    query: String,
    format: Option<Format>,
    params: Option<HashMap<String, StatementParam>>,
}

// TODO - for now the send method just returns the bytes from the response.
//   It may be nicer to have the format parameter dictate how we return from
//   send, e.g., using types more specific to the format selected.
impl<'c> QueryRequestBuilder<'c> {
    /// Specify the format, `json`, `csv`, `pretty`, or `parquet`
    pub fn format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    /// Set a query parameter value with the given `name`
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo WHERE bar = $bar AND baz > $baz")
    ///     .with_param("bar", "bop")
    ///     .with_param("baz", 0.5)
    ///     .send()
    ///     .await
    ///     .expect("send query_sql request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_param<S: Into<String>, P: Into<StatementParam>>(
        mut self,
        name: S,
        param: P,
    ) -> Self {
        self.params
            .get_or_insert_with(Default::default)
            .insert(name.into(), param.into());
        self
    }

    /// Set a query parameters from the given collection of pairs
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use serde_json::json;
    /// use std::collections::HashMap;
    ///
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo WHERE bar = $bar AND foo > $fooz")
    ///     .with_params_from([
    ///         ("bar", json!(false)),
    ///         ("foo", json!(10)),
    ///     ])?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_params_from<S, P, C>(mut self, params: C) -> Result<Self>
    where
        S: Into<String>,
        P: TryInto<StatementParam, Error = iox_query_params::Error>,
        C: IntoIterator<Item = (S, P)>,
    {
        for (name, param) in params.into_iter() {
            let name = name.into();
            let param = param
                .try_into()
                .map_err(|source| Error::ConvertQueryParam {
                    name: name.clone(),
                    source,
                })?;

            self.params
                .get_or_insert_with(Default::default)
                .insert(name, param);
        }
        Ok(self)
    }

    /// Try to set a query parameter value with the given `name`
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use serde_json::json;
    ///
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo WHERE bar = $bar AND baz > $baz")
    ///     .with_try_param("bar", json!("baz"))?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_try_param<S, P>(mut self, name: S, param: P) -> Result<Self>
    where
        S: Into<String>,
        P: TryInto<StatementParam, Error = iox_query_params::Error>,
    {
        let name = name.into();
        let param = param
            .try_into()
            .map_err(|source| Error::ConvertQueryParam {
                name: name.clone(),
                source,
            })?;
        self.params
            .get_or_insert_with(Default::default)
            .insert(name, param);
        Ok(self)
    }

    /// Send the request to `/api/v3/query_sql` or `/api/v3/query_influxql`
    pub async fn send(self) -> Result<Bytes> {
        let url = match self.kind {
            QueryKind::Sql => self.client.base_url.join("/api/v3/query_sql")?,
            QueryKind::InfluxQl => self.client.base_url.join("/api/v3/query_influxql")?,
        };
        let params = QueryParams::from(&self);
        let mut req = self.client.http_client.post(url).json(&params);
        if let Some(token) = &self.client.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req.send().await.map_err(|src| {
            Error::request_send(Method::POST, format!("/api/v3/query_{}", self.kind), src)
        })?;
        let status = resp.status();
        let content = resp.bytes().await.map_err(Error::Bytes)?;

        match status {
            StatusCode::OK => Ok(content),
            code => Err(Error::ApiError {
                code,
                message: String::from_utf8(content.to_vec()).map_err(Error::InvalidUtf8)?,
            }),
        }
    }
}

/// Query parameters for the `/api/v3/query_sql` API
#[derive(Debug, Serialize)]
pub struct QueryParams<'a> {
    db: &'a str,
    #[serde(rename = "q")]
    query: &'a str,
    format: Option<Format>,
    params: Option<&'a HashMap<String, StatementParam>>,
}

impl<'a> From<&'a QueryRequestBuilder<'a>> for QueryParams<'a> {
    fn from(builder: &'a QueryRequestBuilder<'a>) -> Self {
        Self {
            db: &builder.db,
            query: &builder.query,
            format: builder.format,
            params: builder.params.as_ref(),
        }
    }
}

/// The type of query, SQL or InfluxQL
#[derive(Debug, Copy, Clone)]
pub enum QueryKind {
    Sql,
    InfluxQl,
}

impl Display for QueryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryKind::Sql => write!(f, "sql"),
            QueryKind::InfluxQl => write!(f, "influxql"),
        }
    }
}

/// Output format to request from the server in the `/api/v3/query_sql` API
#[derive(Debug, Serialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Json,
    Csv,
    Parquet,
    Pretty,
}

#[derive(Debug, Serialize)]
pub struct CreateLastCacheRequestBuilder<'c> {
    #[serde(skip_serializing)]
    client: &'c Client,
    db: String,
    table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl: Option<u64>,
}

impl<'c> CreateLastCacheRequestBuilder<'c> {
    /// Create a new [`CreateLastCacheRequestBuilder`]
    fn new(client: &'c Client, db: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            client,
            db: db.into(),
            table: table.into(),
            name: None,
            key_columns: None,
            value_columns: None,
            count: None,
            ttl: None,
        }
    }

    /// Specify a cache name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Speciffy the key columns for the cache
    pub fn key_columns(mut self, column_names: impl IntoIterator<Item: Into<String>>) -> Self {
        self.key_columns = Some(column_names.into_iter().map(Into::into).collect());
        self
    }

    /// Specify the value columns for the cache
    pub fn value_columns(mut self, column_names: impl IntoIterator<Item: Into<String>>) -> Self {
        self.value_columns = Some(column_names.into_iter().map(Into::into).collect());
        self
    }

    /// Specify the size, or number of new entries a cache will hold before evicting old ones
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Specify the time-to-live (TTL) in seconds for entries in the cache
    pub fn ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Send the request to `POST /api/v3/configure/last_cache`
    pub async fn send(self) -> Result<Option<LastCacheCreatedResponse>> {
        let url = self.client.base_url.join("/api/v3/configure/last_cache")?;
        let mut req = self.client.http_client.post(url).json(&self);
        if let Some(token) = &self.client.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req.send().await.map_err(|src| {
            Error::request_send(Method::POST, "/api/v3/configure/last_cache", src)
        })?;
        let status = resp.status();
        match status {
            StatusCode::CREATED => {
                let content = resp
                    .json::<LastCacheCreatedResponse>()
                    .await
                    .map_err(Error::Json)?;
                Ok(Some(content))
            }
            StatusCode::NO_CONTENT => Ok(None),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LastCacheCreatedResponse {
    /// The table name the cache is associated with
    pub table: String,
    /// Given name of the cache
    pub name: String,
    /// Columns intended to be used as predicates in the cache
    pub key_columns: Vec<u32>,
    /// Columns that store values in the cache
    pub value_columns: LastCacheValueColumnsDef,
    /// The number of last values to hold in the cache
    pub count: usize,
    /// The time-to-live (TTL) in seconds for entries in the cache
    pub ttl: u64,
}

/// A last cache will either store values for an explicit set of columns, or will accept all
/// non-key columns
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LastCacheValueColumnsDef {
    /// Explicit list of column names
    Explicit { columns: Vec<u32> },
    /// Stores all non-key columns
    AllNonKeyColumns,
}

/// Type for composing requests to the `POST /api/v3/configure/meta_cache` API created by the
/// [`Client::api_v3_configure_meta_cache_create`] method
#[derive(Debug, Serialize)]
pub struct CreateMetaCacheRequestBuilder<'c> {
    #[serde(skip_serializing)]
    client: &'c Client,
    db: String,
    table: String,
    columns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_cardinality: Option<NonZeroUsize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_age: Option<Duration>,
}

impl<'c> CreateMetaCacheRequestBuilder<'c> {
    fn new(
        client: &'c Client,
        db: impl Into<String>,
        table: impl Into<String>,
        columns: impl IntoIterator<Item: Into<String>>,
    ) -> Self {
        Self {
            client,
            db: db.into(),
            table: table.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            name: None,
            max_cardinality: None,
            max_age: None,
        }
    }

    /// Specify the name of the cache to be created, `snake_case` names are encouraged
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Specify the maximum cardinality for the cache as a non-zero unsigned integer
    pub fn max_cardinality(mut self, max_cardinality: NonZeroUsize) -> Self {
        self.max_cardinality = Some(max_cardinality);
        self
    }

    /// Specify the maximum age for entries in the cache
    pub fn max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    /// Send the create cache request
    pub async fn send(self) -> Result<Option<MetaCacheCreatedResponse>> {
        let url = self.client.base_url.join("/api/v3/configure/meta_cache")?;
        let mut req = self.client.http_client.post(url).json(&self);
        if let Some(token) = &self.client.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req.send().await.map_err(|src| {
            Error::request_send(Method::POST, "/api/v3/configure/meta_cache", src)
        })?;
        let status = resp.status();
        match status {
            StatusCode::CREATED => {
                let content = resp
                    .json::<MetaCacheCreatedResponse>()
                    .await
                    .map_err(Error::Json)?;
                Ok(Some(content))
            }
            StatusCode::NO_CONTENT => Ok(None),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaCacheCreatedResponse {
    /// The id of the table the cache was created on
    pub table_id: u32,
    /// The name of the table the cache was created on
    pub table_name: String,
    /// The name of the created cache
    pub cache_name: String,
    /// The columns in the cache
    pub column_ids: Vec<u32>,
    /// The maximum number of unique value combinations the cache will hold
    pub max_cardinality: usize,
    /// The maximum age for entries in the cache
    pub max_age_seconds: u64,
}

#[cfg(test)]
mod tests {
    use mockito::{Matcher, Server};
    use serde_json::json;

    use crate::{Client, Format, Precision};

    #[tokio::test]
    async fn api_v3_write_lp() {
        let token = "super-secret-token";
        let db = "stats";
        let body = "\
            cpu,host=s1 usage=0.5
            cpu,host=s1,region=us-west usage=0.7";

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/write_lp")
            .match_header("Authorization", format!("Bearer {token}").as_str())
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("precision".into(), "millisecond".into()),
                Matcher::UrlEncoded("db".into(), db.into()),
                Matcher::UrlEncoded("accept_partial".into(), "true".into()),
            ]))
            .match_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url())
            .expect("create client")
            .with_auth_token(token);

        client
            .api_v3_write_lp(db)
            .precision(Precision::Millisecond)
            .accept_partial(true)
            .body(body)
            .send()
            .await
            .expect("send write_lp request");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_query_sql() {
        let token = "super-secret-token";
        let db = "stats";
        let query = "SELECT * FROM foo";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_sql")
            .match_header("Authorization", format!("Bearer {token}").as_str())
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "format": "json",
                "params": null,
            })))
            .with_status(200)
            // TODO - could add content-type header but that may be too brittle
            //        at the moment
            //      - this will be JSON Lines at some point
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url())
            .expect("create client")
            .with_auth_token(token);

        let r = client
            .api_v3_query_sql(db, query)
            .format(Format::Json)
            .send()
            .await
            .expect("send request to server");

        assert_eq!(&r, body);

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_query_sql_params() {
        let db = "stats";
        let query = "SELECT * FROM foo WHERE bar = $bar";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_sql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "params": {
                    "bar": "baz",
                    "baz": false,
                },
                "format": null
            })))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url()).expect("create client");

        let r = client
            .api_v3_query_sql(db, query)
            .with_param("bar", "baz")
            .with_param("baz", false)
            .send()
            .await;

        mock.assert_async().await;

        r.expect("sent request successfully");
    }

    #[tokio::test]
    async fn api_v3_query_influxql() {
        let db = "stats";
        let query = "SELECT * FROM foo";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_influxql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "format": "json",
                "params": null,
            })))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url()).expect("create client");

        let r = client
            .api_v3_query_influxql(db, query)
            .format(Format::Json)
            .send()
            .await
            .expect("send request to server");

        assert_eq!(&r, body);

        mock.assert_async().await;
    }
    #[tokio::test]
    async fn api_v3_query_influxql_params() {
        let db = "stats";
        let query = "SELECT * FROM foo WHERE a = $a AND b < $b AND c > $c AND d = $d";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_influxql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "params": {
                    "a": "bar",
                    "b": 123,
                    "c": 1.5,
                    "d": false
                },
                "format": null
            })))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url()).expect("create client");

        let mut builder = client.api_v3_query_influxql(db, query);

        for (name, value) in [
            ("a", json!("bar")),
            ("b", json!(123)),
            ("c", json!(1.5)),
            ("d", json!(false)),
        ] {
            builder = builder.with_try_param(name, value).unwrap();
        }
        let r = builder.send().await;

        mock.assert_async().await;

        r.expect("sent request successfully");
    }
    #[tokio::test]
    async fn api_v3_query_influxql_with_params_from() {
        let db = "stats";
        let query = "SELECT * FROM foo WHERE a = $a AND b < $b AND c > $c AND d = $d";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_influxql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "params": {
                    "a": "bar",
                    "b": 123,
                    "c": 1.5,
                    "d": false
                },
                "format": null
            })))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url()).expect("create client");

        let r = client
            .api_v3_query_influxql(db, query)
            .with_params_from([
                ("a", json!("bar")),
                ("b", json!(123)),
                ("c", json!(1.5)),
                ("d", json!(false)),
            ])
            .unwrap()
            .send()
            .await;

        mock.assert_async().await;

        r.expect("sent request successfully");
    }

    #[tokio::test]
    async fn api_v3_configure_last_cache_create_201() {
        let db = "db";
        let table = "table";
        let name = "cache_name";
        let key_columns = ["col1", "col2"];
        let val_columns = vec!["col3", "col4"];
        let ttl = 120;
        let count = 5;
        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/configure/last_cache")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "table": table,
                "name": name,
                "key_columns": key_columns,
                "value_columns": val_columns,
                "count": count,
                "ttl": ttl,
            })))
            .with_status(201)
            .with_body(
                r#"{
                    "table": "table",
                    "name": "cache_name",
                    "key_columns": [0, 1],
                    "value_columns": {
                        "type": "explicit",
                        "columns": [2, 3]
                    },
                    "ttl": 120,
                    "count": 5
                }"#,
            )
            .create_async()
            .await;
        let client = Client::new(mock_server.url()).unwrap();
        client
            .api_v3_configure_last_cache_create(db, table)
            .name(name)
            .key_columns(key_columns)
            .value_columns(val_columns)
            .ttl(ttl)
            .count(count)
            .send()
            .await
            .expect("creates last cache and parses response");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_configure_last_cache_create_204() {
        let db = "db";
        let table = "table";
        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/configure/last_cache")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "table": table,
            })))
            .with_status(204)
            .create_async()
            .await;
        let client = Client::new(mock_server.url()).unwrap();
        let resp = client
            .api_v3_configure_last_cache_create(db, table)
            .send()
            .await
            .unwrap();
        mock.assert_async().await;
        assert!(resp.is_none());
    }

    #[tokio::test]
    async fn api_v3_configure_last_cache_delete() {
        let db = "db";
        let table = "table";
        let name = "cache_name";
        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("DELETE", "/api/v3/configure/last_cache")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "table": table,
                "name": name,
            })))
            .with_status(200)
            .create_async()
            .await;
        let client = Client::new(mock_server.url()).unwrap();
        client
            .api_v3_configure_last_cache_delete(db, table, name)
            .await
            .unwrap();
        mock.assert_async().await;
    }
}
