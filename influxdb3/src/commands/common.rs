use clap::{Parser, ValueEnum};
use observability_deps::tracing::warn;
use secrecy::Secret;
use std::fmt::Display;
use std::str::FromStr;
use std::{env, error::Error};
use url::Url;

#[derive(Debug, Parser)]
pub struct InfluxDb3Config {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The name of the database to operate on
    #[clap(short = 'd', long = "database", env = "INFLUXDB3_DATABASE_NAME")]
    pub database_name: String,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,
}

#[derive(Debug, ValueEnum, Clone)]
#[clap(rename_all = "snake_case")]
pub enum Format {
    Pretty,
    Json,
    #[clap(name = "jsonl")]
    JsonLines,
    Csv,
    Parquet,
}

impl Format {
    pub fn is_parquet(&self) -> bool {
        matches!(self, Self::Parquet)
    }
}

impl From<Format> for influxdb3_types::http::QueryFormat {
    fn from(this: Format) -> Self {
        match this {
            Format::Pretty => Self::Pretty,
            Format::Json => Self::Json,
            Format::JsonLines => Self::JsonLines,
            Format::Csv => Self::Csv,
            Format::Parquet => Self::Parquet,
        }
    }
}

// A clap argument provided as a key/value pair separated by `SEPARATOR`, which by default is a '='
#[derive(Debug, Clone)]
pub struct SeparatedKeyValue<K, V, const SEPARATOR: char = '='>(pub (K, V));

impl<K, V, const SEPARATOR: char> FromStr for SeparatedKeyValue<K, V, SEPARATOR>
where
    K: FromStr<Err: Into<anyhow::Error>>,
    V: FromStr<Err: Into<anyhow::Error>>,
{
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(SEPARATOR);
        let key = parts.next().ok_or_else(|| anyhow::anyhow!("missing key"))?;
        let value = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing value"))?;

        Ok(Self((
            key.parse().map_err(Into::into)?,
            value.parse().map_err(Into::into)?,
        )))
    }
}

/// A clap argument provided as a list of items separated by `SEPARATOR`, which by default is a ','
#[derive(Debug, Clone)]
pub struct SeparatedList<T, const SEPARATOR: char = ','>(pub Vec<T>);

impl<T, const SEPARATOR: char> FromStr for SeparatedList<T, SEPARATOR>
where
    T: FromStr<Err: Into<anyhow::Error>>,
{
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            s.split(SEPARATOR)
                .map(|s| s.parse::<T>().map_err(Into::into))
                .collect::<Result<Vec<T>, Self::Err>>()?,
        ))
    }
}

impl<T, const SEPARATOR: char> IntoIterator for SeparatedList<T, SEPARATOR> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Uint64,
    Float64,
    Utf8,
    Bool,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0} is not a valid data type, values are int64, uint64, float64, utf8, and bool")]
pub struct ParseDataTypeError(String);

impl FromStr for DataType {
    type Err = ParseDataTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int64" => Ok(Self::Int64),
            "uint64" => Ok(Self::Uint64),
            "float64" => Ok(Self::Float64),
            "utf8" => Ok(Self::Utf8),
            "bool" => Ok(Self::Bool),
            _ => Err(ParseDataTypeError(s.into())),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int64 => write!(f, "int64"),
            Self::Uint64 => write!(f, "uint64"),
            Self::Float64 => write!(f, "float64"),
            Self::Utf8 => write!(f, "utf8"),
            Self::Bool => write!(f, "bool"),
        }
    }
}

impl From<DataType> for String {
    fn from(data: DataType) -> Self {
        data.to_string()
    }
}

/// Parse a single key-value pair
pub fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find(':')
        .ok_or_else(|| format!("invalid FIELD:VALUE. No `:` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

/// If passed in env vars are set, then it writes to log along with what the user can switch to
pub fn warn_use_of_deprecated_env_vars(deprecated_vars: &[(&'static str, &'static str)]) {
    deprecated_vars
        .iter()
        .for_each(|(deprecated_var, migration_msg)| {
            if env::var(deprecated_var).is_ok() {
                warn!("detected deprecated/removed env var {deprecated_var}, {migration_msg}");
            }
        });
}
