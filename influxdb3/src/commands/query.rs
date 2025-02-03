use std::str::Utf8Error;

use clap::{Parser, ValueEnum};
use secrecy::ExposeSecret;
use std::fs;
use std::io::{stdin, BufReader, IsTerminal, Read};
use tokio::{
    fs::OpenOptions,
    io::{self, AsyncWriteExt},
};

use crate::commands::common::Format;

use super::common::InfluxDb3Config;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),

    #[error(transparent)]
    Query(#[from] QueryError),

    #[error("invlid UTF8 received from server")]
    Utf8(#[from] Utf8Error),

    #[error("io error")]
    Io(#[from] io::Error),

    #[error(
        "must specify an output file path with `--output` parameter when formatting\
        the output as `parquet`"
    )]
    NoOutputFileForParquet,
    #[error(
        "No input from stdin detected, no string was passed in,  and no file \
        path was given"
    )]
    NoInput,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "q", trailing_var_arg = true)]
pub struct Config {
    /// Common InfluxDB 3 Core config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The query language used to format the provided query string
    #[clap(
        value_enum,
        long = "language",
        short = 'l',
        default_value_t = QueryLanguage::Sql,
    )]
    language: QueryLanguage,

    /// The format in which to output the query
    ///
    /// If `--format` is set to `parquet`, then you must also specify an output
    /// file path with `--output`.
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// Put all query output into `output`
    #[clap(short = 'o', long = "output")]
    output_file_path: Option<String>,

    /// A file containing sql statements to execute
    #[clap(short = 'f', long = "file")]
    file_path: Option<String>,

    /// The query string to execute
    query: Option<Vec<String>>,
}

#[derive(Debug, ValueEnum, Clone)]
enum QueryLanguage {
    Sql,
    Influxql,
}

pub(crate) async fn command(config: Config) -> Result<()> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let mut client = influxdb3_client::Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }

    let query = if let Some(query) = config.query {
        parse_query(query)?
    } else if let Some(file_path) = config.file_path {
        fs::read_to_string(file_path)?
    } else {
        let stdin = stdin();
        // Checks if stdin has had data passed to it via a pipe
        if stdin.is_terminal() {
            return Err(Error::NoInput);
        }
        let mut reader = BufReader::new(stdin);
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer)?;
        buffer
    };

    // make the query using the client
    let mut resp_bytes = match config.language {
        QueryLanguage::Sql => {
            client
                .api_v3_query_sql(database_name, query)
                .format(config.output_format.clone().into())
                .send()
                .await?
        }
        QueryLanguage::Influxql => {
            client
                .api_v3_query_influxql(database_name, query)
                .format(config.output_format.clone().into())
                .send()
                .await?
        }
    };

    // write to file if output path specified
    if let Some(path) = &config.output_file_path {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await?;
        f.write_all_buf(&mut resp_bytes).await?;
    } else {
        if config.output_format.is_parquet() {
            Err(Error::NoOutputFileForParquet)?
        }
        println!("{}", std::str::from_utf8(&resp_bytes)?);
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryError {
    #[error("no query provided")]
    NoQuery,

    #[error(
        "ensure that a single query string is provided as the final \
        argument, enclosed in quotes"
    )]
    MoreThanOne,
}

/// Parse the user-inputted query string
fn parse_query(mut input: Vec<String>) -> Result<String> {
    if input.is_empty() {
        Err(QueryError::NoQuery)?
    }
    if input.len() > 1 {
        Err(QueryError::MoreThanOne)?
    } else {
        Ok(input.remove(0))
    }
}
