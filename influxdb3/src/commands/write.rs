use std::{
    fs,
    io::{stdin, BufReader, IsTerminal, Read},
};

use clap::Parser;
use influxdb3_client::Precision;
use secrecy::ExposeSecret;
use tokio::io;

use super::common::InfluxDb3Config;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),

    #[error("error reading file")]
    Io(#[from] io::Error),

    #[error("No input from stdin detected, no string was passed in, and no file path was given")]
    NoInput,

    #[error("no line protocol string provided")]
    NoLine,

    #[error(
        "ensure that a single protocol line string is provided as the final \
        argument, enclosed in quotes"
    )]
    MoreThanOne,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "w", trailing_var_arg = true)]
pub struct Config {
    /// Common InfluxDB 3 Core config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// File path to load the write data from
    ///
    /// Currently, only files containing line protocol are supported.
    #[clap(short = 'f', long = "file")]
    file_path: Option<String>,

    /// Flag to request the server accept partial writes
    ///
    /// Invalid lines in the input data will be ignored by the server.
    #[clap(long = "accept-partial")]
    accept_partial_writes: bool,

    /// Give a quoted line protocol line via the command line
    line_protocol: Option<Vec<String>>,

    /// Specify a supported precision (eg: ns, us, ms, s).
    #[clap(short = 'p', long = "precision")]
    precision: Option<Precision>,
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

    let writes = if let Some(line) = config.line_protocol {
        parse_line(line)?
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

    let mut req = client.api_v3_write_lp(database_name);
    if let Some(precision) = config.precision {
        req = req.precision(precision);
    }
    if config.accept_partial_writes {
        req = req.accept_partial(true);
    }
    req.body(writes).send().await?;

    println!("success");

    Ok(())
}

/// Parse the user-inputted line protocol string
/// NOTE: This is only necessary because clap will not accept a single string for a trailing arg
fn parse_line(mut input: Vec<String>) -> Result<String> {
    if input.is_empty() {
        Err(Error::NoLine)?
    }
    if input.len() > 1 {
        Err(Error::MoreThanOne)?
    } else {
        Ok(input.remove(0))
    }
}
