use clap::Parser;
use secrecy::{ExposeSecret, Secret};
use std::io;
use std::str::Utf8Error;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::commands::common::Format;
use system::Error as SystemCommandError;
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),

    #[error(
        "must specify an output file path with `--output` parameter when formatting \
        the output as `parquet`"
    )]
    NoOutputFileForParquet,

    #[error("invalid UTF8 received from server: {0}")]
    Utf8(#[from] Utf8Error),

    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    SystemCommand(#[from] SystemCommandError),
}

mod system;
use system::SystemConfig;

#[derive(Debug, Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, Parser)]
pub enum SubCommand {
    /// List databases
    Databases(DatabaseConfig),

    /// Display system table data.
    System(SystemConfig),
}

#[derive(Debug, Parser)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    auth_token: Option<Secret<String>>,

    /// Include databases that were marked as deleted in the output
    #[clap(long = "show-deleted", default_value = "false")]
    show_deleted: bool,

    /// The format in which to output the list of databases
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// Put the list of databases into `output`
    #[clap(short = 'o', long = "output")]
    output_file_path: Option<String>,
}

pub(crate) async fn command(config: Config) -> Result<(), Error> {
    match config.cmd {
        SubCommand::Databases(DatabaseConfig {
            host_url,
            auth_token,
            show_deleted,
            output_format,
            output_file_path,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;

            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }

            let mut resp_bytes = client
                .api_v3_configure_db_show()
                .with_format(output_format.clone().into())
                .with_show_deleted(show_deleted)
                .send()
                .await?;

            if let Some(path) = output_file_path {
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)
                    .await?;
                f.write_all_buf(&mut resp_bytes).await?;
            } else {
                if output_format.is_parquet() {
                    Err(Error::NoOutputFileForParquet)?
                }
                println!("{}", std::str::from_utf8(&resp_bytes)?);
            }
        }
        SubCommand::System(cfg) => system::command(cfg).await?,
    }

    Ok(())
}
