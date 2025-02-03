use clap::Parser;
use miette::{IntoDiagnostic, Result};
use secrecy::{ExposeSecret, Secret};
use url::Url;

use crate::commands::common::Format;

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
}

pub(crate) async fn command(config: Config) -> Result<()> {
    match config.cmd {
        SubCommand::Databases(DatabaseConfig {
            host_url,
            auth_token,
            show_deleted,
            output_format,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url).into_diagnostic()?;

            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }

            let resp_bytes = client
                .api_v3_configure_db_show()
                .with_format(output_format.into())
                .with_show_deleted(show_deleted)
                .send()
                .await
                .into_diagnostic()?;

            println!("{}", std::str::from_utf8(&resp_bytes).into_diagnostic()?);
        }
        SubCommand::System(cfg) => system::command(cfg).await.into_diagnostic()?,
    }

    Ok(())
}
