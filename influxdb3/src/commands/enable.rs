use crate::commands::common::InfluxDb3Config;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use miette::{IntoDiagnostic, Result};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client> {
        let (host_url, auth_token) = match &self.cmd {
            SubCommand::Trigger(TriggerConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => (host_url, auth_token),
        };
        let mut client = Client::new(host_url.clone()).into_diagnostic()?;
        if let Some(token) = &auth_token {
            client = client.with_auth_token(token.expose_secret());
        }
        Ok(client)
    }
}

#[derive(Debug, clap::Subcommand)]
enum SubCommand {
    /// Enable a trigger to enable plugin execution
    Trigger(TriggerConfig),
}

#[derive(Debug, clap::Parser)]
struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of trigger to enable
    #[clap(required = true)]
    trigger_name: String,
}

pub async fn command(config: Config) -> Result<()> {
    let client = config.get_client()?;
    match config.cmd {
        SubCommand::Trigger(TriggerConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            trigger_name,
        }) => {
            client
                .api_v3_configure_processing_engine_trigger_enable(database_name, &trigger_name)
                .await.into_diagnostic()?;
            println!("Trigger {} enabled successfully", trigger_name);
        }
    }
    Ok(())
}
