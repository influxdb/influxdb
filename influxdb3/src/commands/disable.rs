use crate::commands::common::InfluxDb3Config;
use influxdb3_client::Client;
use miette::{IntoDiagnostic, Result};
use secrecy::ExposeSecret;

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
    /// Disable a plugin trigger
    Trigger(TriggerConfig),
}

#[derive(Debug, clap::Parser)]
struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of trigger to disable
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
                .api_v3_configure_processing_engine_trigger_disable(database_name, &trigger_name)
                .await
                .into_diagnostic()?;
            println!("Trigger {} disabled successfully", trigger_name);
        }
    }
    Ok(())
}
