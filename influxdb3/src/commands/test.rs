use crate::commands::common::{InfluxDb3Config, SeparatedKeyValue, SeparatedList};
use hashbrown::HashMap;
use influxdb3_client::plugin_development::{SchedulePluginTestRequest, WalPluginTestRequest};
use influxdb3_client::Client;
use miette::{IntoDiagnostic, Result, WrapErr};
use secrecy::ExposeSecret;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client> {
        match &self.cmd {
            SubCommand::WalPlugin(WalPluginConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::SchedulePlugin(SchedulePluginConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => {
                let mut client = Client::new(host_url.clone()).into_diagnostic()?;
                if let Some(token) = &auth_token {
                    client = client.with_auth_token(token.expose_secret());
                }
                Ok(client)
            }
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Test a WAL Plugin
    #[clap(name = "wal_plugin")]
    WalPlugin(WalPluginConfig),
    /// Test a Cron Plugin
    #[clap(name = "schedule_plugin")]
    SchedulePlugin(SchedulePluginConfig),
}

#[derive(Debug, clap::Parser)]
pub struct WalPluginConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    /// If given, pass this line protocol as input
    #[clap(long = "lp")]
    pub input_lp: Option<String>,
    /// If given, pass this file of LP as input from on the server `<plugin-dir>/<name>_test/<input-file>`
    #[clap(long = "file")]
    pub input_file: Option<String>,
    /// If given pass this map of string key/value pairs as input arguments
    #[clap(long = "input-arguments")]
    pub input_arguments: Option<SeparatedList<SeparatedKeyValue<String, String>>>,
    /// The file name of the plugin, which should exist on the server in `<plugin-dir>/<filename>`.
    /// The plugin-dir is provided on server startup.
    #[clap(required = true)]
    pub filename: String,
}

#[derive(Debug, clap::Parser)]
pub struct SchedulePluginConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    /// If given pass this map of string key/value pairs as input arguments
    #[clap(long = "input-arguments")]
    pub input_arguments: Option<SeparatedList<SeparatedKeyValue<String, String>>>,
    /// The file name of the plugin, which should exist on the server in `<plugin-dir>/<filename>`.
    /// The plugin-dir is provided on server startup.
    #[clap(required = true)]
    pub filename: String,
    /// Cron schedule to test against. If not given will use * * * * *
    #[clap(long = "schedule")]
    pub schedule: Option<String>,
}

pub async fn command(config: Config) -> Result<()> {
    let client = config.get_client()?;

    match config.cmd {
        SubCommand::WalPlugin(plugin_config) => {
            let input_arguments = plugin_config.input_arguments.map(|a| {
                a.into_iter()
                    .map(|SeparatedKeyValue((k, v))| (k, v))
                    .collect::<HashMap<String, String>>()
            });

            let input_lp = match plugin_config.input_lp {
                Some(lp) => lp,
                None => {
                    let file_path = plugin_config
                        .input_file
                        .wrap_err("either --lp or --file must be provided")?;
                    std::fs::read_to_string(file_path)
                        .into_diagnostic()
                        .wrap_err("unable to read input file")?
                }
            };

            let wal_plugin_test_request = WalPluginTestRequest {
                filename: plugin_config.filename,
                database: plugin_config.influxdb3_config.database_name,
                input_lp,
                input_arguments,
            };

            let response = client
                .wal_plugin_test(wal_plugin_test_request)
                .await
                .into_diagnostic()?;

            println!(
                "{}",
                serde_json::to_string_pretty(&response)
                    .expect("serialize wal plugin test response as JSON")
            );
        }
        SubCommand::SchedulePlugin(plugin_config) => {
            let input_arguments = plugin_config.input_arguments.map(|a| {
                a.into_iter()
                    .map(|SeparatedKeyValue((k, v))| (k, v))
                    .collect::<HashMap<String, String>>()
            });
            let cron_plugin_test_request = SchedulePluginTestRequest {
                filename: plugin_config.filename,
                database: plugin_config.influxdb3_config.database_name,
                schedule: plugin_config.schedule,
                input_arguments,
            };
            let response = client
                .schedule_plugin_test(cron_plugin_test_request)
                .await
                .into_diagnostic()?;

            println!(
                "{}",
                serde_json::to_string_pretty(&response)
                    .expect("serialize cron plugin test response as JSON")
            );
        }
    }

    Ok(())
}
