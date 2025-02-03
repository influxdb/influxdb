use influxdb3_clap_blocks::plugins::ProcessingEngineConfig;
use influxdb3_client::Client;
use miette::{IntoDiagnostic, Result};
use secrecy::{ExposeSecret, Secret};
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Install packages within the plugin environment
    Package(PackageConfig),
}

pub async fn command(config: Config) -> Result<()> {
    match config.cmd {
        SubCommand::Package(package_config) => package_config.run_command().await,
    }
}

#[derive(Debug, clap::Args)]
pub struct PackageConfig {
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

    /// The processing engine config.
    #[clap(flatten)]
    pub processing_engine_config: ProcessingEngineConfig,

    /// Path to requirements.txt file
    #[arg(short = 'r', long = "requirements")]
    requirements: Option<String>,

    /// Package names to install
    #[arg(required_unless_present = "requirements")]
    packages: Vec<String>,
}

impl PackageConfig {
    async fn run_command(&self) -> Result<()> {
        let mut client = Client::new(self.host_url.clone()).into_diagnostic()?;
        if let Some(token) = &self.auth_token {
            client = client.with_auth_token(token.expose_secret());
        }
        if let Some(requirements_path) = &self.requirements {
            client
                .api_v3_configure_processing_engine_trigger_install_requirements(requirements_path)
                .await
                .into_diagnostic()?;
        } else {
            client
                .api_v3_configure_plugin_environment_install_packages(self.packages.clone())
                .await
                .into_diagnostic()?;
        }
        Ok(())
    }
}
