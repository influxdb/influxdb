use super::super::common::{Format, InfluxDb3Config};
use clap::Parser;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::io;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("client error: {0}")]
    InfluxDB3Client(#[from] influxdb3_client::Error),

    #[error("deserializing show tables: {0}")]
    DeserializingShowTables(#[source] serde_json::Error),

    #[error("system table '{0}' not found: {1}")]
    SystemTableNotFound(String, SystemTableNotFound),

    #[error(
        "must specify an output file path with `--output` parameter when formatting \
        the output as `parquet`"
    )]
    NoOutputFileForParquet,

    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "s", trailing_var_arg = true)]
pub struct SystemConfig {
    #[clap(subcommand)]
    subcommand: SubCommand,

    /// Common InfluxDB 3 Core config
    #[clap(flatten)]
    core_config: InfluxDb3Config,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// List available system tables for the connected host.
    TableList(TableListConfig),
    /// Retrieve entries from a specific system table.
    Table(TableConfig),
    /// Summarize various types of system table data.
    Summary(SummaryConfig),
}

pub async fn command(config: SystemConfig) -> Result<()> {
    let mut client = Client::new(config.core_config.host_url.clone())?;
    if let Some(token) = config
        .core_config
        .auth_token
        .as_ref()
        .map(ExposeSecret::expose_secret)
    {
        client = client.with_auth_token(token);
    }

    let runner = SystemCommandRunner {
        client,
        db: config.core_config.database_name.clone(),
    };
    match config.subcommand {
        SubCommand::Table(cfg) => runner.get(cfg).await,
        SubCommand::TableList(cfg) => runner.list(cfg).await,
        SubCommand::Summary(cfg) => runner.summary(cfg).await,
    }
}

struct SystemCommandRunner {
    client: Client,
    db: String,
}

#[derive(Debug, Deserialize)]
struct ShowTablesRow {
    table_name: String,
}

#[derive(Debug, Parser)]
pub struct TableListConfig {
    /// The format in which to output the query
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// Put the table lists output into `output`
    #[clap(short = 'o', long = "output")]
    output_file_path: Option<String>,
}

const SYS_TABLES_QUERY: &str = "WITH cols (table_name, column_name) AS (SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'system' ORDER BY (table_name, column_name)) SELECT table_name, array_agg(column_name) AS column_names FROM cols GROUP BY table_name ORDER BY table_name";

#[derive(Debug)]
pub struct SystemTableNotFound {
    system_tables: Vec<ShowTablesRow>,
}

impl std::fmt::Display for SystemTableNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let system_tables: Vec<String> =
            self.system_tables.iter().fold(Vec::new(), |mut acc, v| {
                acc.push(v.table_name.clone());
                acc
            });
        writeln!(f, "please use a valid system table name: {system_tables:?}")?;
        Ok(())
    }
}

impl SystemCommandRunner {
    async fn list(&self, config: TableListConfig) -> Result<()> {
        let TableListConfig {
            output_format,
            output_file_path,
        } = &config;

        let mut bs = self
            .client
            .api_v3_query_sql(self.db.as_str(), SYS_TABLES_QUERY)
            .format(output_format.clone().into())
            .send()
            .await?;

        if let Some(path) = output_file_path {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .await?;
            f.write_all_buf(&mut bs).await?;
        } else {
            if output_format.is_parquet() {
                Err(Error::NoOutputFileForParquet)?
            }
            println!("{}", String::from_utf8(bs.as_ref().to_vec()).unwrap());
        }

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct TableConfig {
    /// The system table to query.
    system_table: String,

    /// The maximum number of table entries to display in the output. Default is 100 and 0 can be
    /// passed to indicate no limit.
    #[clap(long = "limit", short = 'l', default_value_t = 100)]
    limit: u16,

    /// Order by the specified fields.
    #[clap(long = "order-by", num_args = 1, value_delimiter = ',')]
    order_by: Vec<String>,

    /// Select specified fields from table.
    #[clap(long = "select", short = 's', num_args = 1, value_delimiter = ',')]
    select: Vec<String>,

    /// The format in which to output the query
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// Put the table output into `output`
    #[clap(short = 'o', long = "output")]
    output_file_path: Option<String>,
}

impl SystemCommandRunner {
    async fn get_system_tables(&self) -> Result<Vec<ShowTablesRow>> {
        let bs = self
            .client
            .api_v3_query_sql(self.db.as_str(), SYS_TABLES_QUERY)
            .format(Format::Json.into())
            .send()
            .await?;

        serde_json::from_slice::<Vec<ShowTablesRow>>(bs.as_ref())
            .map_err(Error::DeserializingShowTables)
    }

    async fn get(&self, config: TableConfig) -> Result<()> {
        let Self { client, db } = self;
        let TableConfig {
            system_table: system_table_name,
            limit,
            select,
            order_by,
            output_format,
            output_file_path,
        } = &config;

        let select_expr = if !select.is_empty() {
            select.join(",")
        } else {
            "*".to_string()
        };

        let mut clauses = vec![format!(
            "SELECT {select_expr} FROM system.{system_table_name}"
        )];

        if let Some(default_filter) = default_filter(system_table_name) {
            clauses.push(format!("WHERE {default_filter}"));
        }

        if !order_by.is_empty() {
            clauses.push(format!("ORDER BY {}", order_by.join(",")));
        } else if let Some(default_ordering) = default_ordering(system_table_name) {
            clauses.push(format!("ORDER BY {default_ordering}"));
        }

        if *limit > 0 {
            clauses.push(format!("LIMIT {limit}"));
        }

        let query = clauses.join("\n");

        let mut bs = match client
            .api_v3_query_sql(db, query)
            .format(output_format.clone().into())
            .send()
            .await
        {
            Ok(bs) => bs,
            Err(e) => {
                if matches!(e, influxdb3_client::Error::ApiError { ref message, .. } if message.contains("not found"))
                {
                    let system_tables = self.get_system_tables().await?;
                    return Err(Error::SystemTableNotFound(
                        system_table_name.to_string(),
                        SystemTableNotFound { system_tables },
                    ));
                }
                return Err(e.into());
            }
        };

        if let Some(path) = output_file_path {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .await?;
            f.write_all_buf(&mut bs).await?;
        } else {
            if output_format.is_parquet() {
                Err(Error::NoOutputFileForParquet)?
            }
            println!("{}", String::from_utf8(bs.as_ref().to_vec()).unwrap());
        }
        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct SummaryConfig {
    /// The maximum number of entries from each table to display in the output. Default is 10 and 0
    /// can be passed to indicate no limit.
    #[clap(long = "limit", short = 'l', default_value_t = 10)]
    limit: u16,

    /// The format in which to output the query
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// Put the summary into `output`
    #[clap(short = 'o', long = "output")]
    output_file_path: Option<String>,
}

impl SystemCommandRunner {
    async fn summary(&self, config: SummaryConfig) -> Result<()> {
        self.summarize_all_tables(
            config.limit,
            &config.output_format,
            &config.output_file_path,
        )
        .await?;
        Ok(())
    }

    async fn summarize_all_tables(
        &self,
        limit: u16,
        format: &Format,
        output_file_path: &Option<String>,
    ) -> Result<()> {
        let system_tables = self.get_system_tables().await?;
        for table in system_tables {
            self.summarize_table(table.table_name.as_str(), limit, format, output_file_path)
                .await?;
        }
        Ok(())
    }

    async fn summarize_table(
        &self,
        table_name: &str,
        limit: u16,
        format: &Format,
        output_file_path: &Option<String>,
    ) -> Result<()> {
        let Self { db, client } = self;
        let mut clauses = vec![format!("SELECT * FROM system.{table_name}")];

        if let Some(default_filter) = default_filter(table_name) {
            clauses.push(format!("WHERE {default_filter}"));
        }

        if let Some(default_ordering) = default_ordering(table_name) {
            clauses.push(format!("ORDER BY {default_ordering}"));
        }

        if limit > 0 {
            clauses.push(format!("LIMIT {limit}"));
        }

        let query = clauses.join("\n");

        let mut bs = client
            .api_v3_query_sql(db, query)
            .format(format.clone().into())
            .send()
            .await?;

        if let Some(path) = output_file_path {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .await?;
            f.write_all_buf(&mut bs).await?;
        } else {
            if format.is_parquet() {
                Err(Error::NoOutputFileForParquet)?
            }

            println!("{table_name} summary:");
            println!("{}", String::from_utf8(bs.as_ref().to_vec()).unwrap());
        }

        Ok(())
    }
}

fn default_ordering(table_name: &str) -> Option<String> {
    match table_name {
        "cpu" => Some("usage_percent"),
        "last_caches" => Some("count"),
        "parquet_files" => Some("size_bytes"),
        "queries" => Some("end2end_duration"),
        "distinct_caches" => Some("max_cardinality"),
        _ => None,
    }
    .map(ToString::to_string)
}

fn default_filter(table_name: &str) -> Option<String> {
    match table_name {
        "queries" => Some("query_text !~ '.*(select.queries.|information_schema)*'"),
        _ => None,
    }
    .map(ToString::to_string)
}
