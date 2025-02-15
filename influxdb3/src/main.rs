//! Entrypoint of the influxdb3 binary
#![recursion_limit = "512"] // required for print_cpu
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
missing_debug_implementations,
clippy::explicit_iter_loop,
clippy::use_self,
clippy::clone_on_ref_ptr,
// See https://github.com/influxdata/influxdb_iox/pull/1671
clippy::future_not_send
)]

use dotenvy::dotenv;
use influxdb3_clap_blocks::tokio::TokioIoConfig;
use influxdb3_process::VERSION_STRING;
use observability_deps::tracing::warn;
#[cfg(feature = "system-py")]
use std::env;
#[cfg(feature = "system-py")]
use std::path::{Path, PathBuf};
use trogging::{
    cli::LoggingConfigBuilderExt,
    tracing_subscriber::{prelude::*, Registry},
    TroggingGuard,
};

pub mod commands {
    pub(crate) mod common;
    pub mod create;
    pub mod delete;
    pub mod disable;
    pub mod enable;
    pub mod install;
    pub mod query;
    pub mod serve;
    pub mod show;
    pub mod test;
    pub mod write;
}

enum ReturnCode {
    Failure = 1,
}

#[derive(Debug, clap::Parser)]
#[clap(
name = "influxdb3",
version = &VERSION_STRING[..],
disable_help_flag = true,
arg(
clap::Arg::new("help")
.short('h')
.long("help")
.help("Print help information")
.action(clap::ArgAction::Help)
.global(true)
),
about = "InfluxDB 3 Core server and command line tools",
long_about = r#"InfluxDB 3 Core server and command line tools

Examples:
    # Run the InfluxDB 3 Core server
    influxdb3 serve --object-store file --data-dir ~/.influxdb3 --node_id my_node_name

    # Display all commands short form
    influxdb3 -h

    # Display all commands long form
    influxdb3 --help

    # Run the InfluxDB 3 Core server with extra verbose logging
    influxdb3 serve -v --object-store file --data-dir ~/.influxdb3 --node_id my_node_name

    # Run InfluxDB 3 Core with full debug logging specified with LOG_FILTER
    LOG_FILTER=debug influxdb3 serve --object-store file --data-dir ~/.influxdb3 --node_id my_node_name
"#
)]
struct Config {
    #[clap(flatten)]
    runtime_config: TokioIoConfig,

    #[clap(subcommand)]
    command: Option<Command>,
}

// Ignoring clippy here since this enum is just used for running
// the CLI command
#[allow(clippy::large_enum_variant)]
#[derive(Debug, clap::Parser)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Enable a resource such as a trigger
    Enable(commands::enable::Config),

    /// Create a resource such as a database or auth token
    Create(commands::create::Config),

    /// Disable a resource such as a trigger
    Disable(commands::disable::Config),

    /// Delete a resource such as a database or table
    Delete(commands::delete::Config),

    /// Perform a query against a running InfluxDB 3 Core server
    Query(commands::query::Config),

    /// Run the InfluxDB 3 Core server
    Serve(commands::serve::Config),

    /// Install packages for the processing engine
    Install(commands::install::Config),

    /// List resources on the InfluxDB 3 Core server
    Show(commands::show::Config),

    /// Test things, such as plugins, work the way you expect
    Test(commands::test::Config),

    /// Perform a set of writes to a running InfluxDB 3 Core server
    Write(commands::write::Config),
}

fn main() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    install_crash_handler(); // attempt to render a useful stacktrace to stderr

    #[cfg(feature = "system-py")]
    set_pythonhome();

    #[cfg(all(target_os = "windows", feature = "system-py"))]
    set_pythonpath();

    // load all environment variables from .env before doing anything
    load_dotenv();

    let config: Config = clap::Parser::parse();

    let tokio_runtime = config.runtime_config.builder()?.build()?;

    tokio_runtime.block_on(async move {
        fn handle_init_logs(r: Result<TroggingGuard, trogging::Error>) -> TroggingGuard {
            match r {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Initializing logs failed: {e}");
                    std::process::exit(ReturnCode::Failure as _);
                }
            }
        }

        match config.command {
            None => println!("command required, -h/--help for help"),
            Some(Command::Enable(config)) => {
                if let Err(e) = commands::enable::command(config).await {
                    eprintln!("Enable command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Create(config)) => {
                if let Err(e) = commands::create::command(config).await {
                    eprintln!("Create command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Disable(config)) => {
                if let Err(e) = commands::disable::command(config).await {
                    eprintln!("Disable command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Delete(config)) => {
                if let Err(e) = commands::delete::command(config).await {
                    eprintln!("Delete command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Serve(config)) => {
                let _tracing_guard =
                    handle_init_logs(init_logs_and_tracing(&config.logging_config));
                if let Err(e) = commands::serve::command(config).await {
                    eprintln!("Serve command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Install(config)) => {
                if let Err(e) = commands::install::command(config).await {
                    eprintln!("Install command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Show(config)) => {
                if let Err(e) = commands::show::command(config).await {
                    eprintln!("Show command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Test(config)) => {
                if let Err(e) = commands::test::command(config).await {
                    eprintln!("Test command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Query(config)) => {
                if let Err(e) = commands::query::command(config).await {
                    eprintln!("Query command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Write(config)) => {
                if let Err(e) = commands::write::command(config).await {
                    eprintln!("Write command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
        }
    });

    Ok(())
}

/// Source the .env file before initialising the Config struct - this sets
/// any envs in the file, which the Config struct then uses.
///
/// Precedence is given to existing env variables.
fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config from: {e}");
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}

// Based on ideas from
// https://github.com/servo/servo/blob/f03ddf6c6c6e94e799ab2a3a89660aea4a01da6f/ports/servo/main.rs#L58-L79
#[cfg(unix)]
fn install_crash_handler() {
    unsafe {
        set_signal_handler(libc::SIGSEGV, signal_handler); // handle segfaults
        set_signal_handler(libc::SIGILL, signal_handler); // handle stack overflow and unsupported CPUs
        set_signal_handler(libc::SIGBUS, signal_handler); // handle invalid memory access
    }
}

#[cfg(unix)]
unsafe extern "C" fn signal_handler(sig: i32) {
    use backtrace::Backtrace;
    use std::process::abort;
    let name = std::thread::current()
        .name()
        .map(|n| format!(" for thread \"{n}\""))
        .unwrap_or_else(|| "".to_owned());
    eprintln!(
        "Signal {}, Stack trace{}\n{:?}",
        sig,
        name,
        Backtrace::new()
    );
    abort();
}

// based on https://github.com/adjivas/sig/blob/master/src/lib.rs#L34-L52
#[cfg(unix)]
unsafe fn set_signal_handler(signal: libc::c_int, handler: unsafe extern "C" fn(libc::c_int)) {
    use libc::{sigaction, sigfillset, sighandler_t};
    let mut sigset = std::mem::zeroed();

    // Block all signals during the handler. This is the expected behavior, but
    // it's not guaranteed by `signal()`.
    if sigfillset(&mut sigset) != -1 {
        // Done because sigaction has private members.
        // This is safe because sa_restorer and sa_handlers are pointers that
        // might be null (that is, zero).
        let mut action: sigaction = std::mem::zeroed();

        // action.sa_flags = 0;
        action.sa_mask = sigset;
        action.sa_sigaction = handler as sighandler_t;

        sigaction(signal, &action, std::ptr::null_mut());
    }
}

fn init_logs_and_tracing(
    config: &trogging::cli::LoggingConfig,
) -> Result<TroggingGuard, trogging::Error> {
    let log_layer = trogging::Builder::new()
        .with_default_log_filter("info")
        .with_logging_config(config)
        .build()?;

    let layers = log_layer;

    // Optionally enable the tokio console exporter layer, if enabled.
    //
    // This spawns a background tokio task to serve the instrumentation data,
    // and hooks the instrumentation into the tracing pipeline.
    #[cfg(feature = "tokio_console")]
    let layers = {
        use console_subscriber::ConsoleLayer;
        let console_layer = ConsoleLayer::builder().with_default_env().spawn();
        layers.and_then(console_layer)
    };

    let subscriber = Registry::default().with(layers);
    trogging::install_global(subscriber)
}

// XXX: this should be somewhere more appropriate
#[cfg(feature = "system-py")]
fn set_pythonhome() {
    // This would ideally be detected by pyo3, but it isn't
    match env::var("PYTHONHOME") {
        Ok(_) => {}
        Err(env::VarError::NotPresent) => {
            let exe_path = env::current_exe().unwrap();
            let exe_dir = exe_path.parent().unwrap();

            let pythonhome: PathBuf = if cfg!(target_os = "linux")
                && (exe_dir == Path::new("/usr/bin") || exe_dir == Path::new("/usr/local/bin"))
            {
                // Official Linux builds may be in /usr or /usr/local
                // XXX: handle this for local build and install (eg DESTDIR)
                let parent_dir = exe_dir.parent().unwrap();
                parent_dir.join("lib/influxdb3/python")
            } else {
                exe_dir.join("python")
            };

            if pythonhome.is_dir() {
                unsafe { env::set_var("PYTHONHOME", pythonhome.to_str().unwrap()) };
                //println!("Set PYTHONHOME to '{}'", env::var("PYTHONHOME").unwrap());
            } else {
                // TODO: use logger
                eprintln!("Could not find python installation. May need to set PYTHONHOME");
            }
        }
        Err(e) => {
            eprintln!("Failed to retrieve PYTHONHOME: {e}");
        }
    };
}

// XXX: this should be somewhere more appropriate
#[cfg(target_os = "windows")]
fn set_pythonpath() {
    use std::env;
    let exe_path = env::current_exe().unwrap();
    let exe_dir = exe_path.parent().unwrap();
    let pythonpath = exe_dir.join("python/Lib");

    // This shouldn't be needed, but it is on Windows
    match env::var("PYTHONPATH") {
        Ok(v) => {
            let new_path = format!("{};{}", pythonpath.display(), v);
            unsafe { env::set_var("PYTHONPATH", &new_path) };
            //println!("Updated PYTHONPATH to: {}", env::var("PYTHONPATH").unwrap());
        }
        Err(env::VarError::NotPresent) => {
            unsafe { env::set_var("PYTHONPATH", &pythonpath) };
            //println!("Updated PYTHONPATH to: {}", env::var("PYTHONPATH").unwrap());
        }
        Err(e) => {
            eprintln!("Failed to retrieve PYTHONPATH: {e}");
        }
    }
}
