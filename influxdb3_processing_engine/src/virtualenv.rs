use miette::Diagnostic;
use observability_deps::tracing::debug;
use std::path::Path;
use std::process::Command;
use std::sync::Once;
use thiserror::Error;

static PYTHON_INIT: Once = Once::new();

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_processing_engine::virtualenv),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum VenvError {
    #[error("Failed to initialize virtualenv")]
    InitError(String),
    #[error("Error shelling out")]
    CommandError(#[from] std::io::Error),
}

fn get_python_version() -> Result<(u8, u8), std::io::Error> {
    let output = Command::new("python3")
        .args([
            "-c",
            "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
        ])
        .output()?;

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let mut parts = version.split('.');
    let major: u8 = parts.next().unwrap().parse().unwrap();
    let minor: u8 = parts.next().unwrap().parse().unwrap();

    Ok((major, minor))
}

fn set_pythonpath(venv_dir: &Path) -> Result<(), std::io::Error> {
    let (major, minor) = get_python_version()?;
    let site_packages = venv_dir
        .join("lib")
        .join(format!("python{}.{}", major, minor))
        .join("site-packages");

    debug!("Setting PYTHONPATH to: {}", site_packages.to_string_lossy());
    std::env::set_var("PYTHONPATH", &site_packages);

    Ok(())
}

pub fn init_pyo3() {
    PYTHON_INIT.call_once(|| {
        pyo3::prepare_freethreaded_python();
    })
}
#[cfg(unix)]
pub(crate) fn initialize_venv(venv_path: &Path) -> Result<(), VenvError> {
    use std::process::Command;

    let activate_script = venv_path.join("bin").join("activate");
    if !activate_script.exists() {
        return Err(VenvError::InitError(format!(
            "Activation script not found at {:?}",
            activate_script
        )));
    }
    set_pythonpath(venv_path)?;

    let output = Command::new("bash")
        .arg("-c")
        .arg(format!(
            "source {} && env",
            activate_script.to_str().unwrap()
        ))
        .output()?;

    if !output.status.success() {
        return Err(VenvError::InitError(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    // Apply environment changes
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .for_each(|(key, value)| std::env::set_var(key, value));

    Ok(())
}
