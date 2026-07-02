#![forbid(unsafe_code)]

use serde::Serialize;
use serving::{serve_until_shutdown, GatewayConfig};
use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use thiserror::Error;

const DEFAULT_CONFIG_ENV: &str = "AJAR_GATEWAY_CONFIG";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log_event(
                "error",
                "gateway_start_failed",
                BTreeMap::from([("error".to_owned(), error.public_message())]),
            );
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), GatewayBinaryError> {
    let config_path = parse_config_path(env::args_os().skip(1), env::var_os(DEFAULT_CONFIG_ENV))?;
    let config = load_config(&config_path)?;
    log_event(
        "info",
        "gateway_starting",
        BTreeMap::from([("config_path".to_owned(), config_path.display().to_string())]),
    );

    serve_until_shutdown(config, shutdown_signal()).await?;

    log_event("info", "gateway_stopped", BTreeMap::new());
    Ok(())
}

fn parse_config_path(
    mut args: impl Iterator<Item = OsString>,
    env_path: Option<OsString>,
) -> Result<PathBuf, GatewayBinaryError> {
    let mut config_path = env_path.map(PathBuf::from);

    while let Some(arg) = args.next() {
        if arg == "--config" {
            let value = args.next().ok_or(GatewayBinaryError::MissingConfigValue)?;
            config_path = Some(PathBuf::from(value));
        } else {
            return Err(GatewayBinaryError::UnknownArgument(
                arg.to_string_lossy().into_owned(),
            ));
        }
    }

    config_path.ok_or(GatewayBinaryError::MissingConfigPath)
}

fn load_config(path: &Path) -> Result<GatewayConfig, GatewayBinaryError> {
    let contents = fs::read_to_string(path).map_err(GatewayBinaryError::ConfigRead)?;
    let config =
        toml::from_str::<GatewayConfig>(&contents).map_err(GatewayBinaryError::ConfigParse)?;
    config.validate()?;
    Ok(config)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(_) => {
                let _ctrl_c_result = tokio::signal::ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ctrl_c_result = tokio::signal::ctrl_c().await;
    }
}

#[derive(Debug, Error)]
enum GatewayBinaryError {
    #[error("missing config path")]
    MissingConfigPath,
    #[error("missing value after --config")]
    MissingConfigValue,
    #[error("unknown argument: {0}")]
    UnknownArgument(String),
    #[error("failed to read config")]
    ConfigRead(#[source] std::io::Error),
    #[error("failed to parse config")]
    ConfigParse(#[source] toml::de::Error),
    #[error("serving failed")]
    Serving(#[from] serving::ServingError),
}

impl GatewayBinaryError {
    fn public_message(&self) -> String {
        match self {
            Self::MissingConfigPath => {
                "missing config path; set AJAR_GATEWAY_CONFIG or pass --config".to_owned()
            }
            Self::MissingConfigValue => "missing value after --config".to_owned(),
            Self::UnknownArgument(argument) => format!("unknown argument: {argument}"),
            Self::ConfigRead(_) => "failed to read config".to_owned(),
            Self::ConfigParse(_) => "failed to parse config".to_owned(),
            Self::Serving(error) => error.to_string(),
        }
    }
}

#[derive(Serialize)]
struct LogLine<'a> {
    level: &'a str,
    event: &'a str,
    fields: BTreeMap<String, String>,
}

fn log_event(level: &str, event: &str, fields: BTreeMap<String, String>) {
    let line = LogLine {
        level,
        event,
        fields,
    };
    match serde_json::to_string(&line) {
        Ok(encoded) => eprintln!("{encoded}"),
        Err(_) => {
            eprintln!("{{\"level\":\"error\",\"event\":\"logger_encode_failed\",\"fields\":{{}}}}")
        }
    }
}
