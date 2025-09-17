// Quiet in debug builds:
#![cfg_attr(debug_assertions, allow(unused_imports, unused_variables, dead_code))]
// Strict in non-debug (e.g., release/CI):
#![cfg_attr(not(debug_assertions), deny(warnings))]

mod cli;
mod cmd;
mod context;
mod file_source;
mod file_store;
mod prelude;
mod repo;
mod util;

use crate::prelude::*;
use clap::Parser;
use tracing_subscriber::EnvFilter;

fn init_tracing(verbosity: u8) {
    let base = match verbosity {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(base));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();
    init_tracing(cli.verbose);

    let ctx = context::Context::load().context("loading config")?;
    Ok(cli.run(&ctx).await?)
}
