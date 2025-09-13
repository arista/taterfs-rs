use clap::{Parser, Subcommand, Args, CommandFactory};
use crate::context;
use crate::prelude::*;
use crate::cmd;

#[derive(Parser)]
#[command(name = "tfs", version, about = "FIXME - Example CLI", arg_required_else_help = true)]
pub struct Cli {
    /// Increase output verbosity (-v, -vv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Output as JSON where supported
    #[arg(long)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub async fn run(&self, ctx: &context::Context) -> Result<()> {
        Ok(self.command.run(ctx).await?)
    }
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(subcommand)]
    Test(test::Commands),

    /// Generate shell completions
    Completions(CompletionsArgs),
}

impl Commands {
    pub async fn run(&self, ctx: &context::Context) -> Result<()> {
        Ok(match self {
            Commands::Test(a) => a.run(&ctx).await?,
            Commands::Completions(a) => {
            use clap_complete::{generate};
            use std::io;
            let mut cmd = Cli::command();
            generate(a.shell, &mut cmd, "tfs", &mut io::stdout());
        }
        })
    }
}

pub mod test {
    use super::*;

    #[derive(Subcommand)]
    pub enum Commands {
        ListDirectory(ListDirectoryArgs),
    }

    impl Commands {
        pub async fn run(&self, ctx: &context::Context) -> Result<()> {
            Ok(match self {
                Commands::ListDirectory(a) => cmd::test::list_directory::handle(&ctx, &a).await?
            })
        }
    }

    #[derive(Args)]
    pub struct ListDirectoryArgs {
        #[arg(long)]
        pub path: String,
    }
}

#[derive(Args)]
pub struct CompletionsArgs {
    /// bash, zsh, fish, powershell, elvish
    pub shell: clap_complete::Shell,
}
