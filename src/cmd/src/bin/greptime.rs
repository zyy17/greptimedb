// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![doc = include_str!("../../../../README.md")]

use clap::{Parser, Subcommand};
use cmd::error::Result;
use cmd::options::GlobalOptions;
use cmd::{cli, datanode, frontend, log_versions, metasrv, standalone, start_app, App};
use common_version::{short_version, version};

#[derive(Parser)]
#[command(name = "greptime", author, version, long_version = version!(), about)]
#[command(propagate_version = true)]
pub(crate) struct Command {
    #[clap(subcommand)]
    pub(crate) subcmd: SubCommand,

    #[clap(flatten)]
    pub(crate) global_options: GlobalOptions,
}

#[derive(Subcommand)]
enum SubCommand {
    /// Start datanode service.
    #[clap(name = "datanode")]
    Datanode(datanode::Command),

    /// Start frontend service.
    #[clap(name = "frontend")]
    Frontend(frontend::Command),

    /// Start metasrv service.
    #[clap(name = "metasrv")]
    Metasrv(metasrv::Command),

    /// Run greptimedb as a standalone service.
    #[clap(name = "standalone")]
    Standalone(standalone::Command),

    /// Execute the cli tools for greptimedb.
    #[clap(name = "cli")]
    Cli(cli::Command),
}

impl SubCommand {
    async fn build(&self, global_options: &GlobalOptions) -> Result<Box<dyn App>> {
        match self {
            SubCommand::Datanode(cmd) => cmd
                .build(cmd.load_options(global_options)?)
                .await
                .map(|x| Box::new(x) as _),
            SubCommand::Frontend(cmd) => cmd
                .build(cmd.load_options(global_options)?)
                .await
                .map(|x| Box::new(x) as _),
            SubCommand::Metasrv(cmd) => cmd
                .build(cmd.load_options(global_options)?)
                .await
                .map(|x| Box::new(x) as _),
            SubCommand::Standalone(cmd) => cmd
                .build(cmd.load_options(global_options)?)
                .await
                .map(|x| Box::new(x) as _),
            SubCommand::Cli(cmd) => cmd.build().await.map(|x| Box::new(x) as _),
        }
    }
}

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    setup_human_panic();
    start(Command::parse()).await
}

async fn start(cli: Command) -> Result<()> {
    log_versions(version!(), short_version!());
    start_app(cli.subcmd.build(&cli.global_options).await?).await
}

fn setup_human_panic() {
    let metadata = human_panic::Metadata {
        version: env!("CARGO_PKG_VERSION").into(),
        name: "GreptimeDB".into(),
        authors: Default::default(),
        homepage: "https://github.com/GreptimeTeam/greptimedb/discussions".into(),
    };
    human_panic::setup_panic!(metadata);

    common_telemetry::set_panic_hook();
}
