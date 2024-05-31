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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use common_base::readable_size::ReadableSize;
use common_config::Configurable;
use common_telemetry::info;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_version::{short_version, version};
use common_wal::config::raft_engine::RaftEngineConfig;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use mito2::config::MitoConfig;
use mito2::engine::{MitoEngine, MITO_ENGINE_NAME};
use object_store::manager::ObjectStoreManager;
use object_store::services::Fs;
use object_store::util::join_dir;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::path_utils::region_dir;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCompactRequest, RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::LoadLayeredConfigSnafu;
use crate::options::GlobalOptions;
use crate::{log_versions, App, Result};

pub const APP_NAME: &str = "greptime-compactor";

pub struct Instance {
    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    pub fn new(guard: Vec<WorkerGuard>) -> Self {
        Self { _guard: guard }
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(&self, opts: CompactorOption) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<CompactorOption> {
        self.subcmd.load_options(global_options)
    }
}

impl Configurable<'_> for CompactorOption {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CompactorOption {
    pub logging: LoggingOptions,
    pub tracing: TracingOptions,
}

impl Default for CompactorOption {
    fn default() -> Self {
        Self {
            logging: LoggingOptions::default(),
            tracing: TracingOptions::default(),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(&self, opts: CompactorOption) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(&self, global_options: &GlobalOptions) -> Result<CompactorOption> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(global_options),
        }
    }
}

#[derive(Debug, Parser, Default)]
struct StartCommand {
    #[clap(short, long)]
    pub config_file: Option<String>,

    #[clap(long, default_value = "GREPTIMEDB_COMPACTOR")]
    pub env_prefix: String,
}

impl StartCommand {
    fn load_options(&self, global_options: &GlobalOptions) -> Result<CompactorOption> {
        self.merge_with_cli_options(
            global_options,
            CompactorOption::load_layered_options(
                self.config_file.as_deref(),
                self.env_prefix.as_ref(),
            )
            .context(LoadLayeredConfigSnafu)?,
        )
    }

    // The precedence order is: cli > config file > environment variables > default values.
    pub fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        mut opts: CompactorOption,
    ) -> Result<CompactorOption> {
        if let Some(dir) = &global_options.log_dir {
            opts.logging.dir.clone_from(dir);
        }

        if global_options.log_level.is_some() {
            opts.logging.level.clone_from(&global_options.log_level);
        }

        opts.tracing = TracingOptions {
            #[cfg(feature = "tokio-console")]
            tokio_console_addr: global_options.tokio_console_addr.clone(),
        };

        Ok(opts)
    }

    async fn build(&self, opts: CompactorOption) -> Result<Instance> {
        let guard =
            common_telemetry::init_global_logging(APP_NAME, &opts.logging, &opts.tracing, None);
        log_versions(version!(), short_version!());

        let data_home = "/tmp/greptimedb";
        let catalog = "greptime";
        let schema = "public";
        let region_id = RegionId::from_u64(4398046511104);

        let object_store_manager = Arc::new(create_object_store_manager(data_home));
        let log_store = Arc::new(create_log_stre(data_home).await);

        let engine = MitoEngine::new(
            data_home,
            MitoConfig::default(),
            log_store,
            object_store_manager,
        )
        .await
        .unwrap();

        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: MITO_ENGINE_NAME.to_string(),
                    region_dir: region_dir(format!("{}/{}", catalog, schema).as_str(), region_id),
                    options: HashMap::default(),
                    skip_wal_replay: true,
                }),
            )
            .await
            .unwrap();

        engine.set_writable(region_id, true).unwrap();

        let result = engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
            .unwrap();

        info!("compaction result: {:?}", result);

        Ok(Instance::new(guard))
    }
}

async fn create_log_stre(data_home: &str) -> RaftEngineLogStore {
    let wal_path = Path::new(data_home)
        .join("wal")
        .to_string_lossy()
        .into_owned();
    let cfg = RaftEngineConfig {
        file_size: ReadableSize::kb(128),
        ..Default::default()
    };
    RaftEngineLogStore::try_new(wal_path, cfg).await.unwrap()
}

fn create_object_store_manager(data_home: &str) -> ObjectStoreManager {
    let atomic_write_dir = join_dir(data_home, ".tmp/");
    let mut builder = Fs::default();
    let _ = builder.root(data_home).atomic_write_dir(&atomic_write_dir);
    let object_store = ObjectStore::new(builder).unwrap().finish();
    ObjectStoreManager::new("default", object_store)
}
