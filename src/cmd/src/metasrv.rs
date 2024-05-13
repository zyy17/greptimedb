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

use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common_config::Configurable;
use common_telemetry::info;
use common_telemetry::logging::TracingOptions;
use meta_srv::bootstrap::MetasrvInstance;
use meta_srv::metasrv::MetasrvOptions;
use snafu::ResultExt;

use crate::error::{
    self, LoadLayeredConfigSnafu, MissingConfigSnafu, Result, StartMetaServerSnafu,
};
use crate::options::GlobalOptions;
use crate::App;

pub struct Instance {
    instance: MetasrvInstance,
}

impl Instance {
    fn new(instance: MetasrvInstance) -> Self {
        Self { instance }
    }
}

pub const APP_NAME: &str = "greptime-metasrv";

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        plugins::start_metasrv_plugins(self.instance.plugins())
            .await
            .context(StartMetaServerSnafu)?;

        self.instance.start().await.context(StartMetaServerSnafu)
    }

    async fn stop(&self) -> Result<()> {
        self.instance
            .shutdown()
            .await
            .context(error::ShutdownMetaServerSnafu)
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub fn new_command_builder(self) -> MetasrvCommandBuilder {
        match self.subcmd {
            SubCommand::Start(cmd) => MetasrvCommandBuilder::default().add_command(cmd),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

#[derive(Debug, Default, Parser)]
struct StartCommand {
    #[clap(long)]
    bind_addr: Option<String>,
    #[clap(long)]
    server_addr: Option<String>,
    #[clap(long)]
    store_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(short, long)]
    selector: Option<String>,
    #[clap(long)]
    use_memory_store: Option<bool>,
    #[clap(long)]
    enable_region_failover: Option<bool>,
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    http_timeout: Option<u64>,
    #[clap(long, default_value = "GREPTIMEDB_METASRV")]
    env_prefix: String,
    /// The working home directory of this metasrv instance.
    #[clap(long)]
    data_home: Option<String>,
    /// If it's not empty, the metasrv will store all data with this key prefix.
    #[clap(long, default_value = "")]
    store_key_prefix: String,
    /// The max operations per txn
    #[clap(long)]
    max_txn_ops: Option<usize>,
}

#[derive(Default)]
pub struct MetasrvCommandBuilder {
    metasrv_options: Option<MetasrvOptions>,
    command: Option<StartCommand>,
}

impl MetasrvCommandBuilder {
    fn add_command(mut self, cmd: StartCommand) -> Self {
        self.command = Some(cmd);
        self
    }

    pub fn build_options(mut self, global_options: &GlobalOptions) -> Result<Self> {
        if let Some(cmd) = &self.command {
            self.metasrv_options = Some(
                self.merge_with_cli_options(
                    global_options,
                    MetasrvOptions::load_layered_options(
                        cmd.config_file.as_deref(),
                        cmd.env_prefix.as_ref(),
                    )
                    .map_err(Box::new)
                    .context(LoadLayeredConfigSnafu)?,
                )?,
            );
            Ok(self)
        } else {
            MissingConfigSnafu {
                msg: "Missing command",
            }
            .fail()
        }
    }

    pub async fn build_app(self) -> Result<Box<dyn App>> {
        if let Some(mut options) = self.metasrv_options {
            let _guard = common_telemetry::init_global_logging(
                APP_NAME,
                &options.logging,
                &options.tracing,
                None,
            );

            let plugins = plugins::setup_metasrv_plugins(&mut options)
                .await
                .context(StartMetaServerSnafu)?;

            info!("Metasrv start command: {:#?}", self.command);
            info!("Metasrv options: {:#?}", options);

            let builder = meta_srv::bootstrap::metasrv_builder(&options, plugins.clone(), None)
                .await
                .context(error::BuildMetaServerSnafu)?;
            let metasrv = builder.build().await.context(error::BuildMetaServerSnafu)?;

            let instance = MetasrvInstance::new(options, plugins, metasrv)
                .await
                .context(error::BuildMetaServerSnafu)?;

            Ok(Box::new(Instance::new(instance)))
        } else {
            MissingConfigSnafu {
                msg: "Missing options",
            }
            .fail()
        }
    }

    pub fn get_options(&self) -> Option<MetasrvOptions> {
        self.metasrv_options.clone()
    }

    // The precedence order is: cli > config file > environment variables > default values.
    fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        mut opts: MetasrvOptions,
    ) -> Result<MetasrvOptions> {
        if let Some(cmd) = &self.command {
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

            if let Some(addr) = &cmd.bind_addr {
                opts.bind_addr.clone_from(addr);
            }

            if let Some(addr) = &cmd.server_addr {
                opts.server_addr.clone_from(addr);
            }

            if let Some(addr) = &cmd.store_addr {
                opts.store_addr.clone_from(addr);
            }

            if let Some(selector_type) = &cmd.selector {
                opts.selector = selector_type[..]
                    .try_into()
                    .context(error::UnsupportedSelectorTypeSnafu { selector_type })?;
            }

            if let Some(use_memory_store) = cmd.use_memory_store {
                opts.use_memory_store = use_memory_store;
            }

            if let Some(enable_region_failover) = cmd.enable_region_failover {
                opts.enable_region_failover = enable_region_failover;
            }

            if let Some(http_addr) = &cmd.http_addr {
                opts.http.addr.clone_from(http_addr);
            }

            if let Some(http_timeout) = cmd.http_timeout {
                opts.http.timeout = Duration::from_secs(http_timeout);
            }

            if let Some(data_home) = &cmd.data_home {
                opts.data_home.clone_from(data_home);
            }

            if !cmd.store_key_prefix.is_empty() {
                opts.store_key_prefix.clone_from(&cmd.store_key_prefix)
            }

            if let Some(max_txn_ops) = cmd.max_txn_ops {
                opts.max_txn_ops = max_txn_ops;
            }

            // Disable dashboard in metasrv.
            opts.http.disable_dashboard = true;

            Ok(opts)
        } else {
            MissingConfigSnafu {
                msg: "Missing command",
            }
            .fail()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use common_base::readable_size::ReadableSize;
    use common_config::ENV_VAR_SEP;
    use common_test_util::temp_dir::create_named_temp_file;
    use meta_srv::selector::SelectorType;

    use super::*;

    fn create_options_by_cmd(cmd: Command) -> Result<MetasrvOptions> {
        let builder = cmd
            .new_command_builder()
            .build_options(&GlobalOptions::default())?;
        Ok(builder.get_options().unwrap())
    }

    #[test]
    fn test_read_from_cmd() {
        let options = create_options_by_cmd(Command {
            subcmd: SubCommand::Start(StartCommand {
                bind_addr: Some("127.0.0.1:3002".to_string()),
                server_addr: Some("127.0.0.1:3002".to_string()),
                store_addr: Some("127.0.0.1:2380".to_string()),
                selector: Some("LoadBased".to_string()),
                ..Default::default()
            }),
        })
        .unwrap();

        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.1:2380".to_string(), options.store_addr);
        assert_eq!(SelectorType::LoadBased, options.selector);
    }

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            bind_addr = "127.0.0.1:3002"
            server_addr = "127.0.0.1:3002"
            store_addr = "127.0.0.1:2379"
            selector = "LeaseBased"
            use_memory_store = false

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"

            [failure_detector]
            threshold = 8.0
            min_std_deviation = "100ms"
            acceptable_heartbeat_pause = "3000ms"
            first_heartbeat_estimate = "1000ms"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let options = create_options_by_cmd(Command {
            subcmd: SubCommand::Start(StartCommand {
                config_file: Some(file.path().to_str().unwrap().to_string()),
                ..Default::default()
            }),
        })
        .unwrap();

        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.1:3002".to_string(), options.server_addr);
        assert_eq!("127.0.0.1:2379".to_string(), options.store_addr);
        assert_eq!(SelectorType::LeaseBased, options.selector);
        assert_eq!("debug", options.logging.level.as_ref().unwrap());
        assert_eq!("/tmp/greptimedb/test/logs".to_string(), options.logging.dir);
        assert_eq!(8.0, options.failure_detector.threshold);
        assert_eq!(
            100.0,
            options.failure_detector.min_std_deviation.as_millis() as f32
        );
        assert_eq!(
            3000,
            options
                .failure_detector
                .acceptable_heartbeat_pause
                .as_millis()
        );
        assert_eq!(
            1000,
            options
                .failure_detector
                .first_heartbeat_estimate
                .as_millis()
        );
        assert_eq!(
            options.procedure.max_metadata_value_size,
            Some(ReadableSize::kb(1500))
        );
    }

    #[test]
    fn test_load_log_options_from_cli() {
        let cmd = Command {
            subcmd: SubCommand::Start(StartCommand {
                bind_addr: Some("127.0.0.1:3002".to_string()),
                server_addr: Some("127.0.0.1:3002".to_string()),
                store_addr: Some("127.0.0.1:2380".to_string()),
                selector: Some("LoadBased".to_string()),
                ..Default::default()
            }),
        };

        let options = cmd
            .new_command_builder()
            .build_options(&GlobalOptions {
                log_dir: Some("/tmp/greptimedb/test/logs".to_string()),
                log_level: Some("debug".to_string()),

                #[cfg(feature = "tokio-console")]
                tokio_console_addr: None,
            })
            .unwrap()
            .get_options()
            .unwrap();

        assert_eq!("/tmp/greptimedb/test/logs", options.logging.dir);
        assert_eq!("debug", options.logging.level.as_ref().unwrap());
    }

    #[test]
    fn test_config_precedence_order() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            server_addr = "127.0.0.1:3002"
            datanode_lease_secs = 15
            selector = "LeaseBased"
            use_memory_store = false

            [http]
            addr = "127.0.0.1:4000"

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let env_prefix = "METASRV_UT";
        temp_env::with_vars(
            [
                (
                    // bind_addr = 127.0.0.1:14002
                    [env_prefix.to_string(), "bind_addr".to_uppercase()].join(ENV_VAR_SEP),
                    Some("127.0.0.1:14002"),
                ),
                (
                    // server_addr = 127.0.0.1:13002
                    [env_prefix.to_string(), "server_addr".to_uppercase()].join(ENV_VAR_SEP),
                    Some("127.0.0.1:13002"),
                ),
                (
                    // http.addr = 127.0.0.1:24000
                    [
                        env_prefix.to_string(),
                        "http".to_uppercase(),
                        "addr".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("127.0.0.1:24000"),
                ),
            ],
            || {
                let opts = create_options_by_cmd(Command {
                    subcmd: SubCommand::Start(StartCommand {
                        http_addr: Some("127.0.0.1:14000".to_string()),
                        config_file: Some(file.path().to_str().unwrap().to_string()),
                        env_prefix: env_prefix.to_string(),
                        ..Default::default()
                    }),
                })
                .unwrap();

                // Should be read from env, env > default values.
                assert_eq!(opts.bind_addr, "127.0.0.1:14002");

                // Should be read from config file, config file > env > default values.
                assert_eq!(opts.server_addr, "127.0.0.1:3002");

                // Should be read from cli, cli > config file > env > default values.
                assert_eq!(opts.http.addr, "127.0.0.1:14000");

                // Should be default value.
                assert_eq!(opts.store_addr, "127.0.0.1:2379");
            },
        );
    }
}
