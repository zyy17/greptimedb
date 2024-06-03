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

use api::v1::region::compact_request::Options::Regular;
use async_trait::async_trait;
use clap::Parser;
use mito2::{open_compaction_region, CompactionRequest, Compactor, DefaultCompactor};
use object_store::manager::ObjectStoreManager;
use object_store::services::Fs;
use object_store::util::join_dir;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tracing_appender::non_blocking::WorkerGuard;

use crate::cli::{Instance, Tool};
use crate::error::{CompactRegionSnafu, OpenCompactRegionSnafu, Result};

#[derive(Debug, Default, Parser)]
pub struct CompactCommand {
    /// The id of the region to compact.
    #[clap(long)]
    region_id: u64,

    #[clap(long, default_value = "greptime")]
    catalog: String,

    #[clap(long, default_value = "public")]
    schema: String,

    #[clap(long, default_value = "/tmp/greptimedb")]
    data_home: String,
}

impl CompactCommand {
    pub async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        Ok(Instance::new(
            Box::new(Compact {
                request: CompactionRequest {
                    catalog: self.catalog.clone(),
                    schema: self.schema.clone(),
                    region_id: RegionId::from_u64(self.region_id),
                    options: HashMap::default(),
                    compaction_options: Regular(Default::default()),
                },
                data_home: self.data_home.clone(),
            }),
            guard,
        ))
    }
}

pub struct Compact {
    request: CompactionRequest,
    data_home: String,
}

impl Compact {
    // TODO(zyy17): Need a better way to create object store manager.
    fn create_object_store_manager(&self) -> ObjectStoreManager {
        let atomic_write_dir = join_dir(self.data_home.as_str(), ".tmp/");
        let mut builder = Fs::default();
        let _ = builder
            .root(self.data_home.as_str())
            .atomic_write_dir(&atomic_write_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();
        ObjectStoreManager::new("default", object_store)
    }
}

#[async_trait]
impl Tool for Compact {
    async fn do_work(&self) -> Result<()> {
        let object_store_manager = self.create_object_store_manager();
        let compactor = DefaultCompactor::new_from_request(&self.request).unwrap();
        let compaction_region =
            open_compaction_region(&self.request, self.data_home.as_str(), object_store_manager)
                .await
                .context(OpenCompactRegionSnafu)?;

        compactor
            .compact(compaction_region)
            .await
            .context(CompactRegionSnafu)?;

        Ok(())
    }
}
