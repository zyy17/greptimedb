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

use async_trait::async_trait;
use clap::Parser;
use tracing_appender::non_blocking::WorkerGuard;

use crate::cli::{Instance, Tool};
use crate::error::Result;

#[derive(Debug, Default, Parser)]
pub struct CompactCommand {
    /// The id of the region to compact.
    #[clap(long)]
    region_id: u64,
}

impl CompactCommand {
    pub async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        Ok(Instance::new(Box::new(Compact {}), guard))
    }
}

pub struct Compact {}

impl Compact {}

#[async_trait]
impl Tool for Compact {
    async fn do_work(&self) -> Result<()> {
        Ok(())
    }
}
