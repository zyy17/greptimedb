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
use std::sync::Arc;

use api::v1::region::compact_request;
use object_store::manager::ObjectStoreManagerRef;
use object_store::util::join_dir;
use object_store::ObjectStore;
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::path_utils::region_dir;
use store_api::storage::RegionId;

use crate::access_layer::{AccessLayer, AccessLayerRef, SstWriteRequest};
use crate::cache::CacheManager;
use crate::compaction::compaction_options_to_picker;
use crate::config::MitoConfig;
use crate::error;
use crate::error::{ObjectStoreNotFoundSnafu, Result};
use crate::flush::WriteBufferManagerImpl;
use crate::manifest::action::RegionManifest;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::manifest::storage::manifest_compress_type;
use crate::memtable::time_partition::TimePartitions;
use crate::memtable::MemtableBuilderProvider;
use crate::read::Source;
use crate::region::options::RegionOptions;
use crate::region::version::{VersionBuilder, VersionControl, VersionRef};
use crate::schedule::scheduler::LocalScheduler;
use crate::sst::file::{FileMeta, IndexType};
use crate::sst::file_purger::LocalFilePurger;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::parquet::WriteOptions;

pub struct Compactor {
    pub mito_config: Arc<MitoConfig>,
    pub object_store_manager: ObjectStoreManagerRef,
}

pub struct CompactionRequest {
    pub catalog: String,
    pub schema: String,
    pub region_id: RegionId,
    pub region_options: HashMap<String, String>,
    pub compaction_options: compact_request::Options,
}

impl Compactor {
    pub async fn merge_ssts(
        &mut self,
        req: CompactionRequest,
    ) -> Result<(Vec<FileMeta>, Vec<FileMeta>)> {
        let region_options = RegionOptions::try_from(&req.region_options)?;
        let object_store = self.object_store(&region_options.storage)?;
        let region_dir = region_dir(
            format!("{}/{}", req.catalog, req.schema).as_str(),
            req.region_id,
        );
        let manifest_manager = self
            .manifest_manager(&region_dir, object_store.clone())
            .await?;
        let manifest = manifest_manager.manifest();
        let metadata = manifest.metadata.clone();
        let access_layer = self.access_layer(&region_dir, object_store.clone()).await?;
        let current_version = self
            .version_ref(
                region_options.clone(),
                metadata.clone(),
                access_layer.clone(),
                manifest.clone(),
            )
            .await?;

        let picker = if let compact_request::Options::StrictWindow(window) = &req.compaction_options
        {
            let window = if window.window_seconds == 0 {
                None
            } else {
                Some(window.window_seconds)
            };
            Arc::new(crate::compaction::window::WindowedCompactionPicker::new(
                window,
            )) as Arc<_>
        } else {
            compaction_options_to_picker(&current_version.options.compaction)
        };

        let mut picker_output = picker.pick(current_version.clone());

        let mut futs = Vec::with_capacity(picker_output.compaction_outputs.len());
        let mut compacted_inputs = Vec::with_capacity(
            picker_output
                .compaction_outputs
                .iter()
                .map(|o| o.inputs.len())
                .sum(),
        );

        for output in picker_output.compaction_outputs.drain(..) {
            compacted_inputs.extend(output.inputs.iter().map(|f| f.meta_ref().clone()));

            let write_opts = WriteOptions {
                write_buffer_size: self.mito_config.sst_write_buffer_size,
                ..Default::default()
            };
            let create_inverted_index = self.mito_config.inverted_index.create_on_compaction.auto();
            let mem_threshold_index_create = self
                .mito_config
                .inverted_index
                .mem_threshold_on_create
                .map(|m| m.as_bytes() as _);
            let index_write_buffer_size =
                Some(self.mito_config.inverted_index.write_buffer_size.as_bytes() as usize);

            let metadata = metadata.clone();
            let sst_layer = access_layer.clone();
            let region_id = req.region_id;
            let file_id = output.output_file_id;
            let cache_manager = Arc::new(CacheManager::default());
            let storage = region_options.storage.clone();
            let index_options = current_version.options.index_options.clone();
            let append_mode = current_version.options.append_mode;
            futs.push(async move {
                let reader = crate::compaction::build_sst_reader(
                    metadata.clone(),
                    sst_layer.clone(),
                    Some(cache_manager.clone()),
                    &output.inputs,
                    append_mode,
                    output.filter_deleted,
                    output.output_time_range,
                )
                .await?;
                let file_meta_opt = sst_layer
                    .write_sst(
                        SstWriteRequest {
                            file_id,
                            metadata,
                            source: Source::Reader(reader),
                            cache_manager,
                            storage,
                            create_inverted_index,
                            mem_threshold_index_create,
                            index_write_buffer_size,
                            index_options,
                        },
                        &write_opts,
                    )
                    .await?
                    .map(|sst_info| FileMeta {
                        region_id,
                        file_id,
                        time_range: sst_info.time_range,
                        level: output.output_level,
                        file_size: sst_info.file_size,
                        available_indexes: sst_info
                            .inverted_index_available
                            .then(|| SmallVec::from_iter([IndexType::InvertedIndex]))
                            .unwrap_or_default(),
                        index_file_size: sst_info.index_file_size,
                    });
                Ok(file_meta_opt)
            });
        }

        let mut output_files = Vec::with_capacity(futs.len());
        while !futs.is_empty() {
            let mut task_chunk =
                Vec::with_capacity(crate::compaction::task::MAX_PARALLEL_COMPACTION);
            for _ in 0..crate::compaction::task::MAX_PARALLEL_COMPACTION {
                if let Some(task) = futs.pop() {
                    task_chunk.push(common_runtime::spawn_bg(task));
                }
            }
            let metas = futures::future::try_join_all(task_chunk)
                .await
                .context(error::JoinSnafu)?
                .into_iter()
                .collect::<error::Result<Vec<_>>>()?;
            output_files.extend(metas.into_iter().flatten());
        }

        let inputs = compacted_inputs.into_iter().collect();
        Ok((output_files, inputs))
    }

    fn object_store(&self, name: &Option<String>) -> Result<&ObjectStore> {
        if let Some(name) = name {
            Ok(self
                .object_store_manager
                .find(name)
                .context(ObjectStoreNotFoundSnafu {
                    object_store: name.to_string(),
                })?)
        } else {
            Ok(self.object_store_manager.default_object_store())
        }
    }

    async fn manifest_manager(
        &self,
        region_dir: &str,
        object_store: ObjectStore,
    ) -> Result<RegionManifestManager> {
        let region_manifest_options = RegionManifestOptions {
            manifest_dir: join_dir(&region_dir, "manifest"),
            object_store: object_store.clone(),
            compress_type: manifest_compress_type(self.mito_config.compress_manifest),
            checkpoint_distance: self.mito_config.manifest_checkpoint_distance,
        };

        // TODO(zyy17): handle error.
        let manifest_manager = RegionManifestManager::open(region_manifest_options)
            .await?
            .unwrap();
        Ok(manifest_manager)
    }

    async fn access_layer(
        &self,
        region_dir: &str,
        object_store: ObjectStore,
    ) -> Result<Arc<AccessLayer>> {
        let intermediate_manager =
            IntermediateManager::init_fs(self.mito_config.inverted_index.intermediate_path.clone())
                .await?;

        Ok(Arc::new(AccessLayer::new(
            region_dir.to_string(),
            object_store,
            intermediate_manager,
        )))
    }

    async fn version_ref(
        &self,
        region_options: RegionOptions,
        metadata: RegionMetadataRef,
        access_layer: AccessLayerRef,
        manifest: Arc<RegionManifest>,
    ) -> Result<VersionRef> {
        let write_buffer_manager = Arc::new(WriteBufferManagerImpl::new(
            self.mito_config.global_write_buffer_size.as_bytes() as usize,
        ));
        let memtable_builder_provider = MemtableBuilderProvider::new(
            Some(write_buffer_manager.clone()),
            self.mito_config.clone(),
        );

        let memtable_builder = memtable_builder_provider.builder_for_options(
            region_options.memtable.as_ref(),
            !region_options.append_mode,
        );

        // Initial memtable id is 0.
        let part_duration = region_options.compaction.time_window();
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            memtable_builder.clone(),
            0,
            part_duration,
        ));

        let purge_scheduler = Arc::new(LocalScheduler::new(self.mito_config.max_background_jobs));
        let file_purger = Arc::new(LocalFilePurger::new(
            purge_scheduler.clone(),
            access_layer.clone(),
            None,
        ));

        let version = VersionBuilder::new(metadata, mutable)
            .add_files(file_purger.clone(), manifest.files.values().cloned())
            .flushed_entry_id(manifest.flushed_entry_id)
            .flushed_sequence(manifest.flushed_sequence)
            .truncated_entry_id(manifest.truncated_entry_id)
            .compaction_time_window(manifest.compaction_time_window)
            .options(region_options)
            .build();
        let version_control = Arc::new(VersionControl::new(version));
        let current_version = version_control.current().version;
        Ok(current_version)
    }
}
