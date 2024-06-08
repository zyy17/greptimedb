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
use std::time::{Duration, Instant};

use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::error;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::compaction::compactor::{CompactionRegion, Compactor, DefaultCompactor, MergeOutput};
use crate::compaction::picker::PickerOutput;
use crate::error::{Error, Result};
use crate::manifest::action::RegionEdit;
use crate::request::{BackgroundNotify, CompactionFinished, WorkerRequest};

pub type RemoteJobSchedulerRef = Arc<dyn RemoteJobScheduler>;

/// RemoteJobScheduler is a trait that defines the API to schedule remote jobs.
#[async_trait::async_trait]
pub trait RemoteJobScheduler: Send + Sync + 'static {
    /// Sends a job to the scheduler and returns a unique identifier for the job.
    async fn schedule(&self, job: RemoteJob) -> Result<JobId>;
}

/// JobId is a unique identifier for a remote job and allocated by the scheduler.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(u64);

impl JobId {
    /// Returns the JobId as a u64.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// RemoteJob is a job that can be executed remotely. For example, a remote compaction job.
#[derive(Clone)]
#[allow(dead_code)]
pub enum RemoteJob {
    CompactionJob(CompactionJob),
}

/// CompactionJob is a remote job that compacts a set of files in a compaction service.
#[derive(Clone)]
#[allow(dead_code)]
pub struct CompactionJob {
    pub compaction_region: CompactionRegion,
    pub picker_output: PickerOutput,
}

/// RemoteJobSchedulerOption is an option to create a RemoteJobScheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteJobSchedulerOption {
    /// The address of the remote-job-scheduler.
    pub addr: String,

    /// The interval to check the status of the job.
    #[serde(with = "humantime_serde")]
    pub check_job_status_interval: Duration,

    /// The size of the channel that used for communication between main thread and task manager.
    pub channel_size: usize,
}

impl Default for RemoteJobSchedulerOption {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:10099".to_string(),
            check_job_status_interval: Duration::from_millis(500),
            channel_size: 100,
        }
    }
}

/// RemoteJobSchedulerImpl is an implementation of RemoteJobScheduler.
#[allow(dead_code)]
pub struct RemoteJobSchedulerImpl {
    // The address of the remote-job-scheduler.
    addr: String,

    // Use the compactor to update manifest files.
    compactor: Arc<dyn Compactor>,

    // Request sender of the worker that this scheduler belongs to.
    request_sender: Sender<WorkerRequest>,

    // The sender to send the status of the tasks.
    task_status_sender: Sender<TaskStatus>,

    // The interval to check the status of the job.
    check_job_status_interval: Duration,
}

impl RemoteJobSchedulerImpl {
    pub fn new(option: &RemoteJobSchedulerOption, request_sender: Sender<WorkerRequest>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(option.channel_size);
        let tasks = HashMap::new();

        // Start a task manager to manage the check job status tasks.
        tokio::spawn(async move {
            Self::task_manager(receiver, tasks).await;
        });

        Self {
            addr: option.addr.clone(),
            compactor: Arc::new(DefaultCompactor {}),
            task_status_sender: sender,
            check_job_status_interval: option.check_job_status_interval,
            request_sender,
        }
    }

    async fn task_manager(
        mut task_status_receiver: Receiver<TaskStatus>,
        mut tasks: HashMap<JobId, RepeatedTask<Error>>,
    ) {
        // Start a task manager to manage the tasks.
        while let Some(task_status) = task_status_receiver.recv().await {
            match task_status {
                TaskStatus::Start((job_id, task)) => {
                    let result = task.start(common_runtime::bg_runtime());
                    if let Err(e) = result {
                        error!("Failed to start the task: {:?}", e);
                    } else {
                        // Insert the task into the tasks map.
                        tasks.insert(job_id, task);
                    }
                }
                TaskStatus::Stop(job_id) => {
                    // Stop the check job status task and remove it from the tasks map.
                    if let Some(task) = tasks.remove(&job_id) {
                        let result = task.stop().await;
                        if let Err(e) = result {
                            error!("Failed to stop the task: {:?}", e);
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl RemoteJobScheduler for RemoteJobSchedulerImpl {
    async fn schedule(&self, job: RemoteJob) -> Result<JobId> {
        match job {
            RemoteJob::CompactionJob(job) => {
                let job_id = JobId(0);
                let task: RepeatedTask<Error> = RepeatedTask::new(
                    self.check_job_status_interval,
                    Box::new(CheckJobStatusTask {
                        job_id,
                        compaction_region: job.compaction_region,
                        compactor: self.compactor.clone(),
                        task_status_sender: self.task_status_sender.clone(),
                        request_sender: self.request_sender.clone(),
                    }),
                );

                // FIXME(zyy17): don't use unwrap.
                self.task_status_sender
                    .send(TaskStatus::Start((job_id, task)))
                    .await
                    .unwrap();
                Ok(job_id)
            }
        }
    }
}

/// CheckJobStatusTask is a periodic task that checks the status of a remote job.
struct CheckJobStatusTask {
    // Each task belongs to a remote job.
    job_id: JobId,
    compactor: Arc<dyn Compactor>,
    compaction_region: CompactionRegion,
    request_sender: Sender<WorkerRequest>,
    task_status_sender: Sender<TaskStatus>,
}

enum TaskStatus {
    Start((JobId, RepeatedTask<Error>)),
    Stop(JobId),
}

#[async_trait::async_trait]
impl TaskFunction<Error> for CheckJobStatusTask {
    async fn call(&mut self) -> Result<()> {
        // FIXME(zyy17): The merge_output should be obtained from the remote job scheduler.
        let merge_output = MergeOutput {
            files_to_remove: vec![],
            files_to_add: vec![],
            compaction_time_window: None,
        };

        self.compactor
            .update_manifest(&self.compaction_region, merge_output)
            .await?;

        // FIXME(zyy17): Handle the failure of the compaction.
        let notify = BackgroundNotify::CompactionFinished(CompactionFinished {
            region_id: self.compaction_region.region_id,
            senders: None,
            start_time: Instant::now(),
            edit: RegionEdit {
                files_to_add: vec![],
                files_to_remove: vec![],
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
            },
        });

        if let Err(e) = self
            .request_sender
            .send(WorkerRequest::Background {
                region_id: self.compaction_region.region_id,
                notify,
            })
            .await
        {
            error!(
                "Failed to notify compaction job status for region {}, request: {:?}",
                self.compaction_region.region_id, e.0
            );
        }

        // FIXME(zyy17): don't use unwrap.
        self.task_status_sender
            .send(TaskStatus::Stop(self.job_id))
            .await
            .unwrap();

        Ok(())
    }

    fn name(&self) -> &str {
        "CheckJobStatusTask"
    }
}
