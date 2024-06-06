use std::sync::Arc;
use std::time::Duration;

use api::v1::job::compact_request::Options::Regular;
use api::v1::job::create_job_request::Job::Compact;
use api::v1::job::remote_job_scheduler_client::RemoteJobSchedulerClient;
use api::v1::job::{
    get_job_response, CompactRequest, CreateJobRequest, CreateJobResponse, GetJobRequest,
    GetJobResponse, JobType,
};
use async_trait::async_trait;
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::ChannelManager;
use common_runtime::{RepeatedTask, TaskFunction};
use common_time::timestamp::Timestamp;
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::error::{CreateChannelSnafu, Error, NotStartedSnafu, Result};
use crate::schedule::remote_job_scheduler::TaskSignal::Stop;
use crate::sst::file::{FileId, FileMeta, IndexType};
use crate::{CompactionRegion, Compactor, DefaultCompactor, MergeOutput};

pub type RemoteJobSchedulerRef = Arc<RwLock<dyn RemoteJobScheudler>>;

#[derive(Clone, Copy, Debug)]
pub struct JobId(u64);

impl JobId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

#[async_trait]
pub trait RemoteJobScheudler: Send + Sync + 'static {
    async fn create_job(&mut self, compaction_region: CompactionRegion) -> Result<JobId>;
}

pub struct Scheduler {
    client: Option<Client>,
    // tasks: Vec<RepeatedTask<Error>>,
    compactor: Arc<dyn Compactor>,
    sender: tokio::sync::mpsc::Sender<TaskSignal>,
}

enum TaskSignal {
    Start(RepeatedTask<Error>),
    Stop,
}

#[async_trait]
impl RemoteJobScheudler for Scheduler {
    async fn create_job(&mut self, compaction_region: CompactionRegion) -> Result<JobId> {
        let client = self.client.clone().context(NotStartedSnafu {
            name: "scheduler_client",
        })?;

        let req = CreateJobRequest {
            job: Some(Compact(CompactRequest {
                region_id: compaction_region.region_id.as_u64(),
                options: Some(Regular(Default::default())),
            })),
        };

        let result = client.create_job(req).await?;

        let task: RepeatedTask<Error> = RepeatedTask::new(
            // FIXME(zyy17): The interval should be configurable.
            Duration::from_millis(500),
            Box::new(CheckJobTask {
                client: self.client.clone().unwrap(),
                compaction_region: compaction_region.clone(),
                compactor: self.compactor.clone(),
                sender: self.sender.clone(),
            }),
        );

        self.sender.send(TaskSignal::Start(task)).await.unwrap();

        Ok(JobId(result.job_id))
    }
}

impl Scheduler {
    pub fn new(addr: &str) -> Self {
        let mut tasks = vec![];
        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        tokio::spawn(async move {
            while let Some(signal) = receiver.recv().await {
                match signal {
                    TaskSignal::Start(task) => {
                        task.start(common_runtime::bg_runtime()).unwrap();
                        tasks.push(task);
                    }
                    TaskSignal::Stop => {
                        for task in &tasks {
                            task.stop().await.unwrap();
                        }
                    }
                }
            }
        });
        Self {
            client: Some(Client::new(ChannelManager::default(), addr)),
            //tasks: vec![],
            compactor: Arc::new(DefaultCompactor {}),
            sender,
        }
    }
}

struct CheckJobTask {
    client: Client,
    compaction_region: CompactionRegion,
    compactor: Arc<dyn Compactor>,
    sender: tokio::sync::mpsc::Sender<TaskSignal>,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for CheckJobTask {
    async fn call(&mut self) -> Result<()> {
        let resp = self
            .client
            .get_job(GetJobRequest {
                region_id: self.compaction_region.region_id.as_u64(),
                job_type: JobType::Compaction as i32,
            })
            .await
            .unwrap();

        println!("Received get job response: {:?}", resp);

        return match resp.result {
            Some(get_job_response::Result::Compact(compact_result)) => {
                println!("Received compact result: {:?}", compact_result);
                let mut files_to_add = vec![];
                for file in compact_result.files_to_add {
                    files_to_add.push(covert_filemeta(file));
                }
                let mut files_to_remove = vec![];
                for file in compact_result.files_to_remove {
                    files_to_remove.push(covert_filemeta(file));
                }
                let merge_output = MergeOutput {
                    files_to_add: Some(files_to_add),
                    files_to_remove: Some(files_to_remove),
                    compaction_time_window: Some(compact_result.compaction_time_window),
                };
                self.compactor
                    .update_manifest(self.compaction_region.clone(), merge_output)
                    .await
                    .unwrap();
                self.sender.send(TaskSignal::Stop).await.unwrap();
                Ok(())
            }
            _ => {
                println!("No compact result.");
                self.sender.send(TaskSignal::Stop).await.unwrap();
                Ok(())
            } // Need to implement channel to stop the task.
        };
    }

    fn name(&self) -> &str {
        "CheckJobTask"
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(channel_manager: ChannelManager, addr: &str) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            channel_manager,
            addr: addr.to_string(),
        }));

        Self { inner }
    }

    pub async fn create_job(&self, req: CreateJobRequest) -> Result<CreateJobResponse> {
        let inner = self.inner.read().await;
        inner.create_job(req).await
    }

    pub async fn get_job(&self, req: GetJobRequest) -> Result<GetJobResponse> {
        let inner = self.inner.read().await;
        inner.get_job(req).await
    }
}

#[derive(Debug)]
struct Inner {
    channel_manager: ChannelManager,
    addr: String,
}

impl Inner {
    async fn create_job(&self, req: CreateJobRequest) -> Result<CreateJobResponse> {
        let mut client = self.make_client(&self.addr)?;
        let resp =
            client
                .create_job(req)
                .await
                .map_err(|status| Error::RemoteJobSchedulerServer {
                    code: StatusCode::Internal,
                    msg: status.message().to_string(),
                })?;
        Ok(resp.into_inner())
    }

    async fn get_job(&self, req: GetJobRequest) -> Result<GetJobResponse> {
        let mut client = self.make_client(&self.addr)?;
        let resp = client
            .get_job(req)
            .await
            .map_err(|status| Error::RemoteJobSchedulerServer {
                code: StatusCode::Internal,
                msg: status.message().to_string(),
            })?;
        Ok(resp.into_inner())
    }

    #[inline]
    fn make_client(&self, addr: impl AsRef<str>) -> Result<RemoteJobSchedulerClient<Channel>> {
        let channel = self.channel_manager.get(addr).context(CreateChannelSnafu)?;
        Ok(RemoteJobSchedulerClient::new(channel))
    }
}

fn covert_filemeta(input: api::v1::job::FileMeta) -> FileMeta {
    FileMeta {
        region_id: RegionId::from_u64(input.region_id),
        file_id: FileId::parse_str(input.file_id.as_str()).unwrap(),
        time_range: (
            Timestamp::new_nanosecond(input.time_range.clone().unwrap().start),
            Timestamp::new_nanosecond(input.time_range.clone().unwrap().clone().end),
        ),
        level: input.level as u8,
        file_size: input.file_size,
        available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
        index_file_size: input.index_file_size,
    }
}
