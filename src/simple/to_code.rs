use crate::context::Context;
use crate::job::to_code::{JobCode, ToCode};
use crate::job::{JobLocked, JobToRunAsync};
use crate::JobSchedulerError;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct SimpleJobCode {
    pub job_code: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<JobToRunAsync>>>>>>,
}

impl Default for SimpleJobCode {
    fn default() -> Self {
        SimpleJobCode {
            job_code: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SimpleJobCode {
    async fn listen_for_additions(
        data: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<JobToRunAsync>>>>>>,
        mut rx: Receiver<(Uuid, Arc<RwLock<Box<JobToRunAsync>>>)>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving {:?}", e);
                break;
            }
            let (uuid, val) = val.unwrap();
            let mut w = data.write().await;
            w.insert(uuid, val);
        }
    }
}

impl ToCode<Box<JobToRunAsync>> for SimpleJobCode {
    fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let data = self.job_code.clone();
        let job_create = context.job_create_tx.subscribe();

        Box::pin(async move {
            tokio::spawn(SimpleJobCode::listen_for_additions(data, job_create));
            Ok(())
        })
    }

    fn get(
        &mut self,
        uuid: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<Arc<RwLock<Box<JobToRunAsync>>>>, JobSchedulerError>>
                + Send,
        >,
    > {
        let data = self.job_code.clone();
        Box::pin(async move {
            let r = data.read().await;
            Ok(r.get(&uuid).cloned())
        })
    }
}

impl JobCode for SimpleJobCode {}
