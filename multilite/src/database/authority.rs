//! Single-owner authority I/O for one Multilite database.

use std::sync::Arc;

use async_channel::{Receiver, Sender};
use futures_channel::oneshot;
use homebase_client::{ClientError, PushOutcome, ServerHandle};
use homebase_core::space::SpaceId;
use homebase_core::tag::AdmissionSeq;

use super::DatabaseClient;
use crate::{Error, Result};

const INBOX_CAPACITY: usize = 64;

enum Request {
    Push {
        reply: oneshot::Sender<Result<PushOutcome>>,
    },
    Pull {
        reply: oneshot::Sender<Result<AdmissionSeq>>,
    },
}

/// Sending side of the database's single authority driver.
pub struct Authority {
    outbox: Sender<Request>,
}

impl Authority {
    pub fn new<H>(
        client: Arc<DatabaseClient<H>>,
        space: SpaceId,
    ) -> std::result::Result<Self, AuthorityError>
    where
        H: ServerHandle + Send + Sync + 'static,
    {
        let (outbox, inbox) = async_channel::bounded(INBOX_CAPACITY);
        std::thread::Builder::new()
            .name("multilite-authority".into())
            // Multilite does not require an async runtime yet. The driver
            // itself remains async so this thread can be replaced by a
            // runtime-spawned task when the public API gains async variants.
            .spawn(move || pollster::block_on(run(inbox, client, space)))
            .map_err(|error| AuthorityError::Startup(error.to_string()))?;
        Ok(Self { outbox })
    }

    pub async fn push(&self) -> Result<PushOutcome> {
        let (reply, response) = oneshot::channel();
        self.outbox
            .send(Request::Push { reply })
            .await
            .map_err(|_| unavailable())?;
        response.await.map_err(|_| unavailable())?
    }

    pub fn push_blocking(&self) -> Result<PushOutcome> {
        pollster::block_on(self.push())
    }

    pub async fn pull(&self) -> Result<AdmissionSeq> {
        let (reply, response) = oneshot::channel();
        self.outbox
            .send(Request::Pull { reply })
            .await
            .map_err(|_| unavailable())?;
        response.await.map_err(|_| unavailable())?
    }

    pub fn pull_blocking(&self) -> Result<AdmissionSeq> {
        pollster::block_on(self.pull())
    }
}

async fn run<H>(inbox: Receiver<Request>, client: Arc<DatabaseClient<H>>, space: SpaceId)
where
    H: ServerHandle + Send + Sync + 'static,
{
    while let Ok(request) = inbox.recv().await {
        match request {
            Request::Push { reply } => {
                let result = async {
                    client
                        .space(space)
                        .await?
                        .push()
                        .await
                        .map_err(ClientError::from)
                }
                .await
                .map_err(Error::from);
                let _ = reply.send(result);
            }
            Request::Pull { reply } => {
                let result = async {
                    client
                        .space(space)
                        .await?
                        .pull()
                        .await
                        .map_err(ClientError::from)
                }
                .await
                .map_err(Error::from);
                let _ = reply.send(result);
            }
        }
    }
}

fn unavailable() -> Error {
    Error::BackgroundWorker(AuthorityError::Unavailable.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorityError {
    Startup(String),
    Unavailable,
}

impl std::fmt::Display for AuthorityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Startup(message) => {
                write!(formatter, "could not start authority task: {message}")
            }
            Self::Unavailable => formatter.write_str("authority task is unavailable"),
        }
    }
}
