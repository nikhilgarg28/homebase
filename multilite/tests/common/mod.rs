use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use homebase::Server;
use homebase::actor::{SpaceHandle, Spawner};
use homebase::storage::MemoryStore;
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::space::SpaceId;

pub struct ThreadSpawner;

impl Spawner for ThreadSpawner {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        std::thread::spawn(move || pollster::block_on(task));
    }
}

pub type TestServer = Server<MemoryStore, ManualClock, ThreadSpawner>;

pub fn server() -> Arc<TestServer> {
    Arc::new(Server::new(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(Timestamp(0))),
        ThreadSpawner,
    ))
}

pub fn router(server: Arc<TestServer>) -> impl Fn(&SpaceId) -> Option<SpaceHandle> + Sync {
    move |space| server.space(space)
}
