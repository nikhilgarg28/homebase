//! The ServerHandle conformance suite, driven by the canonical in-process
//! implementation: a closure over a real `homebase_server::Server` —
//! literally `|id| server.space(id)` — with each space actor running on
//! its own thread. This is the reference every future implementation
//! (the gRPC adapter above all) is measured against.

use homebase::server::conformance;
use homebase::{Offline, ServerHandle};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::messages::GetRequest;
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_server::Server;
use homebase_server::actor::{SpaceHandle, Spawner};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A `Sync` spawner for tests: each actor gets a thread and blocks on its
/// mailbox loop; the thread exits when the server (and so every handle)
/// drops.
struct ThreadSpawner;

impl Spawner for ThreadSpawner {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        std::thread::spawn(move || pollster::block_on(task));
    }
}

#[test]
fn closure_over_in_process_server_conforms() {
    let server = Arc::new(Server::new(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(Timestamp(0))),
        ThreadSpawner,
    ));
    let (a, b, unknown) = (SpaceId([1; 16]), SpaceId([2; 16]), SpaceId([9; 16]));
    assert!(server.create_space(a));
    assert!(server.create_space(b));

    let handle = {
        let server = Arc::clone(&server);
        move |id: &SpaceId| server.space(id)
    };
    pollster::block_on(conformance::run_all(&handle, a, b, unknown));
}

#[test]
fn offline_implements_the_contract_but_cannot_exist() {
    fn assert_impl<T: ServerHandle>() {}
    assert_impl::<Offline>();

    // Uninhabited: the only Offline you can hold is the one that isn't.
    let none: Option<Offline> = None;
    assert!(none.is_none());
}

/// The contract promises `Send` futures (multi-threaded executors drive
/// them in production); this pins it at compile time for the closure impl.
#[test]
fn verb_futures_are_send() {
    fn assert_send<F: Future + Send>(_: &F) {}

    let handle = |_: &SpaceId| Option::<SpaceHandle>::None;
    let space = SpaceId([1; 16]);
    let fut = handle.get(&space, GetRequest { keys: vec![] });
    assert_send(&fut);
    drop(fut);
}
