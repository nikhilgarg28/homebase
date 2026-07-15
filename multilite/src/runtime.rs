//! Reusable SQLite execution modes and hook plumbing.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "integrated into MultiliteConnection by later batches"
    )
)]

use std::cell::Cell;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::Connection;
use rusqlite::hooks::{AuthContext, Authorization, PreUpdateCase};

use crate::{Error, Result};

/// Why Multilite is executing SQL on its owned connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionMode {
    /// SQL issued through the application-facing API and subject to capture.
    Public,
    /// Multilite's own schema and metadata work.
    InternalMetadata,
    /// Applying an already-admitted remote operation.
    RemoteApply,
    /// Restoring local SQLite state during explicit repair.
    Repair,
}

/// Policy layered over the reusable SQLite runtime.
pub(crate) trait HookPolicy: Send + 'static {
    type Event: Send + 'static;

    fn authorize(&mut self, mode: ExecutionMode, context: AuthContext<'_>) -> Authorization;

    fn preupdate(
        &mut self,
        mode: ExecutionMode,
        database: &str,
        table: &str,
        update: &PreUpdateCase,
    ) -> Result<Option<Self::Event>>;
}

/// A SQLite connection with scoped execution modes and attributed hook events.
pub(crate) struct RuntimeConnection<P: HookPolicy> {
    connection: Connection,
    state: Arc<Mutex<HookState<P>>>,
    next_savepoint: Cell<u64>,
}

impl<P: HookPolicy> RuntimeConnection<P> {
    pub(crate) fn open(path: impl AsRef<Path>, policy: P) -> Result<Self> {
        Self::new(Connection::open(path)?, policy)
    }

    #[cfg(test)]
    pub(crate) fn open_in_memory(policy: P) -> Result<Self> {
        Self::new(Connection::open_in_memory()?, policy)
    }

    fn new(connection: Connection, policy: P) -> Result<Self> {
        let state = Arc::new(Mutex::new(HookState::new(policy)));

        let authorizer_state = Arc::clone(&state);
        connection.authorizer(Some(move |context: AuthContext<'_>| {
            let mut state = lock(&authorizer_state);
            let mode = state.mode();
            state.policy.authorize(mode, context)
        }))?;

        let preupdate_state = Arc::clone(&state);
        connection.preupdate_hook(Some(
            move |_action, database: &str, table: &str, update: &PreUpdateCase| {
                let mut state = lock(&preupdate_state);
                if state.callback_error.is_some() {
                    return;
                }
                let mode = state.mode();
                match state.policy.preupdate(mode, database, table, update) {
                    Ok(Some(event)) => state.events.push(event),
                    Ok(None) => {}
                    Err(error) => state.callback_error = Some(error),
                }
            },
        ))?;

        Ok(Self {
            connection,
            state,
            next_savepoint: Cell::new(0),
        })
    }

    /// Run one atomic unit and return only the events captured by that unit.
    pub(crate) fn run<T>(
        &self,
        mode: ExecutionMode,
        operation: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<(T, Vec<P::Event>)> {
        let _operation_guard = OperationGuard::enter(Arc::clone(&self.state))?;
        let event_checkpoint = self.event_count();
        let savepoint = SavepointGuard::begin(self, event_checkpoint)?;

        let operation_result = self.with_mode(mode, || operation(&self.connection));
        if let Some(error) = self.take_callback_error() {
            savepoint.rollback()?;
            return Err(error);
        }

        let value = match operation_result {
            Ok(value) => value,
            Err(error) => {
                savepoint.rollback()?;
                return Err(error);
            }
        };

        savepoint.release()?;
        Ok((value, self.split_events(event_checkpoint)))
    }

    fn control_sql(&self, sql: &str) -> Result<()> {
        self.with_mode(ExecutionMode::InternalMetadata, || {
            self.connection.execute_batch(sql).map_err(Into::into)
        })
    }

    fn with_mode<T>(
        &self,
        mode: ExecutionMode,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let _guard = ModeGuard::enter(Arc::clone(&self.state), mode);
        operation()
    }

    fn next_savepoint_name(&self) -> String {
        let next = self.next_savepoint.get();
        self.next_savepoint.set(next.wrapping_add(1));
        format!("_mt_runtime_{next}")
    }

    fn event_count(&self) -> usize {
        lock(&self.state).events.len()
    }

    fn split_events(&self, checkpoint: usize) -> Vec<P::Event> {
        lock(&self.state).events.split_off(checkpoint)
    }

    fn truncate_events(&self, checkpoint: usize) {
        lock(&self.state).events.truncate(checkpoint);
    }

    fn take_callback_error(&self) -> Option<Error> {
        lock(&self.state).callback_error.take()
    }
}

struct HookState<P: HookPolicy> {
    policy: P,
    modes: Vec<ExecutionMode>,
    events: Vec<P::Event>,
    callback_error: Option<Error>,
    operation_active: bool,
}

impl<P: HookPolicy> HookState<P> {
    fn new(policy: P) -> Self {
        Self {
            policy,
            modes: Vec::new(),
            events: Vec::new(),
            callback_error: None,
            operation_active: false,
        }
    }

    fn mode(&self) -> ExecutionMode {
        self.modes
            .last()
            .copied()
            .unwrap_or(ExecutionMode::InternalMetadata)
    }
}

struct OperationGuard<P: HookPolicy> {
    state: Arc<Mutex<HookState<P>>>,
}

impl<P: HookPolicy> OperationGuard<P> {
    fn enter(state: Arc<Mutex<HookState<P>>>) -> Result<Self> {
        {
            let mut state = lock(&state);
            if state.operation_active {
                return Err(Error::CaptureInvariant(
                    "nested runtime operations are not supported",
                ));
            }
            state.operation_active = true;
            state.callback_error = None;
        }
        Ok(Self { state })
    }
}

impl<P: HookPolicy> Drop for OperationGuard<P> {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        state.operation_active = false;
        state.callback_error = None;
    }
}

struct SavepointGuard<'a, P: HookPolicy> {
    runtime: &'a RuntimeConnection<P>,
    name: String,
    event_checkpoint: usize,
    active: bool,
}

impl<'a, P: HookPolicy> SavepointGuard<'a, P> {
    fn begin(runtime: &'a RuntimeConnection<P>, event_checkpoint: usize) -> Result<Self> {
        let name = runtime.next_savepoint_name();
        runtime.control_sql(&format!("SAVEPOINT {name}"))?;
        Ok(Self {
            runtime,
            name,
            event_checkpoint,
            active: true,
        })
    }

    fn release(mut self) -> Result<()> {
        self.runtime
            .control_sql(&format!("RELEASE {}", self.name))?;
        self.active = false;
        Ok(())
    }

    fn rollback(mut self) -> Result<()> {
        self.runtime
            .control_sql(&format!("ROLLBACK TO {}; RELEASE {}", self.name, self.name))?;
        self.runtime.truncate_events(self.event_checkpoint);
        self.active = false;
        Ok(())
    }
}

impl<P: HookPolicy> Drop for SavepointGuard<'_, P> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let _ = self
            .runtime
            .control_sql(&format!("ROLLBACK TO {}; RELEASE {}", self.name, self.name));
        self.runtime.truncate_events(self.event_checkpoint);
        let _ = self.runtime.take_callback_error();
    }
}

struct ModeGuard<P: HookPolicy> {
    state: Arc<Mutex<HookState<P>>>,
    mode: ExecutionMode,
}

impl<P: HookPolicy> ModeGuard<P> {
    fn enter(state: Arc<Mutex<HookState<P>>>, mode: ExecutionMode) -> Self {
        lock(&state).modes.push(mode);
        Self { state, mode }
    }
}

impl<P: HookPolicy> Drop for ModeGuard<P> {
    fn drop(&mut self) {
        let popped = lock(&self.state).modes.pop();
        debug_assert_eq!(popped, Some(self.mode));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopPolicy;

    impl HookPolicy for NoopPolicy {
        type Event = ();

        fn authorize(&mut self, _mode: ExecutionMode, _context: AuthContext<'_>) -> Authorization {
            Authorization::Allow
        }

        fn preupdate(
            &mut self,
            _mode: ExecutionMode,
            _database: &str,
            _table: &str,
            _update: &PreUpdateCase,
        ) -> Result<Option<Self::Event>> {
            Ok(None)
        }
    }

    #[test]
    fn execution_modes_nest_and_restore_lifo() {
        let runtime = RuntimeConnection::open_in_memory(NoopPolicy).unwrap();
        assert_eq!(lock(&runtime.state).mode(), ExecutionMode::InternalMetadata);

        runtime
            .with_mode(ExecutionMode::Public, || {
                assert_eq!(lock(&runtime.state).mode(), ExecutionMode::Public);
                runtime.with_mode(ExecutionMode::Repair, || {
                    assert_eq!(lock(&runtime.state).mode(), ExecutionMode::Repair);
                    Ok(())
                })?;
                assert_eq!(lock(&runtime.state).mode(), ExecutionMode::Public);
                Ok(())
            })
            .unwrap();

        assert_eq!(lock(&runtime.state).mode(), ExecutionMode::InternalMetadata);
    }

    #[test]
    fn runtime_opens_and_persists_a_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.sqlite");

        {
            let runtime = RuntimeConnection::open(&path, NoopPolicy).unwrap();
            runtime
                .run(ExecutionMode::InternalMetadata, |connection| {
                    connection.execute_batch("CREATE TABLE persisted (value INTEGER)")?;
                    Ok(())
                })
                .unwrap();
        }

        let connection = Connection::open(path).unwrap();
        let table_count: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'persisted'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_count, 1);
    }
}
