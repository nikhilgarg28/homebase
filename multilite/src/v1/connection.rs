//! Temporary V1 connection wrapper over the general Multilite database.

use std::path::Path;

use homebase_client::ServerHandle;

use super::schema;
use crate::database::{
    Database, DatabaseId, OfflineServer, OpenOptions, ReplicaInvitation, Statement,
};
use crate::{Params, Result};

/// A V1-format connection layered over a general Multilite database.
pub struct Connection<H = OfflineServer>
where
    H: ServerHandle,
{
    database: Database<H>,
}

impl Connection<OfflineServer> {
    /// Open general database state, then initialize or validate V1 locally.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let database = Database::open(path)?;
        Self::finish_open(database)
    }
}

impl<H: ServerHandle> Connection<H> {
    /// Open with general options, then initialize or validate V1 locally.
    pub fn open_with(path: impl AsRef<Path>, options: OpenOptions<H>) -> Result<Self> {
        let database = Database::open_with(path, options)?;
        Self::finish_open(database)
    }

    fn finish_open(database: Database<H>) -> Result<Self> {
        schema::open(&database)?;
        Ok(Self { database })
    }

    /// Database identity shared by every replica of this file's space.
    pub fn database_id(&self) -> DatabaseId {
        self.database.database_id()
    }

    /// Versioned onboarding material for another local replica.
    pub fn replica_invitation(&self) -> ReplicaInvitation {
        self.database.replica_invitation()
    }

    /// Device identity unique to this local replica file.
    pub fn device_id(&self) -> [u8; 16] {
        self.database.device_id()
    }

    /// Execute one SQLite statement directly.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.database.execute(sql, params)
    }

    /// Prepare one read-only statement.
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        self.database.prepare(sql)
    }
}
