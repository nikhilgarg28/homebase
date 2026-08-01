//! Isolation policy for managed Multilite transactions.

/// Conflict guarantees requested for admitted Multilite transactions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Validate writes and mandatory constraints, but not ordinary reads.
    #[default]
    Snapshot,
    /// Additionally validate every logical read range observed by the update.
    Serializable,
}

/// Options that override one managed update's open-time defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpdateOptions {
    isolation: IsolationLevel,
}

impl UpdateOptions {
    /// Configure one managed update with an explicit isolation level.
    pub const fn new(isolation: IsolationLevel) -> Self {
        Self { isolation }
    }

    /// Isolation level used to plan this update's conflict assertions.
    pub const fn isolation_level(self) -> IsolationLevel {
        self.isolation
    }
}
