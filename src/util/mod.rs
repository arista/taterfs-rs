//! Utility modules for taterfs-rs.

pub mod capacity_manager;
pub mod complete;
pub mod dedup;
pub mod managed_buffers;

pub use capacity_manager::{CapacityManager, ReplenishmentRate, UsedCapacity};
pub use complete::{
    AddAfterDoneError, Complete, CompleteError, Completes, NotifyComplete, WithComplete,
};
pub use dedup::Dedup;
pub use managed_buffers::{ManagedBuffer, ManagedBuffers};
