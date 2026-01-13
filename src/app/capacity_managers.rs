//! Capacity managers for flow control.

use std::time::Duration;

use crate::config::ResolvedCapacityLimits;
use crate::repo::FlowControl;
use crate::util::{CapacityManager, ReplenishmentRate};

/// A collection of capacity managers for flow control.
///
/// Each manager is optional - `None` means that type of limiting is disabled.
#[derive(Clone, Default)]
pub struct CapacityManagers {
    pub concurrent_requests: Option<CapacityManager>,
    pub request_rate: Option<CapacityManager>,
    pub read_throughput: Option<CapacityManager>,
    pub write_throughput: Option<CapacityManager>,
    pub total_throughput: Option<CapacityManager>,
}

impl CapacityManagers {
    /// Create capacity managers from resolved limits.
    ///
    /// For rate/throughput limits, creates managers with 1-second replenishment.
    pub fn from_resolved_limits(limits: &ResolvedCapacityLimits) -> Self {
        Self {
            concurrent_requests: limits
                .max_concurrent_requests
                .map(|v| CapacityManager::new(v as u64)),
            request_rate: limits.max_requests_per_second.map(|v| {
                CapacityManager::with_replenishment(
                    v as u64,
                    ReplenishmentRate::new(v as u64, Duration::from_secs(1)),
                )
            }),
            read_throughput: limits.max_read_bytes_per_second.map(|v| {
                CapacityManager::with_replenishment(
                    v,
                    ReplenishmentRate::new(v, Duration::from_secs(1)),
                )
            }),
            write_throughput: limits.max_write_bytes_per_second.map(|v| {
                CapacityManager::with_replenishment(
                    v,
                    ReplenishmentRate::new(v, Duration::from_secs(1)),
                )
            }),
            total_throughput: limits.max_total_bytes_per_second.map(|v| {
                CapacityManager::with_replenishment(
                    v,
                    ReplenishmentRate::new(v, Duration::from_secs(1)),
                )
            }),
        }
    }

    /// Convert to FlowControl for use with Repo.
    pub fn to_flow_control(&self) -> FlowControl {
        FlowControl {
            concurrent_request_limiter: self.concurrent_requests.clone(),
            request_rate_limiter: self.request_rate.clone(),
            read_throughput_limiter: self.read_throughput.clone(),
            write_throughput_limiter: self.write_throughput.clone(),
            total_throughput_limiter: self.total_throughput.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_capacity_managers_from_limits() {
        let limits = ResolvedCapacityLimits {
            max_concurrent_requests: Some(40),
            max_requests_per_second: Some(100),
            max_read_bytes_per_second: Some(100 * 1024 * 1024),
            max_write_bytes_per_second: None, // Disabled
            max_total_bytes_per_second: Some(200 * 1024 * 1024),
        };

        let managers = CapacityManagers::from_resolved_limits(&limits);

        assert!(managers.concurrent_requests.is_some());
        assert!(managers.request_rate.is_some());
        assert!(managers.read_throughput.is_some());
        assert!(managers.write_throughput.is_none()); // Disabled
        assert!(managers.total_throughput.is_some());
    }

    #[tokio::test]
    async fn test_capacity_managers_to_flow_control() {
        let limits = ResolvedCapacityLimits {
            max_concurrent_requests: Some(40),
            max_requests_per_second: None,
            max_read_bytes_per_second: None,
            max_write_bytes_per_second: None,
            max_total_bytes_per_second: None,
        };

        let managers = CapacityManagers::from_resolved_limits(&limits);
        let flow_control = managers.to_flow_control();

        assert!(flow_control.concurrent_request_limiter.is_some());
        assert!(flow_control.request_rate_limiter.is_none());
    }
}
