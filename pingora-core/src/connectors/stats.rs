// Copyright 2025 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Connection statistics tracking

use crate::protocols::l4::socket::SocketAddr;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Tracks active and idle connection counts for a specific backend
#[derive(Debug, Default)]
pub struct BackendConnectionStats {
    /// Number of connections currently in use (between get_stream and release_stream)
    active: AtomicUsize,
    /// Number of connections sitting idle in the pool
    idle: AtomicUsize,
}

impl BackendConnectionStats {
    /// Create a new [BackendConnectionStats]
    pub fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            idle: AtomicUsize::new(0),
        }
    }

    /// Increment the active connection counter
    pub(crate) fn increment_active(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active connection counter
    pub(crate) fn decrement_active(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment the idle connection counter
    pub(crate) fn increment_idle(&self) {
        self.idle.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the idle connection counter
    pub(crate) fn decrement_idle(&self) {
        self.idle.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get the current active connection count
    pub fn get_active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Get the current idle connection count
    pub fn get_idle(&self) -> usize {
        self.idle.load(Ordering::Relaxed)
    }

    /// Get the total connection count (active + idle)
    pub fn get_total(&self) -> usize {
        self.get_active() + self.get_idle()
    }
}

/// Immutable read-only view of backend connection statistics
///
/// This provides a safe way to read real-time connection counts without
/// allowing modification of the underlying atomic counters.
#[derive(Debug, Clone)]
pub struct BackendStatsView {
    stats: Arc<BackendConnectionStats>,
}

impl BackendStatsView {
    /// Create a new view wrapping the stats
    pub fn new(stats: Arc<BackendConnectionStats>) -> Self {
        Self { stats }
    }

    /// Get the current active connection count
    pub fn active(&self) -> usize {
        self.stats.get_active()
    }

    /// Get the current idle connection count
    pub fn idle(&self) -> usize {
        self.stats.get_idle()
    }

    /// Get the total connection count (active + idle)
    pub fn total(&self) -> usize {
        self.stats.get_total()
    }
}

/// Statistics aggregator for all backends
pub struct ConnectionStatsTracker {
    // Map from SocketAddr to stats (the actual connection statistics)
    stats: RwLock<HashMap<SocketAddr, Arc<BackendConnectionStats>>>,
    // Map from backend hash (u64) to SocketAddr (for looking up which addr a key corresponds to)
    key_to_addr: RwLock<HashMap<u64, SocketAddr>>,
}

impl ConnectionStatsTracker {
    /// Create a new [ConnectionStatsTracker]
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
            key_to_addr: RwLock::new(HashMap::new()),
        }
    }

    /// Pre-register backend addresses to initialize their stats
    ///
    /// This is useful for pre-populating the stats tracker with known backends
    /// before any connections are made, ensuring that all backends appear in
    /// `get_backend_stats()` even if they haven't been connected to yet.
    ///
    /// # Example
    /// ```ignore
    /// let tracker = ConnectionStatsTracker::new();
    /// tracker.register_backends(vec![
    ///     SocketAddr::from_str("127.0.0.1:8080").unwrap(),
    ///     SocketAddr::from_str("127.0.0.1:8081").unwrap(),
    /// ]);
    /// ```
    pub fn register_backends(&self, backends: Vec<SocketAddr>) {
        let mut stats = self.stats.write();
        for addr in backends {
            stats
                .entry(addr)
                .or_insert_with(|| Arc::new(BackendConnectionStats::new()));
        }
    }

    /// Get or create stats for a backend
    fn get_or_create_stats(&self, addr: &SocketAddr) -> Arc<BackendConnectionStats> {
        {
            let stats = self.stats.read();
            if let Some(s) = stats.get(addr) {
                return s.clone();
            }
        }

        let mut stats = self.stats.write();
        // Double-check after acquiring write lock
        if let Some(s) = stats.get(addr) {
            return s.clone();
        }

        let new_stats = Arc::new(BackendConnectionStats::new());
        stats.insert(addr.clone(), new_stats.clone());
        new_stats
    }

    /// Record that a connection was acquired from pool or newly created
    pub fn on_connection_acquired(&self, key: u64, addr: SocketAddr, was_reused: bool) {
        // Store the mapping from key to address
        {
            let mut key_to_addr = self.key_to_addr.write();
            key_to_addr.insert(key, addr.clone());
        }

        let stats = self.get_or_create_stats(&addr);
        stats.increment_active();
        if was_reused {
            stats.decrement_idle();
        }
    }

    /// Record that a connection was released back to pool
    pub fn on_connection_released(&self, key: u64) {
        // Look up the address for this key
        let addr = {
            let key_to_addr = self.key_to_addr.read();
            key_to_addr.get(&key).cloned()
        };

        if let Some(addr) = addr {
            let stats = self.get_or_create_stats(&addr);
            stats.decrement_active();
            stats.increment_idle();
        }
    }

    /// Record that a connection was closed (not returned to pool)
    pub fn on_connection_closed(&self, key: u64) {
        // Look up the address for this key
        let addr = {
            let key_to_addr = self.key_to_addr.read();
            key_to_addr.get(&key).cloned()
        };

        if let Some(addr) = addr {
            let stats = self.get_or_create_stats(&addr);
            stats.decrement_active();
        }
    }

    /// Get all backend connection statistics
    ///
    /// Returns a HashMap where the key is the backend SocketAddr
    /// and the value is a [BackendStatsView] providing real-time access to the stats.
    ///
    /// The returned views give you live access to the atomic counters, so calling
    /// `.active()` or `.idle()` on them will always return the current value.
    pub fn get_backend_stats(&self) -> HashMap<SocketAddr, BackendStatsView> {
        let stats = self.stats.read();
        stats
            .iter()
            .map(|(addr, stat)| (addr.clone(), BackendStatsView::new(stat.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddrV4, str::FromStr};

    use super::*;

    #[test]
    fn test_stats_tracking() {
        let tracker = ConnectionStatsTracker::new();
        let backend1 = SocketAddr::Inet(std::net::SocketAddr::V4(
            SocketAddrV4::from_str("127.0.0.1:3000").unwrap(),
        ));
        let backend2 = SocketAddr::Inet(std::net::SocketAddr::V4(
            SocketAddrV4::from_str("127.0.0.1:3001").unwrap(),
        ));

        // Use simple hash keys for testing
        let key1 = 1u64;
        let key2 = 2u64;

        // Acquire 3 new connections to backend1
        tracker.on_connection_acquired(key1, backend1.clone(), false);
        tracker.on_connection_acquired(key1, backend1.clone(), false);
        tracker.on_connection_acquired(key1, backend1.clone(), false);

        // Acquire 2 new connections to backend2
        tracker.on_connection_acquired(key2, backend2.clone(), false);
        tracker.on_connection_acquired(key2, backend2.clone(), false);

        // Check active counts using the new API
        let stats = tracker.get_backend_stats();
        assert_eq!(stats.get(&backend1).unwrap().active(), 3);
        assert_eq!(stats.get(&backend2).unwrap().active(), 2);

        // Release 1 connection from backend1
        tracker.on_connection_released(key1);

        // Now backend1 should have 2 active, 1 idle
        let view1 = tracker.get_backend_stats().get(&backend1).unwrap().clone();
        assert_eq!(view1.active(), 2);
        assert_eq!(view1.idle(), 1);
        assert_eq!(view1.total(), 3);

        // Acquire a reused connection (should decrease idle, increase active)
        tracker.on_connection_acquired(key1, backend1.clone(), true);

        // The view gives us live data - check again
        assert_eq!(view1.active(), 3); // 3 active
        assert_eq!(view1.idle(), 0); // 0 idle

        // Close a connection (not returned to pool)
        tracker.on_connection_closed(key1);

        assert_eq!(view1.active(), 2);
    }

    #[test]
    fn test_live_stats_view() {
        let tracker = ConnectionStatsTracker::new();
        let backend = SocketAddr::Inet(std::net::SocketAddr::V4(
            SocketAddrV4::from_str("127.0.0.1:3000").unwrap(),
        ));
        let key = 1u64;

        // Acquire connection
        tracker.on_connection_acquired(key, backend.clone(), false);

        // Get a view - this should give us live access
        let view = tracker.get_backend_stats().get(&backend).unwrap().clone();
        assert_eq!(view.active(), 1);
        assert_eq!(view.idle(), 0);

        // Release the connection
        tracker.on_connection_released(key);

        // The same view should now show updated counts
        assert_eq!(view.active(), 0);
        assert_eq!(view.idle(), 1);

        // Acquire it again
        tracker.on_connection_acquired(key, backend, true);

        // View reflects the change immediately
        assert_eq!(view.active(), 1);
        assert_eq!(view.idle(), 0);
    }

    #[test]
    fn test_get_backend_stats_nonexistent() {
        let tracker = ConnectionStatsTracker::new();
        let backend = SocketAddr::Inet(std::net::SocketAddr::V4(
            SocketAddrV4::from_str("127.0.0.1:3000").unwrap(),
        ));
        let key = 1u64;

        // Should return empty map for backend that was never seen
        assert!(tracker.get_backend_stats().get(&backend).is_none());

        // After acquiring a connection, it should exist
        tracker.on_connection_acquired(key, backend.clone(), false);
        assert!(tracker.get_backend_stats().get(&backend).is_some());
    }
}
