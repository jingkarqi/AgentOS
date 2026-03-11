use std::sync::atomic::{AtomicU64, Ordering};

use time::OffsetDateTime;
use uuid::Uuid;

pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

pub trait IdGenerator: Send + Sync {
    fn next_uuid(&self) -> Uuid;
}

#[derive(Debug, Default)]
pub struct RandomIdGenerator;

impl IdGenerator for RandomIdGenerator {
    fn next_uuid(&self) -> Uuid {
        Uuid::new_v4()
    }
}

#[derive(Debug)]
pub struct DeterministicIdGenerator {
    counter: AtomicU64,
}

impl DeterministicIdGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            counter: AtomicU64::new(seed),
        }
    }
}

impl Default for DeterministicIdGenerator {
    fn default() -> Self {
        Self::new(0)
    }
}

impl IdGenerator for DeterministicIdGenerator {
    fn next_uuid(&self) -> Uuid {
        let next = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Uuid::from_u128(next as u128)
    }
}

#[derive(Debug, Clone)]
pub struct FixedClock {
    value: OffsetDateTime,
}

impl FixedClock {
    pub fn new(value: OffsetDateTime) -> Self {
        Self { value }
    }
}

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.value
    }
}
