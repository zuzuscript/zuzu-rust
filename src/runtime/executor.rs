use std::future::Future;

use tokio::runtime::{Builder, Runtime as TokioRuntime};
use tokio::task::LocalSet;

pub(in crate::runtime) struct AsyncExecutor {
    runtime: TokioRuntime,
    local: LocalSet,
}

impl AsyncExecutor {
    pub(in crate::runtime) fn new() -> Self {
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("zuzu-rust async executor should initialize");
        Self {
            runtime,
            local: LocalSet::new(),
        }
    }

    pub(in crate::runtime) fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.local.block_on(&self.runtime, future)
    }

    pub(in crate::runtime) fn spawn_local<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.local.spawn_local(future);
    }

    pub(in crate::runtime) fn enter<T>(&self, f: impl FnOnce() -> T) -> T {
        let _guard = self.runtime.enter();
        f()
    }

    pub(in crate::runtime) fn sleep_until(&self, deadline: std::time::Instant) {
        let now = std::time::Instant::now();
        if deadline <= now {
            self.block_on(tokio::task::yield_now());
            return;
        }
        self.block_on(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )));
    }

    pub(in crate::runtime) fn sleep_for(&self, duration: std::time::Duration) {
        if duration.is_zero() {
            return;
        }
        self.block_on(tokio::time::sleep(duration));
    }
}
