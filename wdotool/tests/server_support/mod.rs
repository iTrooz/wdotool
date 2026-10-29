//! Observable, controllable backend for IPC tests; never touches a desktop.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Notify, Semaphore};
use wdotool_core::backend::mock::MockBackend;
use wdotool_core::*;

/// Shared observability survives moving the backend into the server.
#[derive(Clone)]
pub struct TestBackend {
    pub mock: Arc<MockBackend>,
    pub lost: Arc<AtomicBool>,
    pub shutdowns: Arc<AtomicUsize>,
    pub started: Arc<Notify>,
    pub gate: Arc<Semaphore>,
    pub block_a: bool,
    pub fail_shutdown: bool,
}

impl TestBackend {
    /// Make all external state explicit and independent for each test.
    pub fn new() -> Self {
        Self {
            mock: Arc::new(MockBackend::new()),
            lost: Arc::new(AtomicBool::new(false)),
            shutdowns: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(Notify::new()),
            gate: Arc::new(Semaphore::new(0)),
            block_a: false,
            fail_shutdown: false,
        }
    }
}

#[async_trait]
impl Backend for TestBackend {
    fn name(&self) -> &'static str {
        "test-persistent"
    }
    fn capabilities(&self) -> Capabilities {
        self.mock.capabilities()
    }
    fn check_health(&self) -> Result<()> {
        if self.lost.load(Ordering::SeqCst) {
            Err(WdoError::Backend {
                backend: "test",
                source: "authorization revoked".into(),
            })
        } else {
            Ok(())
        }
    }
    async fn shutdown(&self) -> Result<()> {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        if self.fail_shutdown {
            return Err(WdoError::InvalidArg("injected shutdown failure".into()));
        }
        Ok(())
    }
    async fn key(&self, key: &str, dir: KeyDirection) -> Result<()> {
        self.mock.key(key, dir).await?;
        if key == "a" && self.block_a && dir != KeyDirection::Release {
            self.started.notify_one();
            self.gate.acquire().await.unwrap().forget();
        }
        if key == "a" && !self.block_a && dir == KeyDirection::PressRelease {
            return Err(WdoError::InvalidArg(
                "injected partial-chain failure".into(),
            ));
        }
        Ok(())
    }
    async fn type_text(&self, text: &str, delay: Duration) -> Result<()> {
        self.mock.type_text(text, delay).await
    }
    async fn mouse_move(&self, x: i32, y: i32, absolute: bool) -> Result<()> {
        self.mock.mouse_move(x, y, absolute).await
    }
    async fn mouse_button(&self, btn: MouseButton, dir: KeyDirection) -> Result<()> {
        self.mock.mouse_button(btn, dir).await
    }
    async fn scroll(&self, dx: f64, dy: f64) -> Result<()> {
        self.mock.scroll(dx, dy).await
    }
    async fn list_windows(&self) -> Result<Vec<WindowInfo>> {
        self.mock.list_windows().await
    }
    async fn active_window(&self) -> Result<Option<WindowInfo>> {
        self.mock.active_window().await
    }
    async fn activate_window(&self, id: &WindowId) -> Result<()> {
        self.mock.activate_window(id).await
    }
    async fn close_window(&self, id: &WindowId) -> Result<()> {
        self.mock.close_window(id).await
    }
    async fn pointer_position(&self) -> Result<Option<(i32, i32)>> {
        self.mock.pointer_position().await
    }
}
