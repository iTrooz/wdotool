//! Track explicit holds across commands without duplicating CLI key-chain logic.

use std::sync::Mutex;
use std::time::Duration;

use wdotool_core::{Backend, DynBackend, KeyDirection, MouseButton, Result};

/// Backend calls own the state, so partial chains and cancelled requests count.
pub struct HeldBackend {
    inner: DynBackend,
    keys: Mutex<Vec<String>>,
    buttons: Mutex<Vec<MouseButton>>,
}

impl HeldBackend {
    /// Wrap exactly one backend; never reconnect from this adapter.
    pub fn new(inner: DynBackend) -> Self {
        Self {
            inner,
            keys: Mutex::new(Vec::new()),
            buttons: Mutex::new(Vec::new()),
        }
    }

    /// Attempt every release even when an earlier one failed.
    pub async fn release(&self) -> Result<()> {
        let keys = std::mem::take(&mut *self.keys.lock().unwrap());
        let buttons = std::mem::take(&mut *self.buttons.lock().unwrap());
        let mut result = Ok(());
        for key in keys.into_iter().rev() {
            let released = self.inner.key(&key, KeyDirection::Release).await;
            result = result.and(released);
        }
        for button in buttons.into_iter().rev() {
            let released = self.inner.mouse_button(button, KeyDirection::Release).await;
            result = result.and(released);
        }
        let internal = self.inner.release_input().await;
        result.and(internal)
    }
}

/// Record a possible press before awaiting; remove only after a successful release.
fn before<T: PartialEq + Clone>(held: &Mutex<Vec<T>>, value: &T, dir: KeyDirection) {
    if dir != KeyDirection::Release {
        let mut held = held.lock().unwrap();
        if !held.contains(value) {
            held.push(value.clone());
        }
    }
}

/// Successful taps do not leave an explicit hold behind.
fn after<T: PartialEq>(held: &Mutex<Vec<T>>, value: &T, dir: KeyDirection) {
    if dir != KeyDirection::Press {
        held.lock().unwrap().retain(|v| v != value);
    }
}

#[async_trait::async_trait]
impl Backend for HeldBackend {
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    fn capabilities(&self) -> wdotool_core::Capabilities {
        self.inner.capabilities()
    }
    fn check_health(&self) -> Result<()> {
        self.inner.check_health()
    }

    async fn release_input(&self) -> Result<()> {
        self.release().await
    }

    async fn shutdown(&self) -> Result<()> {
        let released = self.release().await;
        let stopped = self.inner.shutdown().await;
        released.and(stopped)
    }

    async fn key(&self, symbol: &str, dir: KeyDirection) -> Result<()> {
        // Bound cooperative cancellation latency for large zero-delay repeats.
        tokio::task::yield_now().await;
        let symbol = symbol.to_owned();
        before(&self.keys, &symbol, dir);
        self.inner.key(&symbol, dir).await?;
        after(&self.keys, &symbol, dir);
        Ok(())
    }

    async fn mouse_button(&self, button: MouseButton, dir: KeyDirection) -> Result<()> {
        tokio::task::yield_now().await;
        before(&self.buttons, &button, dir);
        self.inner.mouse_button(button, dir).await?;
        after(&self.buttons, &button, dir);
        Ok(())
    }

    async fn type_text(&self, text: &str, delay: Duration) -> Result<()> {
        self.inner.type_text(text, delay).await
    }
    async fn mouse_move(&self, x: i32, y: i32, absolute: bool) -> Result<()> {
        self.inner.mouse_move(x, y, absolute).await
    }
    async fn mouse_move_to_output(&self, output: &str, x: i32, y: i32) -> Result<()> {
        self.inner.mouse_move_to_output(output, x, y).await
    }
    async fn scroll(&self, dx: f64, dy: f64) -> Result<()> {
        self.inner.scroll(dx, dy).await
    }
    async fn list_windows(&self) -> Result<Vec<wdotool_core::WindowInfo>> {
        self.inner.list_windows().await
    }
    async fn active_window(&self) -> Result<Option<wdotool_core::WindowInfo>> {
        self.inner.active_window().await
    }
    async fn activate_window(&self, id: &wdotool_core::WindowId) -> Result<()> {
        self.inner.activate_window(id).await
    }
    async fn close_window(&self, id: &wdotool_core::WindowId) -> Result<()> {
        self.inner.close_window(id).await
    }
    async fn pointer_position(&self) -> Result<Option<(i32, i32)>> {
        self.inner.pointer_position().await
    }
    async fn list_outputs(&self) -> Result<Vec<wdotool_core::OutputInfo>> {
        self.inner.list_outputs().await
    }
    async fn window_geometry(
        &self,
        id: &wdotool_core::WindowId,
    ) -> Result<Option<wdotool_core::WindowGeometry>> {
        self.inner.window_geometry(id).await
    }
}
