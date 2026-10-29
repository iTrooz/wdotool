//! Own the dispatcher lifetime and track the raw input actually sent to EIS.

use std::sync::Mutex;

use super::*;

const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

/// Retain session ownership even if authorization or backend construction is cancelled.
pub(super) struct PortalOwner {
    pub(super) session: Option<Arc<ashpd::desktop::Session<RemoteDesktop>>>,
}

impl PortalOwner {
    /// Adopt a newly created portal session before the next cancellation point.
    pub(super) fn new(session: Option<Arc<ashpd::desktop::Session<RemoteDesktop>>>) -> Self {
        Self { session }
    }

    /// Bound portal cleanup; a revoked or unavailable portal must not hang exit.
    pub(super) async fn close(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = tokio::time::timeout(CLOSE_TIMEOUT, session.close()).await;
        }
    }
}

impl Drop for PortalOwner {
    /// Cancellation may outlive its Tokio runtime, so cleanup owns a small runtime.
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = std::thread::Builder::new()
                .name("portal-close".into())
                .spawn(move || {
                    if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        runtime.block_on(async {
                            let _ = tokio::time::timeout(CLOSE_TIMEOUT, session.close()).await;
                        });
                    }
                });
        }
    }
}

/// Stop-on-drop ownership also covers partially initialized backends.
pub(super) struct Lifetime {
    stop: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Lifetime {
    /// Adopt the dispatcher immediately, before any fallible readiness checks.
    pub(super) fn new(stop: oneshot::Sender<()>, thread: std::thread::JoinHandle<()>) -> Self {
        Self {
            stop: Mutex::new(Some(stop)),
            thread: Mutex::new(Some(thread)),
        }
    }

    /// Signal once; joining happens asynchronously during explicit shutdown.
    fn stop(&self) {
        if let Some(stop) = self.stop.lock().unwrap().take() {
            let _ = stop.send(());
        }
    }

    /// Wait until the dispatcher has closed its EIS and portal resources.
    pub(super) async fn shutdown(&self) -> Result<()> {
        self.stop();
        let thread = self.thread.lock().unwrap().take();
        if let Some(thread) = thread {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(|e| failure(&e.to_string()))?
                .map_err(|_| failure("libei dispatcher panicked"))?;
        }
        Ok(())
    }
}

impl Drop for Lifetime {
    /// Never leave the dispatcher running when construction or its owner fails.
    fn drop(&mut self) {
        self.stop();
    }
}

/// Convert terminal lifecycle errors to the existing public backend error type.
pub(super) fn failure(reason: &str) -> WdoError {
    WdoError::Backend {
        backend: NAME,
        source: reason.to_string().into(),
    }
}

/// Observe portal revocation independently of EIS traffic.
pub(super) async fn portal_closed(session: &Option<Arc<ashpd::desktop::Session<RemoteDesktop>>>) {
    if let Some(session) = session {
        match session.receive_closed().await {
            Ok(stream) => {
                tokio::pin!(stream);
                stream.next().await;
            }
            Err(err) => warn!(?err, "cannot monitor portal session closure"),
        }
    } else {
        std::future::pending::<()>().await;
    }
}

impl State {
    /// Track before queuing so even a failed flush leaves releases to attempt.
    pub(super) fn send_key(
        &mut self,
        keyboard: &ei::Keyboard,
        code: u32,
        state: ei::keyboard::KeyState,
    ) {
        if state == ei::keyboard::KeyState::Press {
            self.held_keys.insert(code);
        } else {
            self.held_keys.remove(&code);
        }
        keyboard.key(code, state);
    }

    /// Buttons have independent evdev codes, not xdotool CLI indices.
    pub(super) fn send_button(
        &mut self,
        button: &ei::Button,
        code: u32,
        state: ei::button::ButtonState,
    ) {
        if state == ei::button::ButtonState::Press {
            self.held_buttons.insert(code);
        } else {
            self.held_buttons.remove(&code);
        }
        button.button(code, state);
    }
}

impl LibeiBackend {
    /// Release raw keys rather than re-resolving symbols against a changed keymap.
    pub(super) fn release_held(&self) -> Result<()> {
        let (keys, buttons) = {
            let st = self.state.lock().unwrap();
            (
                st.held_keys.iter().copied().collect::<Vec<_>>(),
                st.held_buttons.iter().copied().collect::<Vec<_>>(),
            )
        };
        let mut result = Ok(());
        if !keys.is_empty() {
            result = self.emit_frame(DeviceChoice::Keyboard, |device, st| {
                if let Some(kb) = device.interface::<ei::Keyboard>() {
                    for code in keys {
                        st.send_key(&kb, code, ei::keyboard::KeyState::Released);
                    }
                }
            });
        }
        if !buttons.is_empty() {
            let released = self.emit_frame(DeviceChoice::Button, |device, st| {
                if let Some(button) = device.interface::<ei::Button>() {
                    for code in buttons {
                        st.send_button(&button, code, ei::button::ButtonState::Released);
                    }
                }
            });
            result = result.and(released);
        }
        result
    }
}
