use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};

const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CANCEL_MESSAGE: &str = "Canceled because the requesting CLI disconnected.";

thread_local! {
    static CURRENT_CANCEL_TOKEN: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

pub fn with_cancel_token<T>(
    token: Arc<AtomicBool>,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    CURRENT_CANCEL_TOKEN.with(|slot| {
        let previous = slot.replace(Some(token));
        let result = operation();
        slot.replace(previous);
        result
    })
}

pub fn is_canceled() -> bool {
    CURRENT_CANCEL_TOKEN.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|token| token.load(Ordering::SeqCst))
    })
}

pub fn check_canceled() -> Result<()> {
    if is_canceled() {
        return Err(anyhow!(CANCEL_MESSAGE));
    }
    Ok(())
}

pub fn sleep_with_cancellation(duration: Duration) -> Result<()> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        check_canceled()?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(CANCEL_POLL_INTERVAL));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_canceled, sleep_with_cancellation, with_cancel_token};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn cancel_token_short_circuits_checks() {
        let token = Arc::new(AtomicBool::new(true));
        let error = with_cancel_token(token, check_canceled).expect_err("cancel should propagate");
        assert!(
            error.to_string().contains("requesting CLI disconnected"),
            "{error}"
        );
    }

    #[test]
    fn sleep_returns_early_after_cancel() {
        let token = Arc::new(AtomicBool::new(true));
        let started = Instant::now();
        let error = with_cancel_token(token, || sleep_with_cancellation(Duration::from_secs(1)))
            .expect_err("cancel should stop sleep");
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "sleep should stop quickly after cancellation"
        );
        assert!(
            error.to_string().contains("requesting CLI disconnected"),
            "{error}"
        );
    }
}
