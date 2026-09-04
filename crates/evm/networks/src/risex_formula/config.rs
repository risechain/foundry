//! Process-wide provider configuration for the RISEx risk-formula precompile.

use std::sync::atomic::{AtomicU8, Ordering};

use clap::ValueEnum;
use serde::Serialize;

const UNSET_PROVIDER_MODE: u8 = 2;

/// Selects the implementation that serves valid risk-formula requests.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, ValueEnum)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum ProviderMode {
    /// Do not enable a native provider.
    #[default]
    Off = 0,
    /// Use the bounded specialized portfolio-risk provider.
    Specialized = 1,
}

impl ProviderMode {
    const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Specialized,
            _ => Self::Off,
        }
    }
}

/// Failure to install a different process-wide provider mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderModeConflict {
    pub current: ProviderMode,
    pub requested: ProviderMode,
}

static PROVIDER_MODE: AtomicU8 = AtomicU8::new(UNSET_PROVIDER_MODE);

/// Returns the installed mode, or the safe off default before installation.
pub fn provider_mode() -> ProviderMode {
    ProviderMode::from_u8(PROVIDER_MODE.load(Ordering::Relaxed))
}

/// Installs one process-wide mode before constructing an EVM.
///
/// Reinstalling the identical setting is harmless; a different setting is rejected.
pub fn set_provider_mode(requested: ProviderMode) -> Result<(), ProviderModeConflict> {
    match PROVIDER_MODE.compare_exchange(
        UNSET_PROVIDER_MODE,
        requested as u8,
        Ordering::Relaxed,
        Ordering::Relaxed,
    ) {
        Ok(_) => Ok(()),
        Err(current) if current == requested as u8 => Ok(()),
        Err(current) => {
            Err(ProviderModeConflict { current: ProviderMode::from_u8(current), requested })
        }
    }
}

#[cfg(test)]
static TEST_CONFIG_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub(super) fn reset_provider_mode_for_test() -> ProviderModeResetGuard {
    let test_lock = TEST_CONFIG_LOCK.lock().expect("provider mode test lock poisoned");
    PROVIDER_MODE.store(UNSET_PROVIDER_MODE, Ordering::Relaxed);
    ProviderModeResetGuard { test_lock }
}

/// Holds exclusive access to the process mode and resets it on drop.
#[cfg(test)]
pub(super) struct ProviderModeResetGuard {
    test_lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for ProviderModeResetGuard {
    fn drop(&mut self) {
        let _ = &self.test_lock;
        PROVIDER_MODE.store(UNSET_PROVIDER_MODE, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProviderMode, ProviderModeConflict, provider_mode, reset_provider_mode_for_test,
        set_provider_mode,
    };

    #[test]
    fn provider_mode_defaults_to_off() {
        let _reset = reset_provider_mode_for_test();

        assert_eq!(provider_mode(), ProviderMode::Off);
    }

    #[test]
    fn provider_mode_is_idempotent_and_rejects_a_conflict() {
        let _reset = reset_provider_mode_for_test();

        assert_eq!(set_provider_mode(ProviderMode::Specialized), Ok(()));
        assert_eq!(set_provider_mode(ProviderMode::Specialized), Ok(()));
        assert_eq!(provider_mode(), ProviderMode::Specialized);
        assert_eq!(
            set_provider_mode(ProviderMode::Off),
            Err(ProviderModeConflict {
                current: ProviderMode::Specialized,
                requested: ProviderMode::Off,
            }),
        );
    }
}
