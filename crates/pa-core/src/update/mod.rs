//! The update flow's client-side support (spec §4 `Planning` through `Staged`): release-manifest
//! fetch, semver/channel policy, the managed install-root layout, and candidate staging; kept
//! daemon-free so pa-core stays the shared client/service layer.

pub mod download;
pub mod install;
pub mod installer;
pub mod release;
pub mod version;
