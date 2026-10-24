//! Replaceable ContextPolicy contract — SPEC §12.3.1–12.3.2.
//!
//! Strategies (budget compact, snapcompact, others) live in their own plugins
//! and only implement [`ContextPolicy`]. This crate owns what they share: stable
//! history references, the prepare → verify → commit window transaction,
//! unfinished-call preservation, availability declarations and cost reports.
//! A window changes what the model sees next, never the raw history: every
//! committed window keeps its source refs and derivatives, so it stays readable
//! after the plugin that produced it is updated or uninstalled.
pub mod history;
pub mod policy;
pub mod registry;
#[cfg(feature = "store")]
pub mod store_history;
pub mod window;

pub use history::*;
pub use policy::*;
pub use registry::*;
#[cfg(feature = "store")]
pub use store_history::*;
pub use window::*;
