//! Shenko 核心协议类型 — 对应 SPEC §41.3
//! 所有跨模块协议类型定义于此，无业务逻辑。

pub mod agent;
pub mod commit;
pub mod error;
pub mod evidence;
pub mod identity;
pub mod ids;
pub mod object;
pub mod operation;
pub mod store;
pub mod subject;
pub mod task;

pub use agent::*;
pub use commit::*;
pub use error::*;
pub use evidence::*;
pub use identity::*;
pub use ids::*;
pub use object::*;
pub use operation::*;
pub use store::*;
pub use subject::*;
pub use task::*;

pub mod output;
