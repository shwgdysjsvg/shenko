//! Consumer of committed S1 work. Broadcasts are only wakeups/projections.
use shenko_store::Store;
use std::sync::Arc;

pub async fn run_committed_dispatcher(
    store: Arc<Store>,
    cancel: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        tokio::select! { biased;
            _=cancel.cancelled()=>return Ok(()),
            _=tick.tick()=>{
                store.settle_subject_memory()?;
                store.reconcile_subject_publications()?;
            }
        }
    }
}
