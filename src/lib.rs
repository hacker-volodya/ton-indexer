use std::sync::Arc;

use anyhow::Result;

pub use crate::config::*;
use crate::engine::complex_operations::*;
pub use crate::engine::{BlockSubscriber, Engine};
use crate::network::*;
use crate::utils::*;

mod config;
mod engine;
mod network;
mod storage;
pub mod utils;

pub async fn start(
    node_config: NodeConfig,
    global_config: GlobalConfig,
    subscribers: Vec<Arc<dyn BlockSubscriber>>,
) -> Result<()> {
    let engine = Engine::new(node_config, global_config, subscribers).await?;

    start_full_node_service(engine.clone())?;

    let BootData {
        last_mc_block_id,
        shards_client_mc_block_id,
    } = boot(&engine).await?;

    log::info!(
        "Initialized (last block: {}, shards client block id: {})",
        last_mc_block_id,
        shards_client_mc_block_id
    );

    engine
        .listen_broadcasts(ton_block::ShardIdent::masterchain())
        .await?;
    engine
        .listen_broadcasts(
            ton_block::ShardIdent::with_tagged_prefix(
                ton_block::BASE_WORKCHAIN_ID,
                ton_block::SHARD_FULL,
            )
            .convert()?,
        )
        .await?;

    if !engine.check_sync().await? {
        sync(&engine).await?;
    }

    log::info!("Synced!");

    tokio::spawn({
        let engine = engine.clone();
        async move {
            if let Err(e) = walk_masterchain_blocks(&engine, last_mc_block_id).await {
                log::error!(
                    "FATAL ERROR while walking though masterchain blocks: {:?}",
                    e
                );
            }
        }
    });

    tokio::spawn({
        let engine = engine.clone();
        async move {
            if let Err(e) = walk_shard_blocks(&engine, shards_client_mc_block_id).await {
                log::error!("FATAL ERROR while walking though shard blocks: {:?}", e);
            }
        }
    });

    futures::future::pending().await
}

fn start_full_node_service(engine: Arc<Engine>) -> Result<()> {
    let service = FullNodeOverlayService::new();

    let network = engine.network();

    let (_, masterchain_overlay_id) =
        network.compute_overlay_id(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL)?;
    network.add_subscriber(masterchain_overlay_id, service.clone());

    let (_, basechain_overlay_id) =
        network.compute_overlay_id(ton_block::BASE_WORKCHAIN_ID, ton_block::SHARD_FULL)?;
    network.add_subscriber(basechain_overlay_id, service);

    Ok(())
}
