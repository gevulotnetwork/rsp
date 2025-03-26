use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_provider::{Provider, ProviderBuilder, WsConnect};
use clap::Parser;
use cli::Args;
use eth_proofs::EthProofsClient;
use futures::StreamExt;
use rsp_host_executor::{
    alerting::AlertingClient, create_eth_block_execution_strategy_factory, BlockExecutor,
    EthExecutorComponents, ExecutorComponents, FullExecutor,
};
use rsp_provider::create_provider;
use sp1_sdk::{include_elf, CudaProver, SP1Prover};
use tokio::{sync::Semaphore, task};
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod cli;

mod eth_proofs;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Initialize the environment variables.
    dotenv::dotenv().ok();

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    // Initialize the logger.
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::from_default_env()
                .add_directive("sp1_core_machine=warn".parse().unwrap())
                .add_directive("sp1_core_executor=warn".parse().unwrap())
                .add_directive("sp1_prover=warn".parse().unwrap()),
        )
        .init();

    // Parse the command line arguments.
    let args = Args::parse();
    let config = args.as_config().await?;

    let elf = include_elf!("rsp-client").to_vec();
    let block_execution_strategy_factory =
        create_eth_block_execution_strategy_factory(&config.genesis, None);

    let eth_proofs_client = EthProofsClient::new(
        args.eth_proofs_cluster_id,
        args.eth_proofs_endpoint,
        args.eth_proofs_api_token,
    );
    let alerting_client =
        args.pager_duty_integration_key.map(|key| Arc::new(AlertingClient::new(key)));

    let ws = WsConnect::new(args.ws_rpc_url);
    let ws_provider = ProviderBuilder::new().on_ws(ws).await?;
    let http_provider = create_provider::<alloy_network::Ethereum>(args.http_rpc_url);

    // Subscribe to block headers.
    let subscription = ws_provider.subscribe_blocks().await?;
    let mut stream = subscription.into_stream().map(|h| h.number);

    // SP1 - use CPU for execution, CUDA for proving.
    // NOTE: At this point moongate container gets created, stopped when variable gets dropped.
    let moongate_endpoint = None;
    let prover_client = Arc::new(CudaProver::new(SP1Prover::new(), moongate_endpoint));

    let execution_hooks = eth_proofs_client; // For now we can just submit every block to staging.
    let executor = Arc::new(
        FullExecutor::<EthExecutorComponents<_, _>, _>::try_new(
            http_provider.clone(),
            elf,
            block_execution_strategy_factory,
            prover_client,
            execution_hooks,
            config,
        )
        .await?,
    );

    info!("Latest block number: {}", http_provider.get_block_number().await?);

    let concurrent_tasks_semaphore = Arc::new(Semaphore::new(args.max_concurrent_tasks));

    while let Some(block_number) = stream.next().await {
        info!("Received block: {:?}", block_number);

        // Simple coordination logic.
        let target_worker_pos = (block_number % args.total_workers) + 1;
        if target_worker_pos != args.worker_pos {
            info!("Skipping, block {} is for worker {}", block_number, target_worker_pos);
            continue;
        }
        info!("Processing block {} on worker {}", block_number, args.worker_pos);

        let executor = executor.clone();
        let alerting_client = alerting_client.clone();
        let task_permit = concurrent_tasks_semaphore.clone().acquire_owned().await?;

        task::spawn(async move {
            match process_block(block_number, executor, args.execution_retries).await {
                Ok(_) => info!("Successfully processed block {}", block_number),
                Err(err) => {
                    let error_message = format!("Error executing block {}: {}", block_number, err);
                    error!("{error_message}");

                    if let Some(alerting_client) = &alerting_client {
                        alerting_client.send_alert(format!("Gevulot RSP - {error_message}")).await;
                    }
                }
            }

            drop(task_permit);
        });
    }

    Ok(())
}

#[instrument(skip(executor, max_retries))]
async fn process_block<C, P>(
    number: u64,
    executor: Arc<FullExecutor<C, P>>,
    max_retries: usize,
) -> eyre::Result<()>
where
    C: ExecutorComponents<Network = Ethereum>,
    P: Provider<Ethereum> + Clone,
{
    let mut retry_count = 0;

    // Wait for the block to be avaliable in the HTTP provider
    executor.wait_for_block(number).await?;

    loop {
        match executor.execute(number).await {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                warn!("Failed to execute block {number}: {err}, retrying...");
                retry_count += 1;
                if retry_count > max_retries {
                    error!("Max retries reached for block: {number}");
                    return Err(err);
                }
            }
        }
    }
}
