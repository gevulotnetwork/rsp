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

    info!("Running with args: {:?}", args);

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

    let mut hooks_list = ExecutionHooksList::new();
    hooks_list.add_hook(eth_proofs_client); // TODO: Allow disabling this hook.

    let ws = WsConnect::new(args.ws_rpc_url);
    let ws_provider = ProviderBuilder::new().on_ws(ws).await?;
    let http_provider = create_provider::<alloy_network::Ethereum>(args.http_rpc_url);

    // Subscribe to block headers.
    let subscription = ws_provider.subscribe_blocks().await?;
    let mut stream = subscription.into_stream().map(|h| h.number);

    // Pool of SP1 provers.
    let num_provers = args.gpu_count;
    info!("Creating executors pool of {} SP1 provers...", num_provers);
    let (executors_pool_tx, mut executors_pool_rx) = tokio::sync::mpsc::channel(num_provers);
    for gpu_id in 0..num_provers {
        info!("Creating SP1 prover for GPU {}", gpu_id);
        // SP1 - use CPU for execution, CUDA for proving.
        // NOTE: At this point moongate container gets created, stopped when variable gets dropped.
        let moongate_endpoint = None;
        let selected_gpu = Some(gpu_id as u8);
        let prover_client =
            Arc::new(CudaProver::new(SP1Prover::new(), moongate_endpoint, selected_gpu));

        let execution_hooks = hooks_list.clone();
        let executor = Arc::new(
            FullExecutor::<EthExecutorComponents<_, _>, _>::try_new(
                http_provider.clone(),
                elf.clone(),
                block_execution_strategy_factory.clone(),
                prover_client.clone(),
                execution_hooks,
                config.clone(),
            )
            .await?,
        );

        executors_pool_tx.send(executor).await?;
    }
    info!("Executors pool ready");
    info!("Latest block number: {}", http_provider.get_block_number().await?);

    let concurrent_tasks_semaphore = Arc::new(Semaphore::new(args.max_concurrent_tasks));

    let mut handles = Vec::new();

    while let Some(block_number) = stream.next().await {
        info!("Received block: {:?}", block_number);

        // Simple coordination logic.
        let target_worker_pos = (block_number % args.total_workers) + 1;
        if target_worker_pos != args.worker_pos {
            info!("Skipping, block {} is for worker {}", block_number, target_worker_pos);
            continue;
        }

        let executor = match executors_pool_rx.recv().await {
            Some(executor) => executor,
            None => {
                error!("No executors available, exiting...");
                break;
            }
        };
        let alerting_client = alerting_client.clone();
        let executors_pool_tx_clone = executors_pool_tx.clone();
        info!(
            "Block {} will be processed on worker {} (waiting for permit...)",
            block_number, args.worker_pos
        );
        let task_permit = concurrent_tasks_semaphore.clone().acquire_owned().await?;

        info!("Processing block {} on worker {}", block_number, args.worker_pos);
        let task_handle = task::spawn(async move {
            let executor_arc_clone = executor.clone();
            match process_block(block_number, executor_arc_clone, args.execution_retries).await {
                Ok(_) => {
                    info!("Successfully processed block {}", block_number);
                }
                Err(err) => {
                    let error_message = format!("Error executing block {}: {}", block_number, err);
                    error!("{error_message}");

                    if let Some(alerting_client) = &alerting_client {
                        alerting_client.send_alert(format!("Gevulot RSP - {error_message}")).await;
                    }
                }
            }

            // Return the executor back to the pool.
            executors_pool_tx_clone.send(executor).await.unwrap();

            drop(task_permit);
        });
        handles.push(task_handle);
    }

    info!("Waiting for all tasks to finish...");
    futures::future::join_all(handles).await;

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
