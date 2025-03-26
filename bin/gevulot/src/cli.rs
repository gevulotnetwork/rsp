use alloy_chains::Chain;
use clap::Parser;
use rsp_host_executor::Config;
use rsp_primitives::genesis::Genesis;
use url::Url;

/// The arguments for the cli.
#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// The HTTP rpc url used to fetch data about the block.
    #[clap(long, env)]
    pub http_rpc_url: Url,

    /// The WS rpc url used to fetch data about the block.
    #[clap(long, env)]
    pub ws_rpc_url: Url,

    /// Retry count on failed execution.
    #[clap(long, env, default_value_t = 3)]
    pub execution_retries: usize,

    /// Number of all workers in the Gevulot pool.
    #[clap(long, env)]
    pub total_workers: u64,

    /// Assignment of current worker - value between <1, workers>.
    /// Used to split the work between multiple instances.
    #[clap(long, env)]
    pub worker_pos: u64,

    /// The maximum number of concurrent tasks.
    #[clap(long, env)]
    pub max_concurrent_tasks: usize,

    /// The maximum number of concurrent executions.
    #[clap(long, env, default_value_t = 1)]
    pub max_proving_concurrency: usize,

    /// ETH proofs endpoint.
    #[clap(long, env)]
    pub eth_proofs_endpoint: String,

    /// ETH proofs API token.
    #[clap(long, env)]
    pub eth_proofs_api_token: String,

    /// Optional ETH proofs cluster ID.
    #[clap(long, default_value_t = 1)]
    pub eth_proofs_cluster_id: u64,

    /// PagerDuty integration key.
    #[clap(long, env)]
    pub pager_duty_integration_key: Option<String>,
}

impl Args {
    pub async fn as_config(&self) -> eyre::Result<Config> {
        let config = Config {
            chain: Chain::mainnet(),
            genesis: Genesis::Mainnet,
            rpc_url: Some(self.http_rpc_url.clone()),
            cache_dir: None,
            custom_beneficiary: None,
            prove: true,
            max_proving_concurrency: self.max_proving_concurrency,
            opcode_tracking: false,
        };

        Ok(config)
    }
}
