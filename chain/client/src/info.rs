use crate::config_updater::ConfigUpdater;
use crate::{SyncStatus, metrics};
use itertools::Itertools;
use lru::LruCache;
use near_async::messaging::Sender;
use near_async::time::{Clock, Instant};
use near_chain_configs::{ClientConfig, LogSummaryStyle, SyncConfig};
use near_client_primitives::types::StateSyncStatus;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkInfo;
use near_primitives::block::Tip;
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::telemetry::{
    TelemetryAgentInfo, TelemetryChainInfo, TelemetryInfo, TelemetrySystemInfo,
};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochHeight, EpochId, Gas, NumBlocks, ShardId, ValidatorId,
    ValidatorInfoIdentifier,
};
use near_primitives::unwrap_or_return;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{PROTOCOL_VERSION, Version};
use near_primitives::views::{
    CatchupStatusView, ChunkProcessingStatus, CurrentEpochValidatorInfo, EpochValidatorInfo,
    ValidatorKickoutView,
};
use near_telemetry::TelemetryEvent;
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;
use sysinfo::{Pid, ProcessExt, System, SystemExt, get_current_pid, set_open_files_limit};
use time::ext::InstantExt as _;
use tracing::info;

const TERAGAS: f64 = 1_000_000_000_000_f64;

struct ValidatorInfoHelper {
    pub is_validator: bool,
    pub num_validators: usize,
}

/// A helper that prints information about current chain and reports to telemetry.
pub struct InfoHelper {
    clock: Clock,
    /// Nearcore agent (executable) version
    nearcore_version: Version,
    /// System reference.
    sys: System,
    /// Process id to query resources.
    pid: Option<Pid>,
    /// Timestamp when client was started.
    started: Instant,
    /// Total number of blocks processed.
    num_blocks_processed: u64,
    /// Total number of blocks processed.
    num_chunks_in_blocks_processed: u64,
    /// Total gas used during period.
    gas_used: u64,
    /// Telemetry event sender.
    telemetry_sender: Sender<TelemetryEvent>,
    /// Log coloring enabled.
    log_summary_style: LogSummaryStyle,
    /// Epoch id.
    epoch_id: Option<EpochId>,
    /// Timestamp of starting the client.
    pub boot_time_seconds: i64,
    // Allows more detailed logging, for example a list of orphaned blocks.
    enable_multiline_logging: bool,
    // Keeps track of the previous SyncRequirement for updating metrics.
    prev_sync_requirement: Option<String>,
    /// Number of validators (block + chunk producers) per epoch, cached for a small number of epochs.
    num_validators_per_epoch: LruCache<EpochId, usize>,
}

impl InfoHelper {
    pub fn new(
        clock: Clock,
        telemetry_sender: Sender<TelemetryEvent>,
        client_config: &ClientConfig,
    ) -> Self {
        set_open_files_limit(0);
        metrics::export_version(&client_config.chain_id, &client_config.version);
        InfoHelper {
            clock: clock.clone(),
            nearcore_version: client_config.version.clone(),
            sys: System::new(),
            pid: get_current_pid().ok(),
            started: clock.now(),
            num_blocks_processed: 0,
            num_chunks_in_blocks_processed: 0,
            gas_used: 0,
            telemetry_sender,
            log_summary_style: client_config.log_summary_style,
            boot_time_seconds: clock.now_utc().unix_timestamp(),
            epoch_id: None,
            enable_multiline_logging: client_config.enable_multiline_logging,
            prev_sync_requirement: None,
            num_validators_per_epoch: LruCache::new(NonZeroUsize::new(3).unwrap()),
        }
    }

    pub fn chunk_processed(&self, shard_id: ShardId, gas_used: Gas, balance_burnt: Balance) {
        metrics::TGAS_USAGE_HIST
            .with_label_values(&[&shard_id.to_string()])
            .observe(gas_used as f64 / TERAGAS);
        metrics::BALANCE_BURNT.inc_by(balance_burnt as f64);
    }

    pub fn chunk_skipped(&self, shard_id: ShardId) {
        metrics::CHUNK_SKIPPED_TOTAL.with_label_values(&[&shard_id.to_string()]).inc();
    }

    pub fn block_processed(
        &mut self,
        gas_used: Gas,
        num_chunks: u64,
        gas_price: Balance,
        total_supply: Balance,
        last_final_block_height: BlockHeight,
        last_final_ds_block_height: BlockHeight,
        epoch_height: EpochHeight,
        last_final_block_height_in_epoch: Option<BlockHeight>,
    ) {
        self.num_blocks_processed += 1;
        self.num_chunks_in_blocks_processed += num_chunks;
        self.gas_used += gas_used;
        metrics::GAS_USED.inc_by(gas_used as f64);
        metrics::BLOCKS_PROCESSED.inc();
        metrics::CHUNKS_PROCESSED.inc_by(num_chunks);
        metrics::GAS_PRICE.set(gas_price as f64);
        metrics::TOTAL_SUPPLY.set(total_supply as f64);
        metrics::FINAL_BLOCK_HEIGHT.set(last_final_block_height as i64);
        metrics::FINAL_DOOMSLUG_BLOCK_HEIGHT.set(last_final_ds_block_height as i64);
        metrics::EPOCH_HEIGHT.set(epoch_height as i64);
        if let Some(last_final_block_height_in_epoch) = last_final_block_height_in_epoch {
            // In rare cases the final height isn't updated, for example right after a state sync.
            // Don't update the metric in such cases.
            metrics::FINAL_BLOCK_HEIGHT_IN_EPOCH.set(last_final_block_height_in_epoch as i64);
        }
    }

    /// Update metrics to record the shards tracked by the validator.
    fn record_tracked_shards(
        head: &Tip,
        client: &crate::client::Client,
        shard_layout: &ShardLayout,
    ) {
        for shard_id in shard_layout.shard_ids() {
            let tracked = client.shard_tracker.cares_about_shard(&head.prev_block_hash, shard_id);
            metrics::TRACKED_SHARDS.with_label_values(&[&shard_id.to_string()]).set(if tracked {
                1
            } else {
                0
            });
        }
    }

    fn record_block_producers(head: &Tip, client: &crate::client::Client) {
        let me = client.validator_signer.get().map(|x| x.validator_id().clone());
        if let Some(is_bp) = me.map_or(Some(false), |account_id| {
            // In rare cases block producer information isn't available.
            // Don't set the metric in this case.
            client
                .epoch_manager
                .get_epoch_block_producers_ordered(&head.epoch_id)
                .map_or(None, |bp| Some(bp.iter().any(|bp| bp.account_id() == &account_id)))
        }) {
            metrics::IS_BLOCK_PRODUCER.set(if is_bp { 1 } else { 0 });
        }
    }

    fn record_chunk_producers(
        head: &Tip,
        client: &crate::client::Client,
        shard_layout: &ShardLayout,
    ) {
        if let (Some(account_id), Ok(epoch_info)) = (
            client.validator_signer.get().map(|x| x.validator_id().clone()),
            client.epoch_manager.get_epoch_info(&head.epoch_id),
        ) {
            for (shard_index, validators) in
                epoch_info.chunk_producers_settlement().iter().enumerate()
            {
                let Ok(shard_id) = shard_layout.get_shard_id(shard_index) else {
                    continue;
                };
                let is_chunk_producer_for_shard = validators.iter().any(|&validator_id| {
                    *epoch_info.validator_account_id(validator_id) == account_id
                });
                metrics::IS_CHUNK_PRODUCER_FOR_SHARD
                    .with_label_values(&[&shard_id.to_string()])
                    .set(if is_chunk_producer_for_shard { 1 } else { 0 });
            }
        } else {
            for shard_id in shard_layout.shard_ids() {
                metrics::IS_CHUNK_PRODUCER_FOR_SHARD
                    .with_label_values(&[&shard_id.to_string()])
                    .set(0);
            }
        }
    }

    /// The value obtained by multiplying the stake fraction with the expected number of blocks in an epoch
    /// is an estimation, and not an exact value. To obtain a more precise result, it is necessary to examine
    /// all the blocks in the epoch. However, even this method may not be completely accurate because additional
    /// blocks could potentially be added at the end of the epoch.
    fn record_epoch_settlement_info(head: &Tip, client: &crate::client::Client) {
        let epoch_info = client.epoch_manager.get_epoch_info(&head.epoch_id);
        let blocks_in_epoch = client.config.epoch_length;
        let shard_ids = client.epoch_manager.shard_ids(&head.epoch_id).unwrap_or_default();
        let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
        if let Ok(epoch_info) = epoch_info {
            metrics::VALIDATORS_CHUNKS_EXPECTED_IN_EPOCH.reset();
            metrics::VALIDATORS_BLOCKS_EXPECTED_IN_EPOCH.reset();
            metrics::BLOCK_PRODUCER_STAKE.reset();

            let epoch_height = epoch_info.epoch_height().to_string();

            let mut stake_per_bp = HashMap::<ValidatorId, Balance>::new();

            let stake_to_blocks = |stake: Balance, stake_sum: Balance| -> i64 {
                if stake == 0 {
                    0
                } else {
                    (((stake as f64) / (stake_sum as f64)) * (blocks_in_epoch as f64)) as i64
                }
            };

            let mut stake_sum = 0;
            for &id in epoch_info.block_producers_settlement() {
                let stake = epoch_info.validator_stake(id);
                stake_per_bp.insert(id, stake);
                stake_sum += stake;
            }

            stake_per_bp.iter().for_each(|(&id, &stake)| {
                metrics::BLOCK_PRODUCER_STAKE
                    .with_label_values(&[
                        epoch_info.get_validator(id).account_id().as_str(),
                        &epoch_height,
                    ])
                    .set((stake / 1e24 as u128) as i64);
                metrics::VALIDATORS_BLOCKS_EXPECTED_IN_EPOCH
                    .with_label_values(&[
                        epoch_info.get_validator(id).account_id().as_str(),
                        &epoch_height,
                    ])
                    .set(stake_to_blocks(stake, stake_sum))
            });

            for shard_id in shard_ids {
                let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
                let mut stake_per_cp = HashMap::<ValidatorId, Balance>::new();
                stake_sum = 0;
                let chunk_producers_settlement = &epoch_info.chunk_producers_settlement();
                let chunk_producers = chunk_producers_settlement.get(shard_index);
                let Some(chunk_producers) = chunk_producers else {
                    tracing::warn!(target: "stats", %shard_id, ?chunk_producers_settlement, "invalid shard id, not found in the shard settlement");
                    continue;
                };
                for &id in chunk_producers {
                    let stake = epoch_info.validator_stake(id);
                    stake_per_cp.insert(id, stake);
                    stake_sum += stake;
                }

                stake_per_cp.iter().for_each(|(&id, &stake)| {
                    metrics::VALIDATORS_CHUNKS_EXPECTED_IN_EPOCH
                        .with_label_values(&[
                            epoch_info.get_validator(id).account_id().as_str(),
                            &shard_id.to_string(),
                            &epoch_height,
                        ])
                        .set(stake_to_blocks(stake, stake_sum))
                });
            }
        }
    }

    /// Records protocol version of the current epoch.
    fn record_protocol_version(head: &Tip, client: &crate::client::Client) {
        if let Ok(version) = client.epoch_manager.get_epoch_protocol_version(&head.epoch_id) {
            metrics::CURRENT_PROTOCOL_VERSION.set(version as i64);
        }
    }

    /// Returns the number of validators in a given epoch (EpochId).
    ///
    /// The set of validators include both block producers and chunk producers.
    /// This set of validators do not change during the epoch, so it is cached for a small number of epochs.
    /// It does NOT currently consider whether the validators are slashed or not.
    fn get_num_validators(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
        epoch_id: &EpochId,
    ) -> usize {
        *self.num_validators_per_epoch.get_or_insert(*epoch_id, || {
            epoch_manager.get_epoch_all_validators(epoch_id).unwrap_or_default().len()
        })
    }

    /// Print current summary.
    pub fn log_summary(
        &mut self,
        client: &crate::client::Client,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        config_updater: &Option<ConfigUpdater>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        let is_syncing = client.sync_handler.sync_status.is_syncing();
        let head = unwrap_or_return!(client.chain.head());
        let validator_info = if !is_syncing {
            let num_validators =
                self.get_num_validators(client.epoch_manager.as_ref(), &head.epoch_id);
            let account_id = signer.as_ref().map(|x| x.validator_id());
            let is_validator = if let Some(account_id) = account_id {
                client.epoch_manager.get_validator_by_account_id(&head.epoch_id, account_id).is_ok()
            } else {
                false
            };
            Some(ValidatorInfoHelper { is_validator, num_validators })
        } else {
            None
        };

        let header_head = unwrap_or_return!(client.chain.header_head());
        let validator_production_status = if is_syncing {
            // EpochManager::get_validator_info method (which is what runtime
            // adapter calls) is expensive when node is syncing so we’re simply
            // not collecting the statistics.  The statistics are used to update
            // a few Prometheus metrics only so we prefer to leave the metrics
            // unset until node finishes synchronizing.  TODO(#6763): If we
            // manage to get get_validator_info fasts again (or return an error
            // if computation would be too slow), remove the ‘if is_syncing’
            // check.
            Default::default()
        } else {
            let epoch_identifier = ValidatorInfoIdentifier::BlockHash(header_head.last_block_hash);
            client
                .epoch_manager
                .get_validator_info(epoch_identifier)
                .map(get_validator_production_status)
                .unwrap_or_default()
        };

        let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).ok();

        if let Some(shard_layout) = shard_layout.as_ref() {
            InfoHelper::record_tracked_shards(&head, &client, shard_layout);
            InfoHelper::record_chunk_producers(&head, &client, shard_layout);
        }
        InfoHelper::record_block_producers(&head, &client);

        let next_epoch_id = Some(head.epoch_id);
        if self.epoch_id.ne(&next_epoch_id) {
            // We only want to compute this once per epoch to avoid heavy computational work, that can last up to 100ms.
            InfoHelper::record_epoch_settlement_info(&head, &client);
            // This isn't heavy computationally.
            InfoHelper::record_protocol_version(&head, &client);

            self.epoch_id = next_epoch_id;
        }

        self.info(
            &head,
            &client.sync_handler.sync_status,
            client.get_catchup_status().unwrap_or_default(),
            node_id,
            network_info,
            validator_info,
            validator_production_status,
            shard_layout.as_ref(),
            client
                .epoch_manager
                .get_estimated_protocol_upgrade_block_height(head.last_block_hash)
                .unwrap_or(None)
                .unwrap_or(0),
            &client.config,
            config_updater,
            signer,
        );
        self.log_chain_processing_info(client, &head.epoch_id);
    }

    fn info(
        &mut self,
        head: &Tip,
        sync_status: &SyncStatus,
        catchup_status: Vec<CatchupStatusView>,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        validator_info: Option<ValidatorInfoHelper>,
        validator_production_status: Vec<ValidatorProductionStatus>,
        shard_layout: Option<&ShardLayout>,
        protocol_upgrade_block_height: BlockHeight,
        client_config: &ClientConfig,
        config_updater: &Option<ConfigUpdater>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        let use_color = matches!(self.log_summary_style, LogSummaryStyle::Colored);
        let paint = |color: yansi::Color, text: Option<String>| match text {
            None => yansi::Paint::default(String::new()),
            Some(text) if use_color => yansi::Paint::default(text).fg(color).bold(),
            Some(text) => yansi::Paint::default(text),
        };

        let s = |num| if num == 1 { "" } else { "s" };

        let sync_status_log =
            Some(display_sync_status(sync_status, head, &client_config.state_sync.sync));
        let validator_info_log = validator_info.as_ref().map(|info| {
            format!(
                " {}{} validator{}",
                if info.is_validator { "Validator | " } else { "" },
                info.num_validators,
                s(info.num_validators)
            )
        });

        let network_info_log = Some(format!(
            " {} peer{} ⬇ {} ⬆ {}",
            network_info.num_connected_peers,
            s(network_info.num_connected_peers),
            PrettyNumber::bytes_per_sec(network_info.received_bytes_per_sec),
            PrettyNumber::bytes_per_sec(network_info.sent_bytes_per_sec)
        ));

        let now = Instant::now();
        let avg_bls = (self.num_blocks_processed as f64)
            / (now.signed_duration_since(self.started).whole_milliseconds() as f64)
            * 1000.0;
        let avg_gas_used = ((self.gas_used as f64)
            / (now.signed_duration_since(self.started).whole_milliseconds() as f64)
            * 1000.0) as u64;
        let blocks_info_log =
            Some(format!(" {:.2} bps {}", avg_bls, PrettyNumber::gas_per_sec(avg_gas_used)));

        let proc_info = self.pid.filter(|pid| self.sys.refresh_process(*pid)).map(|pid| {
            let proc =
                self.sys.process(pid).expect("refresh_process succeeds, this should be not None");
            (proc.cpu_usage(), proc.memory())
        });
        let machine_info_log = proc_info.as_ref().map(|(cpu, mem)| {
            format!(" CPU: {:.0}%, Mem: {}", cpu, PrettyNumber::bytes(mem * 1024))
        });

        info!(
            target: "stats", "{}{}{}{}{}",
            paint(yansi::Color::Yellow, sync_status_log),
            paint(yansi::Color::White, validator_info_log),
            paint(yansi::Color::Cyan, network_info_log),
            paint(yansi::Color::Green, blocks_info_log),
            paint(yansi::Color::Blue, machine_info_log),
        );
        log_catchup_status(catchup_status);
        if let Some(config_updater) = &config_updater {
            config_updater.report_status();
        }
        let (cpu_usage, memory_usage) = proc_info.unwrap_or_default();
        let is_validator = validator_info.is_some_and(|v| v.is_validator);
        (metrics::IS_VALIDATOR.set(is_validator as i64));
        (metrics::RECEIVED_BYTES_PER_SECOND.set(network_info.received_bytes_per_sec as i64));
        (metrics::SENT_BYTES_PER_SECOND.set(network_info.sent_bytes_per_sec as i64));
        (metrics::CPU_USAGE.set(cpu_usage as i64));
        (metrics::MEMORY_USAGE.set((memory_usage * 1024) as i64));
        (metrics::PROTOCOL_UPGRADE_BLOCK_HEIGHT.set(protocol_upgrade_block_height as i64));

        Self::update_validator_metrics(validator_production_status, shard_layout);

        self.started = self.clock.now();
        self.num_blocks_processed = 0;
        self.num_chunks_in_blocks_processed = 0;
        self.gas_used = 0;

        let telemetry_event = TelemetryEvent {
            content: self.telemetry_info(
                head,
                sync_status,
                node_id,
                network_info,
                client_config,
                cpu_usage,
                memory_usage,
                is_validator,
                signer,
            ),
        };
        self.telemetry_sender.send(telemetry_event);
    }

    /// Updates the prometheus metrics to track the block and chunk production and endorsement by validators.
    fn update_validator_metrics(
        validator_status: Vec<ValidatorProductionStatus>,
        shard_layout: Option<&ShardLayout>,
    ) {
        // In case we can't get the list of validators for the current and the previous epoch,
        // skip updating the per-validator metrics.
        // Note that the metrics are removed for previous epoch validators who are no longer
        // validators (with the status ValidatorProductionStatus::Kickout).
        for status in validator_status {
            match status {
                ValidatorProductionStatus::Validator(stats) => {
                    metrics::VALIDATORS_BLOCKS_PRODUCED
                        .with_label_values(&[stats.account_id.as_str()])
                        .set(stats.num_produced_blocks as i64);
                    metrics::VALIDATORS_BLOCKS_EXPECTED
                        .with_label_values(&[stats.account_id.as_str()])
                        .set(stats.num_expected_blocks as i64);
                    metrics::VALIDATORS_CHUNKS_PRODUCED
                        .with_label_values(&[stats.account_id.as_str()])
                        .set(stats.num_produced_chunks as i64);
                    metrics::VALIDATORS_CHUNKS_EXPECTED
                        .with_label_values(&[stats.account_id.as_str()])
                        .set(stats.num_expected_chunks as i64);
                    for i in 0..stats.shards_produced.len() {
                        let shard = stats.shards_produced[i];
                        metrics::VALIDATORS_CHUNKS_EXPECTED_BY_SHARD
                            .with_label_values(&[stats.account_id.as_str(), &shard.to_string()])
                            .set(stats.num_expected_chunks_per_shard[i] as i64);
                        metrics::VALIDATORS_CHUNKS_PRODUCED_BY_SHARD
                            .with_label_values(&[stats.account_id.as_str(), &shard.to_string()])
                            .set(stats.num_produced_chunks_per_shard[i] as i64);
                    }
                    for i in 0..stats.shards_endorsed.len() {
                        let shard = stats.shards_endorsed[i];
                        metrics::VALIDATORS_CHUNK_ENDORSEMENTS_EXPECTED_BY_SHARD
                            .with_label_values(&[stats.account_id.as_str(), &shard.to_string()])
                            .set(stats.num_expected_endorsements_per_shard[i] as i64);
                        metrics::VALIDATORS_CHUNK_ENDORSEMENTS_PRODUCED_BY_SHARD
                            .with_label_values(&[stats.account_id.as_str(), &shard.to_string()])
                            .set(stats.num_produced_endorsements_per_shard[i] as i64);
                    }
                }
                // If the validator is kicked out, remove the stats for it and for all shards in the current epoch.
                ValidatorProductionStatus::Kickout(account_id) => {
                    let _ = metrics::VALIDATORS_BLOCKS_PRODUCED
                        .remove_label_values(&[account_id.as_str()]);
                    let _ = metrics::VALIDATORS_BLOCKS_EXPECTED
                        .remove_label_values(&[account_id.as_str()]);
                    let _ = metrics::VALIDATORS_CHUNKS_PRODUCED
                        .remove_label_values(&[account_id.as_str()]);
                    let _ = metrics::VALIDATORS_CHUNKS_EXPECTED
                        .remove_label_values(&[account_id.as_str()]);
                    if let Some(shard_layout) = shard_layout {
                        for shard in shard_layout.shard_ids() {
                            let _ = metrics::VALIDATORS_CHUNKS_EXPECTED_BY_SHARD
                                .remove_label_values(&[account_id.as_str(), &shard.to_string()]);
                            let _ = metrics::VALIDATORS_CHUNKS_PRODUCED_BY_SHARD
                                .remove_label_values(&[account_id.as_str(), &shard.to_string()]);
                            let _ = metrics::VALIDATORS_CHUNK_ENDORSEMENTS_EXPECTED_BY_SHARD
                                .remove_label_values(&[account_id.as_str(), &shard.to_string()]);
                            let _ = metrics::VALIDATORS_CHUNK_ENDORSEMENTS_PRODUCED_BY_SHARD
                                .remove_label_values(&[account_id.as_str(), &shard.to_string()]);
                        }
                    }
                }
            }
        }
    }

    fn telemetry_info(
        &self,
        head: &Tip,
        sync_status: &SyncStatus,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        client_config: &ClientConfig,
        cpu_usage: f32,
        memory_usage: u64,
        is_validator: bool,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> serde_json::Value {
        let info = TelemetryInfo {
            agent: TelemetryAgentInfo {
                name: "near-rs".to_string(),
                version: self.nearcore_version.version.clone(),
                build: self.nearcore_version.build.clone(),
                protocol_version: PROTOCOL_VERSION,
            },
            system: TelemetrySystemInfo {
                bandwidth_download: network_info.received_bytes_per_sec,
                bandwidth_upload: network_info.sent_bytes_per_sec,
                cpu_usage,
                memory_usage,
                boot_time_seconds: self.boot_time_seconds,
            },
            chain: TelemetryChainInfo {
                chain_id: client_config.chain_id.clone(),
                node_id: node_id.to_string(),
                account_id: signer.as_ref().map(|bp| bp.validator_id().clone()),
                is_validator,
                status: sync_status.as_variant_name().to_string(),
                latest_block_hash: head.last_block_hash,
                latest_block_height: head.height,
                num_peers: network_info.num_connected_peers,
                block_production_tracking_delay: client_config
                    .block_production_tracking_delay
                    .as_seconds_f64(),
                min_block_production_delay: client_config
                    .min_block_production_delay
                    .as_seconds_f64(),
                max_block_production_delay: client_config
                    .max_block_production_delay
                    .as_seconds_f64(),
                max_block_wait_delay: client_config.max_block_wait_delay.as_seconds_f64(),
            },
            extra_info: serde_json::to_string(&extra_telemetry_info(client_config)).unwrap(),
        };

        let mut json = serde_json::to_value(info).expect("Telemetry must serialize to JSON");
        // Sign telemetry if there is a signer present.
        if let Some(signer) = signer {
            let content = serde_json::to_string(&json).expect("Telemetry must serialize to JSON");
            json["signature"] = signer.sign_bytes(content.as_bytes()).to_string().into();
        }
        json
    }

    fn log_chain_processing_info(&self, client: &crate::Client, epoch_id: &EpochId) {
        let chain = &client.chain;
        let use_color = matches!(self.log_summary_style, LogSummaryStyle::Colored);
        let info = chain.get_chain_processing_info();
        let blocks_info = BlocksInfo { blocks_info: info.blocks_info, use_color };
        tracing::debug!(
            target: "stats",
            "{:?} Orphans: {} With missing chunks: {} In processing {}{}",
            epoch_id,
            info.num_orphans,
            info.num_blocks_missing_chunks,
            info.num_blocks_in_processing,
            if self.enable_multiline_logging { blocks_info.to_string() } else { "".to_owned() },
        );
    }

    // If the `new_sync_requirement` differs from `self.prev_sync_requirement`,
    // then increments a corresponding metric.
    // Uses `String` instead of `SyncRequirement` to avoid circular dependencies.
    pub(crate) fn update_sync_requirements_metrics(&mut self, new_sync_requirement: String) {
        // Compare the new SyncRequirement with the previously seen SyncRequirement.
        let change = match &self.prev_sync_requirement {
            None => Some(new_sync_requirement),
            Some(prev_sync) => {
                let prev_sync_requirement = format!("{prev_sync}");
                if prev_sync_requirement == new_sync_requirement {
                    None
                } else {
                    Some(new_sync_requirement)
                }
            }
        };
        if let Some(new_sync_requirement) = change {
            // Something change, update the metrics and record it.
            metrics::SYNC_REQUIREMENT.with_label_values(&[&new_sync_requirement]).inc();
            metrics::SYNC_REQUIREMENT_CURRENT.with_label_values(&[&new_sync_requirement]).set(1);
            if let Some(prev_sync_requirement) = &self.prev_sync_requirement {
                metrics::SYNC_REQUIREMENT_CURRENT
                    .with_label_values(&[&prev_sync_requirement])
                    .set(0);
            }
            metrics::SYNC_REQUIREMENT_CURRENT.with_label_values(&[&new_sync_requirement]).set(1);
            self.prev_sync_requirement = Some(new_sync_requirement);
        }
    }
}

fn extra_telemetry_info(client_config: &ClientConfig) -> serde_json::Value {
    serde_json::json!({
        "block_production_tracking_delay":  client_config.block_production_tracking_delay.as_seconds_f64(),
        "min_block_production_delay":  client_config.min_block_production_delay.as_seconds_f64(),
        "max_block_production_delay": client_config.max_block_production_delay.as_seconds_f64(),
        "max_block_wait_delay": client_config.max_block_wait_delay.as_seconds_f64(),
    })
}

pub fn log_catchup_status(catchup_status: Vec<CatchupStatusView>) {
    for catchup_status in &catchup_status {
        let shard_sync_string = catchup_status
            .shard_sync_status
            .iter()
            .sorted_by_key(|x| x.0)
            .map(|(shard_id, status_string)| format!("Shard {} {}", shard_id, status_string))
            .join(", ");
        let block_catchup_string = catchup_status
            .blocks_to_catchup
            .iter()
            .map(|block_view| format!("{:?}@{:?}", block_view.hash, block_view.height))
            .join(", ");
        let block_catchup_string =
            if block_catchup_string.is_empty() { "done".to_string() } else { block_catchup_string };

        tracing::info!(
            sync_hash=?catchup_status.sync_block_hash,
            sync_height=?catchup_status.sync_block_height,
            "Catchup Status - shard sync status: {}, next blocks to catch up: {}",
            shard_sync_string,
            block_catchup_string,
        )
    }
}

pub fn display_sync_status(
    sync_status: &SyncStatus,
    head: &Tip,
    state_sync_config: &SyncConfig,
) -> String {
    metrics::SYNC_STATUS.set(sync_status.repr() as i64);
    match sync_status {
        SyncStatus::AwaitingPeers => format!("#{:>8} Waiting for peers", head.height),
        SyncStatus::NoSync => format!("#{:>8} {:>44}", head.height, head.last_block_hash),
        SyncStatus::EpochSync(status) => {
            format!("[EPOCH] {:?}", status)
        }
        SyncStatus::EpochSyncDone => "[EPOCH] Done".to_string(),
        SyncStatus::HeaderSync { start_height, current_height, highest_height } => {
            let percent = if highest_height <= start_height {
                0.0
            } else {
                ((min(current_height, highest_height).saturating_sub(*start_height) * 100) as f64)
                    / (highest_height.saturating_sub(*start_height) as f64)
            };
            format!(
                "#{:>8} Downloading headers {:.2}% ({} left; at {})",
                head.height,
                percent,
                highest_height.saturating_sub(*current_height),
                current_height
            )
        }
        SyncStatus::BlockSync { start_height, current_height, highest_height } => {
            let percent = if highest_height <= start_height {
                0.0
            } else {
                ((current_height - start_height) * 100) as f64
                    / ((highest_height - start_height) as f64)
            };
            format!(
                "#{:>8} Downloading blocks {:.2}% ({} left; at {})",
                head.height,
                percent,
                highest_height.saturating_sub(*current_height),
                current_height
            )
        }
        SyncStatus::StateSync(StateSyncStatus {
            sync_hash,
            sync_status: shard_statuses,
            download_tasks,
            computation_tasks,
        }) => {
            let mut res = format!("State {:?}", sync_hash);
            let mut shard_statuses: Vec<_> = shard_statuses.iter().collect();
            shard_statuses.sort_by_key(|(shard_id, _)| *shard_id);
            for (shard_id, shard_status) in shard_statuses {
                write!(res, "[{}: {}]", shard_id, shard_status.to_string(),).unwrap();
            }
            write!(
                res,
                " ({} downloads, {} computations)",
                download_tasks.len(),
                computation_tasks.len()
            )
            .unwrap();
            if let SyncConfig::Peers = state_sync_config {
                tracing::warn!(
                    target: "stats",
                    "The node is trying to sync its State from its peers. The current implementation of this mechanism is known to be unreliable. It may never complete, or fail randomly and corrupt the DB.\n\
                     Suggestions:\n\
                      * Try to state sync from GCS. See `\"state_sync\"` and `\"state_sync_enabled\"` options in the reference `config.json` file.
                      or
                      * Disable state sync in the config. Add `\"state_sync_enabled\": false` to `config.json`, then download a recent data snapshot and restart the node.");
            };
            res
        }
        SyncStatus::StateSyncDone => "State sync done".to_string(),
    }
}

/// Displays ` {} for {}ms` if second item is `Some`.
struct FormatMillis(&'static str, Option<u128>);

impl std::fmt::Display for FormatMillis {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.1.map_or(Ok(()), |ms| write!(fmt, " {} for {ms}ms", self.0))
    }
}

/// Formats information about each block.  Each information line is *preceded*
/// by a new line character.  There’s no final new line character.  This is
/// meant to be used in logging where final new line is not desired.
struct BlocksInfo {
    blocks_info: Vec<near_primitives::views::BlockProcessingInfo>,
    use_color: bool,
}

impl std::fmt::Display for BlocksInfo {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let paint = |color: yansi::Color, text: String| {
            if self.use_color {
                yansi::Paint::default(text).fg(color).bold()
            } else {
                yansi::Paint::default(text)
            }
        };

        for block_info in &self.blocks_info {
            let mut all_chunks_received = true;
            let chunk_status = block_info
                .chunks_info
                .iter()
                .map(|chunk_info| {
                    if let Some(chunk_info) = chunk_info {
                        all_chunks_received &=
                            matches!(chunk_info.status, ChunkProcessingStatus::Completed);
                        match chunk_info.status {
                            ChunkProcessingStatus::Completed => '✔',
                            ChunkProcessingStatus::Requested => '⬇',
                            ChunkProcessingStatus::NeedToRequest => '.',
                        }
                    } else {
                        'X'
                    }
                })
                .collect::<String>();

            let chunk_status_color =
                if all_chunks_received { yansi::Color::Green } else { yansi::Color::White };

            let chunk_status = paint(chunk_status_color, chunk_status);
            let in_progress = FormatMillis("in progress", Some(block_info.in_progress_ms));
            let in_orphan = FormatMillis("orphan", block_info.orphaned_ms);
            let missing_chunks = FormatMillis("missing chunks", block_info.missing_chunks_ms);

            write!(
                fmt,
                "\n  {} {} {:?}{in_progress}{in_orphan}{missing_chunks} Chunks:({chunk_status}))",
                block_info.height, block_info.hash, block_info.block_status,
            )?;
        }

        Ok(())
    }
}

/// Format number using SI prefixes.
struct PrettyNumber(u64, &'static str);

impl PrettyNumber {
    fn bytes_per_sec(bps: u64) -> Self {
        Self(bps, "B/s")
    }

    fn bytes(bytes: u64) -> Self {
        Self(bytes, "B")
    }

    fn gas_per_sec(gps: u64) -> Self {
        Self(gps, "gas/s")
    }
}

impl std::fmt::Display for PrettyNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(mut num, unit) = *self;
        if num < 1_000 {
            return write!(f, "{} {}", num, unit);
        }
        // cspell:ignore MGTPE
        for prefix in b"kMGTPE" {
            if num < 1_000_000 {
                let precision = if num < 10_000 {
                    2
                } else if num < 100_000 {
                    1
                } else {
                    0
                };
                return write!(
                    f,
                    "{:.*} {}{}",
                    precision,
                    num as f64 / 1_000.0,
                    *prefix as char,
                    unit
                );
            }
            num /= 1000;
        }
        unreachable!()
    }
}

/// Production status of a validator for the current epoch.
/// Either it is an active validator with the number of blocks and chunks expected/produced by the validator,
/// or it is kicked out in the previous epoch (so not expected to produce or validate a block/chunk).
enum ValidatorProductionStatus {
    /// This was an active validator in this epoch with some block/chunk production stats.
    Validator(ValidatorProductionStats),
    /// This validator was not active in this epoch, since it was kicked out in the previous epoch.
    Kickout(AccountId),
}

/// Contains block/chunk production and validation statistics for a validator that was active in the current epoch.
struct ValidatorProductionStats {
    account_id: AccountId,
    num_produced_blocks: NumBlocks,
    num_expected_blocks: NumBlocks,
    num_produced_chunks: NumBlocks,
    num_expected_chunks: NumBlocks,
    /// Shards this validator is assigned to as chunk producer in the current epoch.
    shards_produced: Vec<ShardId>,
    /// Shards this validator is assigned to as chunk validator in the current epoch.
    shards_endorsed: Vec<ShardId>,
    num_produced_chunks_per_shard: Vec<NumBlocks>,
    num_expected_chunks_per_shard: Vec<NumBlocks>,
    num_produced_endorsements_per_shard: Vec<NumBlocks>,
    num_expected_endorsements_per_shard: Vec<NumBlocks>,
}

impl ValidatorProductionStatus {
    pub fn kickout(kickout: ValidatorKickoutView) -> Self {
        Self::Kickout(kickout.account_id)
    }
    pub fn validator(info: CurrentEpochValidatorInfo) -> Self {
        debug_assert_eq!(
            info.shards_produced.len(),
            info.num_expected_chunks_per_shard.len(),
            "Number of shards must match number of shards expected to produce a chunk for"
        );
        debug_assert_eq!(
            info.shards_endorsed.len(),
            info.num_expected_endorsements_per_shard.len(),
            "Number of shards must match number of shards expected to produce a chunk for"
        );
        Self::Validator(ValidatorProductionStats {
            account_id: info.account_id,
            num_produced_blocks: info.num_produced_blocks,
            num_expected_blocks: info.num_expected_blocks,
            num_produced_chunks: info.num_produced_chunks,
            num_expected_chunks: info.num_expected_chunks,
            shards_produced: info.shards_produced,
            shards_endorsed: info.shards_endorsed,
            num_produced_chunks_per_shard: info.num_produced_chunks_per_shard,
            num_expected_chunks_per_shard: info.num_expected_chunks_per_shard,
            num_produced_endorsements_per_shard: info.num_produced_endorsements_per_shard,
            num_expected_endorsements_per_shard: info.num_expected_endorsements_per_shard,
        })
    }
}

/// Converts EpochValidatorInfo into a vector of ValidatorProductionStatus.
fn get_validator_production_status(
    current_validator_epoch_info: EpochValidatorInfo,
) -> Vec<ValidatorProductionStatus> {
    let mut status = vec![];
    // Record kickouts to replace latest stats of kicked out validators with zeros.
    for kickout in current_validator_epoch_info.prev_epoch_kickout {
        status.push(ValidatorProductionStatus::kickout(kickout));
    }
    for validator in current_validator_epoch_info.current_validators {
        status.push(ValidatorProductionStatus::validator(validator));
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use near_async::messaging::{IntoMultiSender, IntoSender, noop};
    use near_async::time::Clock;
    use near_chain::runtime::NightshadeRuntime;
    use near_chain::spice_core::CoreStatementsProcessor;
    use near_chain::types::ChainConfig;
    use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
    use near_chain_configs::{Genesis, MutableConfigValue};
    use near_epoch_manager::EpochManager;
    use near_epoch_manager::shard_tracker::ShardTracker;
    use near_epoch_manager::test_utils::*;
    use near_network::test_utils::peer_id_from_seed;
    use near_store::adapter::StoreAdapter as _;
    use near_store::genesis::initialize_genesis_state;

    #[test]
    fn test_pretty_number() {
        for (want, num) in [
            ("0 U", 0),
            ("1 U", 1),
            ("10 U", 10),
            ("100 U", 100),
            ("1.00 kU", 1_000),
            ("10.0 kU", 10_000),
            ("100 kU", 100_000),
            ("1.00 MU", 1_000_000),
            ("10.0 MU", 10_000_000),
            ("100 MU", 100_000_000),
            ("18.4 EU", u64::MAX),
        ] {
            let got = PrettyNumber(num, "U").to_string();
            assert_eq!(want, &got, "num={}", num);
        }
    }

    #[test]
    fn test_telemetry_info() {
        let config = ClientConfig::test(false, 1230, 2340, 50, false, true, true);
        let validator = MutableConfigValue::new(None, "validator_signer");
        let info_helper = InfoHelper::new(Clock::real(), noop().into_sender(), &config);

        let store = near_store::test_utils::create_test_store();
        let mut genesis = Genesis::test(vec!["test".parse::<AccountId>().unwrap()], 1);
        genesis.config.epoch_length = 123;
        let tempdir = tempfile::tempdir().unwrap();
        initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
        let runtime = NightshadeRuntime::test(
            tempdir.path(),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let doomslug_threshold_mode = DoomslugThresholdMode::TwoThirds;
        let chain = Chain::new(
            Clock::real(),
            epoch_manager.clone(),
            shard_tracker,
            runtime,
            &chain_genesis,
            doomslug_threshold_mode,
            ChainConfig::test(),
            None,
            Default::default(),
            validator.clone(),
            noop().into_multi_sender(),
            CoreStatementsProcessor::new_with_noop_senders(store.chain_store(), epoch_manager),
        )
        .unwrap();

        let telemetry = info_helper.telemetry_info(
            &chain.head().unwrap(),
            &SyncStatus::AwaitingPeers,
            &peer_id_from_seed("zxc"),
            &NetworkInfo {
                connected_peers: vec![],
                num_connected_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                known_producers: vec![],
                tier1_connections: vec![],
                tier1_accounts_keys: vec![],
                tier1_accounts_data: vec![],
            },
            &config,
            0.0,
            0,
            false,
            &validator.get(),
        );
        println!("Got telemetry info: {:?}", telemetry);
        assert_matches!(
            telemetry["extra_info"].as_str().unwrap().find("\"max_block_production_delay\":2.34,"),
            Some(_)
        );
    }

    /// Tests that `num_validators` returns the number of all validators including both block and chunk producers.
    #[test]
    fn test_num_validators() {
        let amount_staked = 1_000_000;
        let validators = vec![
            ("test1".parse().unwrap(), amount_staked),
            ("test2".parse().unwrap(), amount_staked),
            ("test3".parse().unwrap(), amount_staked),
            ("test4".parse().unwrap(), amount_staked),
            ("test5".parse().unwrap(), amount_staked),
        ];
        let num_validators = validators.len();
        let num_block_producer_seats = 3usize;
        assert!(
            num_block_producer_seats < num_validators,
            "for this test, make sure number of validators are more than block producer seats"
        );

        let epoch_id = EpochId::default();
        let epoch_length = 2;
        let num_shards = 2;

        let epoch_manager_adapter = setup_epoch_manager(
            validators,
            epoch_length,
            num_shards,
            num_block_producer_seats.try_into().unwrap(),
            90,
            90,
            0,
            default_reward_calculator(),
        )
        .into_handle();

        // First check that we have different number of block and chunk producers.
        assert_eq!(
            num_block_producer_seats,
            epoch_manager_adapter.get_epoch_block_producers_ordered(&epoch_id).unwrap().len()
        );
        assert_eq!(
            num_validators,
            epoch_manager_adapter.get_epoch_chunk_producers(&epoch_id).unwrap().len()
        );

        // Then check that get_num_validators returns the correct number of validators.
        let client_config = ClientConfig::test(false, 1230, 2340, 50, false, true, true);
        let mut info_helper = InfoHelper::new(Clock::real(), noop().into_sender(), &client_config);
        assert_eq!(
            num_validators,
            info_helper.get_num_validators(&epoch_manager_adapter, &epoch_id)
        );
    }
}
