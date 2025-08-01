use near_chain_configs::{ExternalStorageLocation, SyncConfig};
use near_config_utils::{ValidationError, ValidationErrors};
use std::collections::HashSet;
use std::path::Path;

use crate::config::Config;

/// Validate Config extracted from config.json.
/// This function does not panic. It returns the error if any validation fails.
pub fn validate_config(config: &Config) -> Result<(), ValidationError> {
    let mut validation_errors = ValidationErrors::new();
    let mut config_validator = ConfigValidator::new(config, &mut validation_errors);
    tracing::info!(target: "config", "Validating Config, extracted from config.json...");
    config_validator.validate()
}

struct ConfigValidator<'a> {
    config: &'a Config,
    validation_errors: &'a mut ValidationErrors,
}

impl<'a> ConfigValidator<'a> {
    fn new(config: &'a Config, validation_errors: &'a mut ValidationErrors) -> Self {
        Self { config, validation_errors }
    }

    fn validate(&mut self) -> Result<(), ValidationError> {
        self.validate_all_conditions();
        self.result_with_full_error()
    }

    /// this function would check all conditions, and add all error messages to ConfigValidator.errors
    fn validate_all_conditions(&mut self) {
        self.validate_cold_store_config();
        self.validate_cloud_archival_config();

        if self.config.consensus.min_block_production_delay
            > self.config.consensus.max_block_production_delay
        {
            let error_message = format!(
                "min_block_production_delay: {:?} is greater than max_block_production_delay: {:?}",
                self.config.consensus.min_block_production_delay,
                self.config.consensus.max_block_production_delay
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if self.config.consensus.min_block_production_delay
            > self.config.consensus.max_block_wait_delay
        {
            let error_message = format!(
                "min_block_production_delay: {:?} is greater than max_block_wait_delay: {:?}",
                self.config.consensus.min_block_production_delay,
                self.config.consensus.max_block_wait_delay
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if self.config.consensus.header_sync_expected_height_per_second == 0 {
            let error_message =
                "consensus.header_sync_expected_height_per_second should not be 0".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if self.config.gc.gc_blocks_limit == 0
            || self.config.gc.gc_fork_clean_step == 0
            || self.config.gc.gc_num_epochs_to_keep == 0
        {
            let error_message = format!(
                "gc config values should all be greater than 0, but gc_blocks_limit is {:?}, gc_fork_clean_step is {}, gc_num_epochs_to_keep is {}.",
                self.config.gc.gc_blocks_limit,
                self.config.gc.gc_fork_clean_step,
                self.config.gc.gc_num_epochs_to_keep
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if let Some(state_sync) = &self.config.state_sync {
            if let Some(dump_config) = &state_sync.dump {
                if let Some(restart_dump_for_shards) = &dump_config.restart_dump_for_shards {
                    let unique_values: HashSet<_> = restart_dump_for_shards.iter().collect();
                    if unique_values.len() != restart_dump_for_shards.len() {
                        let error_message = format!(
                            "'config.state_sync.dump.restart_dump_for_shards' contains duplicate values."
                        );
                        self.validation_errors.push_config_semantics_error(error_message);
                    }
                }

                match &dump_config.location {
                    ExternalStorageLocation::S3 { bucket, region } => {
                        if bucket.is_empty() || region.is_empty() {
                            let error_message = format!(
                                "'config.state_sync.dump.location.S3.bucket' and 'config.state_sync.dump.location.S3.region' need to be specified when 'config.state_sync.dump.location.S3' is present."
                            );
                            self.validation_errors.push_config_semantics_error(error_message);
                        }
                    }
                    ExternalStorageLocation::Filesystem { root_dir } => {
                        if root_dir.as_path() == Path::new("") {
                            let error_message = format!(
                                "'config.state_sync.dump.location.Filesystem.root_dir' needs to be specified when 'config.state_sync.dump.location.Filesystem' is present."
                            );
                            self.validation_errors.push_config_semantics_error(error_message);
                        }
                    }
                    ExternalStorageLocation::GCS { bucket } => {
                        if bucket.is_empty() {
                            let error_message = format!(
                                "'config.state_sync.dump.location.GCS.bucket' needs to be specified when 'config.state_sync.dump.location.GCS' is present."
                            );
                            self.validation_errors.push_config_semantics_error(error_message);
                        }
                    }
                }

                if let Some(credentials_file) = &dump_config.credentials_file {
                    if !credentials_file.exists() || !credentials_file.is_file() {
                        let error_message = format!(
                            "'config.state_sync.dump.credentials_file' is provided but the specified file does not exist or is not a file."
                        );
                        self.validation_errors.push_config_semantics_error(error_message);
                    }
                }
            }
            match &state_sync.sync {
                SyncConfig::Peers => {}
                SyncConfig::ExternalStorage(config) => {
                    match &config.location {
                        ExternalStorageLocation::S3 { bucket, region } => {
                            if bucket.is_empty() || region.is_empty() {
                                let error_message = format!(
                                    "'config.state_sync.sync.ExternalStorage.location.S3.bucket' and 'config.state_sync.sync.ExternalStorage.location.S3.region' need to be specified when 'config.state_sync.sync.ExternalStorage.location.S3' is present."
                                );
                                self.validation_errors.push_config_semantics_error(error_message);
                            }
                        }
                        ExternalStorageLocation::Filesystem { root_dir } => {
                            if root_dir.as_path() == Path::new("") {
                                let error_message = format!(
                                    "'config.state_sync.sync.ExternalStorage.location.Filesystem.root_dir' needs to be specified when 'config.state_sync.sync.ExternalStorage.location.Filesystem' is present."
                                );
                                self.validation_errors.push_config_semantics_error(error_message);
                            }
                        }
                        ExternalStorageLocation::GCS { bucket } => {
                            if bucket.is_empty() {
                                let error_message = format!(
                                    "'config.state_sync.sync.ExternalStorage.location.GCS.bucket' needs to be specified when 'config.state_sync.sync.ExternalStorage.location.GCS' is present."
                                );
                                self.validation_errors.push_config_semantics_error(error_message);
                            }
                        }
                    }
                    if config.num_concurrent_requests == 0 {
                        let error_message = format!(
                            "'config.state_sync.sync.ExternalStorage.num_concurrent_requests' needs to be greater than 0"
                        );
                        self.validation_errors.push_config_semantics_error(error_message);
                    }
                }
            }
        }

        let tx_routing_height_horizon = self.config.tx_routing_height_horizon;
        if tx_routing_height_horizon < 2 {
            let error_message = format!(
                "'config.tx_routing_height_horizon' needs to be at least 2, got {tx_routing_height_horizon}."
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }
        if tx_routing_height_horizon > 100 {
            let error_message = format!(
                "'config.tx_routing_height_horizon' can't be too high to avoid spamming the network. Keep it below 100. Got {tx_routing_height_horizon}."
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }
        self.validate_tracked_shards_config();
    }

    fn validate_cold_store_config(&mut self) {
        // Checking that if cold storage is configured, trie changes are definitely saved.
        // Unlike in the previous case, None is not a valid option here.
        if self.config.cold_store.is_some() && self.config.save_trie_changes != Some(true) {
            let error_message = format!(
                "cold_store is configured, but save_trie_changes is {:?}. Trie changes should be saved to support cold storage.",
                self.config.save_trie_changes
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if !self.config.archive {
            if self.config.save_trie_changes == Some(false) {
                let error_message = "Configuration with archive = false and save_trie_changes = false is not supported because non-archival nodes must save trie changes in order to do garbage collection.".to_string();
                self.validation_errors.push_config_semantics_error(error_message);
            }
            if self.config.cold_store.is_some() {
                let error_message =
                    "`archive` is false, but `cold_store` is configured.".to_string();
                self.validation_errors.push_config_semantics_error(error_message);
            }
            if self.config.split_storage.is_some() {
                let error_message =
                    "`archive` is false, but `split_storage` is configured.".to_string();
                self.validation_errors.push_config_semantics_error(error_message);
            }
        }
    }

    fn validate_cloud_archival_config(&mut self) {
        if !self.config.archive && self.config.cloud_storage.is_some() {
            let error_message =
                "`archive` is false, but `cloud_storage` is configured.".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }
        // TODO(cloud_archival) Allow for cloud storage independently of cold store.
        if self.config.cloud_storage.is_some() && self.config.cold_store.is_none() {
            let error_message =
                "`cloud_storage` is configured but `cold_store` is not.".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }
    }

    fn validate_tracked_shards_config(&mut self) {
        if self.config.tracked_shards_config.is_none() {
            return;
        }
        if self.config.tracked_shards.is_some() {
            let error_message = "'config.tracked_shards' and 'config.tracked_shards_config' cannot be both set. Please use 'config.tracked_shards_config' only.".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }
        if self.config.tracked_accounts.is_some() {
            let error_message = "'config.tracked_accounts' and 'config.tracked_shards_config' cannot be both set. Please use 'config.tracked_shards_config' only.".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }
        if self.config.tracked_shadow_validator.is_some() {
            let error_message = "'config.tracked_shadow_validator' and 'config.tracked_shards_config' cannot be both set. Please use 'config.tracked_shards_config' only.".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }
        if self.config.tracked_shard_schedule.is_some() {
            let error_message = "'config.tracked_shard_schedule' and 'config.tracked_shards_config' cannot be both set. Please use 'config.tracked_shards_config' only.".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }
    }

    fn result_with_full_error(&self) -> Result<(), ValidationError> {
        if self.validation_errors.is_empty() {
            Ok(())
        } else {
            let full_err_msg = self.validation_errors.generate_error_message_per_type().unwrap();
            Err(ValidationError::ConfigSemanticsError { error_message: full_err_msg })
        }
    }
}

#[cfg(test)]
mod tests {
    use near_chain_configs::TrackedShardsConfig;

    use super::*;

    #[test]
    #[should_panic(expected = "and 'config.tracked_shards_config' cannot be both set")]
    fn test_cannot_use_both_new_and_old_tracked_shard_config() {
        let mut config = Config::default();
        config.tracked_shards_config = Some(TrackedShardsConfig::AllShards);
        config.tracked_shards = Some(vec![]);
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(expected = "gc config values should all be greater than 0")]
    fn test_gc_config_value_nonzero() {
        let mut config = Config::default();
        config.gc.gc_blocks_limit = 0;
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Configuration with archive = false and save_trie_changes = false is not supported"
    )]
    fn test_archive_false_save_trie_changes_false() {
        let mut config = Config::default();
        config.archive = false;
        config.save_trie_changes = Some(false);
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: Configuration with archive = false and save_trie_changes = false is not supported because non-archival nodes must save trie changes in order to do garbage collection.\\nconfig.json semantic issue: gc config values should all be greater than 0"
    )]
    fn test_multiple_config_validation_errors() {
        let mut config = Config::default();
        config.archive = false;
        config.save_trie_changes = Some(false);
        config.gc.gc_blocks_limit = 0;

        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: cold_store is configured, but save_trie_changes is None. Trie changes should be saved to support cold storage."
    )]
    fn test_cold_store_without_save_trie_changes() {
        let mut config = Config::default();
        config.cold_store = Some(config.store.clone());
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: cold_store is configured, but save_trie_changes is Some(false). Trie changes should be saved to support cold storage."
    )]
    fn test_cold_store_with_save_trie_changes_false() {
        let mut config = Config::default();
        config.cold_store = Some(config.store.clone());
        config.save_trie_changes = Some(false);
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: 'config.tx_routing_height_horizon' needs to be at least 2, got 1."
    )]
    fn test_tx_routing_height_horizon_too_low() {
        let mut config = Config::default();
        config.tx_routing_height_horizon = 1;
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: 'config.tx_routing_height_horizon' can't be too high to avoid spamming the network. Keep it below 100. Got 1000000000."
    )]
    fn test_tx_routing_height_horizon_too_high() {
        let mut config = Config::default();
        config.tx_routing_height_horizon = 1_000_000_000;
        validate_config(&config).unwrap();
    }
}
