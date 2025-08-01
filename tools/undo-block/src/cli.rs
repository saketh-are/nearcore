use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::EpochManager;
use near_store::{Mode, NodeStorage};
use nearcore::load_config;
use std::path::Path;

#[derive(clap::Parser)]
pub struct UndoBlockCommand {
    /// Only reset the block head to the tail block. Does not reset the header head.
    #[arg(short, long)]
    reset_only_body: bool,
}

impl UndoBlockCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let store_opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.config.cloud_storage.as_ref(),
        );

        let storage = store_opener.open_in_mode(Mode::ReadWrite).unwrap();
        let store = storage.get_hot_store();

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        let mut chain_store = ChainStore::new(
            store,
            near_config.client_config.save_trie_changes,
            near_config.genesis.config.transaction_validity_period,
        );

        if self.reset_only_body {
            crate::undo_only_block_head(&mut chain_store, &*epoch_manager)
        } else {
            crate::undo_block(&mut chain_store, &*epoch_manager)
        }
    }
}
