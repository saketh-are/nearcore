use near_store::{
    Mode, NodeStorage, STATE_SNAPSHOT_COLUMNS, StoreConfig,
    checkpoint_hot_storage_and_cleanup_columns,
};
use std::path::{Path, PathBuf};

#[derive(clap::Args)]
pub(crate) struct MakeSnapshotCommand {
    /// Destination directory.
    #[clap(long)]
    destination: PathBuf,
    /// If true, keep only the columns needed for a state snapshot
    #[clap(long)]
    flat_state_only: bool,
}

impl MakeSnapshotCommand {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        store_config: &StoreConfig,
        cold_store_config: Option<&StoreConfig>,
    ) -> anyhow::Result<()> {
        let opener = NodeStorage::opener(home_dir, store_config, cold_store_config, None);
        let node_storage = opener.open_in_mode(Mode::ReadWriteExisting)?;
        let columns_to_keep =
            if self.flat_state_only { Some(STATE_SNAPSHOT_COLUMNS) } else { None };
        checkpoint_hot_storage_and_cleanup_columns(
            &node_storage.get_hot_store(),
            &self.destination,
            columns_to_keep.map(AsRef::as_ref),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::make_snapshot::MakeSnapshotCommand;
    use near_store::{DBCol, Mode, NodeStorage, StoreConfig};

    /// Populates a DB, makes a checkpoint, makes changes to the DB.
    /// Checks that the checkpoint DB can be opened and doesn't contain the latest changes.
    #[test]
    fn test() {
        let home_dir = tempfile::tempdir().unwrap();
        let store_config = StoreConfig::test_config();
        let opener = NodeStorage::opener(home_dir.path(), &store_config, None, None);

        let keys = vec![vec![0], vec![1], vec![2], vec![3]];

        {
            // Populate the DB.
            let node_storage = opener.open().unwrap();
            let mut store_update = node_storage.get_hot_store().store_update();
            for key in &keys {
                store_update.insert(DBCol::Block, key.clone(), vec![42]);
            }
            store_update.commit().unwrap();
            println!("Populated");
            // Drops node_storage, which unlocks the DB.
        }

        let destination = home_dir.path().join("data").join("snapshot");
        let cmd = MakeSnapshotCommand { destination: destination.clone(), flat_state_only: false };
        cmd.run(home_dir.path(), &store_config, None).unwrap();
        println!("Made a checkpoint");

        {
            // Make a change to the original DB.
            let node_storage = opener.open().unwrap();
            let mut store_update = node_storage.get_hot_store().store_update();
            store_update.delete_all(DBCol::Block);
            store_update.commit().unwrap();
            println!("Deleted");
        }

        let node_storage = opener.open_in_mode(Mode::ReadOnly).unwrap();
        let snapshot_node_storage = NodeStorage::opener(&destination, &store_config, None, None)
            .open_in_mode(Mode::ReadOnly)
            .unwrap();
        for key in keys {
            let exists_original = node_storage.get_hot_store().exists(DBCol::Block, &key).unwrap();
            let exists_snapshot =
                snapshot_node_storage.get_hot_store().exists(DBCol::Block, &key).unwrap();
            println!("{exists_original},{exists_snapshot},{key:?}");
            assert!(!exists_original);
            assert!(exists_snapshot);
        }
    }
}
