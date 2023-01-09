use crate::{
    db::Transaction, ExecInput, ExecOutput, Stage, StageError, StageId, UnwindInput, UnwindOutput,
};
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    database::Database,
    models::TransitionIdAddress,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives::{keccak256, Address, StorageEntry, H160, H256, U256};
use std::{collections::BTreeMap, fmt::Debug};
use tracing::*;

const STORAGE_HASHING: StageId = StageId("StorageHashingStage");

/// Storage hashing stage hashes plain storage.
/// This is preparation before generating intermediate hashes and calculating Merkle tree root.
#[derive(Debug)]
pub struct StorageHashingStage {
    /// How many storage slots are going to be read at same time.
    pub batch_size: usize,
}

#[async_trait::async_trait]
impl<DB: Database> Stage<DB> for StorageHashingStage {
    /// Return the id of the stage
    fn id(&self) -> StageId {
        STORAGE_HASHING
    }

    /// Execute the stage.
    async fn execute(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: ExecInput,
    ) -> Result<ExecOutput, StageError> {
        // Number of blocks
        let threshold = 100_000;

        let stage_progress = input.stage_progress.unwrap_or_default();
        let previous_stage_progress = input.previous_stage_progress();

        // if there are more blocks then threshold it is faster to go over Plain state and hash all
        // account otherwise take changesets aggregate the sets and apply hashing to
        // AccountHashing table
        if previous_stage_progress - stage_progress > threshold {
            tx.clear::<tables::HashedStorage>()?;
            tx.commit()?;

            let mut first_key = H160::zero();
            loop {
                let mut storage = tx.cursor_dup::<tables::PlainStorageState>()?;

                let hashed_batch = storage
                    .walk(first_key)?
                    .take(self.batch_size)
                    .map(|res| {
                        res.map(|(address, mut slot)| {
                            // both account address and storage slot key are hashed for merkle tree.
                            slot.key = keccak256(slot.key);
                            (keccak256(address), slot)
                        })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;

                // next key of iterator
                let next_key = storage.next()?;

                let mut hashes = tx.cursor_mut::<tables::HashedStorage>()?;
                // iterate and append presorted hashed slots
                hashed_batch.into_iter().try_for_each(|(k, v)| hashes.append(k, v))?;

                if let Some((next_key, _)) = next_key {
                    first_key = next_key;
                    continue;
                }
                break;
            }
        } else {
            // read storage changeset, merge it into one changeset and calculate storage hashes.
            let from_transition =
                if stage_progress == 0 { 0 } else { tx.get_block_transition(stage_progress - 1)? };
            let to_transition = tx.get_block_transition(previous_stage_progress)?;

            let mut plain_storage = tx.cursor_dup::<tables::PlainStorageState>()?;

            // Aggregate all transition changesets and and make list of storages that have been
            // changed.
            tx.cursor::<tables::StorageChangeSet>()?
                .walk((from_transition, H160::zero()).into())?
                .take_while(|res| {
                    res.as_ref().map(|(k, _)| k.0 .0 <= to_transition).unwrap_or_default()
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                // fold all storages and save its old state so we can remove it from HashedStorage
                // it is needed as it is dup table.
                .fold(
                    BTreeMap::new(),
                    |mut accounts: BTreeMap<Address, BTreeMap<H256, U256>>,
                     (TransitionIdAddress((_, address)), storage_entry)| {
                        accounts
                            .entry(address)
                            .or_default()
                            .insert(storage_entry.key, storage_entry.value);
                        accounts
                    },
                )
                .into_iter()
                // iterate over plain state and get newest storage value.
                // Assumption we are okay with is that plain state represent
                // `previous_stage_progress` state.
                .map(|(address, storage)| {
                    storage
                        .into_iter()
                        .map(|(key, val)| {
                            plain_storage
                                .seek_by_key_subkey(address, key)
                                .map(|ret| (key, (val, ret.map(|e| e.value))))
                        })
                        .collect::<Result<BTreeMap<_, _>, _>>()
                        .map(|storage| (address, storage))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                // Hash the address and key and apply them to HashedStorage (if Storage is None
                // just remove it);
                .try_for_each(|(address, storage)| {
                    let hashed_address = keccak256(address);
                    storage.into_iter().try_for_each(
                        |(key, (old_val, new_val))| -> Result<(), StageError> {
                            let key = keccak256(key);
                            tx.delete::<tables::HashedStorage>(
                                hashed_address,
                                Some(StorageEntry { key, value: old_val }),
                            )?;
                            if let Some(value) = new_val {
                                let val = StorageEntry { key, value };
                                tx.put::<tables::HashedStorage>(hashed_address, val)?
                            }
                            Ok(())
                        },
                    )
                })?;
        }

        info!(target: "sync::stages::hashing_storage", "Stage finished");
        Ok(ExecOutput { stage_progress: input.previous_stage_progress(), done: true })
    }

    /// Unwind the stage.
    async fn unwind(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: UnwindInput,
    ) -> Result<UnwindOutput, StageError> {
        let from_transition_rev =
            if input.unwind_to == 0 { 0 } else { tx.get_block_transition(input.unwind_to - 1)? };
        let to_transition_rev = tx.get_block_transition(input.stage_progress)?;

        let mut hashed_storage = tx.cursor_dup_mut::<tables::HashedStorage>()?;

        // Aggregate all transition changesets and and make list of account that have been changed.
        tx.cursor::<tables::StorageChangeSet>()?
            .walk((from_transition_rev, H160::zero()).into())?
            .take_while(|res| {
                res.as_ref()
                    .map(|(TransitionIdAddress((k, _)), _)| *k <= to_transition_rev)
                    .unwrap_or_default()
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .rev()
            // fold all account to get the old balance/nonces and account that needs to be removed
            .fold(
                BTreeMap::new(),
                |mut accounts: BTreeMap<(Address, H256), U256>,
                 (TransitionIdAddress((_, address)), storage_entry)| {
                    accounts.insert((address, storage_entry.key), storage_entry.value);
                    accounts
                },
            )
            .into_iter()
            // hash addresses and collect it inside sorted BTreeMap.
            // We are doing keccak only once per address.
            .map(|((address, key), value)| ((keccak256(address), keccak256(key)), value))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            // Apply values to HashedStorage (if Value is zero remove it);
            .try_for_each(|((address, key), value)| -> Result<(), StageError> {
                hashed_storage.seek_by_key_subkey(address, key)?;
                hashed_storage.delete_current()?;

                if value != U256::ZERO {
                    let new_entry = StorageEntry { key, value };
                    hashed_storage.append_dup(address, new_entry)?;
                }
                Ok(())
            })?;

        Ok(UnwindOutput { stage_progress: input.unwind_to })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn sanity_test() {}
}
