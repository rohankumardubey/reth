use crate::{
    db::Transaction, ExecInput, ExecOutput, Stage, StageError, StageId, UnwindInput, UnwindOutput,
};
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives::{keccak256, H160};
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
        // TODO introduce threshold for decision if we do hashing of all accounts vs
        // reading changesets and hashing only part of accounts.

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
            hashed_batch.into_iter().map(|(k, v)| hashes.append(k, v)).collect::<Result<_, _>>()?;

            if let Some((next_key, _)) = next_key {
                first_key = next_key;
                continue
            }
            break
        }

        info!(target: "sync::stages::hashing_storage", "Stage finished");
        Ok(ExecOutput { stage_progress: input.previous_stage_progress(), done: true })
    }

    /// Unwind the stage.
    async fn unwind(
        &mut self,
        _tx: &mut Transaction<'_, DB>,
        input: UnwindInput,
    ) -> Result<UnwindOutput, Box<dyn std::error::Error + Send + Sync>> {
        // TODO read StorageChangeSet, set old values and add/delete removed/created storage slots.

        Ok(UnwindOutput { stage_progress: input.unwind_to })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    pub fn sanity_test() {}
}
