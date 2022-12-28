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

const ACCOUNT_HASHING: StageId = StageId("AccountHashingStage");

/// Account hashing stage hashes plain account.
/// This is preparation before generating intermediate hashes and calculating Merkle tree root.
#[derive(Debug)]
pub struct AccountHashingStage {}

#[async_trait::async_trait]
impl<DB: Database> Stage<DB> for AccountHashingStage {
    /// Return the id of the stage
    fn id(&self) -> StageId {
        ACCOUNT_HASHING
    }

    /// Execute the stage.
    async fn execute(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: ExecInput,
    ) -> Result<ExecOutput, StageError> {
        // TODO introduce threshold for decision if we do hashing of all accounts vs
        // reading changesets and hashing only part of accounts.

        tx.clear::<tables::HashedAcccount>()?;
        tx.commit()?;

        // NOTE, this will depend on RAM memory and how many account can be put inside memory.
        let batch_size = 500_000;

        let mut first_key = H160::zero();
        loop {
            let mut accounts = tx.cursor::<tables::PlainAccountState>()?;

            let hashed_batch = accounts
                .walk(first_key)?
                .take(batch_size)
                .map(|res| res.map(|(address, account)| (keccak256(address), account)))
                .collect::<Result<BTreeMap<_, _>, _>>()?;

            // next key of iterator
            let next_key = accounts.next()?;

            let mut hashes = tx.cursor_mut::<tables::HashedAcccount>()?;
            // iterate and append presorted hashed accounts
            hashed_batch.into_iter().map(|(k, v)| hashes.append(k, v)).collect::<Result<_, _>>()?;

            if let Some((next_key, _)) = next_key {
                first_key = next_key;
                continue
            }
            break
        }

        info!(target: "sync::stages::hashing_account", "Stage finished");
        Ok(ExecOutput { stage_progress: input.previous_stage_progress(), done: true })
    }

    /// Unwind the stage.
    async fn unwind(
        &mut self,
        _tx: &mut Transaction<'_, DB>,
        input: UnwindInput,
    ) -> Result<UnwindOutput, Box<dyn std::error::Error + Send + Sync>> {
        // TODO read AccountChangeSet, set old values and add/delete removed/created accounts.

        Ok(UnwindOutput { stage_progress: input.unwind_to })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    pub fn sanity_test() {}
}
