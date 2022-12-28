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
pub struct AccountHashingStage {
    /// How many account are going to be read at same time.
    pub batch_size: usize,
}

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

        let mut first_key = H160::zero();
        loop {
            let mut accounts = tx.cursor::<tables::PlainAccountState>()?;

            let hashed_batch = accounts
                .walk(first_key)?
                .take(self.batch_size)
                .map(|res| res.map(|(address, account)| (keccak256(address), account)))
                .collect::<Result<BTreeMap<_, _>, _>>()?;

            // next key of iterator
            let next_key = accounts.next()?;

            let mut hashes = tx.cursor_mut::<tables::HashedAcccount>()?;
            // iterate and append presorted hashed accounts
            hashed_batch.into_iter().map(|(k, v)| hashes.append(k, v)).collect::<Result<_, _>>()?;

            if let Some((next_key, _)) = next_key {
                first_key = next_key;
                continue;
            }
            break;
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
    use super::AccountHashingStage;
    use crate::{
        test_utils::{
            ExecuteStageTestRunner, StageTestRunner, TestRunnerError, TestTransaction,
            UnwindStageTestRunner,
        },
        ExecInput, ExecOutput, UnwindInput,
    };

    #[test]
    pub fn sanity_test() {}

    pub struct AccountHashingTestRunner {
        tx: TestTransaction,
        threshold: u64,
    }

    impl Default for AccountHashingTestRunner {
        fn default() -> Self {
            Self { threshold: 1000, tx: TestTransaction::default() }
        }
    }

    impl AccountHashingTestRunner {
        fn set_threshold(&mut self, threshold: u64) {
            self.threshold = threshold;
        }
    }

    impl StageTestRunner for AccountHashingTestRunner {
        type S = AccountHashingStage;

        fn tx(&self) -> &TestTransaction {
            &self.tx
        }

        fn stage(&self) -> Self::S {
            AccountHashingStage { batch_size: 10 }
        }
    }

    impl ExecuteStageTestRunner for AccountHashingTestRunner {
        type Seed = Vec<()>;

        fn seed_execution(&mut self, input: ExecInput) -> Result<Self::Seed, TestRunnerError> {
            // let stage_progress = input.stage_progress.unwrap_or_default();
            // let end = input.previous_stage_progress() + 1;

            // let blocks = random_block_range(stage_progress..end, H256::zero(), 0..2);

            // let mut current_tx_id = 0;
            // blocks.iter().try_for_each(|b| -> Result<(), TestRunnerError> {
            //     current_tx_id = self.insert_block(current_tx_id, b, b.number == stage_progress)?;
            //     Ok(())
            // })?;
            Ok(vec![])
        }

        fn validate_execution(
            &self,
            input: ExecInput,
            output: Option<ExecOutput>,
        ) -> Result<(), TestRunnerError> {
            // if let Some(output) = output {
            //     self.tx.query(|tx| {
            //         let start_block = input.stage_progress.unwrap_or_default() + 1;
            //         let end_block = output.stage_progress;

            //         if start_block > end_block {
            //             return Ok(());
            //         }

            //         let start_hash = tx.get::<tables::CanonicalHeaders>(start_block)?.unwrap();
            //         let mut body_cursor = tx.cursor::<tables::BlockBodies>()?;
            //         body_cursor.seek_exact((start_block, start_hash).into())?;

            //         while let Some((_, body)) = body_cursor.next()? {
            //             for tx_id in body.tx_id_range() {
            //                 let transaction = tx
            //                     .get::<tables::Transactions>(tx_id)?
            //                     .expect("no transaction entry");
            //                 let signer =
            //                     transaction.recover_signer().expect("failed to recover signer");
            //                 assert_eq!(Some(signer), tx.get::<tables::TxSenders>(tx_id)?);
            //             }
            //         }

            //         Ok(())
            //     })?;
            // } else {
            //     self.check_no_senders_by_block(input.stage_progress.unwrap_or_default())?;
            // }

            Ok(())
        }
    }

    impl UnwindStageTestRunner for AccountHashingTestRunner {
        fn validate_unwind(&self, input: UnwindInput) -> Result<(), TestRunnerError> {
            //self.check_no_senders_by_block(input.unwind_to)
            Ok(())
        }
    }
}
