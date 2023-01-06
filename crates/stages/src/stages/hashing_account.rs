use crate::{
    db::Transaction, ExecInput, ExecOutput, Stage, StageError, StageId, UnwindInput, UnwindOutput,
};
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives::{keccak256, Account, Address, H160};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};
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
        // Number of blocks
        let threshold = 100_000;

        let stage_progress = input.stage_progress.unwrap_or_default();
        let previous_stage_progress = input.previous_stage_progress();

        // if there are more blocks then threshold it is faster to go over Plain state and hash all
        // account otherwise take changesets aggregate the sets and apply hashing to
        // AccountHashing table
        if previous_stage_progress - stage_progress > threshold {
            // clear table, load all accounts and hash it
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
                hashed_batch.into_iter().try_for_each(|(k, v)| hashes.append(k, v))?;

                if let Some((next_key, _)) = next_key {
                    first_key = next_key;
                    continue
                }
                break
            }
        } else {
            // read account changeset, merge it into one changeset and calculate account hashes.
            let from_transition =
                if stage_progress == 0 { 0 } else { tx.get_block_transition(stage_progress - 1)? };
            let to_transition = tx.get_block_transition(previous_stage_progress)?;

            // Aggregate all transition changesets and and make list of account that have been
            // changed.
            tx.cursor::<tables::AccountChangeSet>()?
                .walk(from_transition)?
                .take_while(|res| {
                    res.as_ref().map(|(k, _)| *k <= to_transition).unwrap_or_default()
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                // fold all account to one set of changed accounts
                .fold(BTreeSet::new(), |mut accounts: BTreeSet<Address>, (_, account_before)| {
                    accounts.insert(account_before.address);
                    accounts
                })
                .into_iter()
                // iterate over plain state and get newest value.
                // Assumption we are okay to make is that plainstate represent
                // `previous_stage_progress` state.
                .map(|address| tx.get::<tables::PlainAccountState>(address).map(|a| (address, a)))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .try_for_each(|(address, account)| {
                    let hashed_address = keccak256(address);
                    if let Some(account) = account {
                        tx.put::<tables::HashedAcccount>(hashed_address, account)
                    } else {
                        tx.delete::<tables::HashedAcccount>(hashed_address, None).map(|_| ())
                    }
                })?;
        }

        info!(target: "sync::stages::hashing_account", "Stage finished");
        Ok(ExecOutput { stage_progress: input.previous_stage_progress(), done: true })
    }

    /// Unwind the stage.
    async fn unwind(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: UnwindInput,
    ) -> Result<UnwindOutput, StageError> {
        // There is no threshold on account unwind, we will always take changesets and
        // apply past values to HashedAccount table.

        let from_transition_rev =
            if input.unwind_to == 0 { 0 } else { tx.get_block_transition(input.unwind_to - 1)? };
        let to_transition_rev = tx.get_block_transition(input.stage_progress)?;

        // Aggregate all transition changesets and and make list of account that have been changed.
        tx.cursor::<tables::AccountChangeSet>()?
            .walk(from_transition_rev)?
            .take_while(|res| {
                res.as_ref().map(|(k, _)| *k <= to_transition_rev).unwrap_or_default()
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .rev()
            // fold all account to get the old balance/nonces and account that needs to be removed
            .fold(
                BTreeMap::new(),
                |mut accounts: BTreeMap<Address, Option<Account>>, (_, account_before)| {
                    accounts.insert(account_before.address, account_before.info);
                    accounts
                },
            )
            .into_iter()
            // hash addresses and collect it inside sorted BTreeMap.
            // We are doing keccak only once per address.
            .map(|(address, account)| (keccak256(address), account))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            // Apply values to HashedState (if Account is None remove it);
            .try_for_each(|(hashed_address, account)| {
                if let Some(account) = account {
                    tx.put::<tables::HashedAcccount>(hashed_address, account)
                } else {
                    tx.delete::<tables::HashedAcccount>(hashed_address, None).map(|_| ())
                }
            })?;

        Ok(UnwindOutput { stage_progress: input.unwind_to })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn sanity_test() {}

    /*

    use reth_primitives::{Account, Address};

    use super::AccountHashingStage;
    use crate::{
        test_utils::{
            ExecuteStageTestRunner, StageTestRunner, TestRunnerError, TestTransaction,
            UnwindStageTestRunner,
        },
        ExecInput, ExecOutput, UnwindInput,
    };

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
        type Seed = Vec<(Address, Account)>;

        fn seed_execution(&mut self, input: ExecInput) -> Result<Self::Seed, TestRunnerError> {
            Ok(vec![])
        }

        fn validate_execution(
            &self,
            input: ExecInput,
            output: Option<ExecOutput>,
        ) -> Result<(), TestRunnerError> {

            Ok(())
        }
    }

    impl UnwindStageTestRunner for AccountHashingTestRunner {
        fn validate_unwind(&self, input: UnwindInput) -> Result<(), TestRunnerError> {
            //self.check_no_senders_by_block(input.unwind_to)
            Ok(())
        }
    }
    */
}
