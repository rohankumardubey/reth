use crate::{
    db::Transaction, ExecInput, ExecOutput, Stage, StageError, StageId, UnwindInput, UnwindOutput,
};
use reth_db::database::Database;
use std::fmt::Debug;
use tracing::*;

const MERKLE_EXECUTION: StageId = StageId("MerkleExecuteStage");
const MERKLE_UNWIND: StageId = StageId("MerkleUnwindStage");

/// Merkle stage uses input from [AccountHashingStage] and [StorageHashingStage] stages
/// and calculated intermediate hashed and state root.
/// As input of this stage depends on Hashing stages this stage will be for execute be put after
/// but for unwind before the hashing stages.
#[derive(Debug)]
pub struct MerkleStage {
    /// Flag if true would do `execute` but skip unwind but if it false it would skip execution but
    /// do unwind.
    pub is_execute: bool,
}

#[async_trait::async_trait]
impl<DB: Database> Stage<DB> for MerkleStage {
    /// Return the id of the stage
    fn id(&self) -> StageId {
        if self.is_execute {
            MERKLE_EXECUTION
        } else {
            MERKLE_UNWIND
        }
    }

    /// Execute the stage.
    async fn execute(
        &mut self,
        _tx: &mut Transaction<'_, DB>,
        input: ExecInput,
    ) -> Result<ExecOutput, StageError> {
        if !self.is_execute {
            info!(target: "sync::stages::merkle::unwind", "Stage is always skipped");
            return Ok(ExecOutput { stage_progress: input.previous_stage_progress(), done: true })
        }

        // Iterate over changeset (similar to Hashing stages) and take new values

        info!(target: "sync::stages::merkle::exec", "Stage finished");
        Ok(ExecOutput { stage_progress: input.previous_stage_progress(), done: true })
    }

    /// Unwind the stage.
    async fn unwind(
        &mut self,
        _tx: &mut Transaction<'_, DB>,
        input: UnwindInput,
    ) -> Result<UnwindOutput, Box<dyn std::error::Error + Send + Sync>> {
        if self.is_execute {
            info!(target: "sync::stages::merkle::exec", "Stage is always skipped");
            return Ok(UnwindOutput { stage_progress: input.unwind_to })
        }

        info!(target: "sync::stages::merkle::unwind", "Stage finished");
        Ok(UnwindOutput { stage_progress: input.unwind_to })
    }
}
