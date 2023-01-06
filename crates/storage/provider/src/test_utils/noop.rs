use crate::{BlockHashProvider, BlockProvider, ChainInfo, HeaderProvider};
use reth_interfaces::Result;
use reth_primitives::{rpc::BlockId, Block, BlockHash, BlockNumber, Header, H256, U256};

/// Supports various api interfaces for testing purposes.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct NoopProvider;

/// Noop implementation for testing purposes
impl BlockHashProvider for NoopProvider {
    fn block_hash(&self, _number: U256) -> Result<Option<H256>> {
        Ok(None)
    }
}

impl BlockProvider for NoopProvider {
    fn chain_info(&self) -> Result<ChainInfo> {
        Ok(ChainInfo {
            best_hash: Default::default(),
            best_number: 0,
            last_finalized: None,
            safe_finalized: None,
        })
    }

    fn block(&self, _id: BlockId) -> Result<Option<Block>> {
        Ok(None)
    }

    fn block_number(&self, _hash: H256) -> Result<Option<BlockNumber>> {
        Ok(None)
    }
}

impl HeaderProvider for NoopProvider {
    fn header(&self, _block_hash: &BlockHash) -> Result<Option<Header>> {
        Ok(None)
    }

    fn header_by_number(&self, _num: u64) -> Result<Option<Header>> {
        Ok(None)
    }

    fn header_td(&self, _hash: &BlockHash) -> Result<Option<U256>> {
        Ok(None)
    }
}
