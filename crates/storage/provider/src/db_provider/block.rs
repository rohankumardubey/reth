use crate::{block::BlockHashProvider, BlockProvider, ChainInfo, HeaderProvider, ProviderImpl};
use reth_db::{database::Database, tables, transaction::DbTx};
use reth_interfaces::Result;
use reth_primitives::{rpc::BlockId, Block, BlockHash, BlockNumber, Header, H256, U256};

impl<DB: Database> HeaderProvider for ProviderImpl<DB> {
    fn header(&self, block_hash: &BlockHash) -> Result<Option<Header>> {
        self.db.view(|tx| tx.get::<tables::Headers>((0, *block_hash).into()))?.map_err(Into::into)
    }

    fn header_by_number(&self, num: BlockNumber) -> Result<Option<Header>> {
        if let Some(hash) = self.db.view(|tx| tx.get::<tables::CanonicalHeaders>(num))?? {
            self.header(&hash)
        } else {
            Ok(None)
        }
    }

    fn header_td(&self, hash: &BlockHash) -> Result<Option<U256>> {
        if let Some(num) = self.db.view(|tx| tx.get::<tables::HeaderNumbers>(*hash))?? {
            let td = self.db.view(|tx| tx.get::<tables::HeaderTD>((num, *hash).into()))??;
            Ok(td.map(|v| v.0))
        } else {
            Ok(None)
        }
    }
}

impl<DB: Database> BlockHashProvider for ProviderImpl<DB> {
    fn block_hash(&self, number: U256) -> Result<Option<H256>> {
        // TODO: This unwrap is potentially unsafe
        self.db
            .view(|tx| tx.get::<tables::CanonicalHeaders>(number.try_into().unwrap()))?
            .map_err(Into::into)
    }
}

impl<DB: Database> BlockProvider for ProviderImpl<DB> {
    fn chain_info(&self) -> Result<ChainInfo> {
        Ok(ChainInfo {
            best_hash: Default::default(),
            best_number: 0,
            last_finalized: None,
            safe_finalized: None,
        })
    }

    fn block(&self, _id: BlockId) -> Result<Option<Block>> {
        // TODO
        Ok(None)
    }

    fn block_number(&self, hash: H256) -> Result<Option<BlockNumber>> {
        self.db.view(|tx| tx.get::<tables::HeaderNumbers>(hash))?.map_err(Into::into)
    }
}
