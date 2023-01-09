use crate::{
    config::{WEI_2ETH, WEI_3ETH, WEI_5ETH},
    revm_wrap::{self, to_reth_acc, SubState},
    Config,
};
use hashbrown::hash_map::Entry;
use reth_db::{models::AccountBeforeTx, tables, transaction::DbTxMut, Error as DbError};
use reth_interfaces::executor::Error;
use reth_primitives::{
    bloom::logs_bloom, Account, Address, Bloom, Header, Log, Receipt, TransactionSignedEcRecovered,
    H160, H256, U256,
};
use reth_provider::StateProvider;
use revm::{
    db::AccountState, Account as RevmAccount, AccountInfo, AnalysisKind, Bytecode, Database,
    Return, SpecId, EVM,
};
use std::collections::BTreeMap;

/// Main block executor
pub struct Executor {
    /// Configuration, Spec and optional flags.
    pub config: Config,
}

/// Contains old/new account changes
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AccountInfoChangeSet {
    /// The account is newly created.
    Created {
        /// The newly created account.
        new: Account,
    },
    /// An account was deleted (selfdestructed) or we have touched
    /// an empty account and we need to remove/destroy it.
    /// (Look at state clearing [EIP-158](https://eips.ethereum.org/EIPS/eip-158))
    Destroyed {
        /// The account that was destroyed.
        old: Account,
    },
    /// The account was changed.
    Changed {
        /// The account after the change.
        new: Account,
        /// The account prior to the change.
        old: Account,
    },
    /// Nothing was changed for the account (nonce/balance).
    NoChange,
}

impl AccountInfoChangeSet {
    /// Apply the changes from the changeset to a database transaction.
    pub fn apply_to_db<'a, TX: DbTxMut<'a>>(
        self,
        tx: &TX,
        address: Address,
        tx_index: u64,
    ) -> Result<(), DbError> {
        match self {
            AccountInfoChangeSet::Changed { old, new } => {
                // insert old account in AccountChangeSet
                // check for old != new was already done
                tx.put::<tables::AccountChangeSet>(
                    tx_index,
                    AccountBeforeTx { address, info: Some(old) },
                )?;
                tx.put::<tables::PlainAccountState>(address, new)?;
            }
            AccountInfoChangeSet::Created { new } => {
                tx.put::<tables::AccountChangeSet>(
                    tx_index,
                    AccountBeforeTx { address, info: None },
                )?;
                tx.put::<tables::PlainAccountState>(address, new)?;
            }
            AccountInfoChangeSet::Destroyed { old } => {
                tx.delete::<tables::PlainAccountState>(address, None)?;
                tx.put::<tables::AccountChangeSet>(
                    tx_index,
                    AccountBeforeTx { address, info: Some(old) },
                )?;
            }
            AccountInfoChangeSet::NoChange => {
                // do nothing storage account didn't change
            }
        }
        Ok(())
    }
}

/// Diff change set that is needed for creating history index and updating current world state.
#[derive(Debug, Clone)]
pub struct AccountChangeSet {
    /// Old and New account account change.
    pub account: AccountInfoChangeSet,
    /// Storage containing key -> (OldValue,NewValue). in case that old value is not existing
    /// we can expect to have U256::ZERO, same with new value.
    pub storage: BTreeMap<U256, (U256, U256)>,
    /// Just to make sure that we are taking selfdestruct cleaning we have this field that wipes
    /// storage. There are instances where storage is changed but account is not touched, so we
    /// can't take into account that if new account is None that it is selfdestruct.
    pub wipe_storage: bool,
}

/// Execution Result containing vector of transaction changesets
/// and block reward if present
#[derive(Debug)]
pub struct ExecutionResult {
    /// Transaction changeset containing [Receipt], changed [Accounts][Account] and Storages.
    pub changesets: Vec<TransactionChangeSet>,
    /// Block reward if present. It represent changeset for block reward slot in
    /// [tables::AccountChangeSet] .
    pub block_reward: Option<BTreeMap<Address, AccountInfoChangeSet>>,
}

/// Commit change to database and return change diff that is used to update state and create
/// history index
///
/// ChangeDiff consists of:
///     address->AccountChangeSet (It contains old and new account info,storage wipe flag, and
/// old/new storage)     bytecode_hash->bytecodes mapping
///
/// BTreeMap is used to have sorted values
pub fn commit_changes<DB: StateProvider>(
    db: &mut SubState<DB>,
    changes: hashbrown::HashMap<H160, RevmAccount>,
) -> (BTreeMap<Address, AccountChangeSet>, BTreeMap<H256, Bytecode>) {
    let mut change = BTreeMap::new();
    let mut new_bytecodes = BTreeMap::new();
    // iterate over all changed accounts
    for (address, account) in changes {
        if account.is_destroyed {
            // get old account that we are destroying.
            let db_account = match db.accounts.entry(address) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(_entry) => {
                    panic!("Left panic to critically jumpout if happens, as every account should be hot loaded.");
                }
            };
            // Insert into `change` a old account and None for new account
            // and mark storage to be mapped
            change.insert(
                address,
                AccountChangeSet {
                    account: AccountInfoChangeSet::Destroyed { old: to_reth_acc(&db_account.info) },
                    storage: BTreeMap::new(),
                    wipe_storage: true,
                },
            );
            // clear cached DB and mark account as not existing
            db_account.storage.clear();
            db_account.account_state = AccountState::NotExisting;
            db_account.info = AccountInfo::default();

            continue
        } else {
            // check if account code is new or old.
            // does it exist inside cached contracts if it doesn't it is new bytecode that
            // we are inserting inside `change`
            if let Some(ref code) = account.info.code {
                if !code.is_empty() {
                    match db.contracts.entry(account.info.code_hash) {
                        Entry::Vacant(entry) => {
                            entry.insert(code.clone());
                            new_bytecodes.insert(H256(account.info.code_hash.0), code.clone());
                        }
                        Entry::Occupied(mut entry) => {
                            entry.insert(code.clone());
                        }
                    }
                }
            }

            // get old account that is going to be overwritten or none if it does not exist
            // and get new account that was just inserted. new account mut ref is used for
            // inserting storage
            let (account_info_changeset, new_account) = match db.accounts.entry(address) {
                Entry::Vacant(entry) => {
                    let entry = entry.insert(Default::default());
                    entry.info = account.info.clone();
                    // account was not existing, so this means new account is created
                    (AccountInfoChangeSet::Created { new: to_reth_acc(&entry.info) }, entry)
                }
                Entry::Occupied(entry) => {
                    let entry = entry.into_mut();

                    // account is present inside cache but is marked as NotExisting.
                    let account_changeset =
                        if matches!(entry.account_state, AccountState::NotExisting) {
                            AccountInfoChangeSet::Created { new: to_reth_acc(&account.info) }
                        } else if entry.info != account.info {
                            AccountInfoChangeSet::Changed {
                                old: to_reth_acc(&entry.info),
                                new: to_reth_acc(&account.info),
                            }
                        } else {
                            AccountInfoChangeSet::NoChange
                        };
                    entry.info = account.info.clone();
                    (account_changeset, entry)
                }
            };

            let mut wipe_storage = false;

            new_account.account_state = if account.storage_cleared {
                new_account.storage.clear();
                wipe_storage = true;
                AccountState::StorageCleared
            } else {
                AccountState::Touched
            };

            // Insert storage.
            let mut storage = BTreeMap::new();

            // insert storage into new db account.
            new_account.storage.extend(account.storage.into_iter().map(|(key, value)| {
                storage.insert(key, (value.original_value(), value.present_value()));
                (key, value.present_value())
            }));

            // Insert into change.
            change.insert(
                address,
                AccountChangeSet { account: account_info_changeset, storage, wipe_storage },
            );
        }
    }
    (change, new_bytecodes)
}

/// After transaction is executed this structure contain
/// transaction [Receipt] every change to state ([Account], Storage, [Bytecode])
/// that this transaction made and its old values
/// so that history account table can be updated.
#[derive(Debug, Clone)]
pub struct TransactionChangeSet {
    /// Transaction receipt
    pub receipt: Receipt,
    /// State change that this transaction made on state.
    pub changeset: BTreeMap<Address, AccountChangeSet>,
    /// new bytecode created as result of transaction execution.
    pub new_bytecodes: BTreeMap<H256, Bytecode>,
}

/// Execute and verify block
pub fn execute_and_verify_receipt<DB: StateProvider>(
    header: &Header,
    transactions: &[TransactionSignedEcRecovered],
    ommers: &[Header],
    config: &Config,
    db: SubState<DB>,
) -> Result<ExecutionResult, Error> {
    let transaction_change_set = execute(header, transactions, ommers, config, db)?;

    let receipts_iter =
        transaction_change_set.changesets.iter().map(|changeset| &changeset.receipt);

    if header.number >= config.spec_upgrades.byzantium {
        verify_receipt(header.receipts_root, header.logs_bloom, receipts_iter)?;
    }
    // TODO Before Byzantium, receipts contained state root that would mean that expensive operation
    // as hashing that is needed for state root got calculated in every transaction
    // This was replaced with is_success flag.
    // See more about EIP here: https://eips.ethereum.org/EIPS/eip-658

    Ok(transaction_change_set)
}

/// Verify receipts
pub fn verify_receipt<'a>(
    expected_receipts_root: H256,
    expected_logs_bloom: Bloom,
    receipts: impl Iterator<Item = &'a Receipt> + Clone,
) -> Result<(), Error> {
    // Check receipts root.
    let receipts_root = reth_primitives::proofs::calculate_receipt_root(receipts.clone());
    if receipts_root != expected_receipts_root {
        return Err(Error::ReceiptRootDiff { got: receipts_root, expected: expected_receipts_root })
    }

    // Create header log bloom.
    let logs_bloom = receipts.fold(Bloom::zero(), |bloom, r| bloom | r.bloom);
    if logs_bloom != expected_logs_bloom {
        return Err(Error::BloomLogDiff {
            expected: Box::new(expected_logs_bloom),
            got: Box::new(logs_bloom),
        })
    }
    Ok(())
}

/// Verify block. Execute all transaction and compare results.
/// Returns ChangeSet on transaction granularity.
/// NOTE: If block reward is still active (Before Paris/Merge) we would return
/// additional TransactionStatechangeset for account that receives the reward.
pub fn execute<DB: StateProvider>(
    header: &Header,
    transactions: &[TransactionSignedEcRecovered],
    ommers: &[Header],
    config: &Config,
    db: SubState<DB>,
) -> Result<ExecutionResult, Error> {
    let mut evm = EVM::new();
    evm.database(db);

    let spec_id = config.spec_upgrades.revm_spec(header.number);
    evm.env.cfg.chain_id = config.chain_id;
    evm.env.cfg.spec_id = config.spec_upgrades.revm_spec(header.number);
    evm.env.cfg.perf_all_precompiles_have_balance = false;
    evm.env.cfg.perf_analyse_created_bytecodes = AnalysisKind::Raw;

    revm_wrap::fill_block_env(&mut evm.env.block, header, spec_id >= SpecId::MERGE);
    let mut cumulative_gas_used = 0;
    // output of verification
    let mut changesets = Vec::with_capacity(transactions.len());

    for transaction in transactions.iter() {
        // The sum of the transaction’s gas limit, Tg, and the gas utilised in this block prior,
        // must be no greater than the block’s gasLimit.
        let block_available_gas = header.gas_limit - cumulative_gas_used;
        if transaction.gas_limit() > block_available_gas {
            return Err(Error::TransactionGasLimitMoreThenAvailableBlockGas {
                transaction_gas_limit: transaction.gas_limit(),
                block_available_gas,
            })
        }

        // Fill revm structure.
        revm_wrap::fill_tx_env(&mut evm.env.tx, transaction);

        // Execute transaction.
        let out = evm.transact();

        // Useful for debugging
        // let out = evm.inspect(revm::inspectors::CustomPrintTracer::default());
        // tracing::trace!(target:"evm","Executing transaction {:?}, \n:{out:?}: {:?}
        // \nENV:{:?}",transaction.hash(),transaction,evm.env);

        let (revm::ExecutionResult { exit_reason, gas_used, logs, .. }, state) = out;

        // Fatal internal error.
        if exit_reason == revm::Return::FatalExternalError {
            return Err(Error::ExecutionFatalError)
        }

        // Success flag was added in `EIP-658: Embedding transaction status code in receipts`.
        // TODO for verification (exit_reason): some error should return EVM error as the block with
        // that transaction can have consensus error that would make block invalid.
        let is_success = match exit_reason {
            revm::return_ok!() => true,
            revm::return_revert!() => false,
            _ => false,
            //e => return Err(Error::EVMError { error_code: e as u32 }),
        };

        // Add spend gas.
        cumulative_gas_used += gas_used;

        // Transform logs to reth format.
        let logs: Vec<Log> = logs
            .into_iter()
            .map(|l| Log {
                address: H160(l.address.0),
                topics: l.topics.into_iter().map(|h| H256(h.0)).collect(),
                data: l.data,
            })
            .collect();

        // commit state
        let (changeset, new_bytecodes) = commit_changes(evm.db().unwrap(), state);

        // Push transaction changeset and calculte header bloom filter for receipt.
        changesets.push(TransactionChangeSet {
            receipt: Receipt {
                tx_type: transaction.tx_type(),
                success: is_success,
                cumulative_gas_used,
                bloom: logs_bloom(logs.iter()),
                logs,
            },
            changeset,
            new_bytecodes,
        })
    }

    // Check if gas used matches the value set in header.
    if header.gas_used != cumulative_gas_used {
        return Err(Error::BlockGasUsed { got: cumulative_gas_used, expected: header.gas_used })
    }

    let mut db = evm.db.expect("It is set at the start of the function");
    let block_reward = block_reward_changeset(header, ommers, &mut db, config)?;

    Ok(ExecutionResult { changesets, block_reward })
}

/// Calculate Block reward changeset
pub fn block_reward_changeset<DB: StateProvider>(
    header: &Header,
    ommers: &[Header],
    db: &mut SubState<DB>,
    config: &Config,
) -> Result<Option<BTreeMap<H160, AccountInfoChangeSet>>, Error> {
    // NOTE: Related to Ethereum reward change, for other network this is probably going to be moved
    // to config.

    // From yellowpapper Page 15:
    // 11.3. Reward Application. The application of rewards to a block involves raising the balance
    // of the accounts of the beneficiary address of the block and each ommer by a certain
    // amount. We raise the block’s beneficiary account by Rblock; for each ommer, we raise the
    // block’s beneficiary by an additional 1/32 of the block reward and the beneficiary of the
    // ommer gets rewarded depending on the blocknumber. Formally we define the function Ω:
    match header.number {
        n if n >= config.spec_upgrades.paris => None,
        n if n >= config.spec_upgrades.petersburg => Some(WEI_2ETH),
        n if n >= config.spec_upgrades.byzantium => Some(WEI_3ETH),
        _ => Some(WEI_5ETH),
    }
    .map(|reward| -> Result<_, _> {
        let mut reward_beneficiaries: BTreeMap<H160, u128> = BTreeMap::new();
        // Calculate Uncle reward
        // OpenEthereum code: https://github.com/openethereum/openethereum/blob/6c2d392d867b058ff867c4373e40850ca3f96969/crates/ethcore/src/ethereum/ethash.rs#L319-L333
        for ommer in ommers {
            let ommer_reward = ((8 + ommer.number - header.number) as u128 * reward) >> 3;
            // From yellowpaper Page 15:
            // If there are collisions of the beneficiary addresses between ommers and the block
            // (i.e. two ommers with the same beneficiary address or an ommer with the
            // same beneficiary address as the present block), additions are applied
            // cumulatively
            *reward_beneficiaries.entry(ommer.beneficiary).or_default() += ommer_reward;
        }
        // insert main block reward
        *reward_beneficiaries.entry(header.beneficiary).or_default() +=
            reward + (reward >> 5) * ommers.len() as u128;

        // apply block rewards to beneficiaries (Main block and ommers);
        reward_beneficiaries
            .into_iter()
            .map(|(beneficiary, reward)| -> Result<_, _> {
                let changeset = db
                    .basic(beneficiary)
                    .map_err(|_| Error::ProviderError)?
                    // if account is present append `Changed` changeset for block reward
                    .map(|acc| {
                        let old = to_reth_acc(&acc);
                        let mut new = old;
                        new.balance += U256::from(reward);
                        AccountInfoChangeSet::Changed { new, old }
                    })
                    // if account is not present append `Created` changeset
                    .unwrap_or(AccountInfoChangeSet::Created {
                        new: Account { nonce: 0, balance: U256::from(reward), bytecode_hash: None },
                    });
                Ok((beneficiary, changeset))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
    })
    .transpose()
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use crate::{config::SpecUpgrades, revm_wrap::State};
    use reth_db::{
        database::Database,
        mdbx::{test_utils, Env, EnvKind, WriteMap},
        transaction::DbTx,
    };
    use reth_primitives::{
        hex_literal::hex, keccak256, Account, Address, Bytes, SealedBlock, StorageKey, H160, H256,
        U256,
    };
    use reth_provider::{AccountProvider, BlockHashProvider, StateProvider};
    use reth_rlp::Decodable;

    use super::*;

    #[derive(Debug, Default, Clone, Eq, PartialEq)]
    struct StateProviderTest {
        accounts: HashMap<Address, (HashMap<StorageKey, U256>, Account)>,
        contracts: HashMap<H256, Bytes>,
        block_hash: HashMap<U256, H256>,
    }

    impl StateProviderTest {
        /// Insert account.
        fn insert_account(
            &mut self,
            address: Address,
            mut account: Account,
            bytecode: Option<Bytes>,
            storage: HashMap<StorageKey, U256>,
        ) {
            if let Some(bytecode) = bytecode {
                let hash = keccak256(&bytecode);
                account.bytecode_hash = Some(hash);
                self.contracts.insert(hash, bytecode);
            }
            self.accounts.insert(address, (storage, account));
        }
    }

    impl AccountProvider for StateProviderTest {
        fn basic_account(&self, address: Address) -> reth_interfaces::Result<Option<Account>> {
            let ret = Ok(self.accounts.get(&address).map(|(_, acc)| *acc));
            ret
        }
    }

    impl BlockHashProvider for StateProviderTest {
        fn block_hash(&self, number: U256) -> reth_interfaces::Result<Option<H256>> {
            Ok(self.block_hash.get(&number).cloned())
        }
    }

    impl StateProvider for StateProviderTest {
        fn storage(
            &self,
            account: Address,
            storage_key: reth_primitives::StorageKey,
        ) -> reth_interfaces::Result<Option<reth_primitives::StorageValue>> {
            Ok(self
                .accounts
                .get(&account)
                .and_then(|(storage, _)| storage.get(&storage_key).cloned()))
        }

        fn bytecode_by_hash(&self, code_hash: H256) -> reth_interfaces::Result<Option<Bytes>> {
            Ok(self.contracts.get(&code_hash).cloned())
        }
    }

    #[test]
    fn sanity_execution() {
        // Got rlp block from: src/GeneralStateTestsFiller/stChainId/chainIdGasCostFiller.json

        let mut block_rlp = hex!("f90262f901f9a075c371ba45999d87f4542326910a11af515897aebce5265d3f6acd1f1161f82fa01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347942adc25665018aa1fe0e6bc666dac8fc2697ff9baa098f2dcd87c8ae4083e7017a05456c14eea4b1db2032126e27b3b1563d57d7cc0a08151d548273f6683169524b66ca9fe338b9ce42bc3540046c828fd939ae23bcba03f4e5c2ec5b2170b711d97ee755c160457bb58d8daa338e835ec02ae6860bbabb901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000083020000018502540be40082a8798203e800a00000000000000000000000000000000000000000000000000000000000000000880000000000000000f863f861800a8405f5e10094100000000000000000000000000000000000000080801ba07e09e26678ed4fac08a249ebe8ed680bf9051a5e14ad223e4b2b9d26e0208f37a05f6e3f188e3e6eab7d7d3b6568f5eac7d687b08d307d3154ccd8c87b4630509bc0").as_slice();
        let block = SealedBlock::decode(&mut block_rlp).unwrap();
        let mut ommer = Header::default();
        let ommer_beneficiary = H160(hex!("3000000000000000000000000000000000000000"));
        ommer.beneficiary = ommer_beneficiary;
        ommer.number = block.number;
        let ommers = vec![ommer];

        let mut db = StateProviderTest::default();

        // pre staet
        db.insert_account(
            H160(hex!("1000000000000000000000000000000000000000")),
            Account { balance: U256::ZERO, nonce: 0x00, bytecode_hash: None },
            Some(hex!("5a465a905090036002900360015500").into()),
            HashMap::new(),
        );

        let account3_old_info = Account {
            balance: U256::from(0x3635c9adc5dea00000u128),
            nonce: 0x00,
            bytecode_hash: None,
        };

        db.insert_account(
            H160(hex!("a94f5374fce5edbc8e2a8697c15331677e6ebf0b")),
            Account {
                balance: U256::from(0x3635c9adc5dea00000u128),
                nonce: 0x00,
                bytecode_hash: None,
            },
            None,
            HashMap::new(),
        );

        let mut config = Config::new_ethereum();
        // make it berlin fork
        config.spec_upgrades = SpecUpgrades::new_berlin_activated();

        let db = SubState::new(State::new(db));
        let transactions: Vec<TransactionSignedEcRecovered> =
            block.body.iter().map(|tx| tx.try_ecrecovered().unwrap()).collect();

        // execute chain and verify receipts
        let out =
            execute_and_verify_receipt(&block.header, &transactions, &ommers, &config, db).unwrap();

        assert_eq!(out.changesets.len(), 1, "Should executed one transaction");

        let changesets = out.changesets[0].clone();
        assert_eq!(changesets.new_bytecodes.len(), 0, "Should have zero new bytecodes");

        let account1 = H160(hex!("1000000000000000000000000000000000000000"));
        let _account1_info = Account { balance: U256::ZERO, nonce: 0x00, bytecode_hash: None };
        let account2 = H160(hex!("2adc25665018aa1fe0e6bc666dac8fc2697ff9ba"));
        let account2_info = Account {
            balance: U256::from(0x1bc16d674ece94bau128 - 0x1bc16d674ec80000u128), /* decrease for
                                                                                   * block reward */
            nonce: 0x00,
            bytecode_hash: None,
        };
        let account3 = H160(hex!("a94f5374fce5edbc8e2a8697c15331677e6ebf0b"));
        let account3_info = Account {
            balance: U256::from(0x3635c9adc5de996b46u128),
            nonce: 0x01,
            bytecode_hash: None,
        };

        assert_eq!(
            changesets.changeset.get(&account1).unwrap().account,
            AccountInfoChangeSet::NoChange,
            "No change to account"
        );
        assert_eq!(
            changesets.changeset.get(&account2).unwrap().account,
            AccountInfoChangeSet::Created { new: account2_info },
            "New account"
        );
        assert_eq!(
            changesets.changeset.get(&account3).unwrap().account,
            AccountInfoChangeSet::Changed { old: account3_old_info, new: account3_info },
            "Change to account state"
        );

        // check block rewards changeset.
        let mut block_rewarded_acc_info = account2_info;
        // add Blocks 2 eth reward and 2>>5 for one ommer
        block_rewarded_acc_info.balance += U256::from(WEI_2ETH + (WEI_2ETH >> 5));

        assert_eq!(
            out.block_reward,
            Some(BTreeMap::from([
                (
                    account2,
                    AccountInfoChangeSet::Changed {
                        new: block_rewarded_acc_info,
                        old: account2_info
                    }
                ),
                (
                    ommer_beneficiary,
                    AccountInfoChangeSet::Created {
                        new: Account {
                            nonce: 0,
                            balance: U256::from((8 * WEI_2ETH) >> 3),
                            bytecode_hash: None
                        }
                    }
                )
            ]))
        );

        assert_eq!(changesets.new_bytecodes.len(), 0, "No new bytecodes");

        // check storage
        let storage = &changesets.changeset.get(&account1).unwrap().storage;
        assert_eq!(storage.len(), 1, "Only one storage change");
        assert_eq!(
            storage.get(&U256::from(1)),
            Some(&(U256::ZERO, U256::from(2))),
            "Storage change from 0 to 2 on slot 1"
        );
    }

    #[test]
    fn apply_account_info_changeset() {
        let db: Arc<Env<WriteMap>> = test_utils::create_test_db(EnvKind::RW);
        let address = H160::zero();
        let tx_num = 0;
        let acc1 = Account { balance: U256::from(1), nonce: 2, bytecode_hash: Some(H256::zero()) };
        let acc2 = Account { balance: U256::from(3), nonce: 4, bytecode_hash: Some(H256::zero()) };

        let tx = db.tx_mut().unwrap();

        // check Changed changeset
        AccountInfoChangeSet::Changed { new: acc1, old: acc2 }
            .apply_to_db(&tx, address, tx_num)
            .unwrap();
        assert_eq!(
            tx.get::<tables::AccountChangeSet>(tx_num),
            Ok(Some(AccountBeforeTx { address, info: Some(acc2) }))
        );
        assert_eq!(tx.get::<tables::PlainAccountState>(address), Ok(Some(acc1)));

        AccountInfoChangeSet::Created { new: acc1 }.apply_to_db(&tx, address, tx_num).unwrap();
        assert_eq!(
            tx.get::<tables::AccountChangeSet>(tx_num),
            Ok(Some(AccountBeforeTx { address, info: None }))
        );
        assert_eq!(tx.get::<tables::PlainAccountState>(address), Ok(Some(acc1)));

        // delete old value, as it is dupsorted
        tx.delete::<tables::AccountChangeSet>(tx_num, None).unwrap();

        AccountInfoChangeSet::Destroyed { old: acc2 }.apply_to_db(&tx, address, tx_num).unwrap();
        assert_eq!(tx.get::<tables::PlainAccountState>(address), Ok(None));
        assert_eq!(
            tx.get::<tables::AccountChangeSet>(tx_num),
            Ok(Some(AccountBeforeTx { address, info: Some(acc2) }))
        );
    }
}
