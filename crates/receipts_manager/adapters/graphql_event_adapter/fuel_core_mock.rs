use fuel_core::service::{
    Config,
    FuelService,
};
use fuel_core_chain_config::{
    CoinConfig,
    Owner,
    StateConfig,
};
use fuel_core_client::client::types::TransactionStatus;
use fuel_core_types::{
    fuel_asm::{
        RegId,
        op,
    },
    fuel_tx::{
        Contract,
        Input,
        TransactionBuilder,
    },
    fuel_vm::checked_transaction::IntoChecked,
};
use fuels::{
    core::constants::WORD_SIZE,
    crypto::SecretKey,
    tx::{
        UtxoId,
        Witness,
    },
    types::{
        Address,
        AssetId,
        ContractId,
        Salt,
    },
};
use futures::StreamExt;
use rand::SeedableRng;

pub struct MockFuelCore {
    _handle: FuelService,
    client: std::sync::Arc<fuel_core_client::client::FuelClient>,
    contract_input: Input,
    secret: SecretKey,
    coin_manager: CoinManager,
}

struct CoinManager {
    pub coins: Vec<CoinConfig>,
    amount: u64,
}

impl CoinManager {
    fn generate_coins(count: usize, owner: Address, amount: u64) -> Vec<CoinConfig> {
        (0..count)
            .map(|i| {
                let utxo_id = UtxoId::new([i as u8; 32].into(), i as u16);
                CoinConfig {
                    tx_id: *utxo_id.tx_id(),
                    output_index: utxo_id.output_index(),
                    owner: Owner::Address(owner),
                    amount,
                    asset_id: AssetId::BASE,
                    ..Default::default()
                }
            })
            .collect()
    }

    pub fn new(count: usize, owner: Address, amount: u64) -> Self {
        Self {
            coins: Self::generate_coins(count, owner, amount),
            amount,
        }
    }

    pub fn pop(&mut self) -> Option<CoinConfig> {
        self.coins.pop()
    }
}

impl MockFuelCore {
    pub async fn new() -> anyhow::Result<Self> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xBAADF00D);
        let secret = SecretKey::random(&mut rng);
        let amount = 10000;
        let owner = Input::owner(&secret.public_key());
        let mut coin_manager = CoinManager::new(10, owner, amount);

        let state_config = StateConfig {
            coins: coin_manager.coins.clone(),
            ..Default::default()
        };

        let config = Config {
            debug: true,
            utxo_validation: true,
            ..Config::local_node_with_state_config(state_config)
        };

        let handle = FuelService::new_node(config).await?;
        let client = std::sync::Arc::new(fuel_core_client::client::FuelClient::from(
            handle.bound_address,
        ));

        let contract_input = {
            let bytecode: Vec<u8> = vec![
                op::movi(0x10, 32),
                op::aloc(0x10),
                op::move_(0x11, RegId::HP),
                // Move 1 base asset to ourselves
                op::tr(RegId::FP, RegId::ONE, 0x11),
                op::ret(RegId::ZERO),
            ]
            .into_iter()
            .collect();
            let bytecode = Witness::from(bytecode);
            let salt = Salt::zeroed();
            let contract = Contract::from(bytecode.as_ref());
            let code_root = contract.root();
            let balance_root = Contract::default_state_root();
            let state_root = Contract::default_state_root();

            let contract_id = Contract::id(&salt, &code_root, &state_root);
            let output = fuel_core_types::fuel_tx::Output::contract_created(
                contract_id,
                state_root,
            );

            let utxo_id_1 = coin_manager.pop().expect("No coins left").utxo_id();

            let create_tx = TransactionBuilder::create(bytecode, salt, vec![])
                .add_unsigned_coin_input(
                    secret,
                    utxo_id_1,
                    amount,
                    Default::default(),
                    Default::default(),
                )
                .add_output(output)
                .finalize_as_transaction()
                .into_checked(Default::default(), &Default::default())
                .expect("Cannot check transaction");

            let contract_input = Input::contract(
                UtxoId::new(create_tx.id(), 1),
                balance_root,
                state_root,
                Default::default(),
                contract_id,
            );

            client
                .submit_and_await_commit(create_tx.transaction())
                .await
                .expect("cannot insert tx into transaction pool");

            contract_input
        };

        let mock_fuel_core = MockFuelCore {
            _handle: handle,
            client,
            contract_input,
            coin_manager,
            secret,
        };

        Ok(mock_fuel_core)
    }

    pub fn client(&self) -> std::sync::Arc<fuel_core_client::client::FuelClient> {
        self.client.clone()
    }

    /// produce a transaction that passes the filter
    pub async fn produce_transaction(&mut self) -> anyhow::Result<()> {
        let contract_id = self.contract_input.contract_id().unwrap();
        let asset_id = AssetId::BASE;

        let script: Vec<u8> = vec![
            op::gtf_args(0x10, 0x00, fuel_core_types::fuel_asm::GTFArgs::ScriptData),
            op::addi(0x11, 0x10, ContractId::LEN.try_into().unwrap()),
            op::addi(0x11, 0x11, WORD_SIZE.try_into().unwrap()),
            op::movi(0x12, 100_000),
            // Trigger the transfer of the one base asset
            op::call(0x10, RegId::ONE, 0x11, 0x12),
            op::ret(RegId::ONE),
        ]
        .into_iter()
        .collect();

        let script_data: Vec<u8> = contract_id
            .iter()
            .copied()
            .chain(
                (0 as fuel_core_types::fuel_asm::Word)
                    .to_be_bytes()
                    .iter()
                    .copied(),
            )
            .chain(
                (0 as fuel_core_types::fuel_asm::Word)
                    .to_be_bytes()
                    .iter()
                    .copied(),
            )
            .chain(asset_id.iter().copied())
            .collect();

        let utxo_id = self.coin_manager.pop().expect("No coins left").utxo_id();
        let amount = self.coin_manager.amount;

        let tx = TransactionBuilder::script(script, script_data)
            .add_unsigned_coin_input(
                self.secret,
                utxo_id,
                amount,
                Default::default(),
                Default::default(),
            )
            .add_input(self.contract_input.clone())
            .add_output(fuel_core_types::fuel_tx::Output::contract(
                1,
                Default::default(),
                Default::default(),
            ))
            .script_gas_limit(1_000_000)
            .add_max_fee_limit(1_000)
            .finalize_as_transaction();

        let client = self.client();
        let mut stream = client
            .submit_and_await_status_opt(&tx, None, Some(true))
            .await?;

        let status = stream
            .next()
            .await
            .expect("Stream should yield at least one item")?;
        assert!(matches!(status, TransactionStatus::Submitted { .. }));
        let status = stream
            .next()
            .await
            .expect("Stream should yield at least one item")?;
        assert!(
            matches!(status, TransactionStatus::PreconfirmationSuccess { .. }),
            "Transaction should be preconfirmed successfully, but got: {status:?}"
        );

        Ok(())
    }

    /// produce a block to trigger the heartbeat
    pub async fn produce_block(&self) -> anyhow::Result<()> {
        self.client().produce_blocks(1, None).await?;
        Ok(())
    }
}
