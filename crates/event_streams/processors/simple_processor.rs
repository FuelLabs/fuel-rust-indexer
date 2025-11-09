use crate::processors::ReceiptParser;
use fuel_core_types::fuel_tx::{
    Receipt,
    TxId,
    TxPointer,
};
use fuels::core::codec::DecoderConfig;

pub struct FnReceiptParser<Fn> {
    parser: Fn,
    decoding_config: DecoderConfig,
}

impl<Fn> FnReceiptParser<Fn> {
    pub fn new(parser: Fn, decoding_config: DecoderConfig) -> Self {
        Self {
            parser,
            decoding_config,
        }
    }
}

impl<Fn, Event> ReceiptParser for FnReceiptParser<Fn>
where
    Fn: FnOnce(DecoderConfig, &Receipt) -> Option<Event> + Copy + Send + Sync + 'static,
{
    type Event = Event;

    fn parse(
        &self,
        _: TxPointer,
        _: u16,
        _: TxId,
        receipt: &Receipt,
    ) -> Option<Vec<Self::Event>> {
        (self.parser)(self.decoding_config, receipt).map(|event| vec![event])
    }
}

pub type LogsProcessor<Fn> = ReceiptProcessor<FnReceiptParser<Fn>>;

#[derive(Clone)]
pub struct ReceiptProcessor<R> {
    parser: R,
}

impl<R> ReceiptProcessor<R> {
    pub fn new(parser: R) -> Self {
        Self { parser }
    }
}

impl<R> ReceiptProcessor<R>
where
    R: ReceiptParser,
{
    pub fn process_iter<'a>(
        &'a self,
        tx_pointer: TxPointer,
        tx_id: TxId,
        receipts: impl Iterator<Item = &'a Receipt> + 'a,
    ) -> impl Iterator<Item = R::Event> + 'a {
        receipts
            .enumerate()
            .filter_map(move |(receipt_index, receipt)| {
                self.parser
                    .parse(tx_pointer, receipt_index as u16, tx_id, receipt)
            })
            .flatten()
    }
}

#[allow(non_snake_case)]
#[cfg(test)]
mod tests {
    use super::*;
    use fuel_core_types::{
        fuel_tx::{
            PanicInstruction,
            Receipt,
        },
        fuel_types::ContractId,
    };
    use fuels::tx::ScriptExecutionResult;

    #[derive(Default)]
    struct MockReceiptParser;

    impl ReceiptParser for MockReceiptParser {
        type Event = String;

        fn parse(
            &self,
            _tx_pointer: TxPointer,
            _receipt_index: u16,
            _tx_id: TxId,
            receipt: &Receipt,
        ) -> Option<Vec<Self::Event>> {
            match receipt {
                Receipt::Transfer { to, amount, .. } => {
                    Some(vec![format!("Transfer to {:?} of amount {}", to, amount)])
                }
                Receipt::TransferOut { to, amount, .. } => Some(vec![format!(
                    "TransferOut to {:?} of amount {}",
                    to, amount
                )]),
                _ => None,
            }
        }
    }

    #[test]
    fn process_iter__for_empty_receipts_yields_empty_results() {
        let processor = ReceiptProcessor::new(MockReceiptParser);

        let receipts: Vec<Receipt> = vec![];

        let results: Vec<_> = processor
            .process_iter(Default::default(), Default::default(), receipts.iter())
            .collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn process_iter__returns_balance_changes_for_transfer_receipts() {
        let processor = ReceiptProcessor::new(MockReceiptParser);

        let contract_in_id = ContractId::new([1; 32]);
        let contract_out_id = ContractId::new([2; 32]);

        let transfer_receipt = Receipt::Transfer {
            id: contract_in_id,
            to: contract_out_id,
            amount: 13,
            asset_id: Default::default(),
            pc: Default::default(),
            is: Default::default(),
        };
        let transfer_out_receipt = Receipt::TransferOut {
            id: contract_out_id,
            to: Default::default(),
            amount: 14,
            asset_id: Default::default(),
            pc: Default::default(),
            is: Default::default(),
        };
        let receipts: Vec<Receipt> = vec![transfer_receipt, transfer_out_receipt];

        let results: Vec<_> = processor
            .process_iter(Default::default(), Default::default(), receipts.iter())
            .collect();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn process_iter__for_non_empty_other_receipts_yields_empty_results() {
        let processor = ReceiptProcessor::new(MockReceiptParser);

        let burn_receipt = Receipt::Burn {
            sub_id: Default::default(),
            contract_id: Default::default(),
            val: Default::default(),
            pc: Default::default(),
            is: Default::default(),
        };
        let log_receipt = Receipt::Log {
            id: Default::default(),
            ra: Default::default(),
            rb: Default::default(),
            rc: Default::default(),
            rd: Default::default(),
            pc: Default::default(),
            is: Default::default(),
        };
        let mint_receipt = Receipt::Mint {
            sub_id: Default::default(),
            contract_id: Default::default(),
            val: Default::default(),
            pc: Default::default(),
            is: Default::default(),
        };
        let panic_receipt = Receipt::Panic {
            id: Default::default(),
            reason: PanicInstruction::error(
                fuel_core_types::fuel_tx::PanicReason::ArithmeticError,
                0,
            ),
            pc: Default::default(),
            is: Default::default(),
            contract_id: Default::default(),
        };
        let return_receipt = Receipt::Return {
            id: Default::default(),
            val: Default::default(),
            pc: Default::default(),
            is: Default::default(),
        };
        let return_data_receipt = Receipt::ReturnData {
            id: Default::default(),
            ptr: Default::default(),
            len: Default::default(),
            digest: Default::default(),
            pc: Default::default(),
            is: Default::default(),
            data: Default::default(),
        };
        let revert_receipt = Receipt::Revert {
            id: Default::default(),
            ra: Default::default(),
            pc: Default::default(),
            is: Default::default(),
        };
        let script_result_receipt = Receipt::ScriptResult {
            result: ScriptExecutionResult::Success,
            gas_used: Default::default(),
        };
        let receipts: Vec<Receipt> = vec![
            burn_receipt,
            log_receipt,
            mint_receipt,
            panic_receipt,
            return_receipt,
            return_data_receipt,
            revert_receipt,
            script_result_receipt,
        ];

        let results: Vec<_> = processor
            .process_iter(Default::default(), Default::default(), receipts.iter())
            .collect();
        assert_eq!(results.len(), 0);
    }
}
