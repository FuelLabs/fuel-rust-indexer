use fuel_core_types::fuel_tx::Receipt;
use fuels::core::{
    codec::{
        ABIDecoder,
        DecoderConfig,
        Log,
    },
    traits::{
        Parameterize,
        Tokenizable,
    },
};

/// Try to parse the `Receipt` to the `T` event.
pub fn try_parse_event<T>(decoder_config: DecoderConfig, receipt: &Receipt) -> Option<T>
where
    T: Log + Tokenizable + Parameterize + 'static,
{
    let receipt_log_id = match receipt {
        Receipt::LogData { rb, .. } => *rb,
        Receipt::Log { rb, .. } => *rb,
        _ => return None,
    };

    if receipt_log_id != T::LOG_ID_U64 {
        return None;
    }

    enum Data<'a> {
        LogData(&'a [u8]),
        LogRa(u64),
    }

    let data = match receipt {
        Receipt::LogData {
            data: Some(data), ..
        } => Some(Data::LogData(data.as_slice())),
        Receipt::Log { ra, .. } => Some(Data::LogRa(*ra)),
        _ => None,
    };

    data.and_then(move |data| {
        let normalized_data = match data {
            Data::LogData(data) => data,
            Data::LogRa(ra) => &ra.to_be_bytes(),
        };

        let token = match ABIDecoder::new(decoder_config)
            .decode(&T::param_type(), normalized_data)
        {
            Ok(token) => token,
            Err(err) => {
                tracing::error!("Unable to decode the token from the receipt {err}");
                return None;
            }
        };

        match T::from_token(token) {
            Ok(value) => Some(value),
            Err(err) => {
                tracing::error!("Unable to decode the token {err}");
                None
            }
        }
    })
}

/// Defines the block of code that parses the `Receipt` into one of the events.
///
/// ```rust
/// use fuel_indexer::try_parse_events;
/// use fuels::core::codec::DecoderConfig;
/// use fuel_core_types::fuel_tx::Receipt;
///
/// fuels::prelude::abigen!(Contract(
///     name = "OrderBook",
///     abi = "crates/indexer/processors/receipt_parser/order-book-abi.json"
/// ));
///
/// enum Event {
///     Created(OrderCreatedEvent),
///     Canceled(OrderCancelledEvent),
/// }
///
/// fn my_own_parse_function(d: DecoderConfig, receipt: &Receipt) -> Option<Event> {
///     try_parse_events!(
///         [d, receipt]
///         OrderCreatedEvent => |event| {
///             Some(Event::Created(event))
///         },
///         OrderCancelledEvent => |event| {
///             Some(Event::Canceled(event))
///         }
///     )
/// }
/// ```
#[macro_export]
macro_rules! try_parse_events {
    ([$decoding_config:ident, $receipt:ident] $($event_ty:ty => |$e:ident| $handler:block),*) => {{
        // Force compilation error if type is incorrect
        let _: &$crate::fuels::core::codec::DecoderConfig = &$decoding_config;
        let _: &$crate::fuel_core_types::fuel_tx::Receipt = &$receipt;

        let (receipt_log_id, contract_id) = match $receipt {
            Receipt::LogData { rb, id, .. } => (*rb, *id),
            Receipt::Log { rb, id, .. } => (*rb, *id),
            _ => return None,
        };

        #[allow(unused_variables)]
        // Allow user to have an access to the `contract_id` if they want.
        let contract_id = contract_id;

        match receipt_log_id {
            $(<$event_ty as $crate::fuels::core::codec::Log>::LOG_ID_U64 => {
                // TODO: Parse in a more optimal way
                let event = $crate::processors::receipt_parser::try_parse_event::<$event_ty>($decoding_config, $receipt)?;

                let handler = |$e: $event_ty| $handler;

                handler(event)
            }),*
            _ => None
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    fuels::prelude::abigen!(Contract(
        name = "OrderBook",
        abi = "crates/indexer/processors/receipt_parser/order-book-abi.json"
    ));

    const RECEIPTS_JSON: &str = include_str!("receipt_parser/receipts.json");

    #[test]
    fn try_parse_event_works() {
        let decoding_config = DecoderConfig::default();

        // Given
        let receipts: Vec<Receipt> = serde_json::from_str(RECEIPTS_JSON).unwrap();

        // When
        let events = receipts
            .into_iter()
            .filter_map(|r| try_parse_event::<OrderCreatedEvent>(decoding_config, &r))
            .collect::<Vec<_>>();

        // Then
        assert_eq!(events.len(), 6);
    }

    #[test]
    fn try_parse_events_works() {
        let decoding_config = DecoderConfig::default();
        let receipts: Vec<Receipt> = serde_json::from_str(RECEIPTS_JSON).unwrap();

        // Given
        #[allow(dead_code)]
        enum Event {
            Created(OrderCreatedEvent),
            Canceled(OrderCancelledEvent),
        }

        fn my_own_parse_function(d: DecoderConfig, receipt: &Receipt) -> Option<Event> {
            try_parse_events!(
                [d, receipt]
                OrderCreatedEvent => |event| {
                    Some(Event::Created(event))
                },
                OrderCancelledEvent => |event| {
                    Some(Event::Canceled(event))
                }
            )
        }

        // When
        let events = receipts
            .into_iter()
            .filter_map(|r| my_own_parse_function(decoding_config, &r))
            .collect::<Vec<_>>();

        // Then
        assert_eq!(events.len(), 10);
    }
}
