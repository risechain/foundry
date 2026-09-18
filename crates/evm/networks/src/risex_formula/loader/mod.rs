#[allow(dead_code)]
#[rustfmt::skip]
pub(crate) mod schema_generated;

mod deferred;
mod effective_market_v1;

#[cfg(test)]
pub(crate) use effective_market_v1::LoaderError;
pub(crate) use effective_market_v1::{
    LoadProgress, LoadRowsError, MarginMode, MarketRow, load_rows_profiled,
};

#[cfg(test)]
pub(crate) const GENERATED_CONTRACTS_COMMIT: &str = "d2aedb90f5e90ca20822525b2a1ef736e4834708";
