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
pub(crate) const GENERATED_CONTRACTS_COMMIT: &str = "adcf51d25b4b99d35481ea1f7b3d9e5bda0abc11";
