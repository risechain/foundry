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
pub(crate) const GENERATED_CONTRACTS_COMMIT: &str = "3167b40a5fbcc74faedb38792625fd2492a11f56";
