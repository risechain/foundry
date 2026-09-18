use alloy_evm::EvmInternalsError;
use alloy_primitives::{Address, B256, I256, U256};
use revm::primitives::KECCAK_EMPTY;

use crate::risex_formula::{
    Request, Status,
    metrics::{Phase, PhaseMeasurer},
    storage::{
        JournalReader, StorageKeyError, checked_slot_offset, extract_signed_bytes,
        extract_unsigned_bits, extract_unsigned_bytes, mapping_slot,
        orders_market_book_slot_from_base, orders_market_books_slot, owner_side_header_slot,
        perps_market_slot, portfolio_bitmap_bucket_slots_from_base, portfolio_slot,
        risk_mark_snapshot_slots, trading_account_slot,
    },
};

#[cfg(test)]
pub(super) struct NoopPhaseMeasurer;

#[cfg(test)]
impl PhaseMeasurer for NoopPhaseMeasurer {
    fn measure<T>(&mut self, _phase: Phase, operation: impl FnOnce() -> T) -> T {
        operation()
    }

    fn measure_excluding<T, const N: usize>(
        &mut self,
        _phase: Phase,
        _excluded: [Phase; N],
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        operation(self)
    }
}

/// The only state-access seam used by the Task 5 loader core.
///
/// `derive` owns every pure storage-key computation. `sload` owns only the
/// live journal access and the minimal conversion into the loader's typed
/// error. The enclosing profiled entry points charge all remaining traversal,
/// decoding, normalization, replay, and row construction to
/// `RowMaterialization` while excluding these two nested phases.
pub(super) struct LoaderContext<'reader, 'journal, 'evm, 'metrics, M> {
    reader: &'reader mut JournalReader<'journal, 'evm>,
    phases: &'metrics mut M,
    funding_dependency: Option<Address>,
    validated_oracle: Option<Address>,
    validated_orders: Option<Address>,
}

impl<'reader, 'journal, 'evm, 'metrics, M: PhaseMeasurer>
    LoaderContext<'reader, 'journal, 'evm, 'metrics, M>
{
    pub(super) const fn new(
        reader: &'reader mut JournalReader<'journal, 'evm>,
        phases: &'metrics mut M,
    ) -> Self {
        Self {
            reader,
            phases,
            funding_dependency: None,
            validated_oracle: None,
            validated_orders: None,
        }
    }

    pub(super) fn derive<T>(&mut self, operation: impl FnOnce() -> T) -> T {
        self.phases.measure(Phase::KeyDerivation, operation)
    }

    pub(super) fn sload(&mut self, address: Address, slot: U256) -> Result<U256, LoaderError> {
        self.phases.measure(Phase::JournalLoad, || {
            self.reader.sload(address, slot).map_err(LoaderError::from)
        })
    }

    fn funding_dependency(&mut self, caller: Address) -> Result<Address, LoaderError> {
        if let Some(dependency) = self.funding_dependency {
            return Ok(dependency);
        }
        let dependency_slot = self.derive(|| {
            checked_slot_offset(
                word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
                schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_FUNDING_RATE_SLOT_OFFSET,
            )
            .map_err(LoaderError::from)
        })?;
        let dependency = address(self.sload(caller, dependency_slot)?);
        if dependency.is_zero() {
            return Err(LoaderError::Unavailable);
        }
        self.validate_dependency_code(dependency)?;
        self.funding_dependency = Some(dependency);
        Ok(dependency)
    }

    fn validate_dependency_code(&mut self, dependency: Address) -> Result<(), LoaderError> {
        let code_hash = self.phases.measure(Phase::JournalLoad, || {
            self.reader.code_hash(dependency).map_err(LoaderError::from)
        })?;
        if code_hash == B256::ZERO || code_hash == KECCAK_EMPTY {
            return Err(LoaderError::Unavailable);
        }
        Ok(())
    }

    fn validate_orders(&mut self, orders: Address) -> Result<(), LoaderError> {
        if self.validated_orders == Some(orders) {
            return Ok(());
        }
        self.validate_dependency_code(orders)?;
        self.validated_orders = Some(orders);
        Ok(())
    }

    fn block_timestamp(&self) -> U256 {
        self.reader.block_timestamp()
    }

    const fn phases_mut(&mut self) -> &mut M {
        self.phases
    }
}

use super::{
    deferred::{ChunkStreamError, MarketState, replay_chunk},
    schema_generated as schema,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoaderError {
    Unavailable,
    BoundExceeded,
    Arithmetic,
    StateLoad,
}

impl LoaderError {
    pub(crate) const fn status(self) -> Status {
        match self {
            Self::Unavailable => Status::Unavailable,
            Self::BoundExceeded => Status::BoundExceeded,
            Self::Arithmetic => Status::ArithmeticError,
            Self::StateLoad => Status::StateLoadError,
        }
    }
}

impl From<StorageKeyError> for LoaderError {
    fn from(value: StorageKeyError) -> Self {
        match value {
            StorageKeyError::ArithmeticOverflow => Self::Arithmetic,
            StorageKeyError::IndexOutOfBounds => Self::BoundExceeded,
            StorageKeyError::InvalidFieldRange | StorageKeyError::DisabledSlot => Self::StateLoad,
        }
    }
}

impl From<EvmInternalsError> for LoaderError {
    fn from(_: EvmInternalsError) -> Self {
        Self::StateLoad
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarginMode {
    Cross,
    Isolated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MarketRow {
    pub(crate) market_id: u16,
    pub(crate) margin_mode: MarginMode,
    pub(crate) effective_position_size: i128,
    pub(crate) effective_position_quote: i128,
    pub(crate) effective_last_funding_payment: i128,
    pub(crate) effective_leverage_wad: U256,
    pub(crate) effective_isolated_balance: u128,
    pub(crate) projected_settlement_pnl: I256,
    pub(crate) effective_buy_order_size: U256,
    pub(crate) effective_sell_order_size: U256,
    pub(crate) effective_order_notional: U256,
    pub(crate) mark_price: U256,
    pub(crate) accumulated_funding_payment: i128,
}

/// Metrics-only progress retained even when a loader attempt terminates early.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LoadProgress {
    pub(crate) rows_started: u32,
    pub(crate) projected_chunks: u32,
}

impl LoadProgress {
    pub(crate) fn begin_row(&mut self) -> Result<(), LoaderError> {
        self.rows_started = self.rows_started.checked_add(1).ok_or(LoaderError::BoundExceeded)?;
        Ok(())
    }

    pub(crate) fn observe_projected_chunk(&mut self) -> Result<(), LoaderError> {
        self.projected_chunks =
            self.projected_chunks.checked_add(1).ok_or(LoaderError::BoundExceeded)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LoadRowsError<E> {
    Loader(LoaderError),
    Sink(E),
}

#[cfg(test)]
pub(crate) fn load_rows<E>(
    reader: &mut JournalReader<'_, '_>,
    caller: Address,
    request: &Request,
    mut sink: impl FnMut(MarketRow) -> Result<(), E>,
) -> Result<LoadProgress, LoadRowsError<E>> {
    let mut phases = NoopPhaseMeasurer;
    let mut progress = LoadProgress::default();
    load_rows_profiled(reader, caller, request, &mut phases, &mut progress, |_, row| sink(row))?;
    Ok(progress)
}

pub(crate) fn load_rows_profiled<E, M: PhaseMeasurer>(
    reader: &mut JournalReader<'_, '_>,
    caller: Address,
    request: &Request,
    phases: &mut M,
    progress: &mut LoadProgress,
    mut sink: impl FnMut(&mut M, MarketRow) -> Result<(), E>,
) -> Result<(), LoadRowsError<E>> {
    phases.measure_excluding(
        Phase::RowMaterialization,
        [
            Phase::KeyDerivation,
            Phase::JournalLoad,
            Phase::FormulaEvaluation,
            Phase::OrderedReduction,
        ],
        |phases| {
            let mut context = LoaderContext::new(reader, phases);
            load_rows_in_context(&mut context, caller, request, progress, &mut sink)
        },
    )
}

fn load_rows_in_context<E, M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    caller: Address,
    request: &Request,
    progress: &mut LoadProgress,
    sink: &mut impl FnMut(&mut M, MarketRow) -> Result<(), E>,
) -> Result<(), LoadRowsError<E>> {
    let seal_slot = context
        .derive(|| {
            checked_slot_offset(
                word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
                schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDER_RISK_ACTIVATION_SEAL_SLOT_OFFSET,
            )
            .map_err(LoaderError::from)
        })
        .map_err(LoadRowsError::Loader)?;
    let seal_word = context.sload(caller, seal_slot).map_err(LoadRowsError::Loader)?;
    let seal = as_u8(
        extract_unsigned_bytes(
            seal_word,
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDER_RISK_ACTIVATION_SEAL_BYTE_OFFSET,
            1,
        )
        .map_err(LoaderError::from)
        .map_err(LoadRowsError::Loader)?,
    )
    .map_err(LoadRowsError::Loader)?;
    let (market_count_slot, portfolio, market_books) = context
        .derive(|| {
            Ok::<_, LoaderError>((
                checked_slot_offset(
                    word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_MARKET_STORAGE_ROOT),
                    schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_MARKET_COUNT_SLOT_OFFSET,
                )
                .map_err(LoaderError::from)?,
                portfolio_slot(request.user_id),
                orders_market_books_slot(caller).map_err(LoaderError::from)?,
            ))
        })
        .map_err(LoadRowsError::Loader)?;
    let market_count_word =
        context.sload(caller, market_count_slot).map_err(LoadRowsError::Loader)?;
    let market_count = as_u16(
        extract_unsigned_bytes(
            market_count_word,
            schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_MARKET_COUNT_BYTE_OFFSET,
            2,
        )
        .map_err(LoaderError::from)
        .map_err(LoadRowsError::Loader)?,
    )
    .map_err(LoadRowsError::Loader)?;
    let last_bucket = u64::from(market_count) / schema::HARD_BOUNDS_BITMAP_BUCKET_WIDTH;
    for bucket in 0..=last_bucket {
        let slots = context
            .derive(|| {
                portfolio_bitmap_bucket_slots_from_base(portfolio, bucket)
                    .map_err(LoaderError::from)
            })
            .map_err(LoadRowsError::Loader)?;
        let isolated = context.sload(caller, slots.isolated).map_err(LoadRowsError::Loader)?;
        let cross = context.sload(caller, slots.cross).map_err(LoadRowsError::Loader)?;
        let mut active = cross | isolated;
        while let Some(market) =
            pop_active_market(&mut active, bucket).map_err(LoadRowsError::Loader)?
        {
            if u64::from(progress.rows_started) >= schema::HARD_BOUNDS_MAX_ACTIVE_ROWS {
                return Err(LoadRowsError::Loader(LoaderError::BoundExceeded));
            }
            if market > u64::from(market_count) || market > schema::HARD_BOUNDS_MAX_MARKET_ID {
                continue;
            }
            progress.begin_row().map_err(LoadRowsError::Loader)?;
            let (row, _) = load_market_row_in_context(
                context,
                caller,
                request,
                market as u16,
                seal,
                market_books,
                progress,
            )
            .map_err(LoadRowsError::Loader)?;
            sink(context.phases_mut(), row).map_err(LoadRowsError::Sink)?;
        }
    }
    Ok(())
}

fn pop_active_market(active: &mut U256, bucket: u64) -> Result<Option<u64>, LoaderError> {
    if active.is_zero() {
        return Ok(None);
    }
    let bit = active.trailing_zeros();
    *active &= *active - U256::ONE;
    bucket
        .checked_mul(schema::HARD_BOUNDS_BITMAP_BUCKET_WIDTH)
        .and_then(|base| base.checked_add(bit as u64))
        .map(Some)
        .ok_or(LoaderError::Arithmetic)
}

#[cfg(test)]
fn load_market_row(
    reader: &mut JournalReader<'_, '_>,
    caller: Address,
    request: &Request,
    market_id: u16,
    seal: u8,
) -> Result<(MarketRow, u32), LoaderError> {
    let mut phases = NoopPhaseMeasurer;
    let mut progress = LoadProgress::default();
    progress.begin_row()?;
    let market_books = orders_market_books_slot(caller).map_err(LoaderError::from)?;
    let mut context = LoaderContext::new(reader, &mut phases);
    load_market_row_in_context(
        &mut context,
        caller,
        request,
        market_id,
        seal,
        market_books,
        &mut progress,
    )
}

#[cfg(test)]
fn load_market_row_profiled<M: PhaseMeasurer>(
    reader: &mut JournalReader<'_, '_>,
    caller: Address,
    request: &Request,
    market_id: u16,
    seal: u8,
    phases: &mut M,
    progress: &mut LoadProgress,
) -> Result<(MarketRow, u32), LoaderError> {
    let market_books = orders_market_books_slot(caller).map_err(LoaderError::from)?;
    phases.measure_excluding(
        Phase::RowMaterialization,
        [Phase::KeyDerivation, Phase::JournalLoad],
        |phases| {
            let mut context = LoaderContext::new(reader, phases);
            load_market_row_in_context(
                &mut context,
                caller,
                request,
                market_id,
                seal,
                market_books,
                progress,
            )
        },
    )
}

fn load_market_row_in_context<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    caller: Address,
    request: &Request,
    market_id: u16,
    seal: u8,
    market_books: U256,
    progress: &mut LoadProgress,
) -> Result<(MarketRow, u32), LoaderError> {
    let (account, market, book) = context.derive(|| {
        Ok::<_, LoaderError>((
            trading_account_slot(market_id, request.user_id),
            perps_market_slot(market_id)?,
            orders_market_book_slot_from_base(market_books, market_id),
        ))
    })?;
    let _canonical_initial_projected_candidate =
        may_have_projected_fills(context, caller, request.user_id, book)?;
    let position0_slot = context.derive(|| {
        checked_slot_offset(
            account,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_SIZE_RECORD_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let position0 = context.sload(caller, position0_slot)?;
    let position0_repeat_slot = context.derive(|| {
        checked_slot_offset(
            account,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_SIZE_RECORD_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let _position0_repeat = context.sload(caller, position0_repeat_slot)?;
    let position1_slot = context.derive(|| {
        checked_slot_offset(
            account,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_LAST_FUNDING_PAYMENT_RECORD_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let position1 = context.sload(caller, position1_slot)?;
    let position1_repeat_slot = context.derive(|| {
        checked_slot_offset(
            account,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_LAST_FUNDING_PAYMENT_RECORD_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let _position1_repeat = context.sload(caller, position1_repeat_slot)?;
    let size = as_i128(extract_signed_bytes(
        position0,
        schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_SIZE_BYTE_OFFSET,
        16,
    )?)?;
    let quote = as_i128(extract_signed_bytes(
        position0,
        schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_QUOTE_AMOUNT_BYTE_OFFSET,
        16,
    )?)?;
    let last_funding = as_i128(extract_signed_bytes(
        position1,
        schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_LAST_FUNDING_PAYMENT_BYTE_OFFSET,
        16,
    )?)?;
    let stored_leverage = as_u8(extract_unsigned_bytes(
        position1,
        schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_LEVERAGE_BYTE_OFFSET,
        1,
    )?)?;
    let margin = as_u8(extract_unsigned_bytes(
        position1,
        schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_MARGIN_MODE_BYTE_OFFSET,
        1,
    )?)?;
    let margin_mode = match u64::from(margin) {
        schema::LOGICAL_FIELDS_MARGIN_MODE_ENCODING_CROSS => MarginMode::Cross,
        schema::LOGICAL_FIELDS_MARGIN_MODE_ENCODING_ISOLATED => MarginMode::Isolated,
        _ => return Err(LoaderError::StateLoad),
    };
    let isolated_balance = as_u128(extract_unsigned_bytes(
        position1,
        schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_POSITION_ISOLATED_USDC_BALANCE_BYTE_OFFSET,
        14,
    )?)?;
    let deferred_slot = context.derive(|| {
        checked_slot_offset(
            market,
            schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_DEFERRED_STATE_RECORD_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let deferred_word = context.sload(caller, deferred_slot)?;
    let deferred = as_u8(extract_unsigned_bytes(
        deferred_word,
        schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_DEFERRED_STATE_BYTE_OFFSET,
        1,
    )?)?;
    let current_seal_slot = context.derive(|| {
        checked_slot_offset(
            word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDER_RISK_ACTIVATION_SEAL_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let current_seal_word = context.sload(caller, current_seal_slot)?;
    let current_seal = as_u8(extract_unsigned_bytes(
        current_seal_word,
        schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDER_RISK_ACTIVATION_SEAL_BYTE_OFFSET,
        1,
    )?)?;
    let deferred_known = deferred & schema::IMPLEMENTATION_CONSTANTS_ORDER_RISK_EPOCH_MASK as u8
        == schema::IMPLEMENTATION_CONSTANTS_ORDER_RISK_ACTIVE_EPOCH as u8;
    let skip_projected_lookup = seal
        == schema::IMPLEMENTATION_CONSTANTS_ORDER_RISK_ACTIVE_EPOCH as u8
        && deferred_known
        && deferred & !(schema::IMPLEMENTATION_CONSTANTS_ORDER_RISK_EPOCH_MASK as u8) == 0;
    let risk_slot = context.derive(|| {
        checked_slot_offset(
            account,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_ORDER_RISK_RECORD_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let risk = context.sload(caller, risk_slot)?;
    let reduce_risk_slot = context.derive(|| {
        checked_slot_offset(
            account,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_FIELDS_REDUCE_ONLY_ORDER_RISK_RECORD_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let reduce_risk = context.sload(caller, reduce_risk_slot)?;
    let ready = deferred_known
        && seal == current_seal
        && current_seal == schema::IMPLEMENTATION_CONSTANTS_ORDER_RISK_ACTIVE_EPOCH as u8
        && risk_ready(risk)?
        && (margin_mode == MarginMode::Isolated || risk_ready(reduce_risk)?);
    let (mut buy_size, mut sell_size, mut notional, projected_candidate) = if ready {
        let config_slot = context.derive(|| {
            checked_slot_offset(
                market,
                schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_STEP_SIZE_RECORD_SLOT_OFFSET,
            )
            .map_err(LoaderError::from)
        })?;
        let config = context.sload(caller, config_slot)?;
        let step_size = extract_unsigned_bytes(
            config,
            schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_STEP_SIZE_BYTE_OFFSET,
            8,
        )?;
        let buy_steps = extract_unsigned_bits(
            risk,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_BUY_OPEN_STEPS_BIT_OFFSET,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_BUY_OPEN_STEPS_BIT_WIDTH,
        )?;
        let sell_steps = extract_unsigned_bits(
            risk,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_SELL_OPEN_STEPS_BIT_OFFSET,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_SELL_OPEN_STEPS_BIT_WIDTH,
        )?;
        let ro_buy = extract_unsigned_bits(
            reduce_risk,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_BUY_OPEN_STEPS_BIT_OFFSET,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_BUY_OPEN_STEPS_BIT_WIDTH,
        )?;
        let ro_sell = extract_unsigned_bits(
            reduce_risk,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_SELL_OPEN_STEPS_BIT_OFFSET,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_SELL_OPEN_STEPS_BIT_WIDTH,
        )?;
        let (buy_size, sell_size) = if margin_mode == MarginMode::Cross {
            (
                (buy_steps + ro_buy).checked_mul(step_size).ok_or(LoaderError::Arithmetic)?,
                (sell_steps + ro_sell).checked_mul(step_size).ok_or(LoaderError::Arithmetic)?,
            )
        } else {
            (U256::ZERO, U256::ZERO)
        };
        let projected_candidate = if skip_projected_lookup {
            false
        } else {
            may_have_projected_fills(context, caller, request.user_id, book)?
        };
        (
            buy_size,
            sell_size,
            extract_unsigned_bits(
                risk,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_NOTIONAL_BIT_OFFSET,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_NOTIONAL_BIT_WIDTH,
            )?,
            projected_candidate,
        )
    } else {
        let projected_candidate = if skip_projected_lookup {
            false
        } else {
            may_have_projected_fills(context, caller, request.user_id, book)?
        };
        let (buy, sell, notional) = if projected_candidate {
            (U256::ZERO, U256::ZERO, U256::ZERO)
        } else {
            load_live_order_risk(context, caller, request.user_id, market_id, book, margin_mode)?
        };
        (buy, sell, notional, projected_candidate)
    };
    let leverage = if stored_leverage == 0 {
        let max_leverage_slot = context.derive(|| {
            checked_slot_offset(
                market,
                schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_MAX_LEVERAGE_RECORD_SLOT_OFFSET,
            )
            .map_err(LoaderError::from)
        })?;
        let config = context.sload(caller, max_leverage_slot)?;
        as_u8(extract_unsigned_bytes(
            config,
            schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_MAX_LEVERAGE_BYTE_OFFSET,
            1,
        )?)?
    } else {
        stored_leverage
    };
    let leverage_wad = U256::from(leverage)
        .checked_mul(U256::from(schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD))
        .ok_or(LoaderError::Arithmetic)?;
    let mut effective = MarketState {
        size,
        quote,
        last_funding_payment: last_funding,
        leverage_wad,
        isolated_balance,
        settlement_pnl: I256::ZERO,
    };
    let mut projected_chunks = 0_u32;
    if projected_candidate {
        let orders_slot = context.derive(|| {
            checked_slot_offset(
                word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
                schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
            )
            .map_err(LoaderError::from)
        })?;
        let orders = address(context.sload(caller, orders_slot)?);
        if orders.is_zero() {
            return Err(LoaderError::Unavailable);
        }
        context.validate_orders(orders)?;
        let streamed = super::deferred::stream_projected_chunks_in_context(
            context,
            orders,
            book,
            request.user_id,
            |chunk| {
                progress.observe_projected_chunk()?;
                if ready {
                    let size = U256::from(chunk.claim_size);
                    if chunk.side == schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_BUY as u8 {
                        buy_size = buy_size.saturating_sub(size);
                    } else {
                        sell_size = sell_size.saturating_sub(size);
                    }
                }
                replay_chunk(&mut effective, chunk, margin)
            },
        );
        projected_chunks = match streamed {
            Ok(count) => count,
            Err(ChunkStreamError::Loader(error) | ChunkStreamError::Sink(error)) => {
                return Err(error);
            }
        };
        if !ready {
            (buy_size, sell_size, notional) = load_live_order_risk(
                context,
                caller,
                request.user_id,
                market_id,
                book,
                margin_mode,
            )?;
        } else if projected_chunks != 0 {
            let (_, _, live_notional) = load_live_order_risk(
                context,
                caller,
                request.user_id,
                market_id,
                book,
                // The canonical refresh asks only for margin-accountable notional.
                MarginMode::Isolated,
            )?;
            notional = live_notional;
        }
    }
    let accumulated = load_funding(context, caller, market_id)?;
    let mark_price = if market_id == request.target_market_id {
        request.target_mark_price
    } else {
        load_localized_mark(context, caller, market_id, request.source_policy)?
    };
    let row = MarketRow {
        market_id,
        margin_mode,
        effective_position_size: effective.size,
        effective_position_quote: effective.quote,
        effective_last_funding_payment: effective.last_funding_payment,
        effective_leverage_wad: leverage_wad,
        effective_isolated_balance: effective.isolated_balance,
        projected_settlement_pnl: effective.settlement_pnl,
        effective_buy_order_size: buy_size,
        effective_sell_order_size: sell_size,
        effective_order_notional: notional,
        mark_price,
        accumulated_funding_payment: accumulated,
    };
    Ok((row, projected_chunks))
}

fn may_have_projected_fills<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    caller: Address,
    user_id: u32,
    book: U256,
) -> Result<bool, LoaderError> {
    let orders_slot = context.derive(|| {
        checked_slot_offset(
            word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let orders = address(context.sload(caller, orders_slot)?);
    if orders.is_zero() {
        return Ok(false);
    }
    context.validate_orders(orders)?;
    let (counters_slot, open_slot) = context.derive(|| {
        let counters_slot = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_DIRTY_LEVEL_COUNT,
        )?;
        let open_orders_seed = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED,
        )?;
        Ok::<_, LoaderError>((counters_slot, mapping_slot(U256::from(user_id), open_orders_seed)))
    })?;
    let counters = context.sload(orders, counters_slot)?;
    let open_orders = context.sload(orders, open_slot)?;
    let _canonical_repeat = context.sload(orders, open_slot)?;
    let dirty = extract_unsigned_bytes(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_COUNTERS_DIRTY_LEVEL_COUNT_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_COUNTERS_DIRTY_LEVEL_COUNT_BYTE_WIDTH)?;
    Ok(!dirty.is_zero() && !open_orders.is_zero())
}

fn load_live_order_risk<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    caller: Address,
    user_id: u32,
    market_id: u16,
    book: U256,
    margin_mode: MarginMode,
) -> Result<(U256, U256, U256), LoaderError> {
    let orders_slot = context.derive(|| {
        checked_slot_offset(
            word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let orders = address(context.sload(caller, orders_slot)?);
    if orders.is_zero() {
        return Err(LoaderError::Unavailable);
    }
    context.validate_orders(orders)?;
    let (open_orders_slot, config_slot) = context.derive(|| {
        let open_orders_seed = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED,
        )?;
        let config_slot = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_CONFIG,
        )?;
        Ok::<_, LoaderError>((mapping_slot(U256::from(user_id), open_orders_seed), config_slot))
    })?;
    let open_orders = context.sload(orders, open_orders_slot)?;
    let config = context.sload(orders, config_slot)?;
    let (buy, sell, notional) = if open_orders.is_zero() {
        (U256::ZERO, U256::ZERO, U256::ZERO)
    } else {
        scan_live_risk_in_context(
            context,
            orders,
            book,
            open_orders,
            false,
            user_id,
            margin_mode,
            config,
        )?
    };
    if margin_mode == MarginMode::Isolated {
        return Ok((buy, sell, notional));
    }
    let reduce_only_orders =
        load_reduce_only_orders(context, orders, caller, user_id, market_id, open_orders_slot)?;
    if reduce_only_orders.is_zero() {
        return Ok((buy, sell, notional));
    }
    let config = context.sload(orders, config_slot)?;
    let (reduce_buy, reduce_sell, _) = scan_live_risk_in_context(
        context,
        orders,
        book,
        reduce_only_orders,
        true,
        user_id,
        margin_mode,
        config,
    )?;
    Ok((
        buy.checked_add(reduce_buy).ok_or(LoaderError::Arithmetic)?,
        sell.checked_add(reduce_sell).ok_or(LoaderError::Arithmetic)?,
        notional,
    ))
}

fn load_reduce_only_orders<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    orders: Address,
    caller: Address,
    user_id: u32,
    market_id: u16,
    open_orders_slot: U256,
) -> Result<U256, LoaderError> {
    let orders_repeat_slot = context.derive(|| {
        checked_slot_offset(
            word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let _orders_repeat = address(context.sload(caller, orders_repeat_slot)?);
    let registration_slot = context.derive(|| {
        mapping_slot(
            U256::from_be_slice(caller.as_slice()),
            word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_HOOK_STORAGE_ROOT),
        )
    });
    let registration = context.sload(orders, registration_slot)?;
    let protocol_id = u32::try_from(registration >> 224).map_err(|_| LoaderError::StateLoad)?;
    let mut open_orders = context.sload(orders, open_orders_slot)?;
    if protocol_id == 0 {
        return Ok(open_orders);
    }
    let side_width = schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_STP_OWNERSHIP_PREFIX_WORD_REDUCE_ONLY_BITMAP_BIT_WIDTH;
    let side_mask = (U256::ONE << side_width) - U256::ONE;
    for side in 0_u8..=1 {
        let shift = u64::from(side) * side_width;
        if ((open_orders >> shift) & side_mask).is_zero() {
            continue;
        }
        let header_slot = context.derive(|| {
            owner_side_header_slot(protocol_id, market_id, user_id, side).map_err(LoaderError::from)
        })?;
        let header = context.sload(orders, header_slot)?;
        if header.bit(
            schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_STP_OWNERSHIP_PREFIX_WORD_READY_BIT_OFFSET
                as usize,
        ) {
            open_orders = (open_orders & !(side_mask << shift)) | ((header & side_mask) << shift);
        }
    }
    Ok(open_orders)
}

#[cfg(test)]
fn scan_live_risk(
    reader: &mut JournalReader<'_, '_>,
    orders: Address,
    book: U256,
    open_orders: U256,
    user_id: u32,
    margin_mode: MarginMode,
    config: U256,
) -> Result<(U256, U256, U256), LoaderError> {
    let mut phases = NoopPhaseMeasurer;
    let mut context = LoaderContext::new(reader, &mut phases);
    scan_live_risk_in_context(
        &mut context,
        orders,
        book,
        open_orders,
        false,
        user_id,
        margin_mode,
        config,
    )
}

#[allow(clippy::too_many_arguments)]
fn scan_live_risk_in_context<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    orders: Address,
    book: U256,
    open_orders: U256,
    reduce_only: bool,
    user_id: u32,
    margin_mode: MarginMode,
    config: U256,
) -> Result<(U256, U256, U256), LoaderError> {
    let step_size = extract_unsigned_bytes(
        config,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_SIZE_BYTE_OFFSET,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_SIZE_BYTE_WIDTH,
    )?;
    let step_price = extract_unsigned_bytes(
        config,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_PRICE_BYTE_OFFSET,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_PRICE_BYTE_WIDTH,
    )?;
    let metadata_seed = context.derive(|| {
        checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED,
        )
        .map_err(LoaderError::from)
    })?;
    let mut buy = U256::ZERO;
    let mut sell = U256::ZERO;
    let mut notional = U256::ZERO;
    for side in 0_u8..=1 {
        let offset = if side == 0 {
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_BUY_BITMAP_BYTE_OFFSET
        } else {
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_SELL_BITMAP_BYTE_OFFSET
        };
        let width = if side == 0 {
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_BUY_BITMAP_BYTE_WIDTH
        } else {
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_SELL_BITMAP_BYTE_WIDTH
        };
        let mut bitmap = extract_unsigned_bytes(open_orders, offset, width)?;
        while !bitmap.is_zero() {
            let slot = bitmap.trailing_zeros() as u64;
            bitmap &= bitmap - U256::ONE;
            if slot > schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OPEN_ORDER_SLOT_MAX
            {
                return Err(LoaderError::BoundExceeded);
            }
            let order_id = (U256::from(slot)
                << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OPEN_ORDER_SLOT_BITS_0)
                | (U256::from(user_id)
                    << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OWNER_BITS_0)
                | U256::from(side);
            let metadata_slot = context.derive(|| mapping_slot(order_id, metadata_seed));
            let metadata = context.sload(orders, metadata_slot)?;
            let size_steps = as_u32(extract_unsigned_bytes(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_WIDTH)?)?;
            let filled_steps = as_u32(extract_unsigned_bytes(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_WIDTH)?)?;
            if size_steps == 0 {
                continue;
            }
            let flags = as_u8(extract_unsigned_bytes(
                metadata,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FLAGS_BYTE_OFFSET,
                1,
            )?)?;
            let is_reduce_only =
                flags & schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ORDER_FLAGS_REDUCE_ONLY as u8 != 0;
            if is_reduce_only != reduce_only {
                continue;
            }
            let queued = super::deferred::queued_steps(size_steps, filled_steps)?;
            if queued == 0 {
                continue;
            }
            let tick = as_u32(extract_unsigned_bytes(
                metadata,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_OFFSET,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_WIDTH,
            )?)?;
            let seq = as_u16(extract_unsigned_bytes(
                metadata,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_OFFSET,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_WIDTH,
            )?)?;
            let (level, counters_slot) = context.derive(|| {
                let level =
                    crate::risex_formula::storage::orders_tick_level_slot_from_book(book, tick)?;
                let counters_slot = checked_slot_offset(
                    level,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS,
                )?;
                Ok::<_, LoaderError>((level, counters_slot))
            })?;
            let counters = context.sload(orders, counters_slot)?;
            let total_claimable = as_u64(extract_unsigned_bytes(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_CLAIMABLE_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_CLAIMABLE_STEPS_BYTE_WIDTH)?)?;
            let total_settled = as_u64(extract_unsigned_bytes(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_SETTLED_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_SETTLED_STEPS_BYTE_WIDTH)?)?;
            let live_claimable = total_claimable.saturating_sub(total_settled);
            let open_steps = if live_claimable == 0 {
                queued
            } else {
                let range_right = super::deferred::prefix_sum_in_context(
                    context,
                    orders,
                    level,
                    metadata_seed,
                    u64::from(seq),
                )?;
                let range_left = range_right.checked_sub(queued).ok_or(LoaderError::Arithmetic)?;
                queued - live_claimable.saturating_sub(range_left).min(queued)
            };
            let order_size =
                U256::from(open_steps).checked_mul(step_size).ok_or(LoaderError::Arithmetic)?;
            if margin_mode == MarginMode::Cross {
                if side == 0 {
                    buy = buy.checked_add(order_size).ok_or(LoaderError::Arithmetic)?;
                } else {
                    sell = sell.checked_add(order_size).ok_or(LoaderError::Arithmetic)?;
                }
            }
            if !reduce_only {
                let price =
                    U256::from(tick).checked_mul(step_price).ok_or(LoaderError::Arithmetic)?;
                let value = order_size
                    .checked_mul(price)
                    .and_then(|value| {
                        value.checked_div(U256::from(
                            schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD,
                        ))
                    })
                    .ok_or(LoaderError::Arithmetic)?;
                notional = notional.checked_add(value).ok_or(LoaderError::Arithmetic)?;
            }
        }
    }
    Ok((buy, sell, notional))
}

fn risk_ready(word_value: U256) -> Result<bool, LoaderError> {
    Ok(extract_unsigned_bits(
        word_value,
        schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_INITIALIZED_BIT_OFFSET,
        1,
    )? == U256::ONE
        && extract_unsigned_bits(
            word_value,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_RESERVED_BIT_OFFSET,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_RESERVED_BIT_WIDTH,
        )?
        .is_zero()
        && extract_unsigned_bits(
            word_value,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_EPOCH_BIT_OFFSET,
            schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_EPOCH_BIT_WIDTH,
        )? == U256::from(schema::IMPLEMENTATION_CONSTANTS_ORDER_RISK_ACTIVE_EPOCH))
}

fn load_funding<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    caller: Address,
    market_id: u16,
) -> Result<i128, LoaderError> {
    let dependency = context.funding_dependency(caller)?;
    let slot = context.derive(|| {
        mapping_slot(
            U256::from(market_id),
            word(schema::STORAGE_NAMESPACES_FUNDING_RATE_COMPACT_FUNDING_STORAGE_ROOT),
        )
    });
    as_i128(extract_signed_bytes(context.sload(dependency, slot)?, 0, 16)?)
}

fn load_localized_mark<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    caller: Address,
    market_id: u16,
    source_policy: U256,
) -> Result<U256, LoaderError> {
    let oracle_slot = context.derive(|| {
        checked_slot_offset(
            word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_RISEX_ORACLE_SLOT_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let oracle = address(context.sload(caller, oracle_slot)?);
    if oracle.is_zero() {
        return Err(LoaderError::Unavailable);
    }
    if context.validated_oracle != Some(oracle) {
        context.validate_dependency_code(oracle)?;
        context.validated_oracle = Some(oracle);
    }
    let ([slot0, slot1], control_slot) = context.derive(|| {
        Ok::<_, LoaderError>((
            risk_mark_snapshot_slots(market_id)?,
            word(schema::STORAGE_NAMESPACES_RISEX_ORACLE_RISK_MARK_SNAPSHOT_CONTROL_SLOT_ROOT),
        ))
    })?;
    let w0 = context.sload(oracle, slot0)?;
    let w1 = context.sload(oracle, slot1)?;
    let control = context.sload(oracle, control_slot)?;
    if w0.is_zero()
        || w1.is_zero()
        || extract_unsigned_bytes(
            w1,
            schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_RESERVED_BYTE_OFFSET,
            schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_RESERVED_BYTE_WIDTH,
        )? != U256::from(
            schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_RESERVED_REQUIRED,
        )
        || !extract_unsigned_bits(
            control,
            schema::STORAGE_PATHS_RISK_MARK_SNAPSHOT_CONTROL_FIELDS_RESERVED_BIT_OFFSET,
            schema::STORAGE_PATHS_RISK_MARK_SNAPSHOT_CONTROL_FIELDS_RESERVED_BIT_WIDTH,
        )?
        .is_zero()
    {
        return Err(LoaderError::Unavailable);
    }
    let paused = extract_unsigned_bits(
        control,
        schema::STORAGE_PATHS_RISK_MARK_SNAPSHOT_CONTROL_FIELDS_PAUSED_BIT_OFFSET,
        1,
    )?;
    let version = extract_unsigned_bits(
        control,
        schema::STORAGE_PATHS_RISK_MARK_SNAPSHOT_CONTROL_FIELDS_POLICY_VERSION_BIT_OFFSET,
        schema::STORAGE_PATHS_RISK_MARK_SNAPSHOT_CONTROL_FIELDS_POLICY_VERSION_BIT_WIDTH,
    )?;
    let record_policy = extract_unsigned_bytes(w1, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_RECORD_POLICY_STATE_BYTE_OFFSET, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_RECORD_POLICY_STATE_BYTE_WIDTH)?;
    let validity = extract_unsigned_bytes(w1, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_SOURCE_VALIDITY_SECONDS_BYTE_OFFSET, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_SOURCE_VALIDITY_SECONDS_BYTE_WIDTH)?;
    let valid_until = extract_unsigned_bytes(w0, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD0_VALID_UNTIL_BYTE_OFFSET, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD0_VALID_UNTIL_BYTE_WIDTH)?;
    let p3 = extract_unsigned_bytes(w0, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD0_P3_BYTE_OFFSET, schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD0_P3_BYTE_WIDTH)?;
    let index = extract_unsigned_bytes(
        w1,
        schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_INDEX_PRICE_BYTE_OFFSET,
        schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD1_INDEX_PRICE_BYTE_WIDTH,
    )?;
    if !paused.is_zero()
        || p3.is_zero()
        || valid_until.is_zero()
        || index.is_zero()
        || record_policy != version
        || source_policy != validity + U256::ONE
        || valid_until < context.block_timestamp()
    {
        return Err(LoaderError::Unavailable);
    }
    let (oracle_orders_slot, oracle_perps_slot) = context.derive(|| {
        Ok::<_, LoaderError>((
            U256::from(
                schema::STORAGE_PATHS_ORACLE_LEGACY_CONTRACT_STORAGE_FIELDS_S_ORDERS_MANAGER_SLOT,
            ),
            U256::from(
                schema::STORAGE_PATHS_ORACLE_LEGACY_CONTRACT_STORAGE_FIELDS_S_PERPS_ENGINE_SLOT,
            ),
        ))
    })?;
    let orders = address(context.sload(oracle, oracle_orders_slot)?);
    let perps = address(context.sload(oracle, oracle_perps_slot)?);
    if orders.is_zero() || perps != caller {
        return Err(LoaderError::Unavailable);
    }
    let premium = extract_signed_bytes(
        w0,
        schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD0_PREMIUM_EMA_BYTE_OFFSET,
        schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_WORD0_PREMIUM_EMA_BYTE_WIDTH,
    )?;
    let candidate = I256::from_raw(index).checked_add(premium).ok_or(LoaderError::Arithmetic)?;
    if candidate <= I256::ZERO {
        return Err(LoaderError::Unavailable);
    }
    context.validate_orders(orders)?;
    let registration_slot = context.derive(|| {
        mapping_slot(
            U256::from_be_slice(caller.as_slice()),
            word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_HOOK_STORAGE_ROOT),
        )
    });
    let registration = context.sload(orders, registration_slot)?;
    let protocol_id = u32::try_from(registration >> 224).map_err(|_| LoaderError::StateLoad)?;
    if protocol_id == 0 {
        return Err(LoaderError::Unavailable);
    }
    let key = (u64::from(protocol_id) << 16) | u64::from(market_id);
    let state_slot = context.derive(|| {
        word(schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_PREFIX)
            | U256::from(key << 1)
    });
    let state = context.sload(orders, state_slot)?;
    let initialized = extract_unsigned_bits(state, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_INITIALIZED_BIT_OFFSET, 1)?;
    let support = extract_unsigned_bits(state, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_SUPPORT_BIT_OFFSET, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_SUPPORT_BIT_WIDTH)?;
    let reserved = extract_unsigned_bits(state, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_RESERVED_BIT_OFFSET, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_RESERVED_BIT_WIDTH)?;
    if initialized != U256::ONE || support != U256::from(schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_SUPPORT_SUPPORTED) || !reserved.is_zero() { return Err(LoaderError::Unavailable); }
    let bid = extract_unsigned_bits(state, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_IMPACT_BID_PRICE_BIT_OFFSET, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_IMPACT_BID_PRICE_BIT_WIDTH)?;
    let ask = extract_unsigned_bits(state, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_IMPACT_ASK_PRICE_BIT_OFFSET, schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_IMPACT_ASK_PRICE_BIT_WIDTH)?;
    let candidate = candidate.into_raw();
    let bid = if bid.is_zero() { index } else { bid };
    let ask = if ask.is_zero() { index } else { ask };
    let midpoint = bid
        .checked_add(ask)
        .and_then(|value| value.checked_div(U256::from(2)))
        .ok_or(LoaderError::Arithmetic)?;
    Ok(median3(candidate, midpoint, p3))
}

fn median3(a: U256, b: U256, c: U256) -> U256 {
    if a > b {
        if b > c {
            b
        } else if a > c {
            c
        } else {
            a
        }
    } else if a > c {
        a
    } else if b > c {
        c
    } else {
        b
    }
}

const fn word(bytes: [u8; 32]) -> U256 {
    U256::from_be_bytes(bytes)
}
fn address(value: U256) -> Address {
    Address::from_slice(&value.to_be_bytes::<32>()[12..])
}
fn as_u8(value: U256) -> Result<u8, LoaderError> {
    u8::try_from(value).map_err(|_| LoaderError::StateLoad)
}
fn as_u16(value: U256) -> Result<u16, LoaderError> {
    u16::try_from(value).map_err(|_| LoaderError::StateLoad)
}
fn as_u128(value: U256) -> Result<u128, LoaderError> {
    u128::try_from(value).map_err(|_| LoaderError::StateLoad)
}
fn as_u32(value: U256) -> Result<u32, LoaderError> {
    u32::try_from(value).map_err(|_| LoaderError::StateLoad)
}
fn as_u64(value: U256) -> Result<u64, LoaderError> {
    u64::try_from(value).map_err(|_| LoaderError::StateLoad)
}
fn as_i128(value: I256) -> Result<i128, LoaderError> {
    i128::try_from(value).map_err(|_| LoaderError::StateLoad)
}

#[cfg(test)]
mod tests {
    use alloy_evm::{EvmInternals, eth::EthEvmContext};
    use alloy_primitives::{Address, B256, Bytes, I256, U256, keccak256};
    use revm::{bytecode::Bytecode, database::InMemoryDB, state::AccountInfo};
    use serde_json::Value;

    use super::{
        LoadProgress, LoadRowsError, LoaderContext, LoaderError, MarginMode, MarketRow,
        load_funding, load_live_order_risk, load_localized_mark, load_market_row,
        load_market_row_profiled, load_reduce_only_orders, load_rows, load_rows_profiled,
        perps_market_slot, pop_active_market, scan_live_risk, schema, trading_account_slot, word,
    };
    use crate::risex_formula::{
        Request, Status,
        metrics::{Phase, PhaseMeasurer},
        storage::{
            JournalReadStats, JournalReader, checked_slot_offset, mapping_slot,
            orders_market_book_slot, orders_tick_level_slot_from_book, owner_side_header_slot,
            risk_mark_snapshot_slots,
        },
    };

    #[derive(Default)]
    struct CountingPhases {
        key_derivations: u32,
        journal_loads: u32,
        row_materializations: u32,
        events: Vec<Phase>,
        durations: [u64; 8],
        clock: u64,
    }

    type ProfiledFixtureResult = (
        Result<(Vec<MarketRow>, LoadProgress), LoadRowsError<()>>,
        Vec<(Address, U256)>,
        CountingPhases,
    );

    impl CountingPhases {
        fn sample(&mut self) -> u64 {
            self.clock += 10;
            self.clock
        }

        fn duration(&self, phase: Phase) -> u64 {
            self.durations[phase as usize]
        }

        fn total_duration(&self) -> u64 {
            self.durations.iter().sum()
        }
    }

    impl PhaseMeasurer for CountingPhases {
        fn measure<T>(&mut self, phase: Phase, operation: impl FnOnce() -> T) -> T {
            self.events.push(phase);
            match phase {
                Phase::KeyDerivation => self.key_derivations += 1,
                Phase::JournalLoad => self.journal_loads += 1,
                Phase::RowMaterialization => self.row_materializations += 1,
                _ => panic!("unexpected loader phase {phase:?}"),
            }
            let start = self.sample();
            let result = operation();
            let end = self.sample();
            self.durations[phase as usize] += end - start;
            result
        }

        fn measure_excluding<T, const N: usize>(
            &mut self,
            phase: Phase,
            excluded: [Phase; N],
            operation: impl FnOnce(&mut Self) -> T,
        ) -> T {
            self.events.push(phase);
            match phase {
                Phase::KeyDerivation => self.key_derivations += 1,
                Phase::JournalLoad => self.journal_loads += 1,
                Phase::RowMaterialization => self.row_materializations += 1,
                _ => panic!("unexpected loader phase {phase:?}"),
            }
            let excluded_before = excluded.map(|excluded| self.durations[excluded as usize]);
            let start = self.sample();
            let result = operation(self);
            let end = self.sample();
            let excluded_duration = excluded
                .iter()
                .zip(excluded_before)
                .map(|(excluded, before)| self.durations[*excluded as usize] - before)
                .sum::<u64>();
            self.durations[phase as usize] += end - start - excluded_duration;
            result
        }
    }

    fn phase_runs(events: &[Phase]) -> Vec<(Phase, usize)> {
        let mut runs = Vec::new();
        for phase in events.iter().copied() {
            if let Some((last, count)) = runs.last_mut()
                && *last == phase
            {
                *count += 1;
                continue;
            }
            runs.push((phase, 1));
        }
        runs
    }

    fn phase_commitment(events: &[Phase]) -> B256 {
        keccak256(events.iter().map(|phase| *phase as u8).collect::<Vec<_>>())
    }

    fn assert_step_clock_durations(
        phases: &CountingPhases,
        key_derivation: u64,
        journal_load: u64,
        row_materialization: u64,
    ) {
        assert_eq!(phases.duration(Phase::KeyDerivation), key_derivation);
        assert_eq!(phases.duration(Phase::JournalLoad), journal_load);
        assert_eq!(phases.duration(Phase::RowMaterialization), row_materialization);
        assert_eq!(phases.total_duration(), key_derivation + journal_load + row_materialization,);
    }

    #[test]
    fn frozen_public_shapes_are_explicit() {
        let _ = core::mem::size_of::<MarketRow>();
        assert_eq!(LoaderError::BoundExceeded, LoaderError::BoundExceeded);
    }

    #[test]
    fn loader_errors_have_one_exhaustive_dispatch_mapping() {
        assert_eq!(LoaderError::Unavailable.status(), Status::Unavailable);
        assert_eq!(LoaderError::BoundExceeded.status(), Status::BoundExceeded);
        assert_eq!(LoaderError::Arithmetic.status(), Status::ArithmeticError);
        assert_eq!(LoaderError::StateLoad.status(), Status::StateLoadError);
    }

    #[test]
    fn loader_progress_overflow_is_transactional_at_each_attempt_boundary() {
        let mut rows = LoadProgress { rows_started: u32::MAX, projected_chunks: 7 };
        assert_eq!(rows.begin_row(), Err(LoaderError::BoundExceeded));
        assert_eq!(rows, LoadProgress { rows_started: u32::MAX, projected_chunks: 7 });

        let mut chunks = LoadProgress { rows_started: 3, projected_chunks: u32::MAX };
        assert_eq!(chunks.observe_projected_chunk(), Err(LoaderError::BoundExceeded));
        assert_eq!(chunks, LoadProgress { rows_started: 3, projected_chunks: u32::MAX });
    }

    #[test]
    fn owner_side_headers_fall_back_independently_and_skip_empty_sides() {
        let orders = Address::repeat_byte(0x15);
        let caller = Address::repeat_byte(0x71);
        let market_id = 9;
        let user_id = 102;
        let registry = word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
        let orders_slot = checked_slot_offset(
            registry,
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
        )
        .unwrap();
        let hook_root = word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_HOOK_STORAGE_ROOT);
        let registration_slot = mapping_slot(U256::from_be_slice(caller.as_slice()), hook_root);
        let book = orders_market_book_slot(caller, market_id).unwrap();
        let open_orders_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED).unwrap();
        let open_orders_slot = mapping_slot(U256::from(user_id), open_orders_seed);
        let ready = U256::ONE << 160;
        let advisory = (U256::ONE << 128) | (U256::from(91) << 136) | (U256::from(17) << 161);
        let buy = U256::ONE << 3;
        let sell = U256::ONE << 5;
        let narrowed_buy = U256::ONE << 7;
        let narrowed_sell = U256::ONE << 9;
        for (
            protocol_id,
            canonical_buy,
            canonical_sell,
            buy_header,
            sell_header,
            expected_buy,
            expected_sell,
        ) in [
            (7, buy, sell, narrowed_buy, narrowed_sell, buy, sell),
            (7, buy, sell, ready | advisory | narrowed_buy, narrowed_sell, narrowed_buy, sell),
            (7, buy, sell, narrowed_buy, ready | advisory, buy, U256::ZERO),
            (
                7,
                U256::ZERO,
                sell,
                ready | narrowed_buy,
                ready | narrowed_sell,
                U256::ZERO,
                narrowed_sell,
            ),
            (7, buy, U256::ZERO, ready, ready | narrowed_sell, U256::ZERO, U256::ZERO),
            (
                7,
                U256::ZERO,
                U256::ZERO,
                ready | narrowed_buy,
                ready | narrowed_sell,
                U256::ZERO,
                U256::ZERO,
            ),
            (0, buy, sell, ready | narrowed_buy, ready | narrowed_sell, buy, sell),
        ] {
            let mut db = InMemoryDB::default();
            db.insert_account_info(caller, AccountInfo::default());
            db.insert_account_info(orders, AccountInfo::default());
            let registration = (U256::from(protocol_id) << 224) | ((U256::ONE << 224) - U256::ONE);
            db.insert_account_storage(orders, registration_slot, registration).unwrap();
            let canonical = canonical_buy | (canonical_sell << 128);
            db.insert_account_storage(orders, open_orders_slot, canonical).unwrap();
            let mut expected_reads = vec![
                (caller, orders_slot),
                (orders, registration_slot),
                (orders, open_orders_slot),
            ];
            for (side, canonical, header) in
                [(0, canonical_buy, buy_header), (1, canonical_sell, sell_header)]
            {
                let slot = owner_side_header_slot(7, market_id, user_id, side).unwrap();
                db.insert_account_storage(orders, slot, header).unwrap();
                if protocol_id != 0 && !canonical.is_zero() {
                    expected_reads.push((orders, slot));
                }
            }
            let mut evm = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut evm);
            let mut reader = JournalReader::new(&mut internals);
            let mut phases = super::NoopPhaseMeasurer;
            let mut context = LoaderContext::new(&mut reader, &mut phases);
            let expected = expected_buy | (expected_sell << 128);
            assert_eq!(
                load_reduce_only_orders(
                    &mut context,
                    orders,
                    caller,
                    user_id,
                    market_id,
                    open_orders_slot
                ),
                Ok(expected)
            );
            assert_eq!(reader.ordered_storage_reads(), expected_reads);
            for (side, header) in [(0, buy_header), (1, sell_header)] {
                assert_eq!(
                    reader
                        .sload(orders, owner_side_header_slot(7, market_id, user_id, side).unwrap())
                        .unwrap(),
                    header
                );
            }
        }
    }

    #[test]
    fn live_risk_keeps_normal_orders_and_filters_reduce_only_candidates() {
        let orders = Address::repeat_byte(0x15);
        let caller = Address::repeat_byte(0x71);
        let user_id = 102;
        let market_id = 9;
        let book = orders_market_book_slot(caller, market_id).unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let registry = word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
        let orders_slot = checked_slot_offset(
            registry,
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
        )
        .unwrap();
        let registration_slot = mapping_slot(
            U256::from_be_slice(caller.as_slice()),
            word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_HOOK_STORAGE_ROOT),
        );
        let open_orders_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED).unwrap();
        let open_orders_slot = mapping_slot(U256::from(user_id), open_orders_seed);
        let header_slot = owner_side_header_slot(7, market_id, user_id, 0).unwrap();
        let config_slot = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_CONFIG,
        )
        .unwrap();
        let config =
            U256::ONE | (U256::from(schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD) << 64);
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, contract_account_info());
        db.insert_account_info(caller, AccountInfo::default());
        db.insert_account_storage(caller, orders_slot, U256::from_be_slice(orders.as_slice()))
            .unwrap();
        db.insert_account_storage(orders, registration_slot, U256::from(7) << 224).unwrap();
        db.insert_account_storage(orders, open_orders_slot, U256::from(0b00111)).unwrap();
        db.insert_account_storage(orders, header_slot, (U256::ONE << 160) | U256::from(0b11010))
            .unwrap();
        db.insert_account_storage(orders, config_slot, config).unwrap();
        for (slot, size, reduce_only, sequence) in [
            (0, 10, false, 1),
            (1, 20, true, 1),
            (2, 30, true, 0),
            (3, 40, true, 1),
            (4, 50, false, 0),
        ] {
            let order_id = (U256::from(slot) << 33) | (U256::from(user_id) << 1);
            let metadata = U256::from(size)
                | (U256::from(7) << 96)
                | (U256::from(sequence) << 168)
                | (U256::from(u8::from(reduce_only)) << 232);
            db.insert_account_storage(orders, mapping_slot(order_id, metadata_seed), metadata)
                .unwrap();
        }
        for (margin_mode, buy) in
            [(MarginMode::Cross, U256::from(70)), (MarginMode::Isolated, U256::ZERO)]
        {
            let mut evm = EthEvmContext::new(db.clone(), Default::default());
            let mut internals = EvmInternals::from_context(&mut evm);
            let mut reader = JournalReader::new(&mut internals);
            let mut phases = super::NoopPhaseMeasurer;
            let mut context = LoaderContext::new(&mut reader, &mut phases);
            assert_eq!(
                load_live_order_risk(&mut context, caller, user_id, market_id, book, margin_mode),
                Ok((buy, U256::ZERO, U256::from(70))),
            );
            assert_eq!(
                reader.ordered_storage_reads().contains(&(orders, header_slot)),
                margin_mode == MarginMode::Cross
            );
            assert_eq!(
                reader.ordered_storage_reads().contains(&(orders, registration_slot)),
                margin_mode == MarginMode::Cross
            );
            if margin_mode == MarginMode::Cross {
                let reads = reader.ordered_storage_reads();
                let normal_last =
                    mapping_slot((U256::from(2) << 33) | (U256::from(user_id) << 1), metadata_seed);
                assert!(
                    reads.iter().position(|read| *read == (orders, normal_last)).unwrap()
                        < reads.iter().position(|read| *read == (orders, header_slot)).unwrap()
                );
            }
        }
    }

    #[test]
    fn live_risk_clean_high_open_order_count_skips_prefix_tree() {
        let orders = Address::repeat_byte(0x15);
        let protocol = Address::repeat_byte(0x71);
        let user_id = 102_u32;
        let book = orders_market_book_slot(protocol, 9).unwrap();
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let v2 = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MEMBER_OFFSET,
        )
        .unwrap();
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        db.insert_account_storage(orders, v2, U256::ONE << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0).unwrap();
        for slot in 0_u64..128 {
            let order_id = (U256::from(slot)
                << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OPEN_ORDER_SLOT_BITS_0)
                | (U256::from(user_id)
                    << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OWNER_BITS_0);
            let metadata = U256::from(10)
                | (U256::from(7) << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_OFFSET * 8))
                | (U256::from(32_768) << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_OFFSET * 8));
            db.insert_account_storage(orders, mapping_slot(order_id, metadata_seed), metadata)
                .unwrap();
        }
        let config = U256::ONE
            << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_SIZE_BYTE_OFFSET
                * 8);
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let (buy, sell, _) = scan_live_risk(
            &mut reader,
            orders,
            book,
            (U256::ONE << 128) - U256::ONE,
            user_id,
            MarginMode::Cross,
            config,
        )
        .unwrap();
        assert_eq!(buy, U256::from(1_280));
        assert!(sell.is_zero());
        assert_eq!(reader.ordered_storage_reads().len(), 128 * 2);
        assert!(!reader.ordered_storage_reads().contains(&(orders, v2)));
    }

    #[test]
    fn live_risk_short_circuits_and_validates_the_inclusive_prefix() {
        let orders = Address::repeat_byte(0x15);
        let protocol = Address::repeat_byte(0x71);
        let user_id = 102_u32;
        let book = orders_market_book_slot(protocol, 9).unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let order_id = U256::from(user_id)
            << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OWNER_BITS_0;
        let metadata_slot = mapping_slot(order_id, metadata_seed);
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let counters_slot = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS,
        )
        .unwrap();
        let root = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MEMBER_OFFSET,
        )
        .unwrap();
        let first_leaves = checked_slot_offset(
            root,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_OFFSET,
        )
        .unwrap();
        for (filled, claimable, settled, seq, mode, leaf, expected) in [
            (10, 0, 1, 0, 7, 0, Ok(0)),
            (0, 0, 0, 5, 7, 0, Ok(10)),
            (0, 1, 2, u16::MAX, 7, 0, Ok(10)),
            (0, 3, 0, 0, 7, 0, Err(LoaderError::Arithmetic)),
            (0, 3, 0, 5, 1, 10, Ok(7)),
            (0, 3, 0, 5, 1, 0, Err(LoaderError::Arithmetic)),
            (0, 3, 0, 5, 2, 10, Err(LoaderError::StateLoad)),
            (0, 3, 0, 5, 3, 10, Err(LoaderError::StateLoad)),
            (0, 3, 0, 5, 4, 10, Err(LoaderError::StateLoad)),
            (0, 3, 0, 5, 5, 10, Err(LoaderError::StateLoad)),
            (0, 3, 0, 5, 6, 10, Err(LoaderError::StateLoad)),
            (0, 3, 0, 5, 7, 10, Err(LoaderError::StateLoad)),
            (0, 3, 0, 32_768, 1, 10, Ok(7)),
        ] {
            let mut db = InMemoryDB::default();
            db.insert_account_info(orders, AccountInfo::default());
            let metadata = U256::from(10)
                | (U256::from(filled) << 32)
                | (U256::from(7) << 96)
                | (U256::from(seq) << 168);
            db.insert_account_storage(orders, metadata_slot, metadata).unwrap();
            db.insert_account_storage(
                orders,
                counters_slot,
                (U256::from(claimable) << 16) | (U256::from(settled) << 80),
            )
            .unwrap();
            db.insert_account_storage(orders, root, U256::from(mode) << 180).unwrap();
            let leaf_index = u64::from(seq.saturating_sub(1));
            let leaves_slot = checked_slot_offset(first_leaves, leaf_index / 8).unwrap();
            db.insert_account_storage(
                orders,
                leaves_slot,
                U256::from(leaf) << ((leaf_index % 8) * 32),
            )
            .unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let expected = expected.map(|buy| (U256::from(buy), U256::ZERO, U256::ZERO));
            assert_eq!(
                scan_live_risk(
                    &mut reader,
                    orders,
                    book,
                    U256::ONE,
                    user_id,
                    MarginMode::Cross,
                    U256::ONE
                ),
                expected,
                "filled={filled} claimable={claimable} settled={settled} seq={seq} mode={mode}"
            );
            let mut reads = vec![(orders, metadata_slot)];
            if filled != 10 {
                reads.push((orders, counters_slot));
                if claimable > settled && seq > 4 {
                    reads.push((orders, root));
                    if mode == 1 {
                        if seq == 32_768 {
                            for offset in [4, 24, 124, 712] {
                                reads.push((orders, checked_slot_offset(root, offset).unwrap()));
                            }
                        }
                        reads.push((orders, leaves_slot));
                    }
                }
            }
            assert_eq!(reader.ordered_storage_reads(), reads);
        }
    }

    #[test]
    fn active_dirty_rows_replay_before_unready_live_risk() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        for (name, cache_ready) in [
            ("production_one_chunk_partial_close_rounding_legacy_prefix", true),
            ("production_two_chunks_full_close_flip_v2_empty_tail", true),
            ("production_one_chunk_partial_close_rounding_legacy_prefix", false),
            ("production_two_chunks_full_close_flip_v2_empty_tail", false),
        ] {
            let case = fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["name"] == name)
                .unwrap();
            let caller: Address = case["addresses"]["caller"].as_str().unwrap().parse().unwrap();
            let orders: Address =
                case["addresses"]["ordersManager"].as_str().unwrap().parse().unwrap();
            let market_id = 60_001_u16;
            let user_id = 102_u32;
            let mut db = InMemoryDB::default();
            for item in case["journalState"].as_array().unwrap() {
                let account: Address = item["address"].as_str().unwrap().parse().unwrap();
                db.insert_account_info(account, AccountInfo::default());
                db.insert_account_storage(
                    account,
                    hex_word(item["slot"].as_str().unwrap()),
                    hex_word(item["value"].as_str().unwrap()),
                )
                .unwrap();
            }
            db.insert_account_info(caller, AccountInfo::default());
            db.insert_account_info(orders, contract_account_info());
            let registry =
                U256::from_be_bytes(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
            install_zero_funding_dependency(&mut db, caller, registry);
            db.insert_account_storage(
                caller,
                checked_slot_offset(
                    registry,
                    schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
                )
                .unwrap(),
                U256::from_be_slice(orders.as_slice()),
            )
            .unwrap();
            db.insert_account_storage(caller, checked_slot_offset(registry, schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDER_RISK_ACTIVATION_SEAL_SLOT_OFFSET).unwrap(), U256::ONE).unwrap();
            let market = perps_market_slot(market_id).unwrap();
            db.insert_account_storage(
                caller,
                checked_slot_offset(
                    market,
                    schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_DEFERRED_STATE_RECORD_SLOT_OFFSET,
                )
                .unwrap(),
                U256::from(0x81) << 248,
            )
            .unwrap();
            db.insert_account_storage(
                caller,
                checked_slot_offset(
                    market,
                    schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_STEP_SIZE_RECORD_SLOT_OFFSET,
                )
                .unwrap(),
                U256::from(1_000_000_000_000_000_000_u128) << 96,
            )
            .unwrap();
            db.insert_account_storage(caller, checked_slot_offset(market, schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_MAX_LEVERAGE_RECORD_SLOT_OFFSET).unwrap(), U256::ONE << (schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_MAX_LEVERAGE_BYTE_OFFSET * 8)).unwrap();
            let initial = &case["orderedRows"][0];
            let account = trading_account_slot(market_id, user_id);
            let size = initial["effectivePositionSize"].as_str().unwrap().parse::<i128>().unwrap();
            let quote =
                initial["effectivePositionQuote"].as_str().unwrap().parse::<i128>().unwrap();
            let mask128 = (U256::ONE << 128) - U256::ONE;
            db.insert_account_storage(
                caller,
                account,
                (I256::unchecked_from(size).into_raw() & mask128)
                    | ((I256::unchecked_from(quote).into_raw() & mask128) << 128),
            )
            .unwrap();
            db.insert_account_storage(
                caller,
                checked_slot_offset(account, 1).unwrap(),
                U256::ONE << 128,
            )
            .unwrap();
            let ready = (U256::ONE
                << schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_INITIALIZED_BIT_OFFSET)
                | (U256::ONE
                    << schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_EPOCH_BIT_OFFSET);
            let ready = if cache_ready { ready } else { U256::ZERO };
            db.insert_account_storage(caller, checked_slot_offset(account, 2).unwrap(), ready)
                .unwrap();
            db.insert_account_storage(caller, checked_slot_offset(account, 3).unwrap(), ready)
                .unwrap();
            let book = orders_market_book_slot(caller, market_id).unwrap();
            db.insert_account_storage(orders, checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_DIRTY_LEVEL_COUNT).unwrap(), U256::ONE << 32).unwrap();
            let keys = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_KEYS).unwrap();
            db.insert_account_storage(orders, keys, U256::ONE).unwrap();
            db.insert_account_storage(
                orders,
                crate::risex_formula::storage::dynamic_array_data_slot(keys),
                (U256::ONE << 32) | U256::from(7),
            )
            .unwrap();
            let open_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED).unwrap();
            db.insert_account_storage(
                orders,
                mapping_slot(U256::from(user_id), open_seed),
                U256::ONE << 128,
            )
            .unwrap();
            let mut context = EthEvmContext::new(db.clone(), Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let request = Request {
                expected_loader_version: 1,
                expected_operation_set_version: 1,
                user_id,
                target_market_id: market_id,
                expected_loader_schema_hash: B256::ZERO,
                base_balance: U256::ZERO,
                source_policy: U256::ZERO,
                target_mark_price: U256::from(7_000_000_000_000_000_000_u128),
            };
            let (row, chunks) =
                load_market_row(&mut reader, caller, &request, market_id, 1).unwrap();
            let final_row = &case["finalRow"];
            assert_eq!(
                u64::from(chunks),
                case["projectedChunks"]["count"].as_u64().unwrap(),
                "{name}"
            );
            assert_eq!(
                row.effective_position_size.to_string(),
                final_row["effectivePositionSize"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(
                row.effective_position_quote.to_string(),
                final_row["effectivePositionQuote"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(
                row.effective_last_funding_payment.to_string(),
                final_row["effectiveLastFundingPayment"].as_str().unwrap(),
                "{name}"
            );
            if !cache_ready {
                let level = orders_tick_level_slot_from_book(book, 7).unwrap();
                let segment_seed = checked_slot_offset(level, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_FILL_SEGMENT_BY_INDEX).unwrap();
                let first_segment = mapping_slot(U256::ZERO, segment_seed);
                let registration_slot = mapping_slot(
                    U256::from_be_slice(caller.as_slice()),
                    word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_HOOK_STORAGE_ROOT),
                );
                let reads = reader.ordered_storage_reads();
                assert!(
                    reads.iter().position(|read| *read == (orders, first_segment)).unwrap()
                        < reads
                            .iter()
                            .position(|read| *read == (orders, registration_slot))
                            .unwrap(),
                    "pending replay precedes unready live risk: {name}"
                );
            }

            let mut context = EthEvmContext::new(db.clone(), Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let mut phases = CountingPhases::default();
            let mut progress = LoadProgress::default();
            progress.begin_row().unwrap();
            let (profiled_row, profiled_chunks) = load_market_row_profiled(
                &mut reader,
                caller,
                &request,
                market_id,
                1,
                &mut phases,
                &mut progress,
            )
            .unwrap();
            assert_eq!(profiled_row, row, "{name}");
            assert_eq!(profiled_chunks, chunks, "{name}");
            assert_eq!(progress.rows_started, 1, "{name}");
            assert_eq!(progress.projected_chunks, chunks, "{name}");
            assert!(phases.key_derivations > 0, "{name}");
            assert_eq!(phases.row_materializations, 1, "{name}");
            if !cache_ready && name == "production_one_chunk_partial_close_rounding_legacy_prefix" {
                // Malformed storage pins validation precedence: replay overflow must win over
                // the later reduce-only candidate's out-of-bounds sequence.
                let registration_slot = mapping_slot(
                    U256::from_be_slice(caller.as_slice()),
                    word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_HOOK_STORAGE_ROOT),
                );
                db.insert_account_storage(orders, registration_slot, U256::from(7) << 224).unwrap();
                let header = owner_side_header_slot(7, market_id, user_id, 1).unwrap();
                db.insert_account_storage(orders, header, (U256::ONE << 160) | U256::from(2))
                    .unwrap();
                let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
                let malformed_id = (U256::ONE << 33) | (U256::from(user_id) << 1) | U256::ONE;
                let malformed = U256::ONE
                    | (U256::from(7) << 96)
                    | (U256::from(32_769) << 168)
                    | (U256::ONE << 232);
                db.insert_account_storage(
                    orders,
                    mapping_slot(malformed_id, metadata_seed),
                    malformed,
                )
                .unwrap();
                {
                    let mut context = EthEvmContext::new(db.clone(), Default::default());
                    let mut internals = EvmInternals::from_context(&mut context);
                    let mut reader = JournalReader::new(&mut internals);
                    let mut phases = super::NoopPhaseMeasurer;
                    let mut context = LoaderContext::new(&mut reader, &mut phases);
                    assert_eq!(
                        load_live_order_risk(
                            &mut context,
                            caller,
                            user_id,
                            market_id,
                            book,
                            MarginMode::Cross
                        ),
                        Err(LoaderError::BoundExceeded),
                    );
                }
                let last_funding = I256::unchecked_from(i128::MIN).into_raw() & mask128;
                db.insert_account_storage(
                    caller,
                    checked_slot_offset(account, 1).unwrap(),
                    last_funding | (U256::ONE << 128),
                )
                .unwrap();
                let mut context = EthEvmContext::new(db, Default::default());
                let mut internals = EvmInternals::from_context(&mut context);
                let mut reader = JournalReader::new(&mut internals);
                assert_eq!(
                    load_market_row(&mut reader, caller, &request, market_id, 1),
                    Err(LoaderError::Arithmetic)
                );
                assert!(!reader.ordered_storage_reads().contains(&(orders, registration_slot)));
            }
        }
    }

    #[test]
    fn wrong_deferred_epoch_rejects_ready_looking_cache_and_loads_live_risk() {
        let caller = Address::repeat_byte(0x71);
        let orders = Address::repeat_byte(0x15);
        let market_id = 9_u16;
        let user_id = 102_u32;
        let registry =
            U256::from_be_bytes(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
        let market = perps_market_slot(market_id).unwrap();
        let account = trading_account_slot(market_id, user_id);
        let book = orders_market_book_slot(caller, market_id).unwrap();
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let order_id = U256::from(user_id)
            << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OWNER_BITS_0;
        let mut db = InMemoryDB::default();
        db.insert_account_info(caller, AccountInfo::default());
        db.insert_account_info(orders, contract_account_info());
        install_zero_funding_dependency(&mut db, caller, registry);
        db.insert_account_storage(
            caller,
            checked_slot_offset(
                registry,
                schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET,
            )
            .unwrap(),
            U256::from_be_slice(orders.as_slice()),
        )
        .unwrap();
        db.insert_account_storage(
            caller,
            checked_slot_offset(
                registry,
                schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDER_RISK_ACTIVATION_SEAL_SLOT_OFFSET,
            )
            .unwrap(),
            U256::ONE,
        )
        .unwrap();
        db.insert_account_storage(
            caller,
            checked_slot_offset(
                market,
                schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_DEFERRED_STATE_RECORD_SLOT_OFFSET,
            )
            .unwrap(),
            U256::from(2) << 248,
        )
        .unwrap();
        db.insert_account_storage(
            caller,
            checked_slot_offset(
                market,
                schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_STEP_SIZE_RECORD_SLOT_OFFSET,
            )
            .unwrap(),
            U256::ONE << 96,
        )
        .unwrap();
        db.insert_account_storage(
            caller,
            checked_slot_offset(
                market,
                schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_MAX_LEVERAGE_RECORD_SLOT_OFFSET,
            )
            .unwrap(),
            U256::ONE << 168,
        )
        .unwrap();
        db.insert_account_storage(
            caller,
            checked_slot_offset(account, 1).unwrap(),
            U256::ONE << 128,
        )
        .unwrap();
        let ready_cache =
            (U256::ONE << 255) | (U256::ONE << 240) | (U256::from(999) << 160) | U256::from(777);
        db.insert_account_storage(caller, checked_slot_offset(account, 2).unwrap(), ready_cache)
            .unwrap();
        db.insert_account_storage(
            caller,
            checked_slot_offset(account, 3).unwrap(),
            (U256::ONE << 255) | (U256::ONE << 240),
        )
        .unwrap();
        let open_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED).unwrap();
        db.insert_account_storage(orders, mapping_slot(U256::from(user_id), open_seed), U256::ONE)
            .unwrap();
        db.insert_account_storage(orders, book, U256::ONE).unwrap();
        db.insert_account_storage(
            orders,
            mapping_slot(order_id, metadata_seed),
            U256::from(10) | (U256::from(7) << 96) | (U256::ONE << 168),
        )
        .unwrap();
        db.insert_account_storage(orders, checked_slot_offset(level, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS).unwrap(), U256::ZERO).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let request = Request {
            expected_loader_version: 1,
            expected_operation_set_version: 1,
            user_id,
            target_market_id: market_id,
            expected_loader_schema_hash: B256::ZERO,
            base_balance: U256::ZERO,
            source_policy: U256::ZERO,
            target_mark_price: U256::from(7),
        };
        let (row, _) = load_market_row(&mut reader, caller, &request, market_id, 1).unwrap();
        assert_eq!(row.effective_buy_order_size, U256::from(10));
        assert_eq!(row.effective_order_notional, U256::ZERO);
        assert_ne!(row.effective_buy_order_size, U256::from(999));
    }

    #[test]
    fn approved_cross_ready_compact_fixture_loads_exact_row() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_cross_ready_compact_funding")
            .unwrap();
        let (rows, progress, reads) = load_fixture(case);
        assert_eq!(progress.rows_started, 1);
        assert_eq!(progress.projected_chunks, 0);
        assert_eq!(rows[0].market_id, 1);
        assert_eq!(rows[0].effective_position_size, 10_000_000_000_000_000);
        assert_eq!(rows[0].effective_position_quote, -1_000_000_000_000_000_000_000);
        assert_eq!(
            rows[0].effective_leverage_wad,
            U256::from(50_u8) * U256::from(10_u64).pow(U256::from(18))
        );
        assert_eq!(
            rows[0].mark_price,
            U256::from(100_000_u64) * U256::from(10_u64).pow(U256::from(18))
        );
        let expected_reads = case["orderedJournalReads"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                (
                    item["address"].as_str().unwrap().parse::<Address>().unwrap(),
                    hex_word(item["slot"].as_str().unwrap()),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(reads, expected_reads);
    }

    #[test]
    fn approved_unready_and_wrong_epoch_cases_take_live_empty_fallback() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        for name in [
            "canonical_unready_risk_live_orders_compact_funding",
            "canonical_wrong_epoch_risk_live_fallback",
        ] {
            let case = fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["name"] == name)
                .unwrap();
            let (rows, _, reads) = load_fixture(case);
            assert_eq!(rows.len(), 1, "{name}");
            assert!(rows[0].effective_buy_order_size.is_zero(), "{name}");
            assert!(rows[0].effective_sell_order_size.is_zero(), "{name}");
            assert!(rows[0].effective_order_notional.is_zero(), "{name}");
            let expected = case["orderedJournalReads"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| {
                    (
                        item["address"].as_str().unwrap().parse::<Address>().unwrap(),
                        hex_word(item["slot"].as_str().unwrap()),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(reads, expected, "{name}");
        }
    }

    #[test]
    fn profiled_live_fallback_has_exact_exclusive_phase_chronology() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_wrong_epoch_risk_live_fallback")
            .unwrap();

        let (result, _reads, phases) = load_fixture_profiled(case);
        let (rows, progress) = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(progress.rows_started, 1);
        assert_eq!(
            phase_runs(&phases.events),
            vec![
                (Phase::RowMaterialization, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 2),
                (Phase::KeyDerivation, 2),
                (Phase::JournalLoad, 2),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 3),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 3),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 2),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 2),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 2),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
            ],
            "pin the live-fallback decode, normalization, and read chronology",
        );
        assert_step_clock_durations(&phases, 220, 300, 530);
    }

    #[test]
    fn approved_isolated_leverage_fallback_matches_row_and_read_order() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_isolated_leverage_fallback")
            .unwrap();
        let (rows, _, reads) = load_fixture(case);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].margin_mode, super::MarginMode::Isolated);
        assert_eq!(rows[0].effective_isolated_balance, 20_000_000_000_000_000_000);
        assert_eq!(
            rows[0].effective_leverage_wad,
            U256::from(50_u8) * U256::from(10_u64).pow(U256::from(18))
        );
        assert_eq!(reads, fixture_reads(case));
    }

    #[test]
    fn zero_stored_leverage_reads_market_max_in_ready_and_live_paths() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        for name in [
            "canonical_cross_ready_compact_funding",
            "canonical_unready_risk_live_orders_compact_funding",
        ] {
            let case = fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["name"] == name)
                .unwrap();
            let (caller, mut db, request) = fixture_world(case);
            let market = perps_market_slot(request.target_market_id).unwrap();
            let account = trading_account_slot(request.target_market_id, request.user_id);
            let position_slot = checked_slot_offset(account, 1).unwrap();
            let position = fixture_storage_word(case, caller, position_slot);
            db.insert_account_storage(
                caller,
                position_slot,
                position & !(U256::from(0xff) << 128_usize),
            )
            .unwrap();

            let max_slot = checked_slot_offset(
                market,
                schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_MAX_LEVERAGE_RECORD_SLOT_OFFSET,
            )
            .unwrap();
            db.insert_account_storage(
                caller,
                max_slot,
                U256::from(37)
                    << (schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_CONFIG_MAX_LEVERAGE_BYTE_OFFSET
                        * 8),
            )
            .unwrap();

            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let mut rows = Vec::new();
            load_rows(&mut reader, caller, &request, |row| {
                rows.push(row);
                Ok::<_, ()>(())
            })
            .unwrap();
            let row = rows.iter().find(|row| row.market_id == request.target_market_id).unwrap();
            assert_eq!(
                row.effective_leverage_wad,
                U256::from(37) * U256::from(schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD),
                "{name}",
            );

            let mut expected = fixture_reads(case);
            let funding_slot =
                U256::from_be_bytes(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
            let funding_index =
                expected.iter().position(|read| *read == (caller, funding_slot)).unwrap();
            expected.insert(funding_index, (caller, max_slot));
            assert_eq!(reader.ordered_storage_reads(), expected, "{name}");
        }
    }

    #[test]
    fn active_market_pop_preserves_bucket_boundary_order() {
        let mut rows = Vec::new();
        for (bucket, mut active) in [(0, U256::ONE << 255), (1, U256::ONE), (255, U256::ONE << 255)]
        {
            while let Some(market) = pop_active_market(&mut active, bucket).unwrap() {
                rows.push(market);
            }
        }
        assert_eq!(rows, [255, 256, 65_535]);
    }

    #[test]
    fn profiled_unavailable_exit_has_exact_exclusive_phase_chronology() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_missing_price_snapshot_is_unavailable")
            .unwrap();

        let (caller, db, request) = fixture_world(case);
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut phases = CountingPhases::default();
        let mut progress = LoadProgress::default();
        progress.begin_row().unwrap();
        let result = load_market_row_profiled(
            &mut reader,
            caller,
            &request,
            2,
            1,
            &mut phases,
            &mut progress,
        );
        let journal_reads = reader.journal_reads();
        assert_eq!(result, Err(LoaderError::Unavailable));
        assert_eq!(u64::from(phases.journal_loads), journal_reads);
        assert_eq!(
            (
                phases.events.len(),
                phases.key_derivations,
                phases.journal_loads,
                phase_commitment(&phases.events),
            ),
            (
                24,
                12,
                11,
                "0xe56b56fc89820489427cc9950ee43a96b4373b24d405f3cb80f189788839adae"
                    .parse::<B256>()
                    .unwrap(),
            ),
            "pin all completed work on the late loader error path",
        );
        assert_step_clock_durations(&phases, 120, 110, 240);
    }

    #[test]
    fn approved_empty_snapshot_emits_nothing_and_matches_exact_reads() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_empty_target_inactive")
            .unwrap();
        let (rows, progress, reads) = load_fixture(case);
        assert!(rows.is_empty());
        assert_eq!(progress, LoadProgress::default());
        let expected = case["orderedJournalReads"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                (
                    item["address"].as_str().unwrap().parse::<Address>().unwrap(),
                    hex_word(item["slot"].as_str().unwrap()),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(reads, expected);
    }

    #[test]
    fn active_target_preserves_zero_mark() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_cross_ready_compact_funding")
            .unwrap();
        let (caller, db, mut request) = fixture_world(case);
        request.target_mark_price = U256::ZERO;
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);

        let (row, _) =
            load_market_row(&mut reader, caller, &request, request.target_market_id, 1).unwrap();
        assert_eq!(row.mark_price, U256::ZERO);
    }

    #[test]
    fn localized_snapshot_needs_protocol_id_without_activation_flags() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        let orders: Address = case["addresses"]["ordersManager"].as_str().unwrap().parse().unwrap();
        let hook_root = word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_HOOK_STORAGE_ROOT);
        let last_id_slot = checked_slot_offset(
            hook_root,
            schema::STORAGE_PATHS_ORDERS_HOOK_REGISTRATION_FIELDS_LAST_PROTOCOL_ID_SLOT_OFFSET,
        )
        .unwrap();
        for registered in [true, false] {
            let (caller, mut db, request) = fixture_world(case);
            db.insert_account_storage(orders, last_id_slot, U256::ZERO).unwrap();
            let registration_slot = mapping_slot(U256::from_be_slice(caller.as_slice()), hook_root);
            if !registered {
                let registration = fixture_storage_word(case, orders, registration_slot);
                db.insert_account_storage(
                    orders,
                    registration_slot,
                    registration & ((U256::ONE << 224) - U256::ONE),
                )
                .unwrap();
            }
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let result = super::load_market_row(&mut reader, caller, &request, 2, 1);
            if registered {
                let (row, _) = result.unwrap();
                assert_eq!(row.market_id, 2);
                assert_eq!(
                    row.mark_price,
                    U256::from_str_radix("4505000000000000000000", 10).unwrap()
                );
            } else {
                assert_eq!(result, Err(LoaderError::Unavailable));
            }
            assert!(!reader.ordered_storage_reads().contains(&(orders, last_id_slot)));
        }
    }

    #[test]
    fn market_row_rejects_an_unset_funding_dependency() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_cross_ready_compact_funding")
            .unwrap();
        let (caller, mut db, request) = fixture_world(case);
        let funding_slot = checked_slot_offset(
            U256::from_be_bytes(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_FUNDING_RATE_SLOT_OFFSET,
        )
        .unwrap();
        db.insert_account_storage(caller, funding_slot, U256::ZERO).unwrap();

        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(
            load_market_row(&mut reader, caller, &request, 1, 1),
            Err(LoaderError::Unavailable),
        );
    }

    #[test]
    fn market_row_rejects_a_codeless_funding_dependency() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_cross_ready_compact_funding")
            .unwrap();
        let (caller, mut db, request) = fixture_world(case);
        let funding: Address = case["addresses"]["fundingRate"].as_str().unwrap().parse().unwrap();
        db.insert_account_info(funding, AccountInfo::default());

        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(
            load_market_row(&mut reader, caller, &request, 1, 1),
            Err(LoaderError::Unavailable),
        );
    }

    #[test]
    fn funding_ignores_retired_cutover_and_legacy_words() {
        let caller = Address::repeat_byte(0xc1);
        let funding = Address::repeat_byte(0xf1);
        let registry = word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
        let compact = mapping_slot(
            U256::ONE,
            word(schema::STORAGE_NAMESPACES_FUNDING_RATE_COMPACT_FUNDING_STORAGE_ROOT),
        );
        let legacy =
            mapping_slot(U256::ONE, word(schema::STORAGE_NAMESPACES_FUNDING_RATE_STORAGE_ROOT));
        for retired_cutover in [U256::ZERO, U256::ONE << 224] {
            let mut db = InMemoryDB::default();
            install_zero_funding_dependency(&mut db, caller, registry);
            db.insert_account_storage(funding, compact, U256::from(123)).unwrap();
            db.insert_account_storage(funding, compact + U256::ONE, retired_cutover).unwrap();
            db.insert_account_storage(funding, legacy, U256::from(999)).unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let mut phases = CountingPhases::default();
            assert_eq!(
                load_funding(&mut LoaderContext::new(&mut reader, &mut phases), caller, 1),
                Ok(123),
            );
            assert_eq!(reader.ordered_storage_reads(), &[(caller, registry), (funding, compact)]);
        }
    }

    #[test]
    fn funding_dependency_is_validated_once_across_two_markets() {
        let caller = Address::repeat_byte(0xc1);
        let registry = word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
        let mut db = InMemoryDB::default();
        install_zero_funding_dependency(&mut db, caller, registry);
        let dependency_slot = checked_slot_offset(
            registry,
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_FUNDING_RATE_SLOT_OFFSET,
        )
        .unwrap();

        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut phases = CountingPhases::default();
        {
            let mut context = LoaderContext::new(&mut reader, &mut phases);
            assert_eq!(load_funding(&mut context, caller, 1), Ok(0));
            assert_eq!(load_funding(&mut context, caller, 2), Ok(0));
        }

        assert_eq!(
            reader
                .ordered_storage_reads()
                .iter()
                .filter(|read| **read == (caller, dependency_slot))
                .count(),
            1,
        );
        assert_eq!(phases.key_derivations, 3);
        assert_eq!(phases.journal_loads, 4);
        assert_eq!(
            reader.stats(),
            JournalReadStats { journal_reads: 4, unique_storage_keys: 3, state_access_gas: 8_900 },
        );
    }

    #[test]
    fn localized_snapshot_rejects_an_unset_oracle() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        let (caller, mut db, mut request) = fixture_world(case);
        request.source_policy = U256::ZERO;
        let oracle_slot = checked_slot_offset(
            U256::from_be_bytes(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT),
            schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_RISEX_ORACLE_SLOT_OFFSET,
        )
        .unwrap();
        db.insert_account_storage(caller, oracle_slot, U256::ZERO).unwrap();

        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(
            load_market_row(&mut reader, caller, &request, 2, 1),
            Err(LoaderError::Unavailable),
        );
    }

    #[test]
    fn localized_snapshot_rejects_codeless_dependencies() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        for name in ["risexOracle", "ordersManager"] {
            let (caller, mut db, request) = fixture_world(case);
            let dependency: Address = case["addresses"][name].as_str().unwrap().parse().unwrap();
            db.insert_account_info(dependency, AccountInfo::default());

            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(
                load_market_row(&mut reader, caller, &request, 2, 1),
                Err(LoaderError::Unavailable),
                "accepted codeless dependency {name} at {dependency}",
            );
        }
    }

    #[test]
    fn localized_dependencies_are_validated_once_per_invocation() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        let (caller, db, request) = fixture_world(case);
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut phases = CountingPhases::default();
        {
            let mut context = LoaderContext::new(&mut reader, &mut phases);
            for _ in 0..2 {
                assert_eq!(
                    load_localized_mark(&mut context, caller, 2, request.source_policy),
                    Ok(U256::from_str_radix("4505000000000000000000", 10).unwrap()),
                );
            }
        }

        assert_eq!(reader.ordered_storage_reads().len(), 16);
        assert_eq!(phases.key_derivations, 10);
        assert_eq!(phases.journal_loads, 18);
        assert_eq!(
            reader.stats(),
            JournalReadStats {
                journal_reads: 18,
                unique_storage_keys: 8,
                state_access_gas: 22_800
            },
        );
    }

    #[test]
    fn localized_snapshot_rejects_nonzero_word1_reserved_bits() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        let (caller, mut db, request) = fixture_world(case);
        let oracle: Address = case["addresses"]["risexOracle"].as_str().unwrap().parse().unwrap();
        let [_, word1_slot] = risk_mark_snapshot_slots(2).unwrap();
        let word1 = hex_word("0x00000000000000000e1000000000000000010000000000f4376f1f7caec40000");
        db.insert_account_storage(oracle, word1_slot, word1 | (U256::ONE << 240)).unwrap();

        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(
            load_market_row(&mut reader, caller, &request, 2, 1),
            Err(LoaderError::Unavailable)
        );
    }

    #[test]
    fn localized_snapshot_rejects_each_zero_sentinel_field() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        let oracle: Address = case["addresses"]["risexOracle"].as_str().unwrap().parse().unwrap();
        let [word0_slot, word1_slot] = risk_mark_snapshot_slots(2).unwrap();
        let word0 = hex_word("0x679d72100000000000f3f20b8dfa69d000000000000000000000000000000000");
        let word1 = hex_word("0x00000000000000000e1000000000000000010000000000f4376f1f7caec40000");
        let low112 = (U256::ONE << 112_usize) - U256::ONE;
        let malformed = [
            (word0 & !(low112 << 112_usize), word1),
            (word0 & !(U256::from(u32::MAX) << 224_usize), word1),
            (word0, word1 & !low112),
        ];

        for (word0, word1) in malformed {
            let (caller, mut db, request) = fixture_world(case);
            db.insert_account_storage(oracle, word0_slot, word0).unwrap();
            db.insert_account_storage(oracle, word1_slot, word1).unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(
                load_market_row(&mut reader, caller, &request, 2, 1),
                Err(LoaderError::Unavailable),
            );
        }
    }

    #[test]
    fn localized_snapshot_substitutes_one_sided_empty_impact_with_index() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        let (caller, mut db, request) = fixture_world(case);
        let oracle: Address = case["addresses"]["risexOracle"].as_str().unwrap().parse().unwrap();
        let orders: Address = case["addresses"]["ordersManager"].as_str().unwrap().parse().unwrap();
        let [word0_slot, word1_slot] = risk_mark_snapshot_slots(2).unwrap();
        let word0 = hex_word("0x679d72100000000000f3f20b8dfa69d000000000000000000000000000000000");
        let word1 = hex_word("0x00000000000000000e1000000000000000010000000000f4376f1f7caec40000");
        let low112 = (U256::ONE << 112_usize) - U256::ONE;
        let index = word1 & low112;
        let expected = index + U256::ONE;
        db.insert_account_storage(
            oracle,
            word0_slot,
            (word0 & !(low112 << 112_usize)) | (expected << 112_usize),
        )
        .unwrap();
        db.insert_account_storage(oracle, word1_slot, word1).unwrap();
        let state_slot = U256::from_be_bytes(
            schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_PREFIX,
        ) | U256::from(((1_u64 << 16) | 2) << 1);
        let ask = (U256::ONE << 88_usize) - U256::ONE;
        let state = (ask << 88_usize)
            | (U256::ONE << schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_INITIALIZED_BIT_OFFSET)
            | (U256::from(schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_SUPPORT_SUPPORTED)
                << schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_LOCAL_IMPACT_PRICE_CACHE_PREFIX_STATE_WORD_SUPPORT_BIT_OFFSET);
        db.insert_account_storage(orders, state_slot, state).unwrap();

        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let (row, _) = load_market_row(&mut reader, caller, &request, 2, 1).unwrap();
        assert_eq!(row.mark_price, expected);
    }

    #[test]
    fn localized_snapshot_rejects_mismatched_oracle_dependencies() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_localized_ready_non_target_price")
            .unwrap();
        let oracle: Address = case["addresses"]["risexOracle"].as_str().unwrap().parse().unwrap();

        for slot in [
            schema::STORAGE_PATHS_ORACLE_LEGACY_CONTRACT_STORAGE_FIELDS_S_ORDERS_MANAGER_SLOT,
            schema::STORAGE_PATHS_ORACLE_LEGACY_CONTRACT_STORAGE_FIELDS_S_PERPS_ENGINE_SLOT,
        ] {
            let (caller, mut db, request) = fixture_world(case);
            db.insert_account_storage(oracle, U256::from(slot), U256::from(1)).unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);

            assert_eq!(
                load_market_row(&mut reader, caller, &request, 2, 1),
                Err(LoaderError::Unavailable),
            );
        }
    }

    fn load_fixture(case: &Value) -> (Vec<MarketRow>, LoadProgress, Vec<(Address, U256)>) {
        let (caller, db, request) = fixture_world(case);
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut rows = Vec::new();
        let progress = load_rows(&mut reader, caller, &request, |row| {
            rows.push(row);
            Ok::<_, ()>(())
        })
        .unwrap();
        let reads = reader.ordered_storage_reads().to_vec();
        (rows, progress, reads)
    }

    fn load_fixture_profiled(case: &Value) -> ProfiledFixtureResult {
        let (caller, db, request) = fixture_world(case);
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut rows = Vec::new();
        let mut phases = CountingPhases::default();
        let mut progress = LoadProgress::default();
        let result = load_rows_profiled(
            &mut reader,
            caller,
            &request,
            &mut phases,
            &mut progress,
            |_, row| {
                rows.push(row);
                Ok::<_, ()>(())
            },
        );
        let result = result.map(|()| (rows, progress));
        let journal_reads = reader.journal_reads();
        let reads = reader.ordered_storage_reads().to_vec();
        assert_eq!(u64::from(phases.journal_loads), journal_reads);
        (result, reads, phases)
    }

    fn fixture_world(case: &Value) -> (Address, InMemoryDB, Request) {
        let caller: Address = case["addresses"]["caller"].as_str().unwrap().parse().unwrap();
        let mut db = InMemoryDB::default();
        for item in case["journalState"].as_array().unwrap() {
            let account: Address = item["address"].as_str().unwrap().parse().unwrap();
            db.insert_account_info(account, AccountInfo::default());
            db.insert_account_storage(
                account,
                hex_word(item["slot"].as_str().unwrap()),
                hex_word(item["value"].as_str().unwrap()),
            )
            .unwrap();
        }
        for name in ["fundingRate", "ordersManager", "risexOracle"] {
            let dependency: Address = case["addresses"][name].as_str().unwrap().parse().unwrap();
            if !dependency.is_zero() {
                db.insert_account_info(dependency, contract_account_info());
            }
        }
        let request = Request {
            expected_loader_version: 1,
            expected_operation_set_version: 1,
            user_id: case["request"]["userId"].as_u64().unwrap() as u32,
            target_market_id: case["request"]["targetMarketId"].as_u64().unwrap() as u16,
            expected_loader_schema_hash: B256::ZERO,
            base_balance: U256::ZERO,
            source_policy: case["request"]["sourcePolicy"]
                .as_str()
                .and_then(|value| U256::from_str_radix(value, 10).ok())
                .unwrap_or(U256::from(3_601)),
            target_mark_price: U256::from_str_radix(
                case["request"]["targetMarkPrice"].as_str().unwrap(),
                10,
            )
            .unwrap(),
        };
        (caller, db, request)
    }

    fn install_zero_funding_dependency(db: &mut InMemoryDB, caller: Address, registry: U256) {
        let funding = Address::repeat_byte(0xf1);
        db.insert_account_info(funding, contract_account_info());
        db.insert_account_storage(
            caller,
            checked_slot_offset(
                registry,
                schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_FUNDING_RATE_SLOT_OFFSET,
            )
            .unwrap(),
            U256::from_be_slice(funding.as_slice()),
        )
        .unwrap();
    }

    fn contract_account_info() -> AccountInfo {
        let code = Bytecode::new_raw(Bytes::from_static(&[0x00]));
        AccountInfo { code_hash: code.hash_slow(), code: Some(code), ..Default::default() }
    }

    fn fixture_reads(case: &Value) -> Vec<(Address, U256)> {
        case["orderedJournalReads"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                (
                    item["address"].as_str().unwrap().parse().unwrap(),
                    hex_word(item["slot"].as_str().unwrap()),
                )
            })
            .collect()
    }

    fn fixture_storage_word(case: &Value, address: Address, slot: U256) -> U256 {
        case["journalState"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| {
                item["address"].as_str().unwrap().parse::<Address>().unwrap() == address
                    && hex_word(item["slot"].as_str().unwrap()) == slot
            })
            .map(|item| hex_word(item["value"].as_str().unwrap()))
            .unwrap_or_default()
    }

    fn hex_word(value: &str) -> U256 {
        U256::from_str_radix(value.strip_prefix("0x").unwrap(), 16).unwrap()
    }
}
