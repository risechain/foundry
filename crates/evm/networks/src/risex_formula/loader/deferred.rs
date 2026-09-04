use alloy_primitives::{Address, I256, U256};

#[cfg(test)]
use super::effective_market_v1::NoopPhaseMeasurer;
use super::{
    effective_market_v1::{LoaderContext, LoaderError},
    schema_generated as schema,
};
#[cfg(test)]
use crate::risex_formula::{
    metrics::Phase,
    storage::{JournalReader, orders_market_book_slot},
};
use crate::risex_formula::{
    metrics::PhaseMeasurer,
    storage::{
        checked_slot_offset, extract_signed_bytes, extract_unsigned_bytes, mapping_slot,
        orders_tick_level_slot_from_book, packed_order_id_element,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProjectedChunk {
    pub(crate) claim_size: u128,
    pub(crate) claim_quote_amount: u128,
    pub(crate) packed_order_context: U256,
    pub(crate) funding_snapshot_x128: i128,
    pub(crate) fee_bps: i16,
    pub(crate) side: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProjectedRunDescriptor {
    packed_order_context: U256,
    claim_size: u128,
    claim_quote_amount: u128,
    funding_snapshot_x128: i128,
    match_seq: u32,
    run_index: u8,
    run_ordinal: u16,
    fee_bps: i16,
    side: u8,
}

impl ProjectedRunDescriptor {
    const fn chunk(self) -> ProjectedChunk {
        ProjectedChunk {
            claim_size: self.claim_size,
            claim_quote_amount: self.claim_quote_amount,
            packed_order_context: self.packed_order_context,
            funding_snapshot_x128: self.funding_snapshot_x128,
            fee_bps: self.fee_bps,
            side: self.side,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingCandidate {
    tick: u32,
    seq_id: u16,
    order_id: U256,
    prefix_before: u64,
    pending_steps: u64,
    claimed_steps: u32,
    single_tick_counters: Option<U256>,
}

#[derive(Clone, Copy, Debug)]
struct DirectRun {
    candidate: PendingCandidate,
    cursor_segment: u32,
    cursor_offset: u32,
    segment_tail: u32,
    fee_bps: i16,
    flags: u8,
    side: u8,
    repeats_segment_payload_read: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MarketState {
    pub(crate) size: i128,
    pub(crate) quote: i128,
    pub(crate) last_funding_payment: i128,
    pub(crate) leverage_wad: U256,
    pub(crate) isolated_balance: u128,
    pub(crate) settlement_pnl: I256,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChunkStreamError<E> {
    Loader(LoaderError),
    Sink(E),
}

#[cfg(test)]
pub(crate) fn stream_projected_chunks<E>(
    reader: &mut JournalReader<'_, '_>,
    orders_manager: Address,
    protocol: Address,
    market_id: u16,
    user_id: u32,
    sink: impl FnMut(ProjectedChunk) -> Result<(), E>,
) -> Result<u32, ChunkStreamError<E>> {
    let mut phases = NoopPhaseMeasurer;
    stream_projected_chunks_profiled(
        reader,
        &mut phases,
        orders_manager,
        protocol,
        market_id,
        user_id,
        sink,
    )
}

#[cfg(test)]
fn stream_projected_chunks_profiled<E, M: PhaseMeasurer>(
    reader: &mut JournalReader<'_, '_>,
    phases: &mut M,
    orders_manager: Address,
    protocol: Address,
    market_id: u16,
    user_id: u32,
    sink: impl FnMut(ProjectedChunk) -> Result<(), E>,
) -> Result<u32, ChunkStreamError<E>> {
    phases.measure_excluding(
        Phase::RowMaterialization,
        [Phase::KeyDerivation, Phase::JournalLoad],
        |phases| {
            let mut context = LoaderContext::new(reader, phases);
            let book = context
                .derive(|| orders_market_book_slot(protocol, market_id))
                .map_err(LoaderError::from)
                .map_err(ChunkStreamError::Loader)?;
            stream_projected_chunks_in_context(&mut context, orders_manager, book, user_id, sink)
        },
    )
}

pub(super) fn stream_projected_chunks_in_context<E, M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    orders_manager: Address,
    book: U256,
    user_id: u32,
    mut sink: impl FnMut(ProjectedChunk) -> Result<(), E>,
) -> Result<u32, ChunkStreamError<E>> {
    let candidates = discover_candidates(context, orders_manager, book, user_id)
        .map_err(ChunkStreamError::Loader)?;
    if candidates.is_empty() {
        return Ok(0);
    }
    let maximum = usize::try_from(schema::HARD_BOUNDS_MAX_PROJECTED_FILL_CHUNKS_PER_MARKET_QUERY)
        .map_err(|_| ChunkStreamError::Loader(LoaderError::BoundExceeded))?;
    let mut descriptors = Vec::with_capacity(candidates.len());
    let metadata_seed = context
        .derive(|| {
            checked_slot_offset(
                book,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED,
            )
            .map_err(LoaderError::from)
        })
        .map_err(ChunkStreamError::Loader)?;
    let mut runs = Vec::with_capacity(candidates.len());
    let repeats_segment_payload_read = candidates.len() > 1;
    for candidate in candidates {
        let (counters_slot, meta_slot) = context
            .derive(|| {
                let level = orders_tick_level_slot_from_book(book, candidate.tick)?;
                let counters_slot = checked_slot_offset(
                    level,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS,
                )?;
                Ok::<_, LoaderError>((
                    counters_slot,
                    mapping_slot(candidate.order_id, metadata_seed),
                ))
            })
            .map_err(ChunkStreamError::Loader)?;
        let counters = if let Some(counters) = candidate.single_tick_counters {
            counters
        } else {
            let counters =
                context.sload(orders_manager, counters_slot).map_err(ChunkStreamError::Loader)?;
            let _ =
                context.sload(orders_manager, counters_slot).map_err(ChunkStreamError::Loader)?;
            counters
        };
        let metadata =
            context.sload(orders_manager, meta_slot).map_err(ChunkStreamError::Loader)?;
        if candidate.single_tick_counters.is_none() {
            let _ = context.sload(orders_manager, meta_slot).map_err(ChunkStreamError::Loader)?;
        }
        runs.push(DirectRun {
            candidate,
            cursor_segment: field_u32(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_SEGMENT_HEAD_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_SEGMENT_HEAD_BYTE_WIDTH).map_err(ChunkStreamError::Loader)?,
            cursor_offset: field_u32(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_SEGMENT_OFFSET_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_SEGMENT_OFFSET_BYTE_WIDTH).map_err(ChunkStreamError::Loader)?,
            segment_tail: field_u32(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_SEGMENT_TAIL_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_SEGMENT_TAIL_BYTE_WIDTH).map_err(ChunkStreamError::Loader)?,
            fee_bps: field_i16(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FEE_BPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FEE_BPS_BYTE_WIDTH).map_err(ChunkStreamError::Loader)?,
            flags: field_u8(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FLAGS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FLAGS_BYTE_WIDTH).map_err(ChunkStreamError::Loader)?,
            side: u8::try_from(candidate.order_id & U256::ONE).map_err(|_| ChunkStreamError::Loader(LoaderError::StateLoad))?,
            repeats_segment_payload_read,
        });
    }
    let config = context.sload(orders_manager, book).map_err(ChunkStreamError::Loader)?;
    let step_size = extract_unsigned_bytes(
        config,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_SIZE_BYTE_OFFSET,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_SIZE_BYTE_WIDTH,
    )
    .map_err(LoaderError::from)
    .map_err(ChunkStreamError::Loader)?;
    let step_price = extract_unsigned_bytes(
        config,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_PRICE_BYTE_OFFSET,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_CONFIG_STEP_PRICE_BYTE_WIDTH,
    )
    .map_err(LoaderError::from)
    .map_err(ChunkStreamError::Loader)?;
    for (run_index, run) in runs.iter_mut().enumerate() {
        collect_direct_run(
            context,
            orders_manager,
            book,
            step_size,
            step_price,
            run,
            u8::try_from(run_index)
                .map_err(|_| ChunkStreamError::Loader(LoaderError::BoundExceeded))?,
            maximum,
            &mut descriptors,
        )
        .map_err(ChunkStreamError::Loader)?;
    }
    descriptors.sort_unstable_by_key(|descriptor| {
        (descriptor.match_seq, descriptor.run_index, descriptor.run_ordinal)
    });
    let descriptor_count = descriptors.len();
    for descriptor in descriptors {
        sink(descriptor.chunk()).map_err(ChunkStreamError::Sink)?;
    }
    u32::try_from(descriptor_count)
        .map_err(|_| ChunkStreamError::Loader(LoaderError::BoundExceeded))
}

fn discover_candidates<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    orders: Address,
    book: U256,
    user_id: u32,
) -> Result<Vec<PendingCandidate>, LoaderError> {
    let count_slot = context.derive(|| {
        checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_DIRTY_LEVEL_COUNT,
        )
        .map_err(LoaderError::from)
    })?;
    let count_word = context.sload(orders, count_slot)?;
    let dirty_count = field_u32(
        count_word,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_COUNTERS_DIRTY_LEVEL_COUNT_BYTE_OFFSET,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_COUNTERS_DIRTY_LEVEL_COUNT_BYTE_WIDTH,
    )?;
    if dirty_count == 0 {
        return Ok(Vec::new());
    }
    let single_tick = if dirty_count == 1 {
        let keys_slot = context.derive(|| {
            checked_slot_offset(
                book,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_KEYS,
            )
            .map_err(LoaderError::from)
        })?;
        let length = u64::try_from(context.sload(orders, keys_slot)?)
            .map_err(|_| LoaderError::BoundExceeded)?;
        if length == 0 {
            return Ok(Vec::new());
        }
        let _canonical_length_repeat = context.sload(orders, keys_slot)?;
        let data_slot =
            context.derive(|| crate::risex_formula::storage::dynamic_array_data_slot(keys_slot));
        let packed = context.sload(orders, data_slot)?;
        Some(
            u32::try_from(
                packed
                    & U256::from(
                        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_DIRTY_HEAD_HEAP_TICK_MASK,
                    ),
            )
            .map_err(|_| LoaderError::StateLoad)?,
        )
    } else {
        None
    };
    let open_orders_slot = context.derive(|| {
        let open_seed = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED,
        )?;
        Ok::<_, LoaderError>(mapping_slot(U256::from(user_id), open_seed))
    })?;
    let open_orders = context.sload(orders, open_orders_slot)?;
    if open_orders.is_zero() {
        return Ok(Vec::new());
    }
    let metadata_seed = context.derive(|| {
        checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED,
        )
        .map_err(LoaderError::from)
    })?;
    let mut candidates = Vec::with_capacity(open_orders.count_ones());
    for side in 0_u8..=1 {
        let offset = if side == 0 {
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_BUY_BITMAP_BYTE_OFFSET
        } else {
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_SELL_BITMAP_BYTE_OFFSET
        };
        let mut bitmap = extract_unsigned_bytes(
            open_orders,
            offset,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_BUY_BITMAP_BYTE_WIDTH,
        )?;
        while !bitmap.is_zero() {
            let slot = bitmap.trailing_zeros() as u64;
            bitmap &= bitmap - U256::ONE;
            let order_id = (U256::from(slot)
                << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OPEN_ORDER_SLOT_BITS_0)
                | (U256::from(user_id)
                    << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OWNER_BITS_0)
                | U256::from(side);
            let metadata_slot = context.derive(|| mapping_slot(order_id, metadata_seed));
            let metadata = context.sload(orders, metadata_slot)?;
            let _ = context.sload(orders, metadata_slot)?;
            let _ = context.sload(orders, metadata_slot)?;
            let _ = context.sload(orders, metadata_slot)?;
            let size = field_u32(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_WIDTH)?;
            let filled = field_u32(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_WIDTH)?;
            let seq_id = u16::try_from(extract_unsigned_bytes(
                metadata,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_OFFSET,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_WIDTH,
            )?)
            .map_err(|_| LoaderError::StateLoad)?;
            if u64::from(seq_id) > schema::HARD_BOUNDS_MAX_TICK_LEVEL_SEQ_ID {
                return Err(LoaderError::BoundExceeded);
            }
            let tick = field_u32(
                metadata,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_OFFSET,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_WIDTH,
            )?;
            let claimed_plus_one = field_u32(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_CLAIMED_STEPS_PLUS_ONE_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_CLAIMED_STEPS_PLUS_ONE_BYTE_WIDTH)?;
            if size == 0 {
                continue;
            }
            let queued = queued_steps(size, filled)?;
            if queued == 0 || seq_id == 0 {
                continue;
            }
            let dirty = if let Some(single) = single_tick {
                tick == single
            } else {
                let position_slot = context.derive(|| {
                    let positions_seed = checked_slot_offset(
                        book,
                        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_POS_PLUS_ONE_PACKED,
                    )?;
                    Ok::<_, LoaderError>(mapping_slot(U256::from(tick >> 4), positions_seed))
                })?;
                let packed = context.sload(orders, position_slot)?;
                !extract_unsigned_bits_local(packed, u64::from((tick & 15) * 16), 16)?.is_zero()
            };
            if !dirty {
                continue;
            }
            let (level, counters_slot) = context.derive(|| {
                let level = orders_tick_level_slot_from_book(book, tick)?;
                let counters_slot = checked_slot_offset(
                    level,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS,
                )?;
                Ok::<_, LoaderError>((level, counters_slot))
            })?;
            let counters = context.sload(orders, counters_slot)?;
            if single_tick.is_some() {
                let _canonical_live_claimable_repeat = context.sload(orders, counters_slot)?;
            }
            let claimable = field_u64(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_CLAIMABLE_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_CLAIMABLE_STEPS_BYTE_WIDTH)?;
            let settled = field_u64(counters, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_SETTLED_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_TICK_LEVEL_COUNTERS_TOTAL_SETTLED_STEPS_BYTE_WIDTH)?;
            let live = claimable.checked_sub(settled).ok_or(LoaderError::StateLoad)?;
            let prefix =
                prefix_before_in_context(context, orders, book, level, metadata_seed, seq_id)?;
            let fifo = live.saturating_sub(prefix).min(queued);
            let flags = field_u8(
                metadata,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FLAGS_BYTE_OFFSET,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FLAGS_BYTE_WIDTH,
            )?;
            let claimed = u64::from(decode_claimed_steps(claimed_plus_one, flags));
            let pending = fifo.saturating_sub(claimed);
            if pending != 0 {
                candidates.push(PendingCandidate {
                    tick,
                    seq_id,
                    order_id,
                    prefix_before: prefix,
                    pending_steps: pending,
                    claimed_steps: u32::try_from(claimed).map_err(|_| LoaderError::StateLoad)?,
                    single_tick_counters: single_tick.map(|_| counters),
                });
            }
        }
    }
    candidates
        .sort_unstable_by_key(|candidate| (candidate.tick, candidate.seq_id, candidate.order_id));
    Ok(candidates)
}

#[allow(clippy::too_many_arguments)]
fn collect_direct_run<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    orders: Address,
    book: U256,
    step_size: U256,
    step_price: U256,
    run: &mut DirectRun,
    run_index: u8,
    maximum: usize,
    descriptors: &mut Vec<ProjectedRunDescriptor>,
) -> Result<(), LoaderError> {
    let segment_seed = context.derive(|| {
        let level = orders_tick_level_slot_from_book(book, run.candidate.tick)?;
        checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_FILL_SEGMENT_BY_INDEX,
        )
        .map_err(LoaderError::from)
    })?;
    let mut skip = projected_fill_skip(run.candidate.prefix_before, run.candidate.claimed_steps)?;
    let mut remaining = run.candidate.pending_steps;
    let mut fee_prefix_steps = u64::from(run.candidate.claimed_steps);
    let mut ordinal = 0_u16;
    while skip != 0 || remaining != 0 {
        if run.cursor_segment >= run.segment_tail {
            break;
        }
        let slot = context.derive(|| mapping_slot(U256::from(run.cursor_segment), segment_seed));
        let segment = context.sload(orders, slot)?;
        let segment_steps = field_u32(
            segment,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_SIZE_STEPS_BYTE_OFFSET,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_SIZE_STEPS_BYTE_WIDTH,
        )?;
        let available = segment_steps.saturating_sub(run.cursor_offset);
        if available == 0 {
            run.cursor_segment =
                run.cursor_segment.checked_add(1).ok_or(LoaderError::Arithmetic)?;
            run.cursor_offset = 0;
            continue;
        }
        if skip != 0 {
            let consumed = u32::try_from(skip.min(u64::from(available)))
                .map_err(|_| LoaderError::Arithmetic)?;
            skip -= u64::from(consumed);
            run.cursor_offset =
                run.cursor_offset.checked_add(consumed).ok_or(LoaderError::Arithmetic)?;
            if run.cursor_offset == segment_steps {
                run.cursor_segment =
                    run.cursor_segment.checked_add(1).ok_or(LoaderError::Arithmetic)?;
                run.cursor_offset = 0;
            }
            continue;
        }
        let consumed = u32::try_from(remaining.min(u64::from(available)))
            .map_err(|_| LoaderError::Arithmetic)?;
        run.cursor_offset =
            run.cursor_offset.checked_add(consumed).ok_or(LoaderError::Arithmetic)?;
        if run.cursor_offset == segment_steps {
            run.cursor_segment =
                run.cursor_segment.checked_add(1).ok_or(LoaderError::Arithmetic)?;
            run.cursor_offset = 0;
        }
        let segment_repeat = context.sload(orders, slot)?;
        if run.repeats_segment_payload_read {
            let _ = context.sload(orders, slot)?;
        }
        if descriptors.len() == maximum {
            return Err(LoaderError::BoundExceeded);
        }
        let price = U256::from(run.candidate.tick)
            .checked_mul(step_price)
            .ok_or(LoaderError::Arithmetic)?;
        let size = U256::from(consumed).checked_mul(step_size).ok_or(LoaderError::Arithmetic)?;
        let numerator = size.checked_mul(price).ok_or(LoaderError::Arithmetic)?;
        let wad = U256::from(schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD);
        let quote = if run.side == schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_SELL as u8 {
            numerator.checked_add(wad - U256::ONE).and_then(|value| value.checked_div(wad))
        } else {
            numerator.checked_div(wad)
        }
        .ok_or(LoaderError::Arithmetic)?;
        let replay_price = quote
            .checked_mul(wad)
            .and_then(|value| value.checked_div(size))
            .ok_or(LoaderError::Arithmetic)?;
        let prefix = if run.side == schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_BUY as u8
            && run.fee_bps > 0
        {
            u32::try_from(fee_prefix_steps).map_err(|_| LoaderError::Arithmetic)?
        } else {
            0
        };
        descriptors.push(ProjectedRunDescriptor {
            claim_size: u128::try_from(size).map_err(|_| LoaderError::Arithmetic)?,
            claim_quote_amount: u128::try_from(quote).map_err(|_| LoaderError::Arithmetic)?,
            packed_order_context: replay_price | (U256::from(run.flags) << schema::IMPLEMENTATION_CONSTANTS_PROJECTED_CHUNK_CONTEXT_ORDER_FLAGS_SHIFT) | (U256::from(prefix) << schema::IMPLEMENTATION_CONSTANTS_PROJECTED_CHUNK_CONTEXT_FEE_PREFIX_STEPS_SHIFT),
            funding_snapshot_x128: field_i128(segment_repeat, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_FUNDING_SNAPSHOT_X128_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_FUNDING_SNAPSHOT_X128_BYTE_WIDTH)?,
            match_seq: field_u32(segment_repeat, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_MATCH_SEQ_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_MATCH_SEQ_BYTE_WIDTH)?,
            run_index,
            run_ordinal: ordinal,
            fee_bps: run.fee_bps,
            side: run.side,
        });
        fee_prefix_steps =
            fee_prefix_steps.checked_add(u64::from(consumed)).ok_or(LoaderError::Arithmetic)?;
        ordinal = ordinal.checked_add(1).ok_or(LoaderError::Arithmetic)?;
        remaining -= u64::from(consumed);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn prefix_before(
    reader: &mut JournalReader<'_, '_>,
    orders: Address,
    book: U256,
    level: U256,
    metadata_seed: U256,
    seq_id: u16,
) -> Result<u64, LoaderError> {
    let mut phases = NoopPhaseMeasurer;
    let mut context = LoaderContext::new(reader, &mut phases);
    prefix_before_in_context(&mut context, orders, book, level, metadata_seed, seq_id)
}

pub(super) fn prefix_before_in_context<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    orders: Address,
    book: U256,
    level: U256,
    metadata_seed: U256,
    seq_id: u16,
) -> Result<u64, LoaderError> {
    if seq_id == 0 {
        return Err(LoaderError::StateLoad);
    }
    if u64::from(seq_id) > schema::HARD_BOUNDS_MAX_TICK_LEVEL_SEQ_ID {
        return Err(LoaderError::BoundExceeded);
    }
    let index = u64::from(seq_id - 1);
    if index == 0 {
        return Ok(0);
    }
    let order_ids = context.derive(|| {
        checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_ORDER_IDS,
        )
        .map_err(LoaderError::from)
    })?;
    if index
        <= schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_DIRECT_SCAN_CUTOFF_INCLUSIVE
    {
        // `LibPackedLeafScan.sumQueuedSteps` first caches `ids.length`, then Solidity's
        // storage-array bounds check reads the same length before each packed element.
        let array_length = u64::try_from(context.sload(orders, order_ids)?)
            .map_err(|_| LoaderError::BoundExceeded)?;
        let mut sum = 0_u64;
        for offset in 0..index {
            if offset >= array_length {
                break;
            }
            let _bounds_length = context.sload(orders, order_ids)?;
            let element = context
                .derive(|| packed_order_id_element(order_ids, offset).map_err(LoaderError::from))?;
            let packed = context.sload(orders, element.slot)?;
            let order_id = extract_unsigned_bytes(packed, element.byte_offset, element.byte_width)?;
            if order_id.is_zero() {
                continue;
            }
            let metadata_slot = context.derive(|| mapping_slot(order_id, metadata_seed));
            let metadata = context.sload(orders, metadata_slot)?;
            let size = field_u32(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_WIDTH)?;
            let filled = field_u32(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_WIDTH)?;
            sum = sum.checked_add(queued_steps(size, filled)?).ok_or(LoaderError::Arithmetic)?;
        }
        return Ok(sum);
    }
    let v2 = context.derive(|| {
        checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MEMBER_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let root = context.sload(orders, v2)?;
    let mode = u64::try_from(extract_unsigned_bits_local(
        root,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_1
            - schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0
            + 1,
    )?)
    .map_err(|_| LoaderError::StateLoad)?;
    if mode > schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MODES_CLEARING {
        return Err(LoaderError::StateLoad);
    }
    if root.bit(
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_STP_TAINT_BIT
            as usize,
    ) {
        return Err(LoaderError::Unavailable);
    }
    if matches!(mode, 1 | 2) {
        let idx0 = index - 1;
        let quadrant_size = schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_NODE_COUNT
            / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8192_NODE_COUNT;
        let quadrant = idx0 / quadrant_size;
        let local_index = idx0 % quadrant_size;
        let leaf_word_local = local_index
            / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_NODES_PER_WORD;
        let sum8_word_local = leaf_word_local
            / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8_NODES_PER_WORD;
        let sum56_word_local = sum8_word_local
            / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM56_NODES_PER_WORD;
        let sum336_word_local = sum56_word_local
            / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM336_NODES_PER_WORD;
        let mut sum = sum_packed_fields(
            root,
            0,
            quadrant,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8192_BIT_WIDTH,
        )?;
        if sum336_word_local != 0 {
            let packed = load_offset(
                context,
                orders,
                v2,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM1680_OFFSET
                    + quadrant,
            )?;
            sum = sum
                .checked_add(sum_packed_fields(packed, 0, sum336_word_local, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM1680_BIT_WIDTH)?)
                .ok_or(LoaderError::Arithmetic)?;
        }
        let field_count = sum56_word_local
            % schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM336_NODES_PER_WORD;
        if field_count != 0 {
            let packed = load_offset(
                context,
                orders,
                v2,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM336_OFFSET
                    + quadrant * (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM336_LENGTH_SLOTS / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8192_NODE_COUNT)
                    + sum336_word_local,
            )?;
            sum = sum
                .checked_add(sum_packed_fields(packed, 0, field_count, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM336_BIT_WIDTH)?)
                .ok_or(LoaderError::Arithmetic)?;
        }
        let field_count = sum8_word_local
            % schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM56_NODES_PER_WORD;
        if field_count != 0 {
            let packed = load_offset(
                context,
                orders,
                v2,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM56_OFFSET
                    + quadrant * (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM56_LENGTH_SLOTS / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8192_NODE_COUNT)
                    + sum56_word_local,
            )?;
            sum = sum
                .checked_add(sum_packed_fields(
                    packed,
                    0,
                    field_count,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM56_BIT_WIDTH,
                )?)
                .ok_or(LoaderError::Arithmetic)?;
        }
        let field_count = leaf_word_local
            % schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8_NODES_PER_WORD;
        if field_count != 0 {
            let packed = load_offset(
                context,
                orders,
                v2,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8_OFFSET
                    + quadrant * (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8_LENGTH_SLOTS / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8192_NODE_COUNT)
                    + sum8_word_local,
            )?;
            sum = sum
                .checked_add(sum_packed_fields(
                    packed,
                    0,
                    field_count,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8_BIT_WIDTH,
                )?)
                .ok_or(LoaderError::Arithmetic)?;
        }
        let leaves = load_offset(
            context,
            orders,
            v2,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_OFFSET
                + quadrant * (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_LENGTH_SLOTS / schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_SUM8192_NODE_COUNT)
                + leaf_word_local,
        )?;
        sum = sum
            .checked_add(sum_packed_fields(
                leaves,
                0,
                (idx0
                    % schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_NODES_PER_WORD)
                    + 1,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_BIT_WIDTH,
            )?)
            .ok_or(LoaderError::Arithmetic)?;
        return Ok(sum);
    }
    let legacy = context.derive(|| {
        checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_MEMBER_OFFSET,
        )
        .map_err(LoaderError::from)
    })?;
    let legacy_root = context.sload(orders, legacy)?;
    if legacy_root.bit(
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_STP_TAINT_BIT as usize,
    ) {
        return Err(LoaderError::Unavailable);
    }
    let idx0 = index - 1;
    let level_4096 = idx0 >> 12;
    let mut sum = if level_4096 == 0 {
        0
    } else {
        let base = context.derive(|| {
            checked_slot_offset(
                legacy,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM4096_OFFSET,
            )
            .map_err(LoaderError::from)
        })?;
        sum_storage_packed_range(
            context,
            orders,
            base,
            0,
            level_4096,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM4096_NODES_PER_WORD,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM4096_BIT_WIDTH,
        )?
    };
    let level_256 = idx0 >> 8;
    let start_256 = level_4096 << 4;
    if level_256 > start_256 {
        let base = context.derive(|| {
            checked_slot_offset(
                legacy,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM256_OFFSET,
            )
            .map_err(LoaderError::from)
        })?;
        sum = sum
            .checked_add(sum_storage_packed_range(
                context,
                orders,
                base,
                start_256,
                level_256,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM256_NODES_PER_WORD,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM256_BIT_WIDTH,
            )?)
            .ok_or(LoaderError::Arithmetic)?;
    }
    let level_16 = idx0 >> 4;
    let start_16 = level_256 << 4;
    if level_16 > start_16 {
        let base = context.derive(|| {
            checked_slot_offset(
                legacy,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_OFFSET,
            )
            .map_err(LoaderError::from)
        })?;
        sum = sum
            .checked_add(sum_storage_packed_range(
                context,
                orders,
                base,
                start_16,
                level_16,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_NODES_PER_WORD,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_BIT_WIDTH,
            )?)
            .ok_or(LoaderError::Arithmetic)?;
    }
    let chunk_base = level_16 << 4;
    let array_length =
        u64::try_from(context.sload(orders, order_ids)?).map_err(|_| LoaderError::BoundExceeded)?;
    for offset in chunk_base..=idx0 {
        if offset >= array_length {
            break;
        }
        let _bounds_length = context.sload(orders, order_ids)?;
        let element = context
            .derive(|| packed_order_id_element(order_ids, offset).map_err(LoaderError::from))?;
        let packed = context.sload(orders, element.slot)?;
        let order_id = extract_unsigned_bytes(packed, element.byte_offset, element.byte_width)?;
        if order_id.is_zero() {
            continue;
        }
        let metadata_slot = context.derive(|| mapping_slot(order_id, metadata_seed));
        let metadata = context.sload(orders, metadata_slot)?;
        let size = field_u32(
            metadata,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_OFFSET,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SIZE_STEPS_BYTE_WIDTH,
        )?;
        let filled = field_u32(metadata, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_OFFSET, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FILLED_STEPS_BYTE_WIDTH)?;
        sum = sum.checked_add(queued_steps(size, filled)?).ok_or(LoaderError::Arithmetic)?;
    }
    let _ = book;
    Ok(sum)
}

pub(super) fn queued_steps(size: u32, filled: u32) -> Result<u64, LoaderError> {
    size.checked_sub(filled).map(u64::from).ok_or(LoaderError::Arithmetic)
}

#[allow(clippy::too_many_arguments)]
fn sum_storage_packed_range<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    account: Address,
    base: U256,
    mut start: u64,
    end: u64,
    nodes_per_word: u64,
    bit_width: u64,
) -> Result<u64, LoaderError> {
    let mut sum = 0_u64;
    while start < end {
        let word_index = start / nodes_per_word;
        let mut position = start % nodes_per_word;
        let slot =
            context.derive(|| checked_slot_offset(base, word_index).map_err(LoaderError::from))?;
        let packed = context.sload(account, slot)?;
        while position < nodes_per_word && start < end {
            let value = u64::try_from(extract_unsigned_bits_local(
                packed,
                position * bit_width,
                bit_width,
            )?)
            .map_err(|_| LoaderError::StateLoad)?;
            sum = sum.checked_add(value).ok_or(LoaderError::Arithmetic)?;
            start += 1;
            position += 1;
        }
    }
    Ok(sum)
}

fn load_offset<M: PhaseMeasurer>(
    context: &mut LoaderContext<'_, '_, '_, '_, M>,
    account: Address,
    base: U256,
    offset: u64,
) -> Result<U256, LoaderError> {
    let slot = context.derive(|| checked_slot_offset(base, offset).map_err(LoaderError::from))?;
    context.sload(account, slot)
}

fn sum_packed_fields(word: U256, from: u64, to: u64, width: u64) -> Result<u64, LoaderError> {
    let mut sum = 0_u64;
    for index in from..to {
        let value = u64::try_from(extract_unsigned_bits_local(word, index * width, width)?)
            .map_err(|_| LoaderError::StateLoad)?;
        sum = sum.checked_add(value).ok_or(LoaderError::Arithmetic)?;
    }
    Ok(sum)
}

fn extract_unsigned_bits_local(word: U256, offset: u64, width: u64) -> Result<U256, LoaderError> {
    if width == 0 || offset.checked_add(width).is_none_or(|end| end > 256) {
        return Err(LoaderError::StateLoad);
    }
    Ok((word >> offset) & ((U256::ONE << width) - U256::ONE))
}

fn field_u64(word: U256, offset: u64, width: u64) -> Result<u64, LoaderError> {
    u64::try_from(extract_unsigned_bytes(word, offset, width)?).map_err(|_| LoaderError::StateLoad)
}

fn field_u8(word: U256, offset: u64, width: u64) -> Result<u8, LoaderError> {
    u8::try_from(extract_unsigned_bytes(word, offset, width)?).map_err(|_| LoaderError::StateLoad)
}
fn decode_claimed_steps(claimed_plus_one: u32, flags: u8) -> u32 {
    let terminal = u64::from(flags)
        & schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ORDER_FLAGS_TERMINAL_CLAIMED_STEPS
        != 0;
    if claimed_plus_one == u32::MAX && terminal {
        u32::MAX
    } else {
        claimed_plus_one.saturating_sub(1)
    }
}
fn projected_fill_skip(prefix_before: u64, claimed_steps: u32) -> Result<u64, LoaderError> {
    prefix_before.checked_add(u64::from(claimed_steps)).ok_or(LoaderError::Arithmetic)
}
fn field_u32(word: U256, offset: u64, width: u64) -> Result<u32, LoaderError> {
    u32::try_from(extract_unsigned_bytes(word, offset, width)?).map_err(|_| LoaderError::StateLoad)
}
fn field_i16(word: U256, offset: u64, width: u64) -> Result<i16, LoaderError> {
    i16::try_from(extract_signed_bytes(word, offset, width)?).map_err(|_| LoaderError::StateLoad)
}
fn field_i128(word: U256, offset: u64, width: u64) -> Result<i128, LoaderError> {
    i128::try_from(extract_signed_bytes(word, offset, width)?).map_err(|_| LoaderError::StateLoad)
}

pub(crate) fn replay_chunk(
    state: &mut MarketState,
    chunk: ProjectedChunk,
    margin_mode: u8,
) -> Result<(), LoaderError> {
    if !matches!(chunk.side, 0 | 1) {
        return Err(LoaderError::StateLoad);
    }
    let old_size = state.size;
    let funding_delta = chunk
        .funding_snapshot_x128
        .checked_sub(state.last_funding_payment)
        .ok_or(LoaderError::Arithmetic)?;
    let funding =
        signed_mul_div(old_size, funding_delta, schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD)?;
    let mut quote =
        I256::unchecked_from(state.quote).checked_sub(funding).ok_or(LoaderError::Arithmetic)?;
    state.last_funding_payment = chunk.funding_snapshot_x128;

    let claim_size = i128::try_from(chunk.claim_size).map_err(|_| LoaderError::Arithmetic)?;
    let signed_size = if chunk.side == schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_BUY as u8 {
        claim_size
    } else {
        claim_size.checked_neg().ok_or(LoaderError::Arithmetic)?
    };
    let new_size = old_size.checked_add(signed_size).ok_or(LoaderError::Arithmetic)?;
    let claim_quote = I256::from_raw(U256::from(chunk.claim_quote_amount));
    let fee = signed_u256_mul_div(
        U256::from(chunk.claim_quote_amount),
        i64::from(chunk.fee_bps),
        schema::IMPLEMENTATION_CONSTANTS_FEES_DENOMINATOR,
    )?;
    quote = if chunk.side == schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_BUY as u8 {
        quote.checked_sub(claim_quote).and_then(|value| value.checked_sub(fee))
    } else {
        quote.checked_add(claim_quote).and_then(|value| value.checked_sub(fee))
    }
    .ok_or(LoaderError::Arithmetic)?;

    let price = chunk.packed_order_context
        & ((U256::ONE << schema::IMPLEMENTATION_CONSTANTS_PROJECTED_CHUNK_CONTEXT_PRICE_BIT_WIDTH)
            - U256::ONE);
    let changed_sign =
        old_size != 0 && new_size != 0 && old_size.is_negative() != new_size.is_negative();
    let reduced = old_size != 0 && old_size.is_negative() != signed_size.is_negative();
    let realized = if changed_sign {
        quote
            .checked_add(signed_i128_mul_u256_wad(new_size, price)?)
            .ok_or(LoaderError::Arithmetic)?
    } else if new_size == 0 {
        quote
    } else {
        I256::ZERO
    };

    if margin_mode == schema::IMPLEMENTATION_CONSTANTS_MARGIN_MODE_ISOLATED as u8 {
        if changed_sign {
            state.isolated_balance =
                isolated_reserve(new_size.unsigned_abs(), price, state.leverage_wad)?;
        } else if new_size == 0 {
            state.isolated_balance = 0;
        } else if !reduced {
            let added = isolated_reserve(chunk.claim_size, price, state.leverage_wad)?;
            state.isolated_balance = state
                .isolated_balance
                .checked_add(added)
                .filter(|value| {
                    u64::from(u128::BITS - value.leading_zeros())
                        <= schema::IMPLEMENTATION_CONSTANTS_NUMERIC_WIDTHS_ISOLATED_BALANCE
                })
                .ok_or(LoaderError::Arithmetic)?;
        }
    }
    quote = quote.checked_sub(realized).ok_or(LoaderError::Arithmetic)?;
    state.quote = i128::try_from(quote).map_err(|_| LoaderError::Arithmetic)?;
    state.size = new_size;
    state.settlement_pnl =
        state.settlement_pnl.checked_add(realized).ok_or(LoaderError::Arithmetic)?;
    Ok(())
}

fn signed_mul_div(left: i128, right: i128, denominator: u64) -> Result<I256, LoaderError> {
    I256::unchecked_from(left)
        .checked_mul(I256::unchecked_from(right))
        .and_then(|value| value.checked_div(I256::unchecked_from(denominator)))
        .ok_or(LoaderError::Arithmetic)
}

fn signed_i128_mul_u256_wad(left: i128, right: U256) -> Result<I256, LoaderError> {
    let magnitude = U256::from(left.unsigned_abs())
        .checked_mul(right)
        .and_then(|value| {
            value.checked_div(U256::from(schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD))
        })
        .ok_or(LoaderError::Arithmetic)?;
    let value = I256::from_raw(magnitude);
    if left.is_negative() { value.checked_neg().ok_or(LoaderError::Arithmetic) } else { Ok(value) }
}

fn signed_u256_mul_div(
    value: U256,
    multiplier: i64,
    denominator: u64,
) -> Result<I256, LoaderError> {
    let magnitude = value
        .checked_mul(U256::from(multiplier.unsigned_abs()))
        .and_then(|value| value.checked_div(U256::from(denominator)))
        .ok_or(LoaderError::Arithmetic)?;
    let result = I256::from_raw(magnitude);
    if multiplier.is_negative() {
        result.checked_neg().ok_or(LoaderError::Arithmetic)
    } else {
        Ok(result)
    }
}

fn isolated_reserve(size: u128, price: U256, leverage: U256) -> Result<u128, LoaderError> {
    if leverage.is_zero() {
        return Err(LoaderError::Arithmetic);
    }
    let numerator = U256::from(size).checked_mul(price).ok_or(LoaderError::Arithmetic)?;
    let reserve = numerator
        .checked_add(leverage - U256::ONE)
        .and_then(|value| value.checked_div(leverage))
        .ok_or(LoaderError::Arithmetic)?;
    let reserve = u128::try_from(reserve).map_err(|_| LoaderError::Arithmetic)?;
    if u64::from(u128::BITS - reserve.leading_zeros())
        > schema::IMPLEMENTATION_CONSTANTS_NUMERIC_WIDTHS_ISOLATED_BALANCE
    {
        return Err(LoaderError::Arithmetic);
    }
    Ok(reserve)
}

#[cfg(test)]
mod tests {
    use alloy_evm::{EvmInternals, eth::EthEvmContext};
    use alloy_primitives::{Address, B256, I256, U256, keccak256};
    use revm::{database::InMemoryDB, state::AccountInfo};
    use serde_json::Value;

    use super::{
        ChunkStreamError, DirectRun, LoaderContext, LoaderError, MarketState, PendingCandidate,
        ProjectedChunk, ProjectedRunDescriptor, collect_direct_run, decode_claimed_steps,
        prefix_before, projected_fill_skip, queued_steps, replay_chunk, schema,
        stream_projected_chunks, stream_projected_chunks_profiled,
    };
    use crate::risex_formula::{
        metrics::{Phase, PhaseMeasurer},
        storage::{
            JournalReader, checked_slot_offset, mapping_slot, orders_market_book_slot,
            orders_tick_level_slot_from_book, packed_order_id_element,
        },
    };

    #[derive(Default)]
    struct TracePhases {
        events: Vec<Phase>,
        durations: [u64; 8],
        clock: u64,
    }

    impl TracePhases {
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

    impl PhaseMeasurer for TracePhases {
        fn measure<T>(&mut self, phase: Phase, operation: impl FnOnce() -> T) -> T {
            self.events.push(phase);
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
        phases: &TracePhases,
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
    fn projected_descriptor_arena_has_the_frozen_bound() {
        assert_eq!(std::mem::size_of::<ProjectedRunDescriptor>(), 96);
        assert_eq!(
            std::mem::size_of::<ProjectedRunDescriptor>()
                * schema::HARD_BOUNDS_MAX_PROJECTED_FILL_CHUNKS_PER_MARKET_QUERY as usize,
            786_432
        );
    }

    #[test]
    fn legacy_prefix_modes_route_through_sum_tree_and_leaf_in_exact_order() {
        let orders = Address::repeat_byte(0x15);
        let protocol = Address::repeat_byte(0x71);
        let book = orders_market_book_slot(protocol, 9).unwrap();
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let order_ids = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_ORDER_IDS,
        )
        .unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let v2 = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MEMBER_OFFSET,
        )
        .unwrap();
        let legacy = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_MEMBER_OFFSET,
        )
        .unwrap();
        let legacy_sum16 = checked_slot_offset(
            legacy,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_OFFSET,
        )
        .unwrap();
        let element = packed_order_id_element(order_ids, 16).unwrap();
        let order_id = U256::from(205);
        for mode in [0_u64, 3, 4] {
            let mut db = InMemoryDB::default();
            db.insert_account_info(orders, AccountInfo::default());
            db.insert_account_storage(orders, v2, U256::from(mode) << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0).unwrap();
            db.insert_account_storage(orders, legacy_sum16, U256::from(10)).unwrap();
            db.insert_account_storage(orders, order_ids, U256::from(18)).unwrap();
            db.insert_account_storage(orders, element.slot, order_id << (element.byte_offset * 8))
                .unwrap();
            db.insert_account_storage(
                orders,
                mapping_slot(order_id, metadata_seed),
                pack_metadata(5, 0, 0, 7, 17, 1, 0),
            )
            .unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(prefix_before(&mut reader, orders, book, level, metadata_seed, 18), Ok(15));
            assert_eq!(
                reader.ordered_storage_reads(),
                &[
                    (orders, v2),
                    (orders, legacy),
                    (orders, legacy_sum16),
                    (orders, order_ids),
                    (orders, order_ids),
                    (orders, element.slot),
                    (orders, mapping_slot(order_id, metadata_seed)),
                ]
            );
        }
        let leaves = checked_slot_offset(
            v2,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_OFFSET,
        )
        .unwrap();
        let packed_leaves = (1_u64..=5).enumerate().fold(U256::ZERO, |word, (index, value)| {
            word | (U256::from(value)
                << (index as u64
                    * schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_BIT_WIDTH))
        });
        for mode in [1_u64, 2] {
            let mut db = InMemoryDB::default();
            db.insert_account_info(orders, AccountInfo::default());
            db.insert_account_storage(orders, v2, U256::from(mode) << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0).unwrap();
            db.insert_account_storage(orders, leaves, packed_leaves).unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(prefix_before(&mut reader, orders, book, level, metadata_seed, 6), Ok(15));
            assert_eq!(reader.ordered_storage_reads(), &[(orders, v2), (orders, leaves)]);
        }
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        db.insert_account_storage(orders, v2, U256::from(5) << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(
            prefix_before(&mut reader, orders, book, level, metadata_seed, 18),
            Err(LoaderError::StateLoad)
        );
        assert_eq!(reader.ordered_storage_reads(), &[(orders, v2)]);

        for (v2_state, legacy_state, reads) in [
            (
                (U256::ONE << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0)
                    | (U256::ONE << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_STP_TAINT_BIT),
                U256::ZERO,
                1,
            ),
            (
                U256::ZERO,
                U256::ONE
                    << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_STP_TAINT_BIT,
                2,
            ),
        ] {
            let mut db = InMemoryDB::default();
            db.insert_account_info(orders, AccountInfo::default());
            db.insert_account_storage(orders, v2, v2_state).unwrap();
            db.insert_account_storage(orders, legacy, legacy_state).unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(
                prefix_before(&mut reader, orders, book, level, metadata_seed, 18),
                Err(LoaderError::Unavailable)
            );
            assert_eq!(reader.ordered_storage_reads().len(), reads);
        }
    }

    #[test]
    fn queued_steps_is_checked_once_for_every_order_metadata_consumer() {
        assert_eq!(queued_steps(5, 2), Ok(3));
        assert_eq!(queued_steps(2, 2), Ok(0));
        assert_eq!(queued_steps(1, 2), Err(LoaderError::Arithmetic));
    }

    #[test]
    fn prefix_sequence_bounds_are_typed_before_reads_and_maximum_is_supported() {
        let orders = Address::repeat_byte(0x15);
        let book = orders_market_book_slot(Address::repeat_byte(0x71), 9).unwrap();
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        let v2 = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MEMBER_OFFSET,
        )
        .unwrap();
        db.insert_account_storage(orders, v2, U256::ONE << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_ROOT_STATE_MODE_BITS_0).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(
            prefix_before(&mut reader, orders, book, level, metadata_seed, 0),
            Err(LoaderError::StateLoad)
        );
        assert!(reader.ordered_storage_reads().is_empty());
        assert_eq!(
            prefix_before(&mut reader, orders, book, level, metadata_seed, 32_769),
            Err(LoaderError::BoundExceeded)
        );
        assert!(reader.ordered_storage_reads().is_empty());
        assert_eq!(prefix_before(&mut reader, orders, book, level, metadata_seed, 32_768), Ok(0));
        assert!(reader.ordered_storage_reads().len() <= 6);
    }

    #[test]
    fn projected_discovery_rejects_sequence_above_capacity_before_prefix_or_sink() {
        let orders = Address::repeat_byte(0x15);
        let protocol = Address::repeat_byte(0x71);
        let market_id = 9_u16;
        let user_id = 102_u32;
        let book = orders_market_book_slot(protocol, market_id).unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let order_id = U256::from(205);
        let keys = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_KEYS).unwrap();
        let open_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED).unwrap();
        for sequence in [32_769_u16, u16::MAX] {
            let mut db = InMemoryDB::default();
            db.insert_account_info(orders, AccountInfo::default());
            db.insert_account_storage(orders, checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_DIRTY_LEVEL_COUNT).unwrap(), U256::ONE << 32).unwrap();
            db.insert_account_storage(orders, keys, U256::ONE).unwrap();
            db.insert_account_storage(
                orders,
                crate::risex_formula::storage::dynamic_array_data_slot(keys),
                (U256::ONE << 32) | U256::from(7),
            )
            .unwrap();
            db.insert_account_storage(
                orders,
                mapping_slot(U256::from(user_id), open_seed),
                U256::ONE << 128,
            )
            .unwrap();
            db.insert_account_storage(
                orders,
                mapping_slot(order_id, metadata_seed),
                pack_metadata(1, 0, 0, 7, sequence, 1, 0),
            )
            .unwrap();
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let mut sink_count = 0;
            assert_eq!(
                stream_projected_chunks(&mut reader, orders, protocol, market_id, user_id, |_| {
                    sink_count += 1;
                    Ok::<_, ()>(())
                }),
                Err(ChunkStreamError::Loader(LoaderError::BoundExceeded))
            );
            assert_eq!(sink_count, 0);
            assert_eq!(reader.ordered_storage_reads().len(), 9);
        }
    }

    #[test]
    fn direct_and_legacy_leaf_scans_stop_at_logical_array_length() {
        let orders = Address::repeat_byte(0x15);
        let book = orders_market_book_slot(Address::repeat_byte(0x71), 9).unwrap();
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let order_ids = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_ORDER_IDS,
        )
        .unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let v2 = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MEMBER_OFFSET,
        )
        .unwrap();
        let legacy = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_MEMBER_OFFSET,
        )
        .unwrap();
        let legacy_sum16 = checked_slot_offset(
            legacy,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_OFFSET,
        )
        .unwrap();
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        db.insert_account_storage(orders, order_ids, U256::from(2)).unwrap();
        let packed_slot = packed_order_id_element(order_ids, 0).unwrap().slot;
        let mut packed_word = U256::ZERO;
        for index in 0_u64..5 {
            let element = packed_order_id_element(order_ids, index).unwrap();
            let order_id = U256::from(205 + index * 2);
            assert_eq!(element.slot, packed_slot);
            packed_word |= order_id << (element.byte_offset * 8);
            db.insert_account_storage(
                orders,
                mapping_slot(order_id, metadata_seed),
                pack_metadata(5, 0, 0, 7, (index + 1) as u16, 1, 0),
            )
            .unwrap();
        }
        db.insert_account_storage(orders, packed_slot, packed_word).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(prefix_before(&mut reader, orders, book, level, metadata_seed, 6), Ok(10));
        assert_eq!(reader.ordered_storage_reads().len(), 9);

        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        db.insert_account_storage(orders, v2, U256::ZERO).unwrap();
        db.insert_account_storage(orders, legacy_sum16, U256::from(10)).unwrap();
        db.insert_account_storage(orders, order_ids, U256::from(16)).unwrap();
        let stale = packed_order_id_element(order_ids, 16).unwrap();
        db.insert_account_storage(orders, stale.slot, U256::from(205) << (stale.byte_offset * 8))
            .unwrap();
        db.insert_account_storage(
            orders,
            mapping_slot(U256::from(205), metadata_seed),
            pack_metadata(99, 0, 0, 7, 17, 1, 0),
        )
        .unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(prefix_before(&mut reader, orders, book, level, metadata_seed, 18), Ok(10));
        assert_eq!(
            reader.ordered_storage_reads(),
            &[(orders, v2), (orders, legacy), (orders, legacy_sum16), (orders, order_ids)]
        );
    }

    #[test]
    fn legacy_prefix_boundaries_match_canonical_chunk_transitions() {
        let orders = Address::repeat_byte(0x15);
        let book = orders_market_book_slot(Address::repeat_byte(0x71), 9).unwrap();
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let order_ids = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_ORDER_IDS,
        )
        .unwrap();
        let metadata_seed = checked_slot_offset(book, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED).unwrap();
        let legacy = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_MEMBER_OFFSET,
        )
        .unwrap();
        let sum256 = checked_slot_offset(
            legacy,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM256_OFFSET,
        )
        .unwrap();
        let sum16 = checked_slot_offset(
            legacy,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_OFFSET,
        )
        .unwrap();
        let packed_256 = repeated_one_fields(schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM256_NODES_PER_WORD, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM256_BIT_WIDTH);
        let packed_16 = repeated_one_fields(schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_NODES_PER_WORD, schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_LEGACY_SUM16_BIT_WIDTH);
        for (seq, expected, reads) in [
            (255_u16, 15_u64, 6_usize),
            (256, 15, 6),
            (257, 15, 6),
            (4_095, 30, 9),
            (4_096, 30, 9),
            (4_097, 30, 9),
        ] {
            let mut db = InMemoryDB::default();
            db.insert_account_info(orders, AccountInfo::default());
            db.insert_account_storage(orders, order_ids, U256::ZERO).unwrap();
            for word_index in 0_u64..40 {
                db.insert_account_storage(
                    orders,
                    checked_slot_offset(sum16, word_index).unwrap(),
                    packed_16,
                )
                .unwrap();
            }
            for word_index in 0_u64..3 {
                db.insert_account_storage(
                    orders,
                    checked_slot_offset(sum256, word_index).unwrap(),
                    packed_256,
                )
                .unwrap();
            }
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(
                prefix_before(&mut reader, orders, book, level, metadata_seed, seq),
                Ok(expected),
                "seq={seq}"
            );
            assert_eq!(reader.ordered_storage_reads().len(), reads, "seq={seq}");
        }
    }

    fn repeated_one_fields(count: u64, width: u64) -> U256 {
        (0..count).fold(U256::ZERO, |word, index| word | (U256::ONE << (index * width)))
    }

    #[test]
    fn approved_equal_sequence_corruption_uses_sorted_run_index() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let proof = &fixture["projectedDiscoveryMergeProofs"]["equalSequenceCorruptionParity"];
        assert_eq!(proof["canonicallyReachable"], false);
        assert_eq!(proof["changedTieOrderRejected"], true);
        let mut descriptors = [
            descriptor_with_quote(9_000_000_000_000_000_000, 1, 1),
            descriptor_with_quote(7_000_000_000_000_000_000, 1, 0),
        ];
        descriptors.sort_unstable_by_key(|descriptor| {
            (descriptor.match_seq, descriptor.run_index, descriptor.run_ordinal)
        });
        let expected = proof["expectedQuoteAmounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().parse::<u128>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor.chunk().claim_quote_amount)
                .collect::<Vec<_>>(),
            expected
        );
    }

    fn descriptor_with_quote(
        claim_quote_amount: u128,
        match_seq: u32,
        run_index: u8,
    ) -> ProjectedRunDescriptor {
        ProjectedRunDescriptor {
            claim_size: 1,
            claim_quote_amount,
            packed_order_context: U256::ZERO,
            funding_snapshot_x128: 0,
            match_seq,
            run_index,
            run_ordinal: 0,
            fee_bps: 0,
            side: 0,
        }
    }

    #[test]
    fn approved_full_close_then_flip_vector_replays_in_canonical_order() {
        let mut state = MarketState {
            size: 1_000_000_000_000_000_000,
            quote: -7_000_000_000_000_000_000,
            last_funding_payment: 0,
            leverage_wad: U256::from(1_000_000_000_000_000_000_u128),
            isolated_balance: 0,
            settlement_pnl: I256::ZERO,
        };
        for (claim_size, funding) in
            [(2_000_000_000_000_000_000_u128, 3_i128), (1_000_000_000_000_000_000_u128, 5_i128)]
        {
            replay_chunk(
                &mut state,
                ProjectedChunk {
                    claim_size,
                    claim_quote_amount: claim_size * 7,
                    packed_order_context: U256::from(7_000_000_000_000_000_000_u128)
                        | (U256::from(64_u8) << 128),
                    funding_snapshot_x128: funding,
                    fee_bps: 1,
                    side: 1,
                },
                0,
            )
            .unwrap();
        }

        assert_eq!(state.size, -2_000_000_000_000_000_000);
        assert_eq!(state.quote, 13_999_993_000_000_000_002);
        assert_eq!(state.last_funding_payment, 5);
        assert_eq!(state.settlement_pnl, I256::try_from(-14_000_000_000_003_i64).unwrap());
    }

    #[test]
    fn approved_compact_runs_execute_actual_8192_limit_and_8193_failure() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        for (name, succeeds) in [
            ("production_projected_chunks_exactly_8192", true),
            ("production_projected_chunks_first_disallowed_8193", false),
        ] {
            let case = fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["name"] == name)
                .unwrap();
            execute_compact_case(case, succeeds);
        }
    }

    #[test]
    fn approved_multitick_world_merges_7_9_7_9() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let proof = &fixture["projectedDiscoveryMergeProofs"]["literalInterleaving"];
        let orders: Address =
            proof["context"]["addresses"]["ordersManager"].as_str().unwrap().parse().unwrap();
        let protocol: Address =
            proof["context"]["addresses"]["protocol"].as_str().unwrap().parse().unwrap();
        let market_id = proof["context"]["request"]["marketId"].as_u64().unwrap() as u16;
        let user_id = proof["context"]["request"]["userId"].as_u64().unwrap() as u32;
        let state = &proof["state"];
        let book = word(state["bookSlot"].as_str().unwrap());
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        db.insert_account_storage(orders, book, word(state["configWord"].as_str().unwrap()))
            .unwrap();
        db.insert_account_storage(
            orders,
            book + U256::ONE,
            word(state["dirtyState"]["countWord"].as_str().unwrap()),
        )
        .unwrap();
        db.insert_account_storage(
            orders,
            word(state["openOrders"]["slot"].as_str().unwrap()),
            word(state["openOrders"]["packedWord"].as_str().unwrap()),
        )
        .unwrap();
        let positions_seed = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_POS_PLUS_ONE_PACKED,
            );
        db.insert_account_storage(
            orders,
            mapping_slot(U256::ZERO, positions_seed),
            word(state["dirtyState"]["packedPositionsWord"].as_str().unwrap()),
        )
        .unwrap();
        let keys_slot = book + U256::from(schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_KEYS);
        db.insert_account_storage(
            orders,
            keys_slot,
            U256::from(state["dirtyState"]["heapLength"].as_u64().unwrap()),
        )
        .unwrap();
        db.insert_account_storage(
            orders,
            crate::risex_formula::storage::dynamic_array_data_slot(keys_slot),
            word(state["dirtyState"]["packedKeysWord"].as_str().unwrap()),
        )
        .unwrap();
        let metadata_seed = book + U256::from(schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED);
        for order in state["referencedOrders"].as_array().unwrap() {
            let order_id = U256::from_str_radix(order["orderId"].as_str().unwrap(), 10).unwrap();
            db.insert_account_storage(
                orders,
                mapping_slot(order_id, metadata_seed),
                word(order["storageWord"].as_str().unwrap()),
            )
            .unwrap();
        }
        for level in state["levels"].as_array().unwrap() {
            let tick = level["tick"].as_u64().unwrap();
            let level_slot = crate::risex_formula::storage::orders_tick_level_slot(
                protocol,
                market_id,
                tick as u32,
            )
            .unwrap();
            for prefix in level["prefixWords"].as_array().unwrap() {
                db.insert_account_storage(
                    orders,
                    word(prefix["slot"].as_str().unwrap()),
                    word(prefix["value"].as_str().unwrap()),
                )
                .unwrap();
            }
            let ids = level["orderIds"].as_array().unwrap();
            db.insert_account_storage(orders, level_slot, U256::from(ids.len())).unwrap();
            let ids_root = crate::risex_formula::storage::dynamic_array_data_slot(level_slot);
            let mut packed_ids = U256::ZERO;
            for (index, id) in ids.iter().enumerate() {
                packed_ids |=
                    U256::from_str_radix(id.as_str().unwrap(), 10).unwrap() << (index * 40);
            }
            db.insert_account_storage(orders, ids_root, packed_ids).unwrap();
            let counters_slot = level_slot + U256::from(schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS);
            let counters = U256::from(level["liveOrders"].as_u64().unwrap())
                | (U256::from_str_radix(level["totalClaimableSteps"].as_str().unwrap(), 10)
                    .unwrap()
                    << 16)
                | (U256::from_str_radix(level["totalSettledSteps"].as_str().unwrap(), 10).unwrap()
                    << 80)
                | (U256::from(level["segmentHead"].as_u64().unwrap()) << 144)
                | (U256::from(level["segmentTail"].as_u64().unwrap()) << 176)
                | (U256::from(level["segmentOffset"].as_u64().unwrap()) << 208);
            db.insert_account_storage(orders, counters_slot, counters).unwrap();
            let segment_seed = level_slot + U256::from(schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_FILL_SEGMENT_BY_INDEX);
            for segment in level["fillSegments"].as_array().unwrap() {
                let run = serde_json::json!({
                    "sizeSteps": segment["sizeSteps"],
                    "matchSeqStart": segment["matchSeq"],
                    "fundingSnapshotX128": segment["fundingSnapshotX128"],
                    "takerFeeBps": segment["takerFeeBps"],
                });
                let index = segment["index"].as_u64().unwrap();
                db.insert_account_storage(
                    orders,
                    mapping_slot(U256::from(index), segment_seed),
                    pack_segment(&run, 0),
                )
                .unwrap();
            }
        }
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut chunks = Vec::new();
        let mut phases = TracePhases::default();
        let count = stream_projected_chunks_profiled(
            &mut reader,
            &mut phases,
            orders,
            protocol,
            market_id,
            user_id,
            |chunk| {
                chunks.push(chunk);
                Ok::<_, ()>(())
            },
        )
        .unwrap();
        assert_eq!(count, 4);
        assert_eq!(
            chunks.iter().map(|chunk| chunk.claim_quote_amount).collect::<Vec<_>>(),
            [7, 9, 7, 9].map(|value| value * 1_000_000_000_000_000_000)
        );
        let expected_reads = proof["orderedReads"]["slots"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .map(|slot| (orders, word(slot.as_str().unwrap())))
            .collect::<Vec<_>>();
        let actual_reads = reader.ordered_storage_reads();
        for (index, (actual, expected)) in actual_reads.iter().zip(&expected_reads).enumerate() {
            assert_eq!(actual, expected, "read index {index}");
        }
        assert_eq!(actual_reads.len(), expected_reads.len(), "read count");
        assert_eq!(
            phase_runs(&phases.events),
            vec![
                (Phase::RowMaterialization, 1),
                (Phase::KeyDerivation, 2),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 2),
                (Phase::JournalLoad, 4),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 4),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 2),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 2),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 4),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 2),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 2),
                (Phase::JournalLoad, 4),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 5),
                (Phase::KeyDerivation, 2),
                (Phase::JournalLoad, 3),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 3),
                (Phase::KeyDerivation, 2),
                (Phase::JournalLoad, 1),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 3),
                (Phase::KeyDerivation, 1),
                (Phase::JournalLoad, 3),
            ],
            "pin the exact exhaustive and exclusive loader phase chronology",
        );
        assert_eq!(
            phases.events.iter().filter(|phase| **phase == Phase::JournalLoad).count(),
            actual_reads.len(),
            "every journal access must cross the profiled loader seam exactly once",
        );
        assert_step_clock_durations(&phases, 310, 510, 830);
    }

    #[test]
    fn approved_compact_exact_256_reconstructs_full_discovery() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let proof = &fixture["projectedDiscoveryMergeProofs"];
        let generation = &proof["exactRunBoundary"]["generation"];
        let literal = &proof["literalInterleaving"];
        let orders: Address =
            literal["context"]["addresses"]["ordersManager"].as_str().unwrap().parse().unwrap();
        let protocol: Address =
            literal["context"]["addresses"]["protocol"].as_str().unwrap().parse().unwrap();
        let market_id = generation["marketId"].as_u64().unwrap() as u16;
        let user_id = generation["userId"].as_u64().unwrap() as u32;
        let book =
            crate::risex_formula::storage::orders_market_book_slot(protocol, market_id).unwrap();
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        let step_size = U256::from_str_radix(generation["stepSize"].as_str().unwrap(), 10).unwrap();
        let step_price =
            U256::from_str_radix(generation["stepPrice"].as_str().unwrap(), 10).unwrap();
        db.insert_account_storage(
            orders,
            book,
            step_size | (step_price << 64) | (U256::ONE << 128),
        )
        .unwrap();
        db.insert_account_storage(orders, book + U256::ONE, U256::from(256_u64) << 32).unwrap();
        let open_seed = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED,
            );
        db.insert_account_storage(orders, mapping_slot(U256::from(user_id), open_seed), U256::MAX)
            .unwrap();
        let metadata_seed = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED,
            );
        let positions_seed = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_POS_PLUS_ONE_PACKED,
            );
        let mut position_words = std::collections::BTreeMap::<u32, U256>::new();
        let mut expected_quotes = Vec::with_capacity(256);
        for index in 0_u32..128 {
            for (side, spec, sequence) in [
                (0_u8, &generation["buy"], index * 2 + 1),
                (1_u8, &generation["sell"], index * 2 + 2),
            ] {
                let tick = spec["tickStart"].as_u64().unwrap() as u32 + index;
                let slot = u64::from(index);
                let order_id = (U256::from(slot)
                    << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OPEN_ORDER_SLOT_BITS_0)
                    | (U256::from(user_id)
                        << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OWNER_BITS_0)
                    | U256::from(side);
                let fee = U256::from(spec["feeBps"].as_u64().unwrap());
                let metadata = U256::ONE
                    | (fee
                        << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FEE_BPS_BYTE_OFFSET * 8))
                    | (U256::from(tick)
                        << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_OFFSET * 8))
                    | (U256::ONE
                        << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_OFFSET * 8))
                    | (U256::ONE
                        << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_CLAIMED_STEPS_PLUS_ONE_BYTE_OFFSET * 8))
                    | (U256::from(64_u8)
                        << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FLAGS_BYTE_OFFSET * 8));
                db.insert_account_storage(orders, mapping_slot(order_id, metadata_seed), metadata)
                    .unwrap();
                *position_words.entry(tick >> 4).or_default() |= U256::ONE << ((tick & 15) * 16);
                let level = crate::risex_formula::storage::orders_tick_level_slot(
                    protocol, market_id, tick,
                )
                .unwrap();
                let counters = U256::ONE | (U256::ONE << 16) | (U256::ONE << 176);
                db.insert_account_storage(
                    orders,
                    level
                        + U256::from(
                            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS,
                        ),
                    counters,
                )
                .unwrap();
                let segment_seed = level
                    + U256::from(
                        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_FILL_SEGMENT_BY_INDEX,
                    );
                let run = serde_json::json!({
                    "sizeSteps": 1,
                    "matchSeqStart": sequence,
                    "fundingSnapshotX128": "0",
                    "takerFeeBps": 0,
                });
                db.insert_account_storage(
                    orders,
                    mapping_slot(U256::ZERO, segment_seed),
                    pack_segment(&run, 0),
                )
                .unwrap();
                expected_quotes.push(U256::from(tick).checked_mul(step_size).unwrap());
            }
        }
        for (bucket, packed) in position_words {
            db.insert_account_storage(
                orders,
                mapping_slot(U256::from(bucket), positions_seed),
                packed,
            )
            .unwrap();
        }
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut commitment = B256::ZERO;
        let domain = keccak256("risex.effective_market_v1.projected_chunk.v1");
        let mut count = 0_u64;
        let loaded =
            stream_projected_chunks(&mut reader, orders, protocol, market_id, user_id, |chunk| {
                assert_eq!(U256::from(chunk.claim_quote_amount), expected_quotes[count as usize]);
                commitment = keccak_words_b256(domain, commitment, count, chunk_hash(chunk));
                count += 1;
                Ok::<_, ()>(())
            })
            .unwrap();
        assert_eq!(loaded, 256);
        assert_eq!(count, 256);
        assert_eq!(
            format!("{commitment:#x}"),
            proof["exactRunBoundary"]["expectedResult"]["chunkCommitment"].as_str().unwrap()
        );
        assert_eq!(reader.ordered_storage_reads().len(), 3_331);
        let read_domain = keccak256("risex.effective_market_v1.journal_read.v1");
        let mut read_commitment = B256::ZERO;
        for (index, (account, slot)) in reader.ordered_storage_reads().iter().enumerate() {
            let item = keccak_words(&[U256::from_be_slice(account.as_slice()), *slot]);
            read_commitment = keccak_words_b256(read_domain, read_commitment, index as u64, item);
        }
        assert_eq!(
            format!("{read_commitment:#x}"),
            proof["exactRunBoundary"]["expectedResult"]["orderedReadCommitment"].as_str().unwrap()
        );
        assert_eq!(proof["exactRunBoundary"]["structural257Proof"]["runtimeStateFor257"], false);
        assert_eq!(proof["exactRunBoundary"]["structural257Proof"]["unrepresentableRunCount"], 257);
    }

    #[test]
    fn approved_malformed_discovery_preserves_nonvalidating_parity() {
        let fixture: Value =
            serde_json::from_slice(include_bytes!("../testdata/effective-market-v1.json")).unwrap();
        let proof = &fixture["projectedDiscoveryMergeProofs"];
        let malformed = &proof["malformed"];
        let literal = &proof["literalInterleaving"];
        let orders: Address =
            literal["context"]["addresses"]["ordersManager"].as_str().unwrap().parse().unwrap();
        let protocol: Address =
            literal["context"]["addresses"]["protocol"].as_str().unwrap().parse().unwrap();
        let market_id = literal["context"]["request"]["marketId"].as_u64().unwrap() as u16;
        let user_id = literal["context"]["request"]["userId"].as_u64().unwrap() as u32;

        for case in malformed["dirtyHeadHeapCountOrLength"]["cases"].as_array().unwrap() {
            let dirty_count = case["mutation"]["dirtyCount"].as_u64().unwrap() as u32;
            let heap_length = case["mutation"]["heapLength"].as_u64().unwrap();
            let db = seed_single_projection_world(
                orders,
                protocol,
                market_id,
                user_id,
                dirty_count,
                heap_length,
                1,
                None,
            );
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let mut chunks = Vec::new();
            stream_projected_chunks(&mut reader, orders, protocol, market_id, user_id, |chunk| {
                chunks.push(chunk);
                Ok::<_, ()>(())
            })
            .unwrap();
            assert_eq!(chunks.len() as u64, case["projectedChunks"]["count"].as_u64().unwrap());
            assert_exact_semantic_reads(&reader, orders, &case["orderedReads"]);
        }

        for (record, mode, succeeds) in [
            (&malformed["invalidV2Mode"], U256::from(5_u64) << 180, false),
            (
                &malformed["corruptedV2CursorParity"],
                (U256::ONE << 180) | (U256::from(8191_u64) << 183),
                true,
            ),
        ] {
            let db = seed_single_projection_world(
                orders,
                protocol,
                market_id,
                user_id,
                1,
                1,
                6,
                Some(mode),
            );
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = EvmInternals::from_context(&mut context);
            let mut reader = JournalReader::new(&mut internals);
            let mut chunks = Vec::new();
            let mut phases = TracePhases::default();
            let result = stream_projected_chunks_profiled(
                &mut reader,
                &mut phases,
                orders,
                protocol,
                market_id,
                user_id,
                |chunk| {
                    chunks.push(chunk);
                    Ok::<_, ()>(())
                },
            );
            let journal_events =
                phases.events.iter().filter(|phase| **phase == Phase::JournalLoad).count();
            assert_eq!(journal_events, reader.ordered_storage_reads().len());
            if succeeds {
                assert_eq!(result.unwrap(), 1);
                assert_eq!(chunks[0].claim_quote_amount, 7_000_000_000_000_000_000);
            } else {
                assert_eq!(result, Err(ChunkStreamError::Loader(super::LoaderError::StateLoad)));
            }
            assert_exact_semantic_reads(&reader, orders, &record["orderedReads"]);
            let expected_phase_trace = if succeeds {
                (
                    35,
                    16,
                    18,
                    "0x5b7e4dbbbd358df5289fdab1d2c3146c36e793dbd0d86fc1753c74a97dd4fe49"
                        .parse::<B256>()
                        .unwrap(),
                )
            } else {
                (
                    23,
                    10,
                    12,
                    "0x99db3b856d7ac2a099b0ba788e16754e9ad32c0a54bc0a8f0360df441f0d34b6"
                        .parse::<B256>()
                        .unwrap(),
                )
            };
            assert_eq!(
                (
                    phases.events.len(),
                    phases.events.iter().filter(|phase| **phase == Phase::KeyDerivation).count(),
                    journal_events,
                    phase_commitment(&phases.events),
                ),
                expected_phase_trace,
                "pin the exact malformed V2 phase trace",
            );
            if succeeds {
                assert_step_clock_durations(&phases, 160, 180, 350);
            } else {
                assert_step_clock_durations(&phases, 100, 120, 230);
            }
        }
    }

    #[test]
    fn terminal_max_claimed_steps_preserves_the_sentinel() {
        let terminal =
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ORDER_FLAGS_TERMINAL_CLAIMED_STEPS as u8;
        assert_eq!(decode_claimed_steps(u32::MAX, terminal), u32::MAX);
        assert_eq!(decode_claimed_steps(u32::MAX, 0), u32::MAX - 1);
    }

    #[test]
    fn claimed_step_skip_is_checked() {
        assert_eq!(projected_fill_skip(7, 5), Ok(12));
        assert_eq!(projected_fill_skip(u64::MAX, 1), Err(LoaderError::Arithmetic));
    }

    #[test]
    fn claimed_steps_skip_prior_segment_before_rounded_replay() {
        let orders = Address::repeat_byte(0x15);
        let protocol = Address::repeat_byte(0x71);
        let book = orders_market_book_slot(protocol, 9).unwrap();
        let level = orders_tick_level_slot_from_book(book, 7).unwrap();
        let segment_seed = checked_slot_offset(
            level,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_FILL_SEGMENT_BY_INDEX,
        )
        .unwrap();
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        for (index, size, funding) in [(0_u64, 1_u64, 11_u64), (1, 2, 22), (2, 1, 33)] {
            let segment =
                U256::from(size) | (U256::from(index + 1) << 32) | (U256::from(funding) << 64);
            db.insert_account_storage(
                orders,
                mapping_slot(U256::from(index), segment_seed),
                segment,
            )
            .unwrap();
        }
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let mut phases = TracePhases::default();
        let mut loader = LoaderContext::new(&mut reader, &mut phases);
        let wad = U256::from(schema::IMPLEMENTATION_CONSTANTS_FIXED_POINT_WAD);
        let mut collect = |side, step_size| {
            let mut run = DirectRun {
                candidate: PendingCandidate {
                    tick: 7,
                    seq_id: 1,
                    order_id: U256::ONE,
                    prefix_before: 0,
                    pending_steps: 2,
                    claimed_steps: 1,
                    single_tick_counters: None,
                },
                cursor_segment: 0,
                cursor_offset: 0,
                segment_tail: 2,
                fee_bps: 0,
                flags: 64,
                side,
                repeats_segment_payload_read: false,
            };
            let mut descriptors = Vec::new();
            collect_direct_run(
                &mut loader,
                orders,
                book,
                step_size,
                wad + U256::ONE,
                &mut run,
                0,
                2,
                &mut descriptors,
            )
            .unwrap();
            assert_eq!(descriptors.len(), 1);
            assert_eq!(descriptors[0].match_seq, 2);
            assert_eq!(descriptors[0].funding_snapshot_x128, 22);
            descriptors[0]
        };

        let buy = collect(schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_BUY as u8, wad + U256::ONE);
        assert_eq!(buy.claim_size, 2_000_000_000_000_000_002);
        assert_eq!(buy.claim_quote_amount, 14_000_000_000_000_000_028);
        assert_eq!(
            buy.packed_order_context & ((U256::ONE << 128) - U256::ONE),
            wad * U256::from(7) + U256::from(6)
        );

        let sell = collect(schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_SELL as u8, U256::from(3));
        assert_eq!(sell.claim_size, 6);
        assert_eq!(sell.claim_quote_amount, 43);
        assert_eq!(
            sell.packed_order_context & ((U256::ONE << 128) - U256::ONE),
            wad * U256::from(43) / U256::from(6)
        );

        let mut fee_run = DirectRun {
            candidate: PendingCandidate {
                tick: 7,
                seq_id: 1,
                order_id: U256::ONE,
                prefix_before: 0,
                pending_steps: 3,
                claimed_steps: 0,
                single_tick_counters: None,
            },
            cursor_segment: 1,
            cursor_offset: 0,
            segment_tail: 3,
            fee_bps: 1,
            flags: 64,
            side: schema::IMPLEMENTATION_CONSTANTS_ORDER_SIDE_BUY as u8,
            repeats_segment_payload_read: false,
        };
        let mut fee_descriptors = Vec::new();
        collect_direct_run(
            &mut loader,
            orders,
            book,
            wad,
            wad,
            &mut fee_run,
            0,
            2,
            &mut fee_descriptors,
        )
        .unwrap();
        assert_eq!(fee_descriptors.len(), 2);
        assert_eq!(
            fee_descriptors[0].packed_order_context
                >> schema::IMPLEMENTATION_CONSTANTS_PROJECTED_CHUNK_CONTEXT_FEE_PREFIX_STEPS_SHIFT,
            U256::ZERO,
        );
        assert_eq!(
            fee_descriptors[1].packed_order_context
                >> schema::IMPLEMENTATION_CONSTANTS_PROJECTED_CHUNK_CONTEXT_FEE_PREFIX_STEPS_SHIFT,
            U256::from(2),
        );

        let mut flip = MarketState {
            size: -1_000_000_000_000_000_001,
            quote: 0,
            last_funding_payment: 22,
            leverage_wad: wad,
            isolated_balance: 0,
            settlement_pnl: I256::ZERO,
        };
        replay_chunk(
            &mut flip,
            buy.chunk(),
            schema::IMPLEMENTATION_CONSTANTS_MARGIN_MODE_CROSS as u8,
        )
        .unwrap();
        assert_eq!(flip.size, 1_000_000_000_000_000_001);
        assert_eq!(flip.settlement_pnl, I256::try_from(-7_000_000_000_000_000_015_i128).unwrap());

        let mut isolated = MarketState {
            size: -1,
            quote: 0,
            last_funding_payment: 22,
            leverage_wad: wad * U256::from(42) + U256::from(42),
            isolated_balance: 0,
            settlement_pnl: I256::ZERO,
        };
        replay_chunk(
            &mut isolated,
            sell.chunk(),
            schema::IMPLEMENTATION_CONSTANTS_MARGIN_MODE_ISOLATED as u8,
        )
        .unwrap();
        assert_eq!(isolated.isolated_balance, 2);
    }

    #[allow(clippy::too_many_arguments)]
    fn seed_single_projection_world(
        orders: Address,
        protocol: Address,
        market_id: u16,
        user_id: u32,
        dirty_count: u32,
        heap_length: u64,
        seq_id: u16,
        v2_root: Option<U256>,
    ) -> InMemoryDB {
        let book =
            crate::risex_formula::storage::orders_market_book_slot(protocol, market_id).unwrap();
        let level =
            crate::risex_formula::storage::orders_tick_level_slot(protocol, market_id, 7).unwrap();
        let mut db = InMemoryDB::default();
        db.insert_account_info(orders, AccountInfo::default());
        db.insert_account_storage(
            orders,
            book,
            U256::from(1_000_000_000_000_000_000_u128)
                | (U256::from(1_000_000_000_000_000_000_u128) << 64)
                | (U256::ONE << 128),
        )
        .unwrap();
        db.insert_account_storage(
            orders,
            book + U256::ONE,
            U256::from(7_u8) | (U256::from(dirty_count) << 32),
        )
        .unwrap();
        let keys_slot = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_KEYS,
            );
        db.insert_account_storage(orders, keys_slot, U256::from(heap_length)).unwrap();
        db.insert_account_storage(
            orders,
            crate::risex_formula::storage::dynamic_array_data_slot(keys_slot),
            U256::from((1_u64 << 24) | 7),
        )
        .unwrap();
        let open_seed = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED,
            );
        db.insert_account_storage(
            orders,
            mapping_slot(U256::from(user_id), open_seed),
            U256::ONE << 128,
        )
        .unwrap();
        let order_id = (U256::from(user_id)
            << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_RESTING_ORDER_ID_OWNER_BITS_0)
            | U256::ONE;
        let metadata_seed = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED,
            );
        db.insert_account_storage(
            orders,
            mapping_slot(order_id, metadata_seed),
            pack_metadata(1, 0, 5, 7, seq_id, 1, 64),
        )
        .unwrap();
        let positions_seed = book
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_POS_PLUS_ONE_PACKED,
            );
        db.insert_account_storage(
            orders,
            mapping_slot(U256::ZERO, positions_seed),
            U256::ONE << (7 * 16),
        )
        .unwrap();
        let counters = U256::ONE | (U256::from(seq_id) << 16) | (U256::ONE << 176);
        db.insert_account_storage(
            orders,
            level
                + U256::from(
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_PACKED_COUNTERS,
                ),
            counters,
        )
        .unwrap();
        if let Some(root) = v2_root {
            let v2 = level
                + U256::from(
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_MEMBER_OFFSET,
                );
            db.insert_account_storage(orders, v2, root).unwrap();
            let leaves =
                (0_u64..5).fold(U256::ZERO, |word, index| word | (U256::ONE << (index * 32)));
            db.insert_account_storage(
                orders,
                v2 + U256::from(
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_TICK_PREFIX_INDEXES_V2_LEAVES_OFFSET,
                ),
                leaves,
            )
            .unwrap();
        }
        let segment_seed = level
            + U256::from(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_FILL_SEGMENT_BY_INDEX,
            );
        let run = serde_json::json!({
            "sizeSteps": seq_id,
            "matchSeqStart": 1,
            "fundingSnapshotX128": "0",
            "takerFeeBps": 0,
        });
        db.insert_account_storage(
            orders,
            mapping_slot(U256::ZERO, segment_seed),
            pack_segment(&run, 0),
        )
        .unwrap();
        db
    }

    fn pack_metadata(
        size: u32,
        filled: u32,
        fee_bps: u16,
        tick: u32,
        seq_id: u16,
        claimed_plus_one: u32,
        flags: u8,
    ) -> U256 {
        U256::from(size)
            | (U256::from(filled) << 32)
            | (U256::from(fee_bps)
                << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FEE_BPS_BYTE_OFFSET * 8))
            | (U256::from(tick)
                << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_OFFSET * 8))
            | (U256::from(seq_id)
                << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_OFFSET * 8))
            | (U256::from(claimed_plus_one)
                << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_CLAIMED_STEPS_PLUS_ONE_BYTE_OFFSET * 8))
            | (U256::from(flags)
                << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FLAGS_BYTE_OFFSET * 8))
    }

    fn assert_exact_semantic_reads(
        reader: &JournalReader<'_, '_>,
        orders: Address,
        expected: &Value,
    ) {
        let expected = expected["slots"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .map(|slot| (orders, word(slot.as_str().unwrap())))
            .collect::<Vec<_>>();
        assert_eq!(reader.ordered_storage_reads(), expected);
    }

    fn execute_compact_case(case: &Value, succeeds: bool) {
        let generation = &case["generation"];
        assert_eq!(generation["kind"], "projected-fill-replay-runs-v1");
        let orders: Address = case["addresses"]["ordersManager"].as_str().unwrap().parse().unwrap();
        let caller: Address = case["addresses"]["caller"].as_str().unwrap().parse().unwrap();
        let segment_seed = word(generation["segmentSeed"].as_str().unwrap());
        let mut db = InMemoryDB::default();
        for item in case["journalState"].as_array().unwrap() {
            let account: Address = item["address"].as_str().unwrap().parse().unwrap();
            db.insert_account_info(account, AccountInfo::default());
            db.insert_account_storage(
                account,
                word(item["slot"].as_str().unwrap()),
                word(item["value"].as_str().unwrap()),
            )
            .unwrap();
        }
        let book = orders_market_book_slot(caller, 60_001).unwrap();
        let dirty_count_slot = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_DIRTY_LEVEL_COUNT,
        )
        .unwrap();
        let keys_slot = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_DIRTY_HEAD_HEAP_KEYS,
        )
        .unwrap();
        db.insert_account_storage(
            orders,
            dirty_count_slot,
            U256::ONE
                << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_QUEUE_COUNTERS_DIRTY_LEVEL_COUNT_BYTE_OFFSET
                    * 8),
        )
        .unwrap();
        db.insert_account_storage(orders, keys_slot, U256::ONE).unwrap();
        db.insert_account_storage(
            orders,
            crate::risex_formula::storage::dynamic_array_data_slot(keys_slot),
            (U256::ONE
                << schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_DIRTY_HEAD_HEAP_MATCH_SEQ_BIT_WIDTH)
                | U256::from(7),
        )
        .unwrap();
        let open_seed = checked_slot_offset(
            book,
            schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED,
        )
        .unwrap();
        let order_count = generation["makerRuns"].as_array().unwrap().len();
        db.insert_account_storage(
            orders,
            mapping_slot(U256::from(102), open_seed),
            ((U256::ONE << order_count) - U256::ONE)
                << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_OPEN_ORDERS_SELL_BITMAP_BYTE_OFFSET
                    * 8),
        )
        .unwrap();
        let journal_domain = keccak256("risex.effective_market_v1.journal_word.v1");
        let mut journal_commitment = B256::ZERO;
        let mut segment_index = 0_u64;
        for run in generation["segmentRuns"].as_array().unwrap() {
            assert_eq!(run["startIndex"].as_u64().unwrap(), segment_index);
            for offset in 0..run["count"].as_u64().unwrap() {
                let index = segment_index + offset;
                let slot = mapping_slot(U256::from(index), segment_seed);
                let value = pack_segment(run, offset);
                db.insert_account_storage(orders, slot, value).unwrap();
                let item = keccak_words(&[U256::from_be_slice(orders.as_slice()), slot, value]);
                journal_commitment =
                    keccak_words_b256(journal_domain, journal_commitment, index, item);
            }
            segment_index += run["count"].as_u64().unwrap();
        }
        assert_eq!(
            format!("{journal_commitment:#x}"),
            generation["segmentJournalCommitment"].as_str().unwrap()
        );
        assert_eq!(segment_index, case["projectedChunks"]["count"].as_u64().unwrap());

        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);
        let chunk_domain = keccak256("risex.effective_market_v1.projected_chunk.v1");
        let mut chunk_commitment = B256::ZERO;
        let mut sink_count = 0_u64;
        let mut replayed = if succeeds {
            let initial = &case["orderedRows"][0];
            MarketState {
                size: initial["effectivePositionSize"].as_str().unwrap().parse().unwrap(),
                quote: initial["effectivePositionQuote"].as_str().unwrap().parse().unwrap(),
                last_funding_payment: initial["effectiveLastFundingPayment"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                leverage_wad: U256::from_str_radix(
                    initial["effectiveLeverageWad"].as_str().unwrap(),
                    10,
                )
                .unwrap(),
                isolated_balance: initial["effectiveIsolatedBalance"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                settlement_pnl: I256::ZERO,
            }
        } else {
            MarketState {
                size: 0,
                quote: 0,
                last_funding_payment: 0,
                leverage_wad: U256::ZERO,
                isolated_balance: 0,
                settlement_pnl: I256::ZERO,
            }
        };
        let result = stream_projected_chunks(&mut reader, orders, caller, 60_001, 102, |chunk| {
            let item = chunk_hash(chunk);
            chunk_commitment = keccak_words_b256(chunk_domain, chunk_commitment, sink_count, item);
            sink_count += 1;
            if succeeds {
                replay_chunk(&mut replayed, chunk, 0).unwrap();
            }
            Ok::<_, ()>(())
        });
        if succeeds {
            assert_eq!(result.unwrap(), 8_192);
            assert_eq!(sink_count, 8_192);
            assert_eq!(
                format!("{chunk_commitment:#x}"),
                case["projectedChunks"]["commitment"]["value"].as_str().unwrap()
            );
            let final_row = &case["finalRow"];
            assert_eq!(
                replayed.size.to_string(),
                final_row["effectivePositionSize"].as_str().unwrap()
            );
            assert_eq!(
                replayed.quote.to_string(),
                final_row["effectivePositionQuote"].as_str().unwrap()
            );
            assert_eq!(
                replayed.last_funding_payment.to_string(),
                final_row["effectiveLastFundingPayment"].as_str().unwrap()
            );
            assert_eq!(
                replayed.settlement_pnl.to_string(),
                final_row["projectedSettlementPnl"].as_str().unwrap()
            );
        } else {
            assert_eq!(result, Err(ChunkStreamError::Loader(super::LoaderError::BoundExceeded)));
            assert_eq!(sink_count, 0);
        }
    }

    fn pack_segment(run: &Value, offset: u64) -> U256 {
        let size = U256::from(run["sizeSteps"].as_u64().unwrap());
        let seq = U256::from(run["matchSeqStart"].as_u64().unwrap() + offset);
        let funding = run["fundingSnapshotX128"].as_str().unwrap().parse::<i128>().unwrap();
        let fee = run["takerFeeBps"].as_i64().unwrap() as i16;
        size
            | (seq << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_MATCH_SEQ_BYTE_OFFSET * 8))
            | ((I256::unchecked_from(funding).into_raw() & ((U256::ONE << 128) - U256::ONE)) << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_FUNDING_SNAPSHOT_X128_BYTE_OFFSET * 8))
            | ((I256::unchecked_from(fee).into_raw() & ((U256::ONE << 16) - U256::ONE)) << (schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_FILL_SEGMENT_TAKER_FEE_BPS_BYTE_OFFSET * 8))
    }

    fn chunk_hash(chunk: ProjectedChunk) -> B256 {
        let mut bytes = [0_u8; 192];
        let words = [
            U256::from(chunk.claim_size),
            U256::from(chunk.claim_quote_amount),
            chunk.packed_order_context,
            I256::unchecked_from(chunk.funding_snapshot_x128).into_raw(),
            I256::unchecked_from(chunk.fee_bps).into_raw(),
            U256::from(chunk.side),
        ];
        for (index, value) in words.into_iter().enumerate() {
            bytes[index * 32..(index + 1) * 32].copy_from_slice(&value.to_be_bytes::<32>());
        }
        keccak256(bytes)
    }

    fn keccak_words(values: &[U256]) -> B256 {
        let mut bytes = Vec::with_capacity(values.len() * 32);
        for value in values {
            bytes.extend_from_slice(&value.to_be_bytes::<32>());
        }
        keccak256(bytes)
    }

    fn keccak_words_b256(domain: B256, previous: B256, index: u64, item: B256) -> B256 {
        let mut bytes = [0_u8; 128];
        bytes[..32].copy_from_slice(domain.as_slice());
        bytes[32..64].copy_from_slice(previous.as_slice());
        bytes[64..96].copy_from_slice(&U256::from(index).to_be_bytes::<32>());
        bytes[96..].copy_from_slice(item.as_slice());
        keccak256(bytes)
    }

    fn word(value: &str) -> U256 {
        U256::from_str_radix(value.strip_prefix("0x").unwrap(), 16).unwrap()
    }
}
