use std::{cell::Cell, error::Error, fmt};

use alloy_evm::{EvmInternals, EvmInternalsError};
use alloy_primitives::{Address, I256, U256, keccak256, map::HashSet};
use revm::context_interface::cfg::gas::{
    COLD_ACCOUNT_ACCESS_COST, COLD_SLOAD_COST, WARM_STORAGE_READ_COST,
};

use super::loader::schema_generated as schema;

/// A checked storage-key derivation or packed-field error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageKeyError {
    ArithmeticOverflow,
    IndexOutOfBounds,
    InvalidFieldRange,
    DisabledSlot,
}

impl fmt::Display for StorageKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow => f.write_str("storage-key arithmetic overflow"),
            Self::IndexOutOfBounds => f.write_str("storage-key index is out of bounds"),
            Self::InvalidFieldRange => f.write_str("packed field range is invalid"),
            Self::DisabledSlot => f.write_str("direct slot is disabled"),
        }
    }
}

impl Error for StorageKeyError {}

/// Journal access counts for one loader invocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct JournalReadStats {
    pub(crate) journal_reads: u64,
    pub(crate) unique_storage_keys: u64,
    pub(crate) state_access_gas: u64,
}

/// Per-call gas consumed by native state reads and deterministic work.
pub(crate) struct GasMeter {
    limit: u64,
    used: Cell<u64>,
    exhausted: Cell<bool>,
}

impl GasMeter {
    pub(crate) const fn new(limit: u64) -> Self {
        Self { limit, used: Cell::new(0), exhausted: Cell::new(false) }
    }

    pub(crate) fn charge(&self, gas: u64) -> bool {
        let Some(used) = self.used.get().checked_add(gas).filter(|used| *used <= self.limit) else {
            self.exhausted.set(true);
            return false;
        };
        self.used.set(used);
        true
    }

    pub(crate) const fn is_exhausted(&self) -> bool {
        self.exhausted.get()
    }

    const fn remaining(&self) -> u64 {
        self.limit - self.used.get()
    }
}

/// Reads RISEx state exclusively through the live EVM journal.
pub(crate) struct JournalReader<'reader, 'evm> {
    internals: &'reader mut EvmInternals<'evm>,
    gas_meter: Option<&'reader GasMeter>,
    journal_reads: u64,
    storage_keys: HashSet<(Address, U256)>,
    state_access_gas: u64,
    #[cfg(test)]
    ordered_storage_reads: Vec<(Address, U256)>,
}

impl<'reader, 'evm> JournalReader<'reader, 'evm> {
    pub(crate) fn new(internals: &'reader mut EvmInternals<'evm>) -> Self {
        Self {
            internals,
            gas_meter: None,
            journal_reads: 0,
            storage_keys: HashSet::default(),
            state_access_gas: 0,
            #[cfg(test)]
            ordered_storage_reads: Vec::new(),
        }
    }

    pub(crate) fn with_gas_meter(
        internals: &'reader mut EvmInternals<'evm>,
        gas_meter: &'reader GasMeter,
    ) -> Self {
        let mut reader = Self::new(internals);
        reader.gas_meter = Some(gas_meter);
        reader
    }

    pub(crate) fn sload(&mut self, address: Address, key: U256) -> Result<U256, EvmInternalsError> {
        let unique_key = self.storage_keys.insert((address, key));
        self.record_journal_read();
        #[cfg(test)]
        self.ordered_storage_reads.push((address, key));
        let gas_meter = self.gas_meter;
        consume_gas(gas_meter, WARM_STORAGE_READ_COST + u64::from(unique_key))?;
        let cold_cost = COLD_SLOAD_COST - WARM_STORAGE_READ_COST;
        let skip_cold_load = gas_meter.is_some_and(|meter| meter.remaining() < cold_cost);
        let mut account =
            match self.internals.load_account_mut_skip_cold_load(address, skip_cold_load) {
                Ok(account) => account,
                Err(error) if error.is_cold_load_skipped() => {
                    consume_gas(gas_meter, cold_cost)?;
                    unreachable!("cold load was skipped only when its gas was unavailable")
                }
                Err(error) => {
                    Self::record_state_access(&mut self.state_access_gas, WARM_STORAGE_READ_COST);
                    return Err(error.unwrap_db_error());
                }
            };
        let value = match account.data.sload(key, skip_cold_load) {
            Ok(value) => value,
            Err(error) if error.is_cold_load_skipped() => {
                drop(account);
                consume_gas(gas_meter, cold_cost)?;
                unreachable!("cold load was skipped only when its gas was unavailable")
            }
            Err(error) => {
                drop(account);
                Self::record_state_access(&mut self.state_access_gas, WARM_STORAGE_READ_COST);
                return Err(EvmInternalsError::database(error.unwrap_db_error()));
            }
        };
        let (value, is_cold) = (value.data.present_value(), value.is_cold);
        drop(account);
        if is_cold {
            consume_gas(gas_meter, cold_cost)?;
        }
        Self::record_state_access(
            &mut self.state_access_gas,
            if is_cold { COLD_SLOAD_COST } else { WARM_STORAGE_READ_COST },
        );
        Ok(value)
    }

    pub(crate) fn code_hash(
        &mut self,
        address: Address,
    ) -> Result<alloy_primitives::B256, EvmInternalsError> {
        self.record_journal_read();
        let gas_meter = self.gas_meter;
        consume_gas(gas_meter, WARM_STORAGE_READ_COST)?;
        let cold_cost = COLD_ACCOUNT_ACCESS_COST - WARM_STORAGE_READ_COST;
        let skip_cold_load = gas_meter.is_some_and(|meter| meter.remaining() < cold_cost);
        let account = match self.internals.load_account_mut_skip_cold_load(address, skip_cold_load)
        {
            Ok(account) => account,
            Err(error) if error.is_cold_load_skipped() => {
                consume_gas(gas_meter, cold_cost)?;
                unreachable!("cold load was skipped only when its gas was unavailable")
            }
            Err(error) => {
                Self::record_state_access(&mut self.state_access_gas, WARM_STORAGE_READ_COST);
                return Err(error.unwrap_db_error());
            }
        };
        let (code_hash, is_cold) = (*account.data.code_hash(), account.is_cold);
        drop(account);
        if is_cold {
            consume_gas(gas_meter, cold_cost)?;
        }
        Self::record_state_access(
            &mut self.state_access_gas,
            if is_cold { COLD_ACCOUNT_ACCESS_COST } else { WARM_STORAGE_READ_COST },
        );
        Ok(code_hash)
    }

    pub(crate) const fn journal_reads(&self) -> u64 {
        self.journal_reads
    }

    pub(crate) fn unique_storage_keys(&self) -> u64 {
        self.storage_keys
            .len()
            .try_into()
            .expect("loader bounds keep the unique-key count within u64")
    }

    pub(crate) fn stats(&self) -> JournalReadStats {
        JournalReadStats {
            journal_reads: self.journal_reads(),
            unique_storage_keys: self.unique_storage_keys(),
            state_access_gas: self.state_access_gas,
        }
    }

    pub(crate) fn block_timestamp(&self) -> U256 {
        self.internals.block_timestamp()
    }

    #[cfg(test)]
    pub(crate) fn ordered_storage_reads(&self) -> &[(Address, U256)] {
        &self.ordered_storage_reads
    }

    const fn record_journal_read(&mut self) {
        self.journal_reads = self
            .journal_reads
            .checked_add(1)
            .expect("loader bounds keep the journal-read count within u64");
    }

    const fn record_state_access(state_access_gas: &mut u64, gas: u64) {
        *state_access_gas = state_access_gas
            .checked_add(gas)
            .expect("loader bounds keep state-access gas within u64");
    }
}

fn consume_gas(gas_meter: Option<&GasMeter>, gas: u64) -> Result<(), EvmInternalsError> {
    if gas_meter.is_none_or(|meter| meter.charge(gas)) { Ok(()) } else { Err(gas_exhausted()) }
}

fn gas_exhausted() -> EvmInternalsError {
    EvmInternalsError::database(std::io::Error::other("RISEx risk formula exhausted supplied gas"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PortfolioBitmapBucketSlots {
    pub(crate) cross: U256,
    pub(crate) isolated: U256,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PortfolioBitmapSlots {
    pub(crate) portfolio: U256,
    pub(crate) bucket: u64,
    pub(crate) cross: U256,
    pub(crate) isolated: U256,
    pub(crate) bit_mask: U256,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PackedArrayElement {
    pub(crate) slot: U256,
    pub(crate) byte_offset: u64,
    pub(crate) byte_width: u64,
}

pub(crate) fn checked_slot_offset(base: U256, offset: u64) -> Result<U256, StorageKeyError> {
    base.checked_add(U256::from(offset)).ok_or(StorageKeyError::ArithmeticOverflow)
}

pub(crate) fn mapping_slot(key: U256, seed: U256) -> U256 {
    let mut encoded = [0_u8; U256::BYTES * 2];
    encoded[..U256::BYTES].copy_from_slice(&key.to_be_bytes::<{ U256::BYTES }>());
    encoded[U256::BYTES..].copy_from_slice(&seed.to_be_bytes::<{ U256::BYTES }>());
    U256::from_be_bytes(keccak256(encoded).0)
}

pub(crate) fn nested_mapping_slot(outer: U256, inner: U256, seed: U256) -> U256 {
    mapping_slot(inner, mapping_slot(outer, seed))
}

pub(crate) fn dynamic_array_data_slot(array_slot: U256) -> U256 {
    U256::from_be_bytes(keccak256(array_slot.to_be_bytes::<{ U256::BYTES }>()).0)
}

#[cfg(test)]
pub(crate) fn fixed_record_slot(
    base: U256,
    record_id: u64,
    record_slots: u64,
    field_offset: u64,
    record_count: u64,
) -> Result<U256, StorageKeyError> {
    if record_id == 0 || record_id >= record_count || field_offset >= record_slots {
        return Err(StorageKeyError::IndexOutOfBounds);
    }
    let offset = record_id
        .checked_mul(record_slots)
        .and_then(|offset| offset.checked_add(field_offset))
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    checked_slot_offset(base, offset)
}

pub(crate) const fn formula_descriptor_slot() -> U256 {
    schema_word(schema::STORAGE_DIRECT_ARENAS_RISK_FORMULA_REGISTRY_DIRECT_BASE_BASE)
}

pub(crate) fn packed_dynamic_array_element(
    array_slot: U256,
    element_index: u64,
    byte_width: u64,
) -> Result<PackedArrayElement, StorageKeyError> {
    let word_bytes = U256::BYTES as u64;
    if byte_width == 0 || byte_width > word_bytes {
        return Err(StorageKeyError::InvalidFieldRange);
    }
    let elements_per_slot = word_bytes / byte_width;
    let word_offset = element_index / elements_per_slot;
    let byte_offset = (element_index % elements_per_slot)
        .checked_mul(byte_width)
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    let slot = dynamic_array_data_slot(array_slot)
        .checked_add(U256::from(word_offset))
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    Ok(PackedArrayElement { slot, byte_offset, byte_width })
}

pub(crate) fn packed_order_id_element(
    array_slot: U256,
    element_index: u64,
) -> Result<PackedArrayElement, StorageKeyError> {
    let byte_width =
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_WIDTH
            .checked_add(
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_SEQ_ID_BYTE_WIDTH,
            )
            .ok_or(StorageKeyError::ArithmeticOverflow)?;
    packed_dynamic_array_element(array_slot, element_index, byte_width)
}

pub(crate) fn extract_unsigned_bits(
    word: U256,
    bit_offset: u64,
    bit_width: u64,
) -> Result<U256, StorageKeyError> {
    let end = bit_offset.checked_add(bit_width).ok_or(StorageKeyError::InvalidFieldRange)?;
    if bit_width == 0 || end > U256::BITS as u64 {
        return Err(StorageKeyError::InvalidFieldRange);
    }
    let shifted = word >> bit_offset;
    if bit_width == U256::BITS as u64 {
        Ok(shifted)
    } else {
        Ok(shifted & ((U256::ONE << bit_width) - U256::ONE))
    }
}

pub(crate) fn extract_unsigned_bytes(
    word: U256,
    byte_offset: u64,
    byte_width: u64,
) -> Result<U256, StorageKeyError> {
    let bit_offset =
        byte_offset.checked_mul(u8::BITS as u64).ok_or(StorageKeyError::InvalidFieldRange)?;
    let bit_width =
        byte_width.checked_mul(u8::BITS as u64).ok_or(StorageKeyError::InvalidFieldRange)?;
    extract_unsigned_bits(word, bit_offset, bit_width)
}

pub(crate) fn extract_signed_bytes(
    word: U256,
    byte_offset: u64,
    byte_width: u64,
) -> Result<I256, StorageKeyError> {
    let bit_width =
        byte_width.checked_mul(u8::BITS as u64).ok_or(StorageKeyError::InvalidFieldRange)?;
    let raw = extract_unsigned_bytes(word, byte_offset, byte_width)?;
    let extended = if bit_width == U256::BITS as u64 {
        raw
    } else if raw.bit((bit_width - 1) as usize) {
        raw | (U256::MAX << bit_width)
    } else {
        raw
    };
    Ok(I256::from_raw(extended))
}

pub(crate) fn risk_mark_snapshot_slots(market_id: u16) -> Result<[U256; 2], StorageKeyError> {
    let record_count = schema::HARD_BOUNDS_MAX_MARKET_ID
        .checked_add(1)
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    let arena_length =
        schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_LENGTH_SLOTS;
    if record_count == 0 || !arena_length.is_multiple_of(record_count) {
        return Err(StorageKeyError::InvalidFieldRange);
    }
    let record_slots = arena_length / record_count;
    if record_slots < 2 {
        return Err(StorageKeyError::InvalidFieldRange);
    }
    let offset = u64::from(market_id)
        .checked_mul(record_slots)
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    let base =
        schema_word(schema::STORAGE_DIRECT_ARENAS_RISEX_ORACLE_RISK_MARK_SNAPSHOT_DIRECT_BASE_BASE);
    let word0 = checked_slot_offset(base, offset)?;
    let word1 = checked_slot_offset(word0, record_slots - 1)?;
    Ok([word0, word1])
}

pub(crate) fn reduce_only_presence_slot(
    protocol_id: u32,
    market_id: u16,
    user_id: u32,
) -> Result<U256, StorageKeyError> {
    if protocol_id == 0 {
        return Err(StorageKeyError::DisabledSlot);
    }
    let prefix = schema_word(
        schema::STORAGE_DIRECT_ARENAS_ORDERS_MANAGER_REDUCE_ONLY_PRESENCE_PREFIX_PREFIX,
    );
    let protocol = U256::from(protocol_id) << (u16::BITS + u32::BITS);
    let market = U256::from(market_id) << u32::BITS;
    Ok(prefix | protocol | market | U256::from(user_id))
}

pub(crate) fn perps_market_slot(market_id: u16) -> Result<U256, StorageKeyError> {
    let root = schema_word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_MARKET_STORAGE_ROOT);
    let seed_offset = schema::STORAGE_PATHS_PERPS_MARKET_FIELDS_MARKET_COUNT_SLOT_OFFSET
        .checked_add(1)
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    let seed = checked_slot_offset(root, seed_offset)?;
    Ok(mapping_slot(U256::from(market_id), seed))
}

pub(crate) fn trading_account_slot(market_id: u16, user_id: u32) -> U256 {
    nested_mapping_slot(
        U256::from(market_id),
        U256::from(user_id),
        schema_word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_TRADING_ACCOUNT_STORAGE_ROOT),
    )
}

#[cfg(test)]
pub(crate) fn portfolio_bitmap_slots(
    user_id: u32,
    market_id: u16,
) -> Result<PortfolioBitmapSlots, StorageKeyError> {
    portfolio_bitmap_slots_from_base(portfolio_slot(user_id), market_id)
}

pub(crate) fn portfolio_slot(user_id: u32) -> U256 {
    mapping_slot(
        U256::from(user_id),
        schema_word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_PORTFOLIO_STORAGE_ROOT),
    )
}

#[cfg(test)]
pub(crate) fn portfolio_bitmap_slots_from_base(
    portfolio: U256,
    market_id: u16,
) -> Result<PortfolioBitmapSlots, StorageKeyError> {
    let bucket_width = schema::HARD_BOUNDS_BITMAP_BUCKET_WIDTH;
    if bucket_width == 0 || bucket_width > U256::BITS as u64 {
        return Err(StorageKeyError::InvalidFieldRange);
    }
    let bucket = u64::from(market_id) / bucket_width;
    let bit: usize = (u64::from(market_id) % bucket_width)
        .try_into()
        .map_err(|_| StorageKeyError::ArithmeticOverflow)?;
    let slots = portfolio_bitmap_bucket_slots_from_base(portfolio, bucket)?;
    Ok(PortfolioBitmapSlots {
        portfolio,
        bucket,
        cross: slots.cross,
        isolated: slots.isolated,
        bit_mask: U256::ONE << bit,
    })
}

pub(crate) fn portfolio_bitmap_bucket_slots_from_base(
    portfolio: U256,
    bucket: u64,
) -> Result<PortfolioBitmapBucketSlots, StorageKeyError> {
    let cross_seed = checked_slot_offset(
        portfolio,
        schema::STORAGE_PATHS_PORTFOLIO_FIELDS_CROSS_MARKETS_MAP_BUCKET_PORTFOLIO_SLOT_OFFSET,
    )?;
    let isolated_seed = checked_slot_offset(
        portfolio,
        schema::STORAGE_PATHS_PORTFOLIO_FIELDS_ISOLATED_MARKETS_MAP_BUCKET_PORTFOLIO_SLOT_OFFSET,
    )?;
    Ok(PortfolioBitmapBucketSlots {
        cross: mapping_slot(U256::from(bucket), cross_seed),
        isolated: mapping_slot(U256::from(bucket), isolated_seed),
    })
}

#[cfg(test)]
pub(crate) fn orders_market_book_slot(
    protocol: Address,
    market_id: u16,
) -> Result<U256, StorageKeyError> {
    Ok(orders_market_book_slot_from_base(orders_market_books_slot(protocol)?, market_id))
}

pub(crate) fn orders_market_books_slot(protocol: Address) -> Result<U256, StorageKeyError> {
    let protocol_slot = mapping_slot(
        U256::from_be_slice(protocol.as_slice()),
        schema_word(schema::STORAGE_NAMESPACES_ORDERS_MANAGER_MARKET_BOOK_STORAGE_ROOT),
    );
    let market_seed = checked_slot_offset(
        protocol_slot,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_MARKET_BOOK_QUEUE,
    )?;
    Ok(market_seed)
}

pub(crate) fn orders_market_book_slot_from_base(market_books: U256, market_id: u16) -> U256 {
    mapping_slot(U256::from(market_id), market_books)
}

#[cfg(test)]
pub(crate) fn orders_tick_level_slot(
    protocol: Address,
    market_id: u16,
    tick: u32,
) -> Result<U256, StorageKeyError> {
    let tick_bits = schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_WIDTH
        .checked_mul(u8::BITS as u64)
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    if tick_bits == 0 || tick_bits >= u64::BITS as u64 {
        return Err(StorageKeyError::InvalidFieldRange);
    }
    let max_tick = (1_u64 << tick_bits) - 1;
    if u64::from(tick) > max_tick {
        return Err(StorageKeyError::IndexOutOfBounds);
    }
    let book = orders_market_book_slot(protocol, market_id)?;
    let tick_seed = checked_slot_offset(
        book,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_TICK_LEVEL,
    )?;
    Ok(mapping_slot(U256::from(tick), tick_seed))
}

pub(crate) fn orders_tick_level_slot_from_book(
    book: U256,
    tick: u32,
) -> Result<U256, StorageKeyError> {
    let tick_bits = schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_TICK_BYTE_WIDTH
        .checked_mul(u8::BITS as u64)
        .ok_or(StorageKeyError::ArithmeticOverflow)?;
    if tick_bits == 0 || u64::from(tick) >= (1_u64 << tick_bits) {
        return Err(StorageKeyError::IndexOutOfBounds);
    }
    let seed = checked_slot_offset(
        book,
        schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_PRICE_LEVEL_QUEUE_TICK_LEVEL,
    )?;
    Ok(mapping_slot(U256::from(tick), seed))
}

const fn schema_word(value: [u8; 32]) -> U256 {
    U256::from_be_bytes(value)
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::OnceLock};

    use alloy_evm::{
        Evm, EvmEnv,
        eth::{EthEvmBuilder, EthEvmContext},
        precompiles::{DynPrecompile, PrecompilesMap},
    };
    use alloy_primitives::{Address, B256, Bytes, I256, U256, address, hex};
    use revm::{
        bytecode::Bytecode,
        context::TxEnv,
        context_interface::cfg::gas::{
            COLD_ACCOUNT_ACCESS_COST, COLD_SLOAD_COST, WARM_STORAGE_READ_COST,
        },
        database::{CacheDB, DatabaseRef, InMemoryDB},
        database_interface::ErasedError,
        precompile::{PrecompileId, PrecompileOutput, Precompiles},
        primitives::TxKind,
        state::AccountInfo,
    };
    use serde_json::Value;

    use super::{
        GasMeter, JournalReadStats, JournalReader, checked_slot_offset, dynamic_array_data_slot,
        extract_signed_bytes, extract_unsigned_bits, fixed_record_slot, formula_descriptor_slot,
        mapping_slot, orders_market_book_slot, orders_market_book_slot_from_base,
        orders_market_books_slot, orders_tick_level_slot, packed_dynamic_array_element,
        packed_order_id_element, perps_market_slot, portfolio_bitmap_slots,
        portfolio_bitmap_slots_from_base, portfolio_slot, reduce_only_presence_slot,
        risk_mark_snapshot_slots, trading_account_slot,
    };
    use crate::risex_formula::loader::{GENERATED_CONTRACTS_COMMIT, schema_generated as schema};

    const SCHEMA_BYTES: &[u8] = include_bytes!("loader/schema_generated.rs");
    const CORPUS_BYTES: &[u8] = include_bytes!("testdata/portfolio_order_risk_v1.json");
    const ARTIFACT_MANIFEST_BYTES: &[u8] = include_bytes!("testdata/artifact-manifest.json");
    const SLOT_VECTOR_BYTES: &[u8] = include_bytes!("testdata/loader-slots-v1.json");
    const STATE_VECTOR_BYTES: &[u8] = include_bytes!("testdata/effective-market-v1.json");

    #[test]
    fn journal_reader_generated_artifacts_match_the_pinned_contracts_commit() {
        assert_eq!(GENERATED_CONTRACTS_COMMIT, "adcf51d25b4b99d35481ea1f7b3d9e5bda0abc11");
        assert_eq!(
            sha256_hex(SCHEMA_BYTES),
            "f0b02010bb6ffd82969cc875f65e301de705ad93f24a3a6b08dd0512466272f3"
        );
        assert_eq!(
            sha256_hex(CORPUS_BYTES),
            "5494eed832754f761ff18b6c8697b6a5bcb88c943d410fd394cb27aaa5a95b17"
        );
        assert_eq!(
            sha256_hex(ARTIFACT_MANIFEST_BYTES),
            "80b69bd2a9b574adcf7ed3129b3c00cf4000ea9e026342b2999dda584afc99cb"
        );
        assert_eq!(
            sha256_hex(SLOT_VECTOR_BYTES),
            "3dcc66671011752c07a364384dc36dd01ef376c99030ccb4a167c1fe5556b0d0"
        );
        assert_eq!(
            sha256_hex(STATE_VECTOR_BYTES),
            "1916eca4f11f384a2af82848449fc38a33c8772ce76eb306ed80e5c30c9b2793"
        );

        let manifest: Value = serde_json::from_slice(ARTIFACT_MANIFEST_BYTES).unwrap();
        assert_eq!(
            manifest["outputs"]["foundry/schema_generated.rs"]["sha256"],
            sha256_hex(SCHEMA_BYTES)
        );
        assert_eq!(
            manifest["outputs"]["foundry/portfolio_order_risk_v1.json"]["sha256"],
            sha256_hex(CORPUS_BYTES)
        );
        assert_eq!(schema::LOADER_SOURCE_SHA256_HEX, manifest["inputs"]["loaderSourceSha256"]);
        assert_eq!(schema::LOADER_SCHEMA_HASH_HEX, manifest["inputs"]["loaderCanonicalKeccak256"]);
    }

    #[test]
    fn journal_reader_derives_all_solidity_slot_vectors_from_generated_constants() {
        let fixture = slot_vectors();

        for vector in fixture["vectors"]["direct"].as_array().unwrap() {
            let name = vector["name"].as_str().unwrap();
            if name.starts_with("risk_mark_market_") {
                let [word0, word1] =
                    risk_mark_snapshot_slots(vector["marketId"].as_u64().unwrap() as u16).unwrap();
                assert_eq!(word0, word(&vector["word0Slot"]), "{name} word 0");
                assert_eq!(word1, word(&vector["word1Slot"]), "{name} word 1");
            } else {
                let slot = reduce_only_presence_slot(
                    vector["protocolId"].as_u64().unwrap() as u32,
                    vector["marketId"].as_u64().unwrap() as u16,
                    vector["userId"].as_u64().unwrap() as u32,
                )
                .unwrap();
                assert_eq!(slot, word(&vector["slot"]), "{name}");
            }
        }

        for vector in fixture["vectors"]["mapping"].as_array().unwrap() {
            let name = vector["name"].as_str().unwrap();
            let market_id = vector["key"].as_u64().unwrap() as u16;
            let actual = if name.starts_with("perps_market_") {
                perps_market_slot(market_id).unwrap()
            } else if name == "compact_funding_max" {
                mapping_slot(
                    U256::from(market_id),
                    schema_word(
                        schema::STORAGE_NAMESPACES_FUNDING_RATE_COMPACT_FUNDING_STORAGE_ROOT,
                    ),
                )
            } else {
                mapping_slot(
                    U256::from(market_id),
                    schema_word(schema::STORAGE_NAMESPACES_FUNDING_RATE_STORAGE_ROOT),
                )
            };
            let expected = vector.get("slot").unwrap_or(&vector["recordSlot"]);
            assert_eq!(actual, word(expected), "{name}");
            if let Some(cutover) = vector.get("cutoverWordSlot") {
                assert_eq!(
                    checked_slot_offset(
                        actual,
                        schema::STORAGE_PATHS_FUNDING_FIELDS_COMPACT_CUTOVER_AT_RECORD_SLOT_OFFSET,
                    )
                    .unwrap(),
                    word(cutover),
                    "{name} cutover"
                );
            }
        }

        for vector in fixture["vectors"]["nestedMapping"].as_array().unwrap() {
            let actual = trading_account_slot(
                vector["outerKey"].as_u64().unwrap() as u16,
                vector["innerKey"].as_u64().unwrap() as u32,
            );
            assert_eq!(actual, word(&vector["recordSlot"]), "{}", vector["name"]);
        }

        for vector in fixture["vectors"]["bitmap"].as_array().unwrap() {
            let actual = portfolio_bitmap_slots(
                vector["userId"].as_u64().unwrap() as u32,
                vector["marketId"].as_u64().unwrap() as u16,
            )
            .unwrap();
            assert_eq!(
                actual,
                portfolio_bitmap_slots_from_base(
                    portfolio_slot(vector["userId"].as_u64().unwrap() as u32),
                    vector["marketId"].as_u64().unwrap() as u16,
                )
                .unwrap()
            );
            assert_eq!(actual.portfolio, word(&vector["portfolioSlot"]));
            assert_eq!(actual.bucket, vector["bucket"].as_u64().unwrap());
            assert_eq!(actual.cross, word(&vector["crossBucketSlot"]));
            assert_eq!(actual.isolated, word(&vector["isolatedBucketSlot"]));
            assert_eq!(actual.bit_mask, word(&vector["bitMask"]));
        }

        let books = &fixture["vectors"]["ordersBook"];
        let protocol: Address = books["protocol"].as_str().unwrap().parse().unwrap();
        for vector in books["markets"].as_array().unwrap() {
            let book =
                orders_market_book_slot(protocol, vector["marketId"].as_u64().unwrap() as u16)
                    .unwrap();
            assert_eq!(
                book,
                orders_market_book_slot_from_base(
                    orders_market_books_slot(protocol).unwrap(),
                    vector["marketId"].as_u64().unwrap() as u16,
                )
            );
            assert_eq!(book, word(&vector["bookSlot"]));
            assert_eq!(
                checked_slot_offset(
                    book,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_QUEUE_DIRTY_LEVEL_COUNT,
                )
                .unwrap(),
                word(&vector["queueCountersSlot"])
            );
            assert_eq!(
                checked_slot_offset(
                    book,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_METADATA_BY_ORDER_ID_SEED,
                )
                .unwrap(),
                word(&vector["metadataSeed"])
            );
            assert_eq!(
                checked_slot_offset(
                    book,
                    schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_ABSOLUTE_RECORD_OFFSETS_OPEN_ORDERS_BY_USER_ID_SEED,
                )
                .unwrap(),
                word(&vector["openOrdersSeed"])
            );
        }

        for vector in fixture["vectors"]["dynamicArray"].as_array().unwrap() {
            let tick_level = orders_tick_level_slot(
                protocol,
                vector["marketId"].as_u64().unwrap() as u16,
                vector["tick"].as_u64().unwrap() as u32,
            )
            .unwrap();
            assert_eq!(tick_level, word(&vector["tickLevelSlot"]));
            let array_slot = checked_slot_offset(
                tick_level,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_COMPILED_OFFSETS_TICK_LEVEL_ORDER_IDS,
            )
            .unwrap();
            assert_eq!(dynamic_array_data_slot(array_slot), word(&vector["dataRoot"]));
            let element =
                packed_order_id_element(array_slot, vector["elementIndex"].as_u64().unwrap())
                    .unwrap();
            assert_eq!(element.slot, word(&vector["elementSlot"]));
            assert_eq!(element.byte_offset, vector["byteOffset"].as_u64().unwrap());
            assert_eq!(element.byte_width, vector["byteWidth"].as_u64().unwrap());
        }

        let registry = &fixture["vectors"]["registry"];
        let registry_root =
            schema_word(schema::STORAGE_NAMESPACES_PERPS_MANAGER_REGISTRY_STORAGE_ROOT);
        for dependency in registry["perpsDependencies"].as_array().unwrap() {
            let offset = match dependency["field"].as_str().unwrap() {
                "fundingRate" => {
                    schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_FUNDING_RATE_SLOT_OFFSET
                }
                "ordersManager" => {
                    schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ORDERS_MANAGER_SLOT_OFFSET
                }
                "risexOracle" => {
                    schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_RISEX_ORACLE_SLOT_OFFSET
                }
                _ => schema::STORAGE_PATHS_PERPS_REGISTRY_FIELDS_ACCOUNT_REGISTRY_SLOT_OFFSET,
            };
            assert_eq!(
                checked_slot_offset(registry_root, offset).unwrap(),
                word(&dependency["slot"])
            );
        }

        let descriptor = registry["formulaDescriptors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|descriptor| descriptor["valid"].as_bool().unwrap())
            .unwrap();
        assert_eq!(formula_descriptor_slot(), word(&descriptor["packedDescriptorSlot"]));
    }

    #[test]
    fn journal_reader_uses_the_generated_formula_descriptor_slot() {
        assert_eq!(
            formula_descriptor_slot(),
            word_str("0x311eaf2cf29c4b2f8b8ce017889b075a91bdb7242e61b82230138ee4fa0a3700")
        );
    }

    #[test]
    fn journal_reader_packed_extractors_use_generated_offsets_and_reject_invalid_ranges() {
        let packed = word_str("0x8001000000006600000000550000000000000000000000000000000000001234");
        assert_eq!(
            extract_unsigned_bits(
                packed,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_NOTIONAL_BIT_OFFSET,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_NOTIONAL_BIT_WIDTH,
            )
            .unwrap(),
            U256::from(0x1234_u64)
        );
        assert_eq!(
            extract_unsigned_bits(
                packed,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_BUY_OPEN_STEPS_BIT_OFFSET,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_BUY_OPEN_STEPS_BIT_WIDTH,
            )
            .unwrap(),
            U256::from(0x55_u64)
        );
        assert_eq!(
            extract_unsigned_bits(
                packed,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_SELL_OPEN_STEPS_BIT_OFFSET,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_SELL_OPEN_STEPS_BIT_WIDTH,
            )
            .unwrap(),
            U256::from(0x66_u64)
        );
        assert_eq!(
            extract_unsigned_bits(
                packed,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_EPOCH_BIT_OFFSET,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_EPOCH_BIT_WIDTH,
            )
            .unwrap(),
            U256::ONE
        );
        assert_eq!(
            extract_unsigned_bits(
                packed,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_INITIALIZED_BIT_OFFSET,
                schema::STORAGE_PATHS_TRADING_ACCOUNT_ORDER_RISK_PACKING_INITIALIZED_BIT_WIDTH,
            )
            .unwrap(),
            U256::ONE
        );

        let signed = word_str("0x00000000000000000000000000000000000000000000fffe0000000000000000");
        assert_eq!(
            extract_signed_bytes(
                signed,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FEE_BPS_BYTE_OFFSET,
                schema::STORAGE_PATHS_ORDERS_MARKET_BOOK_PACKING_ORDER_METADATA_FEE_BPS_BYTE_WIDTH,
            )
            .unwrap(),
            I256::try_from(-2_i64).unwrap()
        );

        assert!(extract_unsigned_bits(U256::ZERO, 256, 1).is_err());
        assert!(extract_unsigned_bits(U256::ZERO, 255, 2).is_err());
        assert!(extract_unsigned_bits(U256::ZERO, 0, 0).is_err());
        assert!(fixed_record_slot(U256::MAX, 1, 2, 0, 2).is_err());
        assert!(packed_dynamic_array_element(U256::ZERO, 0, 0).is_err());
        assert!(reduce_only_presence_slot(0, u16::MAX, u32::MAX).is_err());
        assert!(orders_tick_level_slot(Address::ZERO, u16::MAX, 1 << (u8::BITS * 3)).is_err());
    }

    #[test]
    fn journal_reader_observes_overlay_and_nested_checkpoint_revert() {
        let address = address!("000000000000000000000000000000000000a001");
        let key = U256::from(7);
        let mut db = InMemoryDB::default();
        db.insert_account_info(address, AccountInfo::default());
        db.insert_account_storage(address, key, U256::from(11)).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = alloy_evm::EvmInternals::from_context(&mut context);

        let parent = internals.checkpoint();
        internals.sstore(address, key, U256::from(22)).unwrap();
        let child = internals.checkpoint();
        internals.sstore(address, key, U256::from(33)).unwrap();

        {
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(reader.sload(address, key).unwrap(), U256::from(33));
        }
        internals.checkpoint_revert(child);
        {
            let mut reader = JournalReader::new(&mut internals);
            assert_eq!(reader.sload(address, key).unwrap(), U256::from(22));
        }
        internals.checkpoint_revert(parent);
        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(reader.sload(address, key).unwrap(), U256::from(11));
    }

    #[test]
    fn journal_reader_counts_reads_and_deduplicates_storage_keys() {
        let address = address!("000000000000000000000000000000000000a002");
        let mut db = InMemoryDB::default();
        db.insert_account_storage(address, U256::ZERO, U256::from(5)).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = alloy_evm::EvmInternals::from_context(&mut context);
        let mut reader = JournalReader::new(&mut internals);

        assert_eq!(reader.sload(address, U256::ZERO).unwrap(), U256::from(5));
        assert_eq!(reader.sload(address, U256::ZERO).unwrap(), U256::from(5));
        assert_eq!(
            reader.stats(),
            JournalReadStats { journal_reads: 2, unique_storage_keys: 1, state_access_gas: 2_200 },
        );
    }

    #[test]
    fn journal_reader_observes_a_journaled_code_hash() {
        let address = address!("000000000000000000000000000000000000a006");
        let mut context = EthEvmContext::new(InMemoryDB::default(), Default::default());
        let mut internals = alloy_evm::EvmInternals::from_context(&mut context);
        let code = Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00]));
        let expected = code.hash_slow();
        internals.load_account_mut(address).unwrap().data.set_code(expected, code);

        let mut reader = JournalReader::new(&mut internals);
        assert_eq!(reader.code_hash(address).unwrap(), expected);
        assert_eq!(reader.stats().state_access_gas, WARM_STORAGE_READ_COST);
    }

    #[test]
    fn journal_reader_preserves_precharged_gas_on_database_errors() {
        #[derive(Debug)]
        struct FailingReadDb;

        impl DatabaseRef for FailingReadDb {
            type Error = ErasedError;

            fn basic_ref(&self, _: Address) -> Result<Option<AccountInfo>, Self::Error> {
                Err(ErasedError::new(std::io::Error::other("injected account read failure")))
            }

            fn storage_ref(&self, _: Address, _: U256) -> Result<U256, Self::Error> {
                Err(ErasedError::new(std::io::Error::other("injected storage read failure")))
            }

            fn code_by_hash_ref(&self, _: B256) -> Result<Bytecode, Self::Error> {
                unreachable!()
            }

            fn block_hash_ref(&self, _: u64) -> Result<B256, Self::Error> {
                unreachable!()
            }
        }

        let address = Address::repeat_byte(0xa8);
        for (cached_account, code_hash) in [(false, false), (true, false), (false, true)] {
            let mut db = CacheDB::new(FailingReadDb);
            if cached_account {
                db.insert_account_info(address, AccountInfo::default());
            }
            let mut context = EthEvmContext::new(db, Default::default());
            let mut internals = alloy_evm::EvmInternals::from_context(&mut context);
            let meter = GasMeter::new(10_000);
            let mut reader = JournalReader::with_gas_meter(&mut internals, &meter);

            for attempts in 1..=2 {
                let error = if code_hash {
                    reader.code_hash(address).unwrap_err()
                } else {
                    reader.sload(address, U256::ZERO).unwrap_err()
                };
                assert!(error.to_string().contains(if cached_account {
                    "injected storage read failure"
                } else {
                    "injected account read failure"
                }));
                assert_eq!(
                    reader.stats(),
                    JournalReadStats {
                        journal_reads: attempts,
                        unique_storage_keys: u64::from(!code_hash),
                        state_access_gas: attempts * WARM_STORAGE_READ_COST,
                    },
                );
                assert_eq!(
                    meter.used.get(),
                    reader.state_access_gas + reader.unique_storage_keys()
                );
                assert!(!meter.is_exhausted());
            }
        }
    }

    #[test]
    fn journal_reader_charges_existing_warmness_with_a_tight_budget() {
        let address = address!("000000000000000000000000000000000000a005");
        let mut db = InMemoryDB::default();
        db.insert_account_storage(address, U256::ZERO, U256::from(5)).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = alloy_evm::EvmInternals::from_context(&mut context);
        internals.sload(address, U256::ZERO).unwrap();
        let meter = GasMeter::new(WARM_STORAGE_READ_COST + 1);
        let mut reader = JournalReader::with_gas_meter(&mut internals, &meter);

        assert_eq!(reader.sload(address, U256::ZERO).unwrap(), U256::from(5));
        assert_eq!(
            reader.stats(),
            JournalReadStats {
                journal_reads: 1,
                unique_storage_keys: 1,
                state_access_gas: WARM_STORAGE_READ_COST,
            },
        );
    }

    #[test]
    fn journal_reader_preflights_cold_state_before_loading_it() {
        let address = address!("000000000000000000000000000000000000a007");
        let mut db = InMemoryDB::default();
        db.insert_account_storage(address, U256::ZERO, U256::from(5)).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut internals = alloy_evm::EvmInternals::from_context(&mut context);

        let meter = GasMeter::new(COLD_SLOAD_COST);
        let mut reader = JournalReader::with_gas_meter(&mut internals, &meter);
        assert!(reader.sload(address, U256::ZERO).is_err());
        drop(reader);
        let Err(error) = internals.load_account_mut_skip_cold_load(address, true) else {
            panic!("insufficient-gas sload warmed the account")
        };
        assert!(error.is_cold_load_skipped());

        let meter = GasMeter::new(COLD_ACCOUNT_ACCESS_COST - 1);
        let mut reader = JournalReader::with_gas_meter(&mut internals, &meter);
        assert!(reader.code_hash(address).is_err());
        drop(reader);
        let Err(error) = internals.load_account_mut_skip_cold_load(address, true) else {
            panic!("insufficient-gas code-hash read warmed the account")
        };
        assert!(error.is_cold_load_skipped());
    }

    #[test]
    fn journal_reader_native_sload_makes_the_following_evm_sload_warm() {
        let cold = execute_warmness_probe(false);
        let warm = execute_warmness_probe(true);

        assert_eq!(cold.output().unwrap(), warm.output().unwrap());
        assert_eq!(warm.output().unwrap().as_ref(), &U256::from(7).to_be_bytes::<32>());
        assert_eq!(cold.tx_gas_used() - warm.tx_gas_used(), 2_000);
    }

    fn execute_warmness_probe(native_read: bool) -> revm::context::result::ExecutionResult {
        let contract = address!("000000000000000000000000000000000000a003");
        let sender = address!("000000000000000000000000000000000000a004");
        let precompile = address!("000000000000000000000000000000000000f101");
        let mut db = InMemoryDB::default();
        db.insert_account_info(
            contract,
            AccountInfo {
                code: Some(Bytecode::new_raw(Bytes::from(warmness_probe_code(precompile)))),
                ..Default::default()
            },
        );
        db.insert_account_storage(contract, U256::ZERO, U256::from(7)).unwrap();
        db.insert_account_info(
            sender,
            AccountInfo { balance: U256::from(1_000_000_000_u64), ..Default::default() },
        );

        let mut precompiles = PrecompilesMap::from_static(Precompiles::osaka());
        precompiles.apply_precompile(&precompile, move |_| {
            Some(DynPrecompile::new_stateful(
                PrecompileId::Custom(Cow::Borrowed("journal-reader-warmness")),
                move |mut input| {
                    if native_read {
                        let caller = input.caller;
                        JournalReader::new(input.internals_mut())
                            .sload(caller, U256::ZERO)
                            .unwrap();
                    }
                    Ok(PrecompileOutput::new(0, Bytes::new(), input.reservoir))
                },
            ))
        });
        let mut evm = EthEvmBuilder::new(db, EvmEnv::default()).precompiles(precompiles).build();
        evm.transact(
            TxEnv::builder()
                .caller(sender)
                .kind(TxKind::Call(contract))
                .gas_limit(1_000_000)
                .build()
                .unwrap(),
        )
        .unwrap()
        .result
    }

    fn warmness_probe_code(precompile: Address) -> Vec<u8> {
        let mut code = vec![0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x73];
        code.extend_from_slice(precompile.as_slice());
        code.extend([
            0x61, 0xff, 0xff, 0xfa, 0x50, 0x60, 0, 0x54, 0x60, 0, 0x52, 0x60, 0x20, 0x60, 0, 0xf3,
        ]);
        code
    }

    fn slot_vectors() -> &'static Value {
        static VECTORS: OnceLock<Value> = OnceLock::new();
        VECTORS.get_or_init(|| serde_json::from_slice(SLOT_VECTOR_BYTES).unwrap())
    }

    fn schema_word(value: [u8; 32]) -> U256 {
        U256::from_be_bytes(value)
    }

    fn word(value: &Value) -> U256 {
        word_str(value.as_str().unwrap())
    }

    fn word_str(value: &str) -> U256 {
        let bytes = hex::decode(value.strip_prefix("0x").unwrap()).unwrap();
        U256::from_be_slice(&bytes)
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(revm::precompile::crypto().sha256(bytes))
    }
}
