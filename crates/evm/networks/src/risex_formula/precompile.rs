//! Feature-gated RISEx risk-formula precompile shell.

use std::borrow::Cow;

use alloy_evm::precompiles::{DynPrecompile, PrecompileInput};
use alloy_primitives::{Address, Bytes, address};
#[cfg(feature = "risex-risk-precompile")]
use alloy_primitives::{B256, I256, U256, b256};
use revm::precompile::{PrecompileHalt, PrecompileId, PrecompileOutput, PrecompileResult};

use super::{InvocationMetadata, Request, Response, Status, begin_invocation, submit_invocation};
#[cfg(feature = "risex-risk-precompile")]
use super::{
    abi::{SUPPORTED_LOADER_VERSION, SUPPORTED_OPERATION_SET_VERSION},
    formula::specialized::{AggregateOutputs, SpecializedEvaluator},
    loader::{LoadProgress, LoadRowsError, MarketRow, load_rows_profiled, schema_generated},
    metrics::{ClockSet, InvocationRecorder},
    storage::{
        GasMeter, JournalReadStats, JournalReader, extract_unsigned_bytes, formula_descriptor_slot,
    },
};
use crate::risex_formula::metrics::Phase;

/// Address reserved for the RISEx risk-formula precompile.
pub const RISEX_RISK_FORMULA_ADDRESS: Address =
    address!("000000000000000000000000000000000000f100");

/// ID for the RISEx risk-formula precompile.
static PRECOMPILE_ID_RISEX_RISK_FORMULA: PrecompileId =
    PrecompileId::Custom(Cow::Borrowed("risex-risk-formula"));

#[cfg(feature = "risex-risk-precompile")]
const SPECIALIZED_INSTRUCTIONS_PER_ROW: u64 = 43;
#[cfg(feature = "risex-risk-precompile")]
const RESPONSE_WORDS: u64 = 5;
#[cfg(feature = "risex-risk-precompile")]
const FIXED_SPECIALIZED_WORK_UNITS: u64 = 1 + RESPONSE_WORDS;
#[cfg(feature = "risex-risk-precompile")]
const SUPPORTED_LOADER_SCHEMA_HASH: B256 = B256::new(schema_generated::LOADER_SCHEMA_HASH);
#[cfg(feature = "risex-risk-precompile")]
const SPECIALIZED_FORMULA_BLOB_CODE_HASH: B256 =
    b256!("14afad781b4af3cb77cbe7dc02cdeef706a124c43c343e8983abf078f9483b82");

/// Returns the stateful RISEx risk-formula precompile shell.
pub fn risk_formula_precompile() -> DynPrecompile {
    DynPrecompile::new_stateful(
        PRECOMPILE_ID_RISEX_RISK_FORMULA.clone(),
        risk_formula_precompile_call,
    )
}

fn risk_formula_precompile_call(input: PrecompileInput<'_>) -> PrecompileResult {
    if !is_valid_static_risex_formula_call(&input) {
        return Ok(PrecompileOutput::halt(
            PrecompileHalt::Other("RISEx risk formula requires static execution".into()),
            input.reservoir,
        ));
    }

    #[cfg(feature = "risex-risk-precompile")]
    let mut input = input;

    #[cfg(feature = "risex-risk-precompile")]
    let gas_meter = {
        let meter = GasMeter::new(input.gas);
        if !meter.charge(FIXED_SPECIALIZED_WORK_UNITS) {
            return Ok(PrecompileOutput::halt(PrecompileHalt::OutOfGas, input.reservoir));
        }
        meter
    };

    let mut metrics = begin_invocation();
    let request = metrics.measure_phase(Phase::RequestValidation, || Request::decode(input.data));
    let mut metadata = InvocationMetadata::default();
    #[cfg(not(feature = "risex-risk-precompile"))]
    let gas_used = 0;
    #[cfg(feature = "risex-risk-precompile")]
    let mut gas_used = FIXED_SPECIALIZED_WORK_UNITS;
    let output = match request {
        Ok(request) => {
            #[cfg(not(feature = "risex-risk-precompile"))]
            {
                metadata.operation = Some(3);
            }
            #[cfg(feature = "risex-risk-precompile")]
            {
                let result = execute_specialized(&mut input, &request, &mut metrics, &gas_meter);
                metadata = result.invocation_metadata();
                gas_used = result.gas_used();
                if gas_meter.is_exhausted() || gas_used > input.gas {
                    return Ok(PrecompileOutput::halt(PrecompileHalt::OutOfGas, input.reservoir));
                }
                let response = metrics.measure_phase(Phase::OutputEncoding, || {
                    result.response.unwrap_or_else(|| Response::with_status(result.status)).encode()
                });
                Bytes::copy_from_slice(&response)
            }
            #[cfg(not(feature = "risex-risk-precompile"))]
            {
                metadata.status = Status::Unavailable as u8;
                Bytes::new()
            }
        }
        Err(status) => {
            metadata.operation = supported_operation_wire_code(input.data);
            metadata.status = status as u8;
            let response = metrics
                .measure_phase(Phase::OutputEncoding, || Response::with_status(status).encode());
            Bytes::copy_from_slice(&response)
        }
    };

    submit_invocation(metrics, metadata);
    Ok(PrecompileOutput::new(gas_used, output, input.reservoir))
}

#[cfg(feature = "risex-risk-precompile")]
struct SpecializedCallResult {
    response: Option<Response>,
    status: Status,
    stats: JournalReadStats,
    active_markets: u32,
    projected_chunks: u32,
    work_units: u64,
}

#[cfg(feature = "risex-risk-precompile")]
impl SpecializedCallResult {
    const fn invocation_metadata(&self) -> InvocationMetadata {
        InvocationMetadata {
            operation: Some(3),
            status: self.status as u8,
            journal_reads: self.stats.journal_reads,
            unique_storage_keys: self.stats.unique_storage_keys,
            active_markets: self.active_markets,
            projected_chunks: self.projected_chunks,
            work_units: self.work_units,
        }
    }

    const fn gas_used(&self) -> u64 {
        self.stats
            .state_access_gas
            .checked_add(self.work_units)
            .expect("loader bounds keep specialized gas within u64")
    }
}

#[cfg(feature = "risex-risk-precompile")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AttemptProgress {
    load: LoadProgress,
    evaluation: EvaluationProgress,
}

#[cfg(feature = "risex-risk-precompile")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct EvaluationProgress {
    evaluator_invocations: u32,
    reducer_operations: u64,
}

#[cfg(feature = "risex-risk-precompile")]
impl EvaluationProgress {
    fn begin_evaluator(&mut self) -> Result<(), Status> {
        self.evaluator_invocations =
            self.evaluator_invocations.checked_add(1).ok_or(Status::BoundExceeded)?;
        Ok(())
    }

    fn observe_reducer_operations(&mut self, operations: u8) -> Result<(), Status> {
        self.reducer_operations = self
            .reducer_operations
            .checked_add(u64::from(operations))
            .ok_or(Status::BoundExceeded)?;
        Ok(())
    }
}

#[cfg(feature = "risex-risk-precompile")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkUnitOverflow {
    completed: u64,
}

#[cfg(feature = "risex-risk-precompile")]
fn evaluate_and_reduce_row<C: ClockSet>(
    metrics: &mut InvocationRecorder<C>,
    progress: &mut EvaluationProgress,
    gas_meter: &GasMeter,
    aggregate: &mut AggregateOutputs,
    target: &mut [U256; 2],
    target_market_id: u16,
    row: MarketRow,
) -> Result<(), Status> {
    progress.begin_evaluator()?;
    if !gas_meter.charge(1 + SPECIALIZED_INSTRUCTIONS_PER_ROW) {
        return Err(Status::StateLoadError);
    }
    let outputs = metrics
        .measure_phase(Phase::FormulaEvaluation, || SpecializedEvaluator::evaluate(&row))
        .map_err(|_| Status::ArithmeticError)?;
    let reduction =
        metrics.measure_phase(Phase::OrderedReduction, || aggregate.reduce_observed(outputs));
    let counter_result = progress.observe_reducer_operations(reduction.operations);
    if !gas_meter.charge(u64::from(reduction.operations)) {
        return Err(Status::StateLoadError);
    }
    if reduction.result.is_err() {
        return Err(Status::ArithmeticError);
    }
    counter_result?;
    if row.market_id == target_market_id {
        *target = [row.effective_buy_order_size, row.effective_sell_order_size];
    }
    Ok(())
}

#[cfg(feature = "risex-risk-precompile")]
fn finish_specialized_attempt(
    outcome: Result<Response, Status>,
    progress: AttemptProgress,
    stats: JournalReadStats,
) -> SpecializedCallResult {
    let work = deterministic_work_units(&progress, stats.unique_storage_keys);
    let (work_units, overflowed) = match work {
        Ok(work_units) => (work_units, false),
        Err(error) => (error.completed, true),
    };
    match outcome {
        Ok(response) if !overflowed => SpecializedCallResult {
            response: Some(response),
            status: Status::Ok,
            stats,
            active_markets: progress.load.rows_started,
            projected_chunks: progress.load.projected_chunks,
            work_units,
        },
        Ok(_) => SpecializedCallResult {
            response: None,
            status: Status::BoundExceeded,
            stats,
            active_markets: progress.load.rows_started,
            projected_chunks: progress.load.projected_chunks,
            work_units,
        },
        Err(status) => SpecializedCallResult {
            response: None,
            status,
            stats,
            active_markets: progress.load.rows_started,
            projected_chunks: progress.load.projected_chunks,
            work_units,
        },
    }
}

#[cfg(feature = "risex-risk-precompile")]
fn execute_specialized<C: ClockSet>(
    input: &mut PrecompileInput<'_>,
    request: &Request,
    metrics: &mut InvocationRecorder<C>,
    gas_meter: &GasMeter,
) -> SpecializedCallResult {
    let caller = input.caller;
    let mut reader = JournalReader::with_gas_meter(input.internals_mut(), gas_meter);
    let mut progress = AttemptProgress::default();
    let response = (|| -> Result<Response, Status> {
        validate_request_compatibility(request)?;
        metrics.measure_phase(Phase::DescriptorFormulaLoad, || {
            load_and_validate_descriptor(&mut reader, caller)
        })?;
        let base_balance = I256::from_raw(request.base_balance);
        let mut aggregate = AggregateOutputs::new(base_balance);
        let mut target = [U256::ZERO; 2];
        let result = load_rows_profiled(
            &mut reader,
            caller,
            request,
            metrics,
            &mut progress.load,
            |metrics, row| {
                evaluate_and_reduce_row(
                    metrics,
                    &mut progress.evaluation,
                    gas_meter,
                    &mut aggregate,
                    &mut target,
                    request.target_market_id,
                    row,
                )
            },
        );
        preserve_specialized_stream_error(result)?;
        Ok(Response {
            status: Status::Ok,
            cross_balance: aggregate.cross_balance.into_raw(),
            total_cross_initial_margin: aggregate.total_initial_margin,
            effective_target_buy_size: target[0],
            effective_target_sell_size: target[1],
        })
    })();
    let _ = gas_meter.charge(u64::from(progress.load.projected_chunks));
    finish_specialized_attempt(response, progress, reader.stats())
}

#[cfg(feature = "risex-risk-precompile")]
const fn preserve_specialized_stream_error(
    result: Result<(), LoadRowsError<Status>>,
) -> Result<(), Status> {
    match result {
        Ok(()) => Ok(()),
        Err(LoadRowsError::Loader(error)) => Err(error.status()),
        Err(LoadRowsError::Sink(status)) => Err(status),
    }
}

#[cfg(feature = "risex-risk-precompile")]
fn validate_request_compatibility(request: &Request) -> Result<(), Status> {
    if request.expected_loader_version != SUPPORTED_LOADER_VERSION {
        return Err(Status::UnsupportedLoader);
    }
    if request.expected_loader_schema_hash != SUPPORTED_LOADER_SCHEMA_HASH {
        return Err(Status::UnsupportedLoaderSchema);
    }
    if request.expected_operation_set_version != SUPPORTED_OPERATION_SET_VERSION {
        return Err(Status::UnsupportedOperationSet);
    }
    Ok(())
}

#[cfg(feature = "risex-risk-precompile")]
fn load_and_validate_descriptor(
    reader: &mut JournalReader<'_, '_>,
    caller: Address,
) -> Result<(), Status> {
    let packed =
        reader.sload(caller, formula_descriptor_slot()).map_err(|_| Status::StateLoadError)?;
    let blob_word = extract_unsigned_bytes(
        packed,
        schema_generated::STORAGE_DIRECT_ARENAS_RISK_FORMULA_REGISTRY_DIRECT_BASE_WORD0_BLOB_BYTE_OFFSET,
        schema_generated::STORAGE_DIRECT_ARENAS_RISK_FORMULA_REGISTRY_DIRECT_BASE_WORD0_BLOB_BYTE_WIDTH,
    )
    .map_err(|_| Status::StateLoadError)?;
    let blob_bytes = blob_word.to_be_bytes::<32>();
    let blob = Address::from_slice(&blob_bytes[12..]);
    if blob.is_zero() {
        return Err(Status::FormulaInactive);
    }
    let epoch = extract_unsigned_bytes(
        packed,
        schema_generated::STORAGE_DIRECT_ARENAS_RISK_FORMULA_REGISTRY_DIRECT_BASE_WORD0_FORMULA_EPOCH_BYTE_OFFSET,
        schema_generated::STORAGE_DIRECT_ARENAS_RISK_FORMULA_REGISTRY_DIRECT_BASE_WORD0_FORMULA_EPOCH_BYTE_WIDTH,
    )
    .map_err(|_| Status::StateLoadError)?;
    let used_bits = (schema_generated::STORAGE_DIRECT_ARENAS_RISK_FORMULA_REGISTRY_DIRECT_BASE_WORD0_FORMULA_EPOCH_BYTE_OFFSET
        + schema_generated::STORAGE_DIRECT_ARENAS_RISK_FORMULA_REGISTRY_DIRECT_BASE_WORD0_FORMULA_EPOCH_BYTE_WIDTH)
        * u8::BITS as u64;
    if epoch.is_zero() || !(packed >> used_bits).is_zero() {
        return Err(Status::FormulaInvalid);
    }
    if reader.code_hash(blob).map_err(|_| Status::StateLoadError)?
        != SPECIALIZED_FORMULA_BLOB_CODE_HASH
    {
        return Err(Status::BlobCodeHashMismatch);
    }
    Ok(())
}

#[cfg(feature = "risex-risk-precompile")]
fn deterministic_work_units(
    progress: &AttemptProgress,
    unique_journal_state_reads: u64,
) -> Result<u64, WorkUnitOverflow> {
    fn add(completed: u64, value: u64) -> Result<u64, WorkUnitOverflow> {
        completed.checked_add(value).ok_or(WorkUnitOverflow { completed })
    }

    let mut completed = 1_u64;
    completed = add(completed, unique_journal_state_reads)?;
    completed = add(completed, u64::from(progress.load.rows_started))?;
    completed = add(completed, u64::from(progress.load.projected_chunks))?;
    let evaluator_units = u64::from(progress.evaluation.evaluator_invocations)
        .checked_mul(SPECIALIZED_INSTRUCTIONS_PER_ROW)
        .ok_or(WorkUnitOverflow { completed })?;
    completed = add(completed, evaluator_units)?;
    completed = add(completed, progress.evaluation.reducer_operations)?;
    add(completed, RESPONSE_WORDS)
}

fn supported_operation_wire_code(input: &[u8]) -> Option<u8> {
    (input.len() == 160 && input[30] == 3).then_some(3)
}

/// Returns whether the call is a direct, zero-value invocation in a static EVM context.
///
/// A zero-value `CALL` inherited from an enclosing static frame is intentionally accepted: it has
/// the same EVM write prohibition as a direct `STATICCALL`.
fn is_valid_static_risex_formula_call(input: &PrecompileInput<'_>) -> bool {
    input.is_static_call()
        && input.value.is_zero()
        && input.target_address == RISEX_RISK_FORMULA_ADDRESS
        && input.bytecode_address == RISEX_RISK_FORMULA_ADDRESS
}

#[cfg(test)]
mod tests {
    use alloy_evm::{EvmInternals, eth::EthEvmContext, precompiles::PrecompileInput};
    use alloy_primitives::{Address, U256};
    #[cfg(feature = "risex-risk-precompile")]
    use alloy_primitives::{Bytes, address, keccak256};
    use revm::database::EmptyDB;
    #[cfg(feature = "risex-risk-precompile")]
    use revm::{database::InMemoryDB, state::AccountInfo};

    #[cfg(feature = "risex-risk-precompile")]
    use super::{
        AggregateOutputs, AttemptProgress, FIXED_SPECIALIZED_WORK_UNITS, GasMeter, LoadRowsError,
        SPECIALIZED_FORMULA_BLOB_CODE_HASH, SUPPORTED_LOADER_SCHEMA_HASH, evaluate_and_reduce_row,
        execute_specialized, finish_specialized_attempt, preserve_specialized_stream_error,
    };
    use super::{RISEX_RISK_FORMULA_ADDRESS, risk_formula_precompile_call};
    #[cfg(feature = "risex-risk-precompile")]
    use crate::risex_formula::{
        loader::{LoaderError, MarginMode, MarketRow},
        metrics::{ClockSet, InvocationRecorder, MetricsError, Phase, SystemClocks},
        storage::{JournalReadStats, formula_descriptor_slot},
    };

    #[cfg(feature = "risex-risk-precompile")]
    #[derive(Default)]
    struct StepClocks {
        thread: u64,
        process: u64,
        wall: u64,
    }

    #[cfg(feature = "risex-risk-precompile")]
    impl ClockSet for StepClocks {
        fn thread_cpu_ns(&mut self) -> Result<u64, MetricsError> {
            self.thread += 10;
            Ok(self.thread)
        }

        fn process_cpu_ns(&mut self) -> Result<u64, MetricsError> {
            self.process += 100;
            Ok(self.process)
        }

        fn wall_ns(&mut self) -> Result<u64, MetricsError> {
            self.wall += 1_000;
            Ok(self.wall)
        }
    }

    #[test]
    fn malformed_wire_returns_a_canonical_unsupported_abi_response() {
        let mut context = EthEvmContext::new(EmptyDB::default(), Default::default());
        let malformed = [0_u8; 159];

        let output = risk_formula_precompile_call(PrecompileInput {
            data: &malformed,
            gas: 1_000_000,
            reservoir: 0,
            caller: Address::ZERO,
            value: U256::ZERO,
            is_static: true,
            internals: EvmInternals::from_context(&mut context),
            target_address: RISEX_RISK_FORMULA_ADDRESS,
            bytecode_address: RISEX_RISK_FORMULA_ADDRESS,
        })
        .unwrap();

        assert_eq!(output.bytes.len(), 160);
        #[cfg(feature = "risex-risk-precompile")]
        assert_eq!(output.gas_used, FIXED_SPECIALIZED_WORK_UNITS);
        #[cfg(not(feature = "risex-risk-precompile"))]
        assert_eq!(output.gas_used, 0);
        assert_eq!(
            &output.bytes[..32],
            &alloy_primitives::hex::decode(
                "0000000000000000000000000000000000000000000000000000020152534631",
            )
            .unwrap(),
        );

        #[cfg(feature = "risex-risk-precompile")]
        {
            let mut context = EthEvmContext::new(EmptyDB::default(), Default::default());
            let output = risk_formula_precompile_call(PrecompileInput {
                data: &malformed,
                gas: FIXED_SPECIALIZED_WORK_UNITS - 1,
                reservoir: 0,
                caller: Address::ZERO,
                value: U256::ZERO,
                is_static: true,
                internals: EvmInternals::from_context(&mut context),
                target_address: RISEX_RISK_FORMULA_ADDRESS,
                bytecode_address: RISEX_RISK_FORMULA_ADDRESS,
            })
            .unwrap();
            assert_eq!(
                output.status,
                revm::precompile::PrecompileStatus::Halt(
                    revm::precompile::PrecompileHalt::OutOfGas,
                ),
            );
        }
    }

    #[test]
    fn nonzero_value_halts_even_in_a_direct_static_context() {
        let mut context = EthEvmContext::new(EmptyDB::default(), Default::default());
        let input = [0_u8; 160];

        let output = risk_formula_precompile_call(PrecompileInput {
            data: &input,
            gas: 1_000_000,
            reservoir: 0,
            caller: Address::ZERO,
            value: U256::from(1),
            is_static: true,
            internals: EvmInternals::from_context(&mut context),
            target_address: RISEX_RISK_FORMULA_ADDRESS,
            bytecode_address: RISEX_RISK_FORMULA_ADDRESS,
        })
        .unwrap();

        assert!(output.is_halt());
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_provider_returns_exact_response_for_an_empty_snapshot() {
        let caller = address!("000000000000000000000000000000000000c001");
        let runtime = specialized_runtime();
        assert_eq!(keccak256(&runtime), SPECIALIZED_FORMULA_BLOB_CODE_HASH);
        let db = specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH);
        let mut request = specialized_request();
        request[64..96].copy_from_slice(&U256::from(7).to_be_bytes::<32>());
        let output = call_specialized(db, caller, &request);

        assert_eq!(output.len(), 160);
        assert_eq!(output[26], 0);
        assert_eq!(&output[18..22], &1_u32.to_be_bytes());
        assert_eq!(U256::from_be_slice(&output[32..64]), U256::from(7));
        assert!(output[64..].iter().all(|byte| *byte == 0));
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_provider_charges_native_work_and_rejects_insufficient_gas() {
        let caller = address!("000000000000000000000000000000000000c001");
        let request = specialized_request();
        let ample = call_specialized_with_gas(
            specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH),
            caller,
            &request,
            1_000_000,
        );

        assert!(ample.gas_used > 0);
        let exact = call_specialized_with_gas(
            specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH),
            caller,
            &request,
            ample.gas_used,
        );
        assert!(!exact.is_halt());
        assert_eq!(exact.gas_used, ample.gas_used);

        let insufficient = call_specialized_with_gas(
            specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH),
            caller,
            &request,
            ample.gas_used - 1,
        );
        assert_eq!(
            insufficient.status,
            revm::precompile::PrecompileStatus::Halt(revm::precompile::PrecompileHalt::OutOfGas),
        );

        let fixed_work_only_budget = call_specialized_with_gas(
            specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH),
            caller,
            &request,
            FIXED_SPECIALIZED_WORK_UNITS,
        );
        assert!(fixed_work_only_budget.is_halt());

        let decoded = crate::risex_formula::Request::decode(&request).unwrap();
        let mut context = EthEvmContext::new(
            specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH),
            Default::default(),
        );
        let mut input = PrecompileInput {
            data: &request,
            gas: FIXED_SPECIALIZED_WORK_UNITS,
            reservoir: 0,
            caller,
            value: U256::ZERO,
            is_static: true,
            internals: EvmInternals::from_context(&mut context),
            target_address: RISEX_RISK_FORMULA_ADDRESS,
            bytecode_address: RISEX_RISK_FORMULA_ADDRESS,
        };
        let gas_meter = GasMeter::new(FIXED_SPECIALIZED_WORK_UNITS);
        assert!(gas_meter.charge(FIXED_SPECIALIZED_WORK_UNITS));
        let mut metrics = InvocationRecorder::begin(false, None::<SystemClocks>);
        let result = execute_specialized(&mut input, &decoded, &mut metrics, &gas_meter);

        assert!(gas_meter.is_exhausted());
        assert_eq!(result.stats.journal_reads, 1);
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_provider_returns_typed_statuses_without_partial_output() {
        let caller = address!("000000000000000000000000000000000000c001");
        let mut inactive_db = InMemoryDB::default();
        inactive_db.insert_account_info(caller, AccountInfo::default());
        inactive_db
            .insert_account_storage(caller, formula_descriptor_slot(), U256::from(2) << 160_usize)
            .unwrap();
        let inactive = call_specialized(inactive_db, caller, &specialized_request());
        assert_status_only(&inactive, crate::risex_formula::Status::FormulaInactive);

        let unknown_hash = keccak256([0]);
        let unknown_db = specialized_descriptor_db(caller, unknown_hash);
        let mismatch = call_specialized(unknown_db, caller, &specialized_request());
        assert_status_only(&mismatch, crate::risex_formula::Status::BlobCodeHashMismatch);

        let blob =
            U256::from_be_slice(address!("000000000000000000000000000000000000b10b").as_slice());
        for packed in [blob, blob | U256::from(1) << 160 | U256::from(1) << 192] {
            let mut malformed_db =
                specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH);
            malformed_db.insert_account_storage(caller, formula_descriptor_slot(), packed).unwrap();
            let malformed = call_specialized(malformed_db, caller, &specialized_request());
            assert_status_only(&malformed, crate::risex_formula::Status::FormulaInvalid);
        }

        let mut unsupported_loader = specialized_request();
        unsupported_loader[19..23].copy_from_slice(&2_u32.to_be_bytes());
        let mut db = InMemoryDB::default();
        db.insert_account_info(caller, AccountInfo::default());
        let unsupported = call_specialized(db, caller, &unsupported_loader);
        assert_status_only(&unsupported, crate::risex_formula::Status::UnsupportedLoader);
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_provider_accepts_only_the_shadow_operation() {
        let caller = address!("000000000000000000000000000000000000c001");
        let db = specialized_descriptor_db(caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH);
        let output = call_specialized(db, caller, &specialized_request());
        assert_eq!(output.len(), 160);
        assert_eq!(output[26], crate::risex_formula::Status::Ok as u8);

        for unsupported_operation in [1, 2] {
            let mut request = specialized_request();
            request[30] = unsupported_operation;
            let output = call_specialized(InMemoryDB::default(), caller, &request);
            assert_status_only(&output, crate::risex_formula::Status::UnsupportedAbi);
        }
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn populated_success_records_exclusive_loader_subphases_and_reconciled_work() {
        let (db, caller, request_bytes) = populated_specialized_fixture();
        let request = crate::risex_formula::Request::decode(&request_bytes).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut input = PrecompileInput {
            data: &request_bytes,
            gas: 1_000_000,
            reservoir: 0,
            caller,
            value: U256::ZERO,
            is_static: true,
            internals: EvmInternals::from_context(&mut context),
            target_address: RISEX_RISK_FORMULA_ADDRESS,
            bytecode_address: RISEX_RISK_FORMULA_ADDRESS,
        };
        let mut metrics = InvocationRecorder::begin(true, StepClocks::default());
        let gas_meter = GasMeter::new(u64::MAX);

        let result = execute_specialized(&mut input, &request, &mut metrics, &gas_meter);
        let expected_work_units = result.work_units;
        assert_eq!(result.stats.journal_reads, 27);
        let encoded = metrics.measure_phase(Phase::OutputEncoding, || {
            result
                .response
                .unwrap_or_else(|| crate::risex_formula::Response::with_status(result.status))
                .encode()
        });
        let record = metrics.finish(result.invocation_metadata()).unwrap().unwrap();
        let record = serde_json::to_value(record).unwrap();

        assert_eq!(encoded.len(), 160);
        assert_eq!(encoded[26], crate::risex_formula::Status::Ok as u8);
        // The ten-nanosecond StepClock pins 20 key operations and 25 loader
        // reads. Two additional journal reads load the packed descriptor root
        // and its blob account, while the outer row timer subtracts every nested phase.
        assert_eq!(record["key_derivation_cpu_ns"], 200);
        assert_eq!(record["journal_load_cpu_ns"], 250);
        assert_eq!(record["row_materialization_cpu_ns"], 480);
        assert_eq!(record["formula_evaluation_cpu_ns"], 10);
        assert_eq!(record["ordered_reduction_cpu_ns"], 10);
        let phase_sum = [
            "request_validation_cpu_ns",
            "descriptor_formula_load_cpu_ns",
            "key_derivation_cpu_ns",
            "journal_load_cpu_ns",
            "row_materialization_cpu_ns",
            "formula_evaluation_cpu_ns",
            "ordered_reduction_cpu_ns",
            "output_encoding_cpu_ns",
        ]
        .into_iter()
        .map(|phase| record[phase].as_u64().unwrap())
        .sum::<u64>();
        assert_eq!(record["accounted_phase_cpu_ns"], phase_sum);
        assert_eq!(record["active_markets"], 1);
        assert_eq!(record["projected_chunks"], 0);
        assert_eq!(record["work_units"].as_u64().unwrap(), expected_work_units);
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn late_loader_failure_records_completed_loader_phases_without_partial_output() {
        let (db, caller, mut request_bytes) = populated_specialized_fixture();
        request_bytes[11..13].copy_from_slice(&2_u16.to_be_bytes());
        request_bytes[96..128].copy_from_slice(&U256::ONE.to_be_bytes::<32>());
        let request = crate::risex_formula::Request::decode(&request_bytes).unwrap();
        let mut context = EthEvmContext::new(db, Default::default());
        let mut input = PrecompileInput {
            data: &request_bytes,
            gas: 1_000_000,
            reservoir: 0,
            caller,
            value: U256::ZERO,
            is_static: true,
            internals: EvmInternals::from_context(&mut context),
            target_address: RISEX_RISK_FORMULA_ADDRESS,
            bytecode_address: RISEX_RISK_FORMULA_ADDRESS,
        };
        let mut metrics = InvocationRecorder::begin(true, StepClocks::default());
        let gas_meter = GasMeter::new(u64::MAX);

        let result = execute_specialized(&mut input, &request, &mut metrics, &gas_meter);
        assert_eq!(result.stats.journal_reads, 28);
        let encoded = metrics.measure_phase(Phase::OutputEncoding, || {
            result
                .response
                .unwrap_or_else(|| crate::risex_formula::Response::with_status(result.status))
                .encode()
        });
        let record = metrics.finish(result.invocation_metadata()).unwrap().unwrap();
        let record = serde_json::to_value(record).unwrap();
        assert_status_only(&encoded, crate::risex_formula::Status::Unavailable);
        assert_eq!(record["status"], crate::risex_formula::Status::Unavailable as u8);
        assert_eq!(record["active_markets"], 1);
        assert!(record["work_units"].as_u64().unwrap() > 0);
        // The late missing-price exit still completed 21 key operations, 26
        // loader reads, and all pure row-building control work before failing.
        assert_eq!(record["key_derivation_cpu_ns"], 210);
        assert_eq!(record["journal_load_cpu_ns"], 260);
        assert_eq!(record["row_materialization_cpu_ns"], 480);
        assert_eq!(record["formula_evaluation_cpu_ns"], 0);
        assert_eq!(record["ordered_reduction_cpu_ns"], 0);
        assert_phase_total_reconciles(&record);
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn late_loader_failure_keeps_completed_attempt_work_out_of_the_error_response() {
        let mut progress = AttemptProgress::default();
        progress.load.begin_row().unwrap();
        progress.load.observe_projected_chunk().unwrap();
        progress.evaluation.begin_evaluator().unwrap();
        progress.evaluation.observe_reducer_operations(3).unwrap();

        let result = finish_specialized_attempt(
            Err(crate::risex_formula::Status::Unavailable),
            progress,
            JournalReadStats { journal_reads: 17, unique_storage_keys: 12, state_access_gas: 0 },
        );

        assert_eq!(result.status, crate::risex_formula::Status::Unavailable);
        assert_eq!(result.active_markets, 1);
        assert_eq!(result.projected_chunks, 1);
        assert_eq!(result.work_units, 66);
        assert_status_only(
            &result
                .response
                .unwrap_or_else(|| crate::risex_formula::Response::with_status(result.status))
                .encode(),
            crate::risex_formula::Status::Unavailable,
        );
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn late_evaluator_failure_counts_the_failed_invocation_but_no_reducer() {
        let mut metrics = InvocationRecorder::begin(true, StepClocks::default());
        let mut progress = AttemptProgress::default();
        let mut aggregate = AggregateOutputs::new(alloy_primitives::I256::ZERO);
        let mut target = [U256::ZERO; 2];
        let gas_meter = GasMeter::new(u64::MAX);

        progress.load.begin_row().unwrap();
        evaluate_and_reduce_row(
            &mut metrics,
            &mut progress.evaluation,
            &gas_meter,
            &mut aggregate,
            &mut target,
            1,
            zero_row(1),
        )
        .unwrap();
        progress.load.begin_row().unwrap();
        let mut invalid = zero_row(2);
        invalid.effective_position_size = 1;
        invalid.effective_leverage_wad = U256::ZERO;
        invalid.mark_price = U256::ONE;
        let status = evaluate_and_reduce_row(
            &mut metrics,
            &mut progress.evaluation,
            &gas_meter,
            &mut aggregate,
            &mut target,
            1,
            invalid,
        )
        .unwrap_err();
        let result = finish_specialized_attempt(
            Err(status),
            progress,
            JournalReadStats { journal_reads: 17, unique_storage_keys: 12, state_access_gas: 0 },
        );

        assert_eq!(result.status, crate::risex_formula::Status::ArithmeticError);
        assert_eq!(result.active_markets, 2);
        assert_eq!(result.projected_chunks, 0);
        assert_eq!(result.work_units, 108);
        let record = metrics.finish(result.invocation_metadata()).unwrap().unwrap();
        let record = serde_json::to_value(record).unwrap();
        assert_eq!(record["formula_evaluation_cpu_ns"], 20);
        assert_eq!(record["ordered_reduction_cpu_ns"], 10);
        assert_status_only(
            &result
                .response
                .unwrap_or_else(|| crate::risex_formula::Response::with_status(result.status))
                .encode(),
            crate::risex_formula::Status::ArithmeticError,
        );
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn late_reducer_failure_counts_only_reducer_operations_actually_attempted() {
        let mut metrics = InvocationRecorder::begin(true, StepClocks::default());
        let mut progress = AttemptProgress::default();
        let mut aggregate = AggregateOutputs::new(alloy_primitives::I256::MAX);
        let mut target = [U256::ZERO; 2];
        let gas_meter = GasMeter::new(u64::MAX);

        progress.load.begin_row().unwrap();
        evaluate_and_reduce_row(
            &mut metrics,
            &mut progress.evaluation,
            &gas_meter,
            &mut aggregate,
            &mut target,
            1,
            zero_row(1),
        )
        .unwrap();
        progress.load.begin_row().unwrap();
        let mut overflowing = zero_row(2);
        overflowing.projected_settlement_pnl = alloy_primitives::I256::ONE;
        let status = evaluate_and_reduce_row(
            &mut metrics,
            &mut progress.evaluation,
            &gas_meter,
            &mut aggregate,
            &mut target,
            1,
            overflowing,
        )
        .unwrap_err();
        let result = finish_specialized_attempt(
            Err(status),
            progress,
            JournalReadStats { journal_reads: 17, unique_storage_keys: 12, state_access_gas: 0 },
        );

        assert_eq!(result.status, crate::risex_formula::Status::ArithmeticError);
        assert_eq!(result.active_markets, 2);
        assert_eq!(result.work_units, 109);
        let record = metrics.finish(result.invocation_metadata()).unwrap().unwrap();
        let record = serde_json::to_value(record).unwrap();
        assert_eq!(record["formula_evaluation_cpu_ns"], 20);
        assert_eq!(record["ordered_reduction_cpu_ns"], 20);
        assert_status_only(
            &result
                .response
                .unwrap_or_else(|| crate::risex_formula::Response::with_status(result.status))
                .encode(),
            crate::risex_formula::Status::ArithmeticError,
        );
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn attempt_counter_overflow_preserves_completed_work() {
        let mut evaluation = super::EvaluationProgress {
            evaluator_invocations: u32::MAX,
            reducer_operations: u64::MAX,
        };
        assert_eq!(evaluation.begin_evaluator(), Err(crate::risex_formula::Status::BoundExceeded),);
        assert_eq!(evaluation.evaluator_invocations, u32::MAX);
        assert_eq!(
            evaluation.observe_reducer_operations(1),
            Err(crate::risex_formula::Status::BoundExceeded),
        );
        assert_eq!(evaluation.reducer_operations, u64::MAX);

        let overflow = AttemptProgress {
            evaluation: super::EvaluationProgress {
                evaluator_invocations: 0,
                reducer_operations: u64::MAX,
            },
            ..Default::default()
        };
        let successful_overflow = finish_specialized_attempt(
            Ok(crate::risex_formula::Response::with_status(crate::risex_formula::Status::Ok)),
            overflow,
            JournalReadStats::default(),
        );
        assert_eq!(successful_overflow.status, crate::risex_formula::Status::BoundExceeded);
        assert_eq!(successful_overflow.work_units, 1);
        assert!(successful_overflow.response.is_none());

        let earlier_error = finish_specialized_attempt(
            Err(crate::risex_formula::Status::Unavailable),
            overflow,
            JournalReadStats::default(),
        );
        assert_eq!(earlier_error.status, crate::risex_formula::Status::Unavailable);
        assert_eq!(earlier_error.work_units, 1);
        assert!(earlier_error.response.is_none());
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn reducer_arithmetic_error_precedes_simultaneous_counter_overflow() {
        let mut metrics = InvocationRecorder::begin(false, None::<SystemClocks>);
        let mut progress =
            super::EvaluationProgress { evaluator_invocations: 0, reducer_operations: u64::MAX };
        let mut aggregate = AggregateOutputs::new(alloy_primitives::I256::MAX);
        let mut target = [U256::ZERO; 2];
        let mut row = zero_row(2);
        row.projected_settlement_pnl = alloy_primitives::I256::ONE;
        let gas_meter = GasMeter::new(u64::MAX);

        let status = evaluate_and_reduce_row(
            &mut metrics,
            &mut progress,
            &gas_meter,
            &mut aggregate,
            &mut target,
            1,
            row,
        )
        .unwrap_err();

        assert_eq!(status, crate::risex_formula::Status::ArithmeticError);
        assert_eq!(progress.evaluator_invocations, 1);
        assert_eq!(progress.reducer_operations, u64::MAX);
        assert_eq!(aggregate.cross_balance, alloy_primitives::I256::MAX);
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn successful_reduction_still_reports_counter_overflow_as_bound_exceeded() {
        let mut metrics = InvocationRecorder::begin(false, None::<SystemClocks>);
        let mut progress =
            super::EvaluationProgress { evaluator_invocations: 0, reducer_operations: u64::MAX };
        let mut aggregate = AggregateOutputs::new(alloy_primitives::I256::ZERO);
        let mut target = [U256::ZERO; 2];
        let gas_meter = GasMeter::new(u64::MAX);

        let status = evaluate_and_reduce_row(
            &mut metrics,
            &mut progress,
            &gas_meter,
            &mut aggregate,
            &mut target,
            1,
            zero_row(2),
        )
        .unwrap_err();

        assert_eq!(status, crate::risex_formula::Status::BoundExceeded);
        assert_eq!(progress.evaluator_invocations, 1);
        assert_eq!(progress.reducer_operations, u64::MAX);
        assert_eq!(aggregate, AggregateOutputs::new(alloy_primitives::I256::ZERO));
        assert_eq!(target, [U256::ZERO; 2]);
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_stream_retains_only_the_requested_target_outputs() {
        let mut metrics = InvocationRecorder::begin(false, None::<SystemClocks>);
        let mut progress = super::EvaluationProgress::default();
        let mut aggregate = AggregateOutputs::new(alloy_primitives::I256::ZERO);
        let mut target = [U256::ZERO; 2];
        let mut first = zero_row(7);
        first.projected_settlement_pnl = alloy_primitives::I256::ONE;
        first.effective_buy_order_size = U256::from(4);
        first.effective_sell_order_size = U256::from(5);
        first.mark_price = U256::ONE;
        first.effective_leverage_wad = U256::from(3);
        let mut second = zero_row(8);
        second.projected_settlement_pnl = alloy_primitives::I256::ONE;
        second.effective_buy_order_size = U256::from(6);
        second.effective_sell_order_size = U256::from(7);
        second.mark_price = U256::ONE;
        second.effective_leverage_wad = U256::from(7);
        let gas_meter = GasMeter::new(u64::MAX);

        evaluate_and_reduce_row(
            &mut metrics,
            &mut progress,
            &gas_meter,
            &mut aggregate,
            &mut target,
            7,
            first,
        )
        .unwrap();
        evaluate_and_reduce_row(
            &mut metrics,
            &mut progress,
            &gas_meter,
            &mut aggregate,
            &mut target,
            7,
            second,
        )
        .unwrap();

        assert_eq!(target, [U256::from(4), U256::from(5)]);
        assert_eq!(aggregate.cross_balance, alloy_primitives::I256::try_from(2_i128).unwrap());
        assert_eq!(aggregate.total_initial_margin, U256::from(3));
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_stream_preserves_loader_errors_and_classifies_sink_errors() {
        assert_eq!(
            preserve_specialized_stream_error(Err(
                LoadRowsError::Loader(LoaderError::Unavailable,)
            )),
            Err(crate::risex_formula::Status::Unavailable),
        );
        assert_eq!(
            preserve_specialized_stream_error(Err(LoadRowsError::Loader(
                LoaderError::BoundExceeded,
            ))),
            Err(crate::risex_formula::Status::BoundExceeded),
        );
        assert_eq!(
            preserve_specialized_stream_error(Err(LoadRowsError::Loader(LoaderError::Arithmetic,))),
            Err(crate::risex_formula::Status::ArithmeticError),
        );
        assert_eq!(
            preserve_specialized_stream_error(Err(LoadRowsError::Loader(LoaderError::StateLoad,))),
            Err(crate::risex_formula::Status::StateLoadError),
        );
        assert_eq!(
            preserve_specialized_stream_error(Err(LoadRowsError::Sink(
                crate::risex_formula::Status::ArithmeticError,
            ))),
            Err(crate::risex_formula::Status::ArithmeticError),
        );
        assert_eq!(preserve_specialized_stream_error(Ok(())), Ok(()),);
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_runtime() -> Vec<u8> {
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("testdata/portfolio_order_risk_v1.json")).unwrap();
        let runtime = corpus["runtimeHex"].as_str().unwrap();
        alloy_primitives::hex::decode(runtime.strip_prefix("0x").unwrap()).unwrap()
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_request() -> [u8; 160] {
        let mut request = [0_u8; 160];
        request[27] = 1;
        request[30] = 3;
        request[31] = 1;
        request[19..23].copy_from_slice(&1_u32.to_be_bytes());
        request[17..19].copy_from_slice(&1_u16.to_be_bytes());
        request[32..64].copy_from_slice(SUPPORTED_LOADER_SCHEMA_HASH.as_slice());
        request
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn specialized_descriptor_db(
        caller: Address,
        descriptor_hash: alloy_primitives::B256,
    ) -> InMemoryDB {
        let mut db = InMemoryDB::default();
        db.insert_account_info(caller, AccountInfo::default());
        install_specialized_descriptor(&mut db, caller, descriptor_hash);
        db
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn install_specialized_descriptor(
        db: &mut InMemoryDB,
        caller: Address,
        descriptor_hash: alloy_primitives::B256,
    ) {
        let blob = address!("000000000000000000000000000000000000b10b");
        db.insert_account_info(
            blob,
            AccountInfo { code_hash: descriptor_hash, code: None, ..Default::default() },
        );
        let packed = U256::from_be_slice(blob.as_slice()) | (U256::from(1) << 160_usize);
        db.insert_account_storage(caller, formula_descriptor_slot(), packed).unwrap();
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn populated_specialized_fixture() -> (InMemoryDB, Address, [u8; 160]) {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("testdata/effective-market-v1.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "canonical_cross_ready_compact_funding")
            .unwrap();
        let caller: Address = case["addresses"]["caller"].as_str().unwrap().parse().unwrap();
        let mut db = InMemoryDB::default();
        for item in case["journalState"].as_array().unwrap() {
            let account: Address = item["address"].as_str().unwrap().parse().unwrap();
            db.insert_account_info(account, AccountInfo::default());
            db.insert_account_storage(
                account,
                U256::from_str_radix(item["slot"].as_str().unwrap().trim_start_matches("0x"), 16)
                    .unwrap(),
                U256::from_str_radix(item["value"].as_str().unwrap().trim_start_matches("0x"), 16)
                    .unwrap(),
            )
            .unwrap();
        }
        let funding: Address = case["addresses"]["fundingRate"].as_str().unwrap().parse().unwrap();
        let funding_code = revm::bytecode::Bytecode::new_raw(Bytes::from_static(&[0x00]));
        db.insert_account_info(
            funding,
            AccountInfo {
                code_hash: funding_code.hash_slow(),
                code: Some(funding_code),
                ..Default::default()
            },
        );
        install_specialized_descriptor(&mut db, caller, SPECIALIZED_FORMULA_BLOB_CODE_HASH);
        let mut request = specialized_request();
        request[11..13].copy_from_slice(&1_u16.to_be_bytes());
        request[13..17].copy_from_slice(&1_u32.to_be_bytes());
        request[128..160].copy_from_slice(
            &U256::from_str_radix("100000000000000000000000", 10).unwrap().to_be_bytes::<32>(),
        );
        (db, caller, request)
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn zero_row(market_id: u16) -> MarketRow {
        MarketRow {
            market_id,
            margin_mode: MarginMode::Cross,
            effective_position_size: 0,
            effective_position_quote: 0,
            effective_last_funding_payment: 0,
            effective_leverage_wad: U256::ONE,
            effective_isolated_balance: 0,
            projected_settlement_pnl: alloy_primitives::I256::ZERO,
            effective_buy_order_size: U256::ZERO,
            effective_sell_order_size: U256::ZERO,
            effective_order_notional: U256::ZERO,
            mark_price: U256::ZERO,
            accumulated_funding_payment: 0,
        }
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn call_specialized(db: InMemoryDB, caller: Address, request: &[u8; 160]) -> Bytes {
        call_specialized_with_gas(db, caller, request, 1_000_000).bytes
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn call_specialized_with_gas(
        db: InMemoryDB,
        caller: Address,
        request: &[u8; 160],
        gas: u64,
    ) -> revm::precompile::PrecompileOutput {
        let mut context = EthEvmContext::new(db, Default::default());
        risk_formula_precompile_call(PrecompileInput {
            data: request,
            gas,
            reservoir: 0,
            caller,
            value: U256::ZERO,
            is_static: true,
            internals: EvmInternals::from_context(&mut context),
            target_address: RISEX_RISK_FORMULA_ADDRESS,
            bytecode_address: RISEX_RISK_FORMULA_ADDRESS,
        })
        .unwrap()
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn assert_status_only(output: &[u8], status: crate::risex_formula::Status) {
        assert_eq!(output.len(), 160);
        assert_eq!(output[26], status as u8);
        assert!(output[..26].iter().all(|byte| *byte == 0));
        assert_eq!(&output[27..32], &[1, 0x52, 0x53, 0x46, 0x31]);
        assert!(output[32..].iter().all(|byte| *byte == 0));
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn assert_phase_total_reconciles(record: &serde_json::Value) {
        let phase_sum = [
            "request_validation_cpu_ns",
            "descriptor_formula_load_cpu_ns",
            "key_derivation_cpu_ns",
            "journal_load_cpu_ns",
            "row_materialization_cpu_ns",
            "formula_evaluation_cpu_ns",
            "ordered_reduction_cpu_ns",
            "output_encoding_cpu_ns",
        ]
        .into_iter()
        .map(|phase| record[phase].as_u64().unwrap())
        .sum::<u64>();
        assert_eq!(record["accounted_phase_cpu_ns"], phase_sum);
    }
}
