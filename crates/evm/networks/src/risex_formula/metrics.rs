//! Native CPU metrics for RISEx risk-formula invocations.

use std::{
    error::Error,
    fmt,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use cpu_time::{ProcessTime, ThreadTime};
use serde::Serialize;

use super::ProviderMode;

const SCHEMA_VERSION: u16 = 1;
const MAX_INVOCATIONS: usize = 4096;

static METRICS_ENABLED: AtomicBool = AtomicBool::new(false);
static METRICS_BUFFER: Mutex<MetricsBuffer> = Mutex::new(MetricsBuffer::new());

/// A clock or aggregation failure deferred until Forge has completed the test run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricsError {
    Clock(String),
    ClockOverflow(&'static str),
    ClockExhausted,
    BufferOverflow,
    BufferPoisoned,
    InvocationCountOverflow,
    Serialization(String),
    PeakRss(String),
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => write!(f, "native metrics clock failed: {error}"),
            Self::ClockOverflow(clock) => {
                write!(f, "native metrics {clock} timestamp exceeded u64 nanoseconds")
            }
            Self::ClockExhausted => f.write_str("native metrics test clock was exhausted"),
            Self::BufferOverflow => {
                write!(f, "native metrics exceeded the {MAX_INVOCATIONS}-invocation limit")
            }
            Self::BufferPoisoned => f.write_str("native metrics buffer lock was poisoned"),
            Self::InvocationCountOverflow => {
                f.write_str("native metrics invocation count exceeded u32")
            }
            Self::Serialization(error) => {
                write!(f, "failed to serialize native metrics: {error}")
            }
            Self::PeakRss(error) => write!(f, "failed to sample process peak RSS: {error}"),
        }
    }
}

impl Error for MetricsError {}

/// Clock sources used by one invocation recorder.
pub trait ClockSet {
    fn thread_cpu_ns(&mut self) -> Result<u64, MetricsError>;
    fn process_cpu_ns(&mut self) -> Result<u64, MetricsError>;
    fn wall_ns(&mut self) -> Result<u64, MetricsError>;
}

/// Production clock sources owned by one precompile invocation.
pub struct SystemClocks {
    wall_origin: Instant,
}

impl Default for SystemClocks {
    fn default() -> Self {
        Self { wall_origin: Instant::now() }
    }
}

impl ClockSet for SystemClocks {
    fn thread_cpu_ns(&mut self) -> Result<u64, MetricsError> {
        let now = ThreadTime::try_now().map_err(|error| MetricsError::Clock(error.to_string()))?;
        checked_nanoseconds("thread CPU", now.as_duration().as_nanos())
    }

    fn process_cpu_ns(&mut self) -> Result<u64, MetricsError> {
        let now = ProcessTime::try_now().map_err(|error| MetricsError::Clock(error.to_string()))?;
        checked_nanoseconds("process CPU", now.as_duration().as_nanos())
    }

    fn wall_ns(&mut self) -> Result<u64, MetricsError> {
        checked_nanoseconds("wall", self.wall_origin.elapsed().as_nanos())
    }
}

fn checked_nanoseconds(clock: &'static str, nanoseconds: u128) -> Result<u64, MetricsError> {
    u64::try_from(nanoseconds).map_err(|_| MetricsError::ClockOverflow(clock))
}

impl<C: ClockSet> ClockSet for Option<C> {
    fn thread_cpu_ns(&mut self) -> Result<u64, MetricsError> {
        self.as_mut().expect("enabled native metrics recorder has clocks").thread_cpu_ns()
    }

    fn process_cpu_ns(&mut self) -> Result<u64, MetricsError> {
        self.as_mut().expect("enabled native metrics recorder has clocks").process_cpu_ns()
    }

    fn wall_ns(&mut self) -> Result<u64, MetricsError> {
        self.as_mut().expect("enabled native metrics recorder has clocks").wall_ns()
    }
}

/// Individually timed native phases in schema order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    RequestValidation = 0,
    DescriptorFormulaLoad = 1,
    KeyDerivation = 2,
    JournalLoad = 3,
    RowMaterialization = 4,
    FormulaEvaluation = 5,
    OrderedReduction = 6,
    OutputEncoding = 7,
}

/// Minimal phase-timing interface used by the streaming loader without coupling it to a clock.
pub(crate) trait PhaseMeasurer {
    fn measure<T>(&mut self, phase: Phase, operation: impl FnOnce() -> T) -> T;

    fn measure_excluding<T, const N: usize>(
        &mut self,
        phase: Phase,
        excluded: [Phase; N],
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T;
}

/// Non-clock metadata finalized by the provider for one invocation.
#[derive(Clone, Copy, Debug, Default)]
pub struct InvocationMetadata {
    pub operation: Option<u8>,
    pub status: u8,
    pub journal_reads: u64,
    pub unique_storage_keys: u64,
    pub active_markets: u32,
    pub projected_chunks: u32,
    pub work_units: u64,
}

/// One canonical schema-v1 invocation record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InvocationRecord {
    schema_version: u16,
    record_type: &'static str,
    invocation_index: u32,
    operation: Option<u8>,
    status: u8,
    request_validation_cpu_ns: u64,
    descriptor_formula_load_cpu_ns: u64,
    key_derivation_cpu_ns: u64,
    journal_load_cpu_ns: u64,
    row_materialization_cpu_ns: u64,
    formula_evaluation_cpu_ns: u64,
    ordered_reduction_cpu_ns: u64,
    output_encoding_cpu_ns: u64,
    accounted_phase_cpu_ns: u64,
    main_thread_cpu_ns: u64,
    process_cpu_ns: u64,
    wall_ns: u64,
    journal_reads: u64,
    unique_storage_keys: u64,
    active_markets: u32,
    projected_chunks: u32,
    work_units: u64,
}

/// The first line of every successful metrics file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RunRecord {
    schema_version: u16,
    record_type: &'static str,
    provider_mode: ProviderMode,
    peak_rss_bytes: u64,
    invocation_count: u32,
}

impl RunRecord {
    pub const fn new(
        provider_mode: ProviderMode,
        peak_rss_bytes: u64,
        invocation_count: u32,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            record_type: "run",
            provider_mode,
            peak_rss_bytes,
            invocation_count,
        }
    }
}

/// Stack-local recorder for one accepted precompile invocation.
pub struct InvocationRecorder<C> {
    enabled: bool,
    clocks: C,
    start_thread_ns: u64,
    start_process_ns: u64,
    start_wall_ns: u64,
    phases: [u64; 8],
    error: Option<MetricsError>,
}

impl<C: ClockSet> InvocationRecorder<C> {
    pub fn begin(enabled: bool, clocks: C) -> Self {
        let mut recorder = Self {
            enabled,
            clocks,
            start_thread_ns: 0,
            start_process_ns: 0,
            start_wall_ns: 0,
            phases: [0; 8],
            error: None,
        };
        if enabled {
            recorder.start_thread_ns = recorder.sample_thread();
            recorder.start_process_ns = recorder.sample_process();
            recorder.start_wall_ns = recorder.sample_wall();
        }
        recorder
    }

    pub fn measure_phase<T>(&mut self, phase: Phase, operation: impl FnOnce() -> T) -> T {
        if !self.enabled {
            return operation();
        }
        let start = self.sample_thread();
        let result = operation();
        let end = self.sample_thread();
        self.record_phase_at(phase, start, end);
        result
    }

    pub fn measure_phase_excluding<T, const N: usize>(
        &mut self,
        phase: Phase,
        excluded: [Phase; N],
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        if !self.enabled {
            return operation(self);
        }
        let excluded_before = excluded.map(|excluded| self.phases[excluded as usize]);
        let start = self.sample_thread();
        let result = operation(self);
        let end = self.sample_thread();
        let excluded_duration =
            excluded.iter().zip(excluded_before).fold(0_u64, |duration, (excluded, before)| {
                duration.saturating_add(self.phases[*excluded as usize].saturating_sub(before))
            });
        let duration = end.saturating_sub(start).saturating_sub(excluded_duration);
        let index = phase as usize;
        self.phases[index] = self.phases[index].saturating_add(duration);
        result
    }

    pub const fn record_phase_at(&mut self, phase: Phase, start_ns: u64, end_ns: u64) {
        let index = phase as usize;
        self.phases[index] = self.phases[index].saturating_add(end_ns.saturating_sub(start_ns));
    }

    pub fn finish(
        self,
        metadata: InvocationMetadata,
    ) -> Result<Option<InvocationRecord>, MetricsError> {
        self.finish_with_clocks(metadata).map(|(record, _)| record)
    }

    pub fn finish_with_clocks(
        mut self,
        metadata: InvocationMetadata,
    ) -> Result<(Option<InvocationRecord>, C), MetricsError> {
        if !self.enabled {
            return Ok((None, self.clocks));
        }
        let end_thread_ns = self.sample_thread();
        let end_process_ns = self.sample_process();
        let end_wall_ns = self.sample_wall();
        if let Some(error) = self.error {
            return Err(error);
        }
        let accounted_phase_cpu_ns =
            self.phases.iter().fold(0_u64, |sum, value| sum.saturating_add(*value));
        let record = InvocationRecord {
            schema_version: SCHEMA_VERSION,
            record_type: "invocation",
            invocation_index: 0,
            operation: metadata.operation,
            status: metadata.status,
            request_validation_cpu_ns: self.phases[Phase::RequestValidation as usize],
            descriptor_formula_load_cpu_ns: self.phases[Phase::DescriptorFormulaLoad as usize],
            key_derivation_cpu_ns: self.phases[Phase::KeyDerivation as usize],
            journal_load_cpu_ns: self.phases[Phase::JournalLoad as usize],
            row_materialization_cpu_ns: self.phases[Phase::RowMaterialization as usize],
            formula_evaluation_cpu_ns: self.phases[Phase::FormulaEvaluation as usize],
            ordered_reduction_cpu_ns: self.phases[Phase::OrderedReduction as usize],
            output_encoding_cpu_ns: self.phases[Phase::OutputEncoding as usize],
            accounted_phase_cpu_ns,
            main_thread_cpu_ns: end_thread_ns.saturating_sub(self.start_thread_ns),
            process_cpu_ns: end_process_ns.saturating_sub(self.start_process_ns),
            wall_ns: end_wall_ns.saturating_sub(self.start_wall_ns),
            journal_reads: metadata.journal_reads,
            unique_storage_keys: metadata.unique_storage_keys,
            active_markets: metadata.active_markets,
            projected_chunks: metadata.projected_chunks,
            work_units: metadata.work_units,
        };
        Ok((Some(record), self.clocks))
    }

    fn sample_thread(&mut self) -> u64 {
        Self::sample(&mut self.error, self.clocks.thread_cpu_ns())
    }

    fn sample_process(&mut self) -> u64 {
        Self::sample(&mut self.error, self.clocks.process_cpu_ns())
    }

    fn sample_wall(&mut self) -> u64 {
        Self::sample(&mut self.error, self.clocks.wall_ns())
    }

    fn sample(error: &mut Option<MetricsError>, sample: Result<u64, MetricsError>) -> u64 {
        match sample {
            Ok(value) => value,
            Err(sample_error) => {
                error.get_or_insert(sample_error);
                0
            }
        }
    }
}

impl<C: ClockSet> PhaseMeasurer for InvocationRecorder<C> {
    fn measure<T>(&mut self, phase: Phase, operation: impl FnOnce() -> T) -> T {
        self.measure_phase(phase, operation)
    }

    fn measure_excluding<T, const N: usize>(
        &mut self,
        phase: Phase,
        excluded: [Phase; N],
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        self.measure_phase_excluding(phase, excluded, operation)
    }
}

/// Process-local bounded invocation buffer.
#[derive(Debug, Default)]
pub struct MetricsBuffer {
    records: Vec<InvocationRecord>,
    error: Option<MetricsError>,
}

impl MetricsBuffer {
    const fn new() -> Self {
        Self { records: Vec::new(), error: None }
    }

    pub fn push(&mut self, mut record: InvocationRecord) -> Result<(), MetricsError> {
        if self.records.len() >= MAX_INVOCATIONS {
            return Err(MetricsError::BufferOverflow);
        }
        record.invocation_index =
            u32::try_from(self.records.len()).map_err(|_| MetricsError::InvocationCountOverflow)?;
        self.records.push(record);
        Ok(())
    }

    pub fn drain(&mut self) -> Result<Vec<InvocationRecord>, MetricsError> {
        if let Some(error) = self.error.take() {
            self.records.clear();
            return Err(error);
        }
        Ok(std::mem::take(&mut self.records))
    }

    fn submit(&mut self, result: Result<Option<InvocationRecord>, MetricsError>) {
        match result {
            Ok(Some(record)) => {
                if let Err(error) = self.push(record) {
                    self.error.get_or_insert(error);
                }
            }
            Ok(None) => {}
            Err(error) => {
                self.error.get_or_insert(error);
            }
        }
    }

    fn clear(&mut self) {
        self.records.clear();
        self.error = None;
    }
}

/// Enables or disables metrics before runner construction.
pub fn set_metrics_enabled(enabled: bool) {
    METRICS_ENABLED.store(enabled, Ordering::Release);
}

/// Clears buffered records before runner construction.
pub fn clear_metrics() -> Result<(), MetricsError> {
    METRICS_BUFFER.lock().map_err(|_| MetricsError::BufferPoisoned)?.clear();
    Ok(())
}

/// Starts a production recorder without sampling clocks when metrics are disabled.
pub fn begin_invocation() -> InvocationRecorder<Option<SystemClocks>> {
    begin_invocation_with(METRICS_ENABLED.load(Ordering::Acquire), SystemClocks::default)
}

fn begin_invocation_with<C: ClockSet>(
    enabled: bool,
    clock_factory: impl FnOnce() -> C,
) -> InvocationRecorder<Option<C>> {
    InvocationRecorder::begin(enabled, enabled.then(clock_factory))
}

/// Finalizes and buffers one accepted invocation with a single post-timing lock.
pub fn submit_invocation(
    recorder: InvocationRecorder<Option<SystemClocks>>,
    metadata: InvocationMetadata,
) {
    if !recorder.enabled {
        return;
    }
    let result = recorder.finish(metadata);
    match METRICS_BUFFER.lock() {
        Ok(mut buffer) => buffer.submit(result),
        Err(mut poisoned) => {
            poisoned.get_mut().error.get_or_insert(MetricsError::BufferPoisoned);
        }
    }
}

/// Drains all invocation records after the Forge test outcome is complete.
pub fn drain_metrics() -> Result<Vec<InvocationRecord>, MetricsError> {
    METRICS_BUFFER.lock().map_err(|_| MetricsError::BufferPoisoned)?.drain()
}

/// Serializes a complete run followed by its invocation records as JSON Lines.
pub fn serialize_jsonl(
    provider_mode: ProviderMode,
    peak_rss_bytes: u64,
    invocations: &[InvocationRecord],
) -> Result<Vec<u8>, MetricsError> {
    let invocation_count =
        u32::try_from(invocations.len()).map_err(|_| MetricsError::InvocationCountOverflow)?;
    let mut output =
        serde_json::to_vec(&RunRecord::new(provider_mode, peak_rss_bytes, invocation_count))
            .map_err(|error| MetricsError::Serialization(error.to_string()))?;
    output.push(b'\n');
    for invocation in invocations {
        output.extend(
            serde_json::to_vec(invocation)
                .map_err(|error| MetricsError::Serialization(error.to_string()))?,
        );
        output.push(b'\n');
    }
    Ok(output)
}

/// Samples process peak RSS once and normalizes it to bytes.
#[cfg(unix)]
pub fn peak_rss_bytes() -> Result<u64, MetricsError> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `usage` points to writable storage for `getrusage`, and is read only on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(MetricsError::PeakRss(std::io::Error::last_os_error().to_string()));
    }
    // SAFETY: a successful `getrusage` call initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    let raw = u64::try_from(usage.ru_maxrss)
        .map_err(|_| MetricsError::PeakRss("negative ru_maxrss".to_string()))?;
    #[cfg(target_os = "macos")]
    return Ok(raw);
    #[cfg(not(target_os = "macos"))]
    return raw
        .checked_mul(1024)
        .ok_or_else(|| MetricsError::PeakRss("ru_maxrss byte conversion overflowed".to_string()));
}

/// Peak RSS is unavailable on non-Unix platforms supported by this experiment.
#[cfg(not(unix))]
pub fn peak_rss_bytes() -> Result<u64, MetricsError> {
    Err(MetricsError::PeakRss("getrusage is unavailable on this platform".to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;

    use super::{
        ClockSet, InvocationMetadata, InvocationRecorder, MetricsBuffer, MetricsError, Phase,
        ProviderMode, RunRecord, begin_invocation_with, checked_nanoseconds,
    };

    #[derive(Default)]
    struct SequenceClocks {
        thread: VecDeque<u64>,
        process: VecDeque<u64>,
        wall: VecDeque<u64>,
        samples: AtomicUsize,
    }

    impl SequenceClocks {
        fn new(thread: impl IntoIterator<Item = u64>, process: [u64; 2], wall: [u64; 2]) -> Self {
            Self {
                thread: thread.into_iter().collect(),
                process: process.into(),
                wall: wall.into(),
                samples: AtomicUsize::new(0),
            }
        }

        fn sample_count(&self) -> usize {
            self.samples.load(Ordering::Relaxed)
        }
    }

    impl ClockSet for SequenceClocks {
        fn thread_cpu_ns(&mut self) -> Result<u64, MetricsError> {
            self.samples.fetch_add(1, Ordering::Relaxed);
            self.thread.pop_front().ok_or(MetricsError::ClockExhausted)
        }

        fn process_cpu_ns(&mut self) -> Result<u64, MetricsError> {
            self.samples.fetch_add(1, Ordering::Relaxed);
            self.process.pop_front().ok_or(MetricsError::ClockExhausted)
        }

        fn wall_ns(&mut self) -> Result<u64, MetricsError> {
            self.samples.fetch_add(1, Ordering::Relaxed);
            self.wall.pop_front().ok_or(MetricsError::ClockExhausted)
        }
    }

    struct OverflowClocks;

    impl ClockSet for OverflowClocks {
        fn thread_cpu_ns(&mut self) -> Result<u64, MetricsError> {
            Err(MetricsError::ClockOverflow("thread CPU"))
        }

        fn process_cpu_ns(&mut self) -> Result<u64, MetricsError> {
            Err(MetricsError::ClockOverflow("process CPU"))
        }

        fn wall_ns(&mut self) -> Result<u64, MetricsError> {
            Err(MetricsError::ClockOverflow("wall"))
        }
    }

    fn metadata() -> InvocationMetadata {
        InvocationMetadata {
            operation: Some(1),
            status: 1,
            journal_reads: 0,
            unique_storage_keys: 0,
            active_markets: 0,
            projected_chunks: 0,
            work_units: 0,
        }
    }

    #[test]
    fn phase_total_saturates() {
        let clocks = SequenceClocks::new(
            [10, 20, u64::MAX, u64::MAX, u64::MAX, u64::MAX],
            [100, 120],
            [1_000, 1_030],
        );
        let mut recorder = InvocationRecorder::begin(true, clocks);
        recorder.record_phase_at(Phase::RequestValidation, 0, u64::MAX);
        recorder.record_phase_at(Phase::OutputEncoding, 0, 1);
        let record = recorder.finish(metadata()).unwrap().unwrap();

        assert_eq!(record.accounted_phase_cpu_ns, u64::MAX);
    }

    #[test]
    fn repeated_streaming_phase_measurements_accumulate() {
        let clocks = SequenceClocks::new([10, 20, 25, 30, 37, 40], [100, 150], [1_000, 1_080]);
        let mut recorder = InvocationRecorder::begin(true, clocks);

        recorder.measure_phase(Phase::FormulaEvaluation, || ());
        recorder.measure_phase(Phase::FormulaEvaluation, || ());
        let record = recorder.finish(metadata()).unwrap().unwrap();

        assert_eq!(record.formula_evaluation_cpu_ns, 12);
        assert_eq!(record.accounted_phase_cpu_ns, 12);
    }

    #[test]
    fn streaming_loader_phase_excludes_nested_formula_and_reduction_cpu() {
        let clocks =
            SequenceClocks::new([10, 20, 25, 30, 31, 35, 40, 45], [100, 150], [1_000, 1_080]);
        let mut recorder = InvocationRecorder::begin(true, clocks);

        recorder.measure_phase_excluding(
            Phase::JournalLoad,
            [Phase::FormulaEvaluation, Phase::OrderedReduction],
            |recorder| {
                recorder.measure_phase(Phase::FormulaEvaluation, || ());
                recorder.measure_phase(Phase::OrderedReduction, || ());
            },
        );
        let record = recorder.finish(metadata()).unwrap().unwrap();

        assert_eq!(record.journal_load_cpu_ns, 11);
        assert_eq!(record.formula_evaluation_cpu_ns, 5);
        assert_eq!(record.ordered_reduction_cpu_ns, 4);
        assert_eq!(record.accounted_phase_cpu_ns, 20);
    }

    #[test]
    fn invocation_schema_serializes_exact_v1_names_and_types() {
        let clocks = SequenceClocks::new([10, 40], [100, 150], [1_000, 1_080]);
        let mut recorder = InvocationRecorder::begin(true, clocks);
        recorder.record_phase_at(Phase::RequestValidation, 0, u64::MAX);
        recorder.record_phase_at(Phase::OutputEncoding, 0, 1);
        let record = recorder.finish(metadata()).unwrap().unwrap();

        assert_eq!(
            serde_json::to_value(record).unwrap(),
            json!({
                "schema_version": 1,
                "record_type": "invocation",
                "invocation_index": 0,
                "operation": 1,
                "status": 1,
                "request_validation_cpu_ns": u64::MAX,
                "descriptor_formula_load_cpu_ns": 0,
                "key_derivation_cpu_ns": 0,
                "journal_load_cpu_ns": 0,
                "row_materialization_cpu_ns": 0,
                "formula_evaluation_cpu_ns": 0,
                "ordered_reduction_cpu_ns": 0,
                "output_encoding_cpu_ns": 1,
                "accounted_phase_cpu_ns": u64::MAX,
                "main_thread_cpu_ns": 30,
                "process_cpu_ns": 50,
                "wall_ns": 80,
                "journal_reads": 0,
                "unique_storage_keys": 0,
                "active_markets": 0,
                "projected_chunks": 0,
                "work_units": 0
            }),
        );
    }

    #[test]
    fn run_schema_serializes_exact_v1_names_and_types() {
        let run = RunRecord::new(ProviderMode::Off, 4096, 1);

        assert_eq!(
            serde_json::to_value(run).unwrap(),
            json!({
                "schema_version": 1,
                "record_type": "run",
                "provider_mode": "off",
                "peak_rss_bytes": 4096,
                "invocation_count": 1
            }),
        );
    }

    #[test]
    fn one_recorder_appends_at_most_one_record() {
        let clocks = SequenceClocks::new([10, 20], [100, 120], [1_000, 1_030]);
        let record = InvocationRecorder::begin(true, clocks).finish(metadata()).unwrap().unwrap();
        let mut buffer = MetricsBuffer::default();

        buffer.push(record).unwrap();

        assert_eq!(buffer.drain().unwrap().len(), 1);
    }

    #[test]
    fn invocation_buffer_defers_overflow_until_drain() {
        let clocks = SequenceClocks::new([10, 20], [100, 120], [1_000, 1_030]);
        let record = InvocationRecorder::begin(true, clocks).finish(metadata()).unwrap().unwrap();
        let mut buffer = MetricsBuffer::default();

        for _ in 0..4096 {
            buffer.submit(Ok(Some(record.clone())));
        }
        buffer.submit(Ok(Some(record)));

        assert_eq!(buffer.drain(), Err(MetricsError::BufferOverflow));
    }

    #[test]
    fn disabled_metrics_do_not_sample_any_clock() {
        let clocks = SequenceClocks::default();
        let recorder = InvocationRecorder::begin(false, clocks);

        let (record, clocks) = recorder.finish_with_clocks(metadata()).unwrap();

        assert!(record.is_none());
        assert_eq!(clocks.sample_count(), 0);
    }

    #[test]
    fn disabled_production_path_does_not_construct_clock_origins() {
        let constructions = std::cell::Cell::new(0);

        let recorder = begin_invocation_with(false, || {
            constructions.set(constructions.get() + 1);
            SequenceClocks::default()
        });
        let (record, _) = recorder.finish_with_clocks(metadata()).unwrap();

        assert!(record.is_none());
        assert_eq!(constructions.get(), 0);
    }

    #[test]
    fn representational_clock_overflow_is_deferred_until_drain() {
        assert_eq!(
            checked_nanoseconds("thread CPU", u128::from(u64::MAX) + 1),
            Err(MetricsError::ClockOverflow("thread CPU")),
        );

        let recorder = InvocationRecorder::begin(true, OverflowClocks);
        let mut buffer = MetricsBuffer::default();
        buffer.submit(recorder.finish(metadata()));

        assert_eq!(buffer.drain(), Err(MetricsError::ClockOverflow("thread CPU")));
    }
}
