#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn nested_frames_compute_inclusive_and_self_cpu() {
        let mut profiler = FrameCpuProfiler::default();
        profiler.enter_at(0, 100);
        profiler.enter_at(1, 130);
        profiler.exit_at(180).unwrap();
        profiler.exit_at(250).unwrap();

        let metrics = profiler.finish().unwrap();
        assert_eq!(metrics[&0].inclusive_ns, 150);
        assert_eq!(metrics[&0].self_ns, 100);
        assert_eq!(metrics[&1].inclusive_ns, 50);
        assert_eq!(metrics[&1].self_ns, 50);
        assert_eq!(metrics[&1].root_percent_bps, 3333);
    }

    #[test]
    fn unmatched_exit_is_rejected() {
        let mut profiler = FrameCpuProfiler::default();

        assert_eq!(profiler.exit_at(100), Err(CpuProfilerError::UnmatchedExit));
    }

    #[test]
    fn unfinished_frames_are_rejected() {
        let mut profiler = FrameCpuProfiler::default();
        profiler.enter_at(0, 100);

        assert_eq!(profiler.finish(), Err(CpuProfilerError::UnfinishedFrames));
    }

    #[test]
    fn clock_regression_saturates_to_zero_duration() {
        let mut profiler = FrameCpuProfiler::default();
        profiler.enter_at(0, 100);
        profiler.exit_at(90).unwrap();

        let metrics = profiler.finish().unwrap();
        assert_eq!(metrics[&0].inclusive_ns, 0);
        assert_eq!(metrics[&0].self_ns, 0);
        assert_eq!(metrics[&0].root_percent_bps, 0);
    }

    #[test]
    fn zero_root_duration_has_zero_percentages() {
        let mut profiler = FrameCpuProfiler::default();
        profiler.enter_at(0, 100);
        profiler.enter_at(1, 100);
        profiler.exit_at(100).unwrap();
        profiler.exit_at(100).unwrap();

        let metrics = profiler.finish().unwrap();
        assert_eq!(metrics[&0].root_percent_bps, 0);
        assert_eq!(metrics[&1].root_percent_bps, 0);
    }

    #[test]
    fn multiple_completed_roots_are_rejected() {
        let mut profiler = FrameCpuProfiler::default();
        profiler.enter_at(0, 0);
        profiler.exit_at(10).unwrap();
        profiler.enter_at(1, 20);
        profiler.exit_at(30).unwrap();

        assert_eq!(profiler.finish(), Err(CpuProfilerError::MultipleRoots));
    }

    #[test]
    fn duplicate_node_finalization_is_rejected() {
        let mut profiler = FrameCpuProfiler::default();
        profiler.enter_at(0, 0);
        profiler.exit_at(10).unwrap();
        profiler.enter_at(0, 20);

        assert_eq!(profiler.exit_at(30), Err(CpuProfilerError::DuplicateNodeFinalization(0)));
    }

    #[test]
    fn recorder_uses_clock_sequence_for_nested_frames() {
        let mut recorder = CpuTraceRecorder::new(SequenceClock::new([100, 130, 180, 250]));
        recorder.enter(0).unwrap();
        recorder.enter(1).unwrap();
        recorder.exit().unwrap();
        recorder.exit().unwrap();

        let metrics = recorder.finish().unwrap();
        assert_eq!(metrics[&0].inclusive_ns, 150);
        assert_eq!(metrics[&0].self_ns, 100);
        assert_eq!(metrics[&1].root_percent_bps, 3333);
    }

    #[test]
    fn recorder_returns_clock_errors() {
        let mut recorder = CpuTraceRecorder::new(FailingClock);

        assert!(matches!(recorder.enter(0), Err(CpuRecorderError::Clock(_))));
    }

    #[test]
    fn system_thread_cpu_clock_samples_a_timestamp() {
        let mut clock = SystemThreadCpuClock;

        assert!(clock.now_ns().is_ok());
    }

    #[test]
    fn duration_formatting_uses_stable_units_and_precision() {
        assert_eq!(format_duration(999), "999ns");
        assert_eq!(format_duration(1_000), "1.0us");
        assert_eq!(format_duration(1_999), "2.0us");
        assert_eq!(format_duration(1_000_000), "1.00ms");
        assert_eq!(format_duration(1_234_567), "1.23ms");
        assert_eq!(format_duration(1_000_000_000), "1.000s");
        assert_eq!(format_duration(1_234_567_890), "1.235s");
    }

    struct SequenceClock {
        timestamps: VecDeque<u64>,
    }

    impl SequenceClock {
        fn new(timestamps: impl IntoIterator<Item = u64>) -> Self {
            Self { timestamps: timestamps.into_iter().collect() }
        }
    }

    impl CpuClock for SequenceClock {
        fn now_ns(&mut self) -> Result<u64, CpuClockError> {
            self.timestamps.pop_front().ok_or(CpuClockError::Exhausted)
        }
    }

    struct FailingClock;

    impl CpuClock for FailingClock {
        fn now_ns(&mut self) -> Result<u64, CpuClockError> {
            Err(CpuClockError::Exhausted)
        }
    }
}
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, error::Error, fmt};

/// Engine-neutral CPU timing data for a single call trace node.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CallTraceCpuMetrics {
    /// Total CPU time spent while the call frame was active, in nanoseconds.
    pub inclusive_ns: u64,
    /// CPU time spent by the call frame excluding child call frames, in nanoseconds.
    pub self_ns: u64,
    /// Inclusive CPU time as a percentage of the completed root, in basis points.
    pub root_percent_bps: u16,
}

/// CPU timing data keyed by call trace node index.
pub type CpuTraceMetrics = BTreeMap<usize, CallTraceCpuMetrics>;

/// Errors produced by a CPU clock.
#[derive(Debug)]
pub enum CpuClockError {
    /// The system thread CPU clock failed.
    System(std::io::Error),
    /// A deterministic test clock has no remaining timestamp.
    Exhausted,
}

impl fmt::Display for CpuClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System(error) => write!(f, "system thread CPU clock failed: {error}"),
            Self::Exhausted => f.write_str("CPU clock has no remaining timestamp"),
        }
    }
}

impl Error for CpuClockError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::System(error) => Some(error),
            Self::Exhausted => None,
        }
    }
}

impl From<std::io::Error> for CpuClockError {
    fn from(error: std::io::Error) -> Self {
        Self::System(error)
    }
}

/// Source of monotonically sampled CPU timestamps, expressed in nanoseconds.
pub trait CpuClock {
    /// Samples the current CPU timestamp.
    fn now_ns(&mut self) -> Result<u64, CpuClockError>;
}

/// CPU clock backed by the current operating system thread.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemThreadCpuClock;

impl CpuClock for SystemThreadCpuClock {
    fn now_ns(&mut self) -> Result<u64, CpuClockError> {
        let now = cpu_time::ThreadTime::try_now()?;
        Ok(u64::try_from(now.as_duration().as_nanos()).unwrap_or(u64::MAX))
    }
}

/// Structural errors produced while finalizing call frame CPU metrics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CpuProfilerError {
    /// An exit was observed without a corresponding entry.
    UnmatchedExit,
    /// One or more entered frames have not exited.
    UnfinishedFrames,
    /// A trace node was finalized more than once.
    DuplicateNodeFinalization(usize),
    /// More than one completed top-level root was observed.
    MultipleRoots,
}

impl fmt::Display for CpuProfilerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnmatchedExit => f.write_str("CPU profiler received an unmatched frame exit"),
            Self::UnfinishedFrames => f.write_str("CPU profiler has unfinished call frames"),
            Self::DuplicateNodeFinalization(node) => {
                write!(f, "CPU profiler finalized trace node {node} more than once")
            }
            Self::MultipleRoots => f.write_str("CPU profiler observed multiple call-tree roots"),
        }
    }
}

impl Error for CpuProfilerError {}

/// CPU profiler errors while recording timestamps from a clock.
#[derive(Debug)]
pub enum CpuRecorderError {
    /// Sampling the CPU clock failed.
    Clock(CpuClockError),
    /// The call frame sequence was structurally invalid.
    Profiler(CpuProfilerError),
    /// An opcode step was still active when CPU tracing finished.
    UnfinishedOpcodeStep,
}

impl fmt::Display for CpuRecorderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => error.fmt(f),
            Self::Profiler(error) => error.fmt(f),
            Self::UnfinishedOpcodeStep => f.write_str("CPU recorder has an unfinished opcode step"),
        }
    }
}

impl Error for CpuRecorderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Clock(error) => Some(error),
            Self::Profiler(error) => Some(error),
            Self::UnfinishedOpcodeStep => None,
        }
    }
}

#[derive(Clone, Debug)]
struct ActiveFrame {
    node: usize,
    start_ns: u64,
    child_ns: u64,
}

/// Pure stack-based call frame CPU profiler.
#[derive(Clone, Debug, Default)]
pub struct FrameCpuProfiler {
    frames: Vec<ActiveFrame>,
    roots: Vec<usize>,
    metrics: CpuTraceMetrics,
}

impl FrameCpuProfiler {
    /// Returns the currently active trace node, if any.
    pub fn active_node(&self) -> Option<usize> {
        self.frames.last().map(|frame| frame.node)
    }

    /// Marks the start of a trace node at an already sampled CPU timestamp.
    pub fn enter_at(&mut self, node: usize, now_ns: u64) {
        self.frames.push(ActiveFrame { node, start_ns: now_ns, child_ns: 0 });
    }

    /// Marks the end of the most recently entered trace node at an already sampled timestamp.
    pub fn exit_at(&mut self, now_ns: u64) -> Result<(), CpuProfilerError> {
        let frame = self.frames.pop().ok_or(CpuProfilerError::UnmatchedExit)?;
        if self.metrics.contains_key(&frame.node) {
            return Err(CpuProfilerError::DuplicateNodeFinalization(frame.node));
        }

        let inclusive_ns = now_ns.saturating_sub(frame.start_ns);
        let self_ns = inclusive_ns.saturating_sub(frame.child_ns);
        self.metrics
            .insert(frame.node, CallTraceCpuMetrics { inclusive_ns, self_ns, root_percent_bps: 0 });

        if let Some(parent) = self.frames.last_mut() {
            parent.child_ns = parent.child_ns.saturating_add(inclusive_ns);
        } else {
            self.roots.push(frame.node);
        }
        Ok(())
    }

    /// Completes the profile and derives root-relative percentages.
    pub fn finish(mut self) -> Result<CpuTraceMetrics, CpuProfilerError> {
        if !self.frames.is_empty() {
            return Err(CpuProfilerError::UnfinishedFrames);
        }
        if self.roots.len() > 1 {
            return Err(CpuProfilerError::MultipleRoots);
        }
        let Some(&root) = self.roots.first() else {
            return Ok(self.metrics);
        };
        let root_duration = self.metrics[&root].inclusive_ns;
        for metric in self.metrics.values_mut() {
            metric.root_percent_bps = percentage_bps(metric.inclusive_ns, root_duration);
        }
        Ok(self.metrics)
    }
}

fn percentage_bps(value: u64, total: u64) -> u16 {
    if total == 0 {
        return 0;
    }
    ((u128::from(value) * 10_000 / u128::from(total)).min(10_000)) as u16
}

/// Couples a CPU clock with the pure call frame profiler for inspector integration.
#[derive(Clone, Debug)]
pub struct CpuTraceRecorder<C> {
    clock: C,
    profiler: FrameCpuProfiler,
}

impl<C> CpuTraceRecorder<C> {
    /// Creates a recorder backed by `clock`.
    pub fn new(clock: C) -> Self {
        Self { clock, profiler: FrameCpuProfiler::default() }
    }
}

impl<C: CpuClock> CpuTraceRecorder<C> {
    /// Returns the currently active trace node, if any.
    pub fn active_node(&self) -> Option<usize> {
        self.profiler.active_node()
    }

    /// Samples the CPU clock without changing call-frame state.
    pub fn sample_now(&mut self) -> Result<u64, CpuRecorderError> {
        self.clock.now_ns().map_err(CpuRecorderError::Clock)
    }

    /// Samples the CPU clock and enters a call trace node.
    pub fn enter(&mut self, node: usize) -> Result<(), CpuRecorderError> {
        let now_ns = self.clock.now_ns().map_err(CpuRecorderError::Clock)?;
        self.profiler.enter_at(node, now_ns);
        Ok(())
    }

    /// Samples the CPU clock and exits the current call trace node.
    pub fn exit(&mut self) -> Result<(), CpuRecorderError> {
        let now_ns = self.clock.now_ns().map_err(CpuRecorderError::Clock)?;
        self.profiler.exit_at(now_ns).map_err(CpuRecorderError::Profiler)
    }

    /// Completes the call tree and returns its metrics.
    pub fn finish(self) -> Result<CpuTraceMetrics, CpuRecorderError> {
        self.profiler.finish().map_err(CpuRecorderError::Profiler)
    }
}

/// Formats a CPU duration in a stable, locale-independent unit.
pub(crate) fn format_duration(duration_ns: u64) -> String {
    match duration_ns {
        0..1_000 => format!("{duration_ns}ns"),
        1_000..1_000_000 => format!("{:.1}us", duration_ns as f64 / 1_000.0),
        1_000_000..1_000_000_000 => format!("{:.2}ms", duration_ns as f64 / 1_000_000.0),
        _ => format!("{:.3}s", duration_ns as f64 / 1_000_000_000.0),
    }
}
