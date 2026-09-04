//! CLI coverage for RISEx native metrics export.

use serde_json::Value;

const JOURNAL_PROBE: &str = include_str!("../fixtures/risex-formula/JournalProbe.t.sol");
const FORMULA_CORPUS: &str =
    include_str!("../../../evm/networks/src/risex_formula/testdata/portfolio_order_risk_v1.json");
const WIRE_PROBE: &str = include_str!("../fixtures/risex-formula/WireProbe.t.sol");

const FIXTURE: &str = r#"
pragma solidity ^0.8.18;

contract RisexFormulaMetricsTest {
    function testProviderOff() external view {
        bytes memory request = hex"00000000000000000000005566112233440001000000010000000001000001010123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefffffffffffffffffffffffffffffffffffffffffffffffeeddccbbaa99887767112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        (bool success, bytes memory response) = address(0xf100).staticcall(request);
        require(success);
        require(response.length == 0);
    }
}
"#;

forgetest_init!(risex_formula_metrics_export_is_post_run_and_does_not_touch_stdout, |prj, cmd| {
    prj.add_test("RisexFormulaMetrics.t.sol", FIXTURE);
    let metrics = prj.root().join("risex-metrics.jsonl");
    cmd.forge_fuse().arg("build").assert_success();
    std::fs::File::create(&metrics).unwrap();

    cmd.forge_fuse().args([
        "test",
        "--quiet",
        "--threads",
        "1",
        "--cpu-trace",
        "--risex-risk-provider",
        "off",
        "--risex-risk-metrics",
        metrics.to_str().unwrap(),
    ]);
    cmd.assert_empty_stdout();

    let lines = std::fs::read_to_string(metrics).unwrap();
    let records =
        lines.lines().map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["record_type"], "run");
    assert_eq!(records[0]["provider_mode"], "off");
    assert_eq!(records[0]["invocation_count"], 0);
    assert!(records[0]["peak_rss_bytes"].as_u64().unwrap() > 0);
});

forgetest_init!(risex_formula_metrics_flags_validate_before_test_execution, |prj, cmd| {
    prj.add_test("RisexFormulaMetrics.t.sol", FIXTURE);
    let absolute = prj.root().join("risex-metrics.jsonl");

    cmd.forge_fuse().args(["test", "--risex-risk-metrics", absolute.to_str().unwrap()]);
    cmd.assert_failure();

    cmd.forge_fuse().args(["test", "--cpu-trace", "--risex-risk-metrics", "relative.jsonl"]);
    cmd.assert_failure();

    std::fs::write(&absolute, "occupied\n").unwrap();
    cmd.forge_fuse().args([
        "test",
        "--cpu-trace",
        "--risex-risk-metrics",
        absolute.to_str().unwrap(),
    ]);
    cmd.assert_failure();
    assert_eq!(std::fs::read_to_string(absolute).unwrap(), "occupied\n");
});

forgetest_init!(risex_formula_metrics_require_single_thread, |prj, cmd| {
    prj.add_test("RisexFormulaMetrics.t.sol", FIXTURE);
    let metrics = prj.root().join("risex-metrics.jsonl");
    std::fs::File::create(&metrics).unwrap();

    cmd.forge_fuse().args([
        "test",
        "--threads",
        "2",
        "--cpu-trace",
        "--risex-risk-metrics",
        metrics.to_str().unwrap(),
    ]);
    cmd.assert_failure().stderr_eq(str![[r#"
Error: `--risex-risk-metrics` requires single-threaded execution (`--threads 1`); resolved 2 threads

"#]]);
    assert_eq!(std::fs::metadata(metrics).unwrap().len(), 0);
});

forgetest_init!(risex_formula_metrics_reject_mutation_mode, |prj, cmd| {
    prj.add_test("RisexFormulaMetrics.t.sol", FIXTURE);
    let metrics = prj.root().join("risex-metrics.jsonl");
    std::fs::File::create(&metrics).unwrap();

    cmd.forge_fuse().args([
        "test",
        "--threads",
        "1",
        "--cpu-trace",
        "--risex-risk-metrics",
        metrics.to_str().unwrap(),
        "--mutate",
        "--mutation-jobs",
        "1",
    ]);
    cmd.assert_failure().stderr_eq(str![[r#"
Error: `--risex-risk-metrics` cannot be combined with `--mutate`

"#]]);
    assert_eq!(std::fs::metadata(metrics).unwrap().len(), 0);
});

forgetest_init!(risex_formula_metrics_reject_watch_mode, |prj, cmd| {
    prj.add_test("Broken.t.sol", "not solidity");
    let metrics = prj.root().join("risex-metrics.jsonl");

    cmd.forge_fuse().args([
        "test",
        "--threads",
        "1",
        "--cpu-trace",
        "--risex-risk-metrics",
        metrics.to_str().unwrap(),
        "--watch",
    ]);
    cmd.assert_failure().stderr_eq(str![[r#"
Error: `--risex-risk-metrics` cannot be combined with `--watch`

"#]]);
    assert!(!metrics.exists());
});

#[cfg(not(unix))]
forgetest_init!(risex_formula_metrics_reject_unsupported_platform_before_compile, |prj, cmd| {
    prj.add_test("Broken.t.sol", "not solidity");
    let metrics = prj.root().join("risex-metrics.jsonl");

    cmd.forge_fuse().args([
        "test",
        "--cpu-trace",
        "--risex-risk-metrics",
        metrics.to_str().unwrap(),
    ]);
    cmd.assert_failure().stderr_eq(str![[r#"
Error: `--risex-risk-metrics` is unsupported on this platform

"#]]);
    assert!(!metrics.exists());
});

forgetest_init!(risex_formula_snapshot_rejects_provider_before_compile, |prj, cmd| {
    prj.add_test("Broken.t.sol", "not solidity");

    cmd.forge_fuse().args(["snapshot", "--risex-risk-provider", "specialized"]);
    cmd.assert_failure().stderr_eq(str![[r#"
Error: RISEx risk flags are only supported by `forge test`; `forge snapshot` cannot use them

"#]]);
});

forgetest_init!(risex_formula_snapshot_watch_rejects_provider_before_starting, |prj, cmd| {
    prj.add_test("Broken.t.sol", "not solidity");

    cmd.forge_fuse().args(["snapshot", "--watch", "--risex-risk-provider", "specialized"]);
    cmd.assert_failure().stderr_eq(str![[r#"
Error: RISEx risk flags are only supported by `forge test`; `forge snapshot` cannot use them

"#]]);
});

forgetest_init!(risex_formula_coverage_rejects_metrics_before_compile, |prj, cmd| {
    prj.add_test("Broken.t.sol", "not solidity");
    let metrics = prj.root().join("risex-metrics.jsonl");

    cmd.forge_fuse().args(["coverage", "--risex-risk-metrics", metrics.to_str().unwrap()]);
    cmd.assert_failure().stderr_eq(str![[r#"
Error: RISEx risk flags are only supported by `forge test`; `forge coverage` cannot use them

"#]]);
    assert!(!metrics.exists());
});

forgetest_init!(risex_formula_provider_does_not_require_cpu_trace, |prj, cmd| {
    prj.add_test("RisexFormulaMetrics.t.sol", FIXTURE);
    cmd.forge_fuse().arg("build").assert_success();

    cmd.forge_fuse().args(["test", "--quiet", "--risex-risk-provider", "off"]);
    cmd.assert_empty_stdout();
});

forgetest_init!(risex_formula_journal_state_and_reverts_are_frame_aware, |prj, cmd| {
    let corpus: Value = serde_json::from_str(FORMULA_CORPUS).unwrap();
    let source = JOURNAL_PROBE
        .replace("__RUNTIME_HEX__", corpus["runtimeHex"].as_str().unwrap().trim_start_matches("0x"))
        .replace("__BLOB_CODE_HASH__", corpus["identities"]["blobCodeHash"].as_str().unwrap())
        .replace(
            "__LOADER_SCHEMA_HASH__",
            corpus["identities"]["loaderSchemaHash"].as_str().unwrap(),
        );
    prj.add_test("JournalProbe.t.sol", &source);

    cmd.forge_fuse().args([
        "test",
        "--json",
        "-vvvv",
        "--match-contract",
        "JournalProbeTest",
        "--threads",
        "1",
        "--risex-risk-provider",
        "specialized",
    ]);
    let output = cmd.assert_success().get_output().stdout.clone();
    let results: Value = serde_json::from_slice(&output).unwrap();
    let result = &results["test/JournalProbe.t.sol:JournalProbeTest"]["test_results"]["testParentJournalAndNestedRevertIsolation()"];
    let execution =
        result["traces"].as_array().unwrap().iter().find(|trace| trace[0] == "Execution").unwrap();
    let arena = execution[1]["arena"].as_array().unwrap();
    let parent = arena.iter().find(|node| node["parent"].is_null()).unwrap();
    let parent_logs = parent["logs"].as_array().unwrap();
    assert_eq!(parent_logs.len(), 1, "parent frame log count");
    assert_eq!(
        parent_logs[0]["raw_log"]["topics"][0],
        "0xba8d2397586c3a434c5630c4aef7a0d4d86dbcf33ed3a67f9bc73af7c9541da1",
    );
    let reverted_child = arena.iter().find(|node| {
        node["trace"]["status"] == "Revert"
            && node["logs"].as_array().is_some_and(|logs| {
                logs.iter().any(|log| {
                    log["raw_log"]["topics"][0]
                        == "0xd993731a43161283c604d220ed070660541f21636ed05673d44871cd293f018e"
                })
            })
    });
    assert!(reverted_child.is_some(), "child log was not scoped to its reverted frame");
});

forgetest_init!(risex_formula_wire_boundaries_and_malformed_input_are_exact, |prj, cmd| {
    prj.add_test("WireProbe.t.sol", WIRE_PROBE);

    cmd.forge_fuse().args([
        "test",
        "--match-contract",
        "WireProbeTest",
        "--threads",
        "1",
        "--risex-risk-provider",
        "specialized",
    ]);
    cmd.assert_success();
});

forgetest_init!(risex_formula_provider_off_keeps_empty_output, |prj, cmd| {
    prj.add_test("WireProbe.t.sol", WIRE_PROBE);

    cmd.forge_fuse().args([
        "test",
        "--match-contract",
        "ProviderOffProbeTest",
        "--threads",
        "1",
        "--risex-risk-provider",
        "off",
    ]);
    cmd.assert_success();
});
