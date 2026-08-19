//! Tests for opt-in thread CPU trace rendering.

use foundry_test_utils::{str, util::OutputExt};

const FIXTURE: &str = r#"
pragma solidity ^0.8.18;

contract CpuTraceTarget {
    function ping() external pure returns (uint256) {
        return 42;
    }
}

contract CpuTraceTest {
    function testCpuTrace() external {
        CpuTraceTarget target = new CpuTraceTarget();
        require(target.ping() == 42);
    }
}
"#;

const FORK_FIXTURE: &str = r#"
pragma solidity ^0.8.18;

contract ForkCpuTraceTarget {
    function ping() external pure returns (uint256) {
        return 42;
    }
}

contract ForkCpuTraceTest {
    function testForkCpuTrace() external {
        require(block.chainid == 424242);
        ForkCpuTraceTarget target = new ForkCpuTraceTarget();
        require(target.ping() == 42);
    }
}
"#;

const OPCODE_FIXTURE: &str = r#"
pragma solidity ^0.8.18;

contract OpcodeCpuTraceTest {
    function testOpcodeCpuTrace() external {
        assembly {
            sstore(0, 42)
            if iszero(eq(sload(0), 42)) { revert(0, 0) }
        }
    }
}
"#;

forgetest_init!(cpu_trace_disabled_output_remains_exact, |prj, cmd| {
    prj.add_test("CpuTrace.t.sol", FIXTURE);

    cmd.forge_fuse().args(["test", "-vvvv"]).assert_success().stdout_eq(str![[r#"
...
Ran 1 test for test/CpuTrace.t.sol:CpuTraceTest
[PASS] testCpuTrace() ([GAS])
Traces:
  [..] CpuTraceTest::testCpuTrace()
    ├─ [..] → new CpuTraceTarget@0x5615dEB798BB3E4dFa0139dFa1b3D433Cc23b72f
    │   └─ ← [Return] 175 bytes of code
    ├─ [..] CpuTraceTarget::ping() [staticcall]
    │   └─ ← [Return] 42
    └─ ← [Stop]

Suite result: ok. 1 passed; 0 failed; 0 skipped; [ELAPSED]

Ran 1 test suite [ELAPSED]: 1 tests passed, 0 failed, 0 skipped (1 total tests)

"#]]);
});

forgetest_init!(cpu_trace_enabled_renders_cpu_beside_gas, |prj, cmd| {
    prj.add_test("CpuTrace.t.sol", FIXTURE);

    let output = cmd
        .forge_fuse()
        .args(["test", "--cpu-trace", "-vvvv"])
        .assert_success()
        .get_output()
        .stdout_lossy();

    assert!(output.contains(" gas | "), "missing gas column: {output}");
    assert!(output.contains(" cpu | "), "missing CPU column: {output}");
    assert!(output.contains(" self | "), "missing self CPU column: {output}");
    assert!(output.contains("%] "), "missing root-relative CPU percentage: {output}");
});
forgetest_init!(cpu_trace_renders_cpu_for_selected_opcodes, |prj, cmd| {
    prj.add_test("OpcodeCpuTrace.t.sol", OPCODE_FIXTURE);

    let output = cmd
        .forge_fuse()
        .args(["test", "--cpu-trace", "--opcodes", "SLOAD,SSTORE", "-vvvvv"])
        .assert_success()
        .get_output()
        .stdout_lossy();

    let sload = output.lines().find(|line| line.contains("] SLOAD")).unwrap();
    assert!(sload.contains(" cpu] SLOAD"), "missing SLOAD CPU timing: {sload}");
    let sstore = output.lines().find(|line| line.contains("] SSTORE")).unwrap();
    assert!(sstore.contains(" cpu] SSTORE"), "missing SSTORE CPU timing: {sstore}");
});

forgetest_init!(selected_opcodes_remain_gas_only_without_cpu_trace, |prj, cmd| {
    prj.add_test("OpcodeCpuTrace.t.sol", OPCODE_FIXTURE);

    let output = cmd
        .forge_fuse()
        .args(["test", "--opcodes", "SLOAD,SSTORE", "-vvvvv"])
        .assert_success()
        .get_output()
        .stdout_lossy();

    let sload = output.lines().find(|line| line.contains("] SLOAD")).unwrap();
    assert_eq!(sload.trim(), "├─ [100] SLOAD");
    let sstore = output.lines().find(|line| line.contains("] SSTORE")).unwrap();
    assert!(!sstore.contains(" cpu]"), "unexpected SSTORE CPU timing: {sstore}");
});

forgetest_init!(cpu_flamechart_writes_cpu_weighted_svg, |prj, cmd| {
    prj.add_test("CpuTrace.t.sol", FIXTURE);

    cmd.forge_fuse().args(["test", "--cpu-flamechart", "--no-open"]).assert_success();

    let flamechart = std::fs::read_to_string(
        prj.root().join("cache/cpu_flamechart_CpuTraceTest_testCpuTrace.svg"),
    )
    .unwrap();
    assert!(flamechart.contains("CPU flamechart CpuTraceTest::testCpuTrace"));
    assert!(flamechart.contains("CpuTraceTest.testCpuTrace()"));
    assert!(flamechart.contains("CpuTraceTarget.ping()"));
    assert!(flamechart.contains("cpu nanoseconds"));
});

forgetest_async!(cpu_trace_enabled_renders_cpu_on_fork, |prj, cmd| {
    let (_, handle) = anvil::spawn(anvil::NodeConfig::test().with_chain_id(Some(424242u64))).await;
    prj.add_test("ForkCpuTrace.t.sol", FORK_FIXTURE);

    let output = cmd
        .forge_fuse()
        .args(["test", "--fork-url", &handle.http_endpoint(), "--cpu-trace", "-vvvv"])
        .assert_success()
        .get_output()
        .stdout_lossy();

    assert!(
        output.contains("ForkCpuTraceTest::testForkCpuTrace()"),
        "missing fork test trace: {output}"
    );
    assert!(output.contains(" gas | "), "missing gas column: {output}");
    assert!(output.contains(" cpu | "), "missing CPU column: {output}");
    assert!(output.contains(" self | "), "missing self CPU column: {output}");
    assert!(output.contains("%] "), "missing root-relative CPU percentage: {output}");
});
