//! Tests for opt-in thread CPU trace rendering.

use foundry_test_utils::{TestCommand, snapbox::cmd::OutputAssert, str};

const CPU_REDACTIONS: &[(&str, &str)] = &[
    ("[CPU]", r"\d+(?:\.\d+)?(?:ns|us|ms|s) cpu"),
    ("[SELF_CPU]", r"\d+(?:\.\d+)?(?:ns|us|ms|s) self"),
    ("[CPU_PERCENT]", r"\d+(?:\.\d+)?%"),
];

fn assert_cpu_trace(cmd: &mut TestCommand) -> OutputAssert {
    cmd.assert_with(CPU_REDACTIONS).success()
}

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

const NESTED_OPCODE_FIXTURE: &str = r#"
pragma solidity ^0.8.18;

contract NestedOpcodeCpuTarget {
    function write() external {
        assembly {
            sstore(0, 42)
        }
    }
}

contract NestedOpcodeCpuTest {
    function testNestedOpcodeCpu() external {
        NestedOpcodeCpuTarget target = new NestedOpcodeCpuTarget();
        (bool success,) = address(target).call(
            abi.encodeCall(NestedOpcodeCpuTarget.write, ())
        );
        require(success);
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

    cmd.forge_fuse().args(["test", "--cpu-trace", "-vvvv"]);
    assert_cpu_trace(&mut cmd).stdout_eq(str![[r#"
...
Ran 1 test for test/CpuTrace.t.sol:CpuTraceTest
[PASS] testCpuTrace() ([GAS])
Traces:
  [[..] gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] CpuTraceTest::testCpuTrace()
    ├─ [[..] gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] → new CpuTraceTarget@0x5615dEB798BB3E4dFa0139dFa1b3D433Cc23b72f
    │   └─ ← [Return] 175 bytes of code
    ├─ [[..] gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] CpuTraceTarget::ping() [staticcall]
    │   └─ ← [Return] 42
    └─ ← [Stop]

Suite result: ok. 1 passed; 0 failed; 0 skipped; [ELAPSED]

Ran 1 test suite [ELAPSED]: 1 tests passed, 0 failed, 0 skipped (1 total tests)

"#]]);
});
forgetest_init!(cpu_trace_renders_cpu_for_selected_opcodes, |prj, cmd| {
    prj.add_test("OpcodeCpuTrace.t.sol", OPCODE_FIXTURE);

    cmd.forge_fuse().args(["test", "--cpu-trace", "--opcodes", "SLOAD,SSTORE", "-vvvvv"]);
    assert_cpu_trace(&mut cmd).stdout_eq(str![[r#"
...
Ran 1 test for test/OpcodeCpuTrace.t.sol:OpcodeCpuTraceTest
[PASS] testOpcodeCpuTrace() ([GAS])
Traces:
  [22348 gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] OpcodeCpuTraceTest::testOpcodeCpuTrace()
    ├─ [22100 gas | [CPU]] SSTORE 0x0: 0x0 → 0x2a
    ├─ [100 gas | [CPU]] SLOAD
    └─ ← [Stop]

Suite result: ok. 1 passed; 0 failed; 0 skipped; [ELAPSED]

Ran 1 test suite [ELAPSED]: 1 tests passed, 0 failed, 0 skipped (1 total tests)

"#]]);
});

forgetest_init!(selected_opcodes_remain_gas_only_without_cpu_trace, |prj, cmd| {
    prj.add_test("OpcodeCpuTrace.t.sol", OPCODE_FIXTURE);

    cmd.forge_fuse().args(["test", "--opcodes", "SLOAD,SSTORE", "-vvvvv"]);
    cmd.assert_success().stdout_eq(str![[r#"
...
Ran 1 test for test/OpcodeCpuTrace.t.sol:OpcodeCpuTraceTest
[PASS] testOpcodeCpuTrace() ([GAS])
Traces:
  [22348] OpcodeCpuTraceTest::testOpcodeCpuTrace()
    ├─ [22100] SSTORE 0x0: 0x0 → 0x2a
    ├─ [100] SLOAD
    └─ ← [Stop]

Suite result: ok. 1 passed; 0 failed; 0 skipped; [ELAPSED]

Ran 1 test suite [ELAPSED]: 1 tests passed, 0 failed, 0 skipped (1 total tests)

"#]]);
});

forgetest_init!(cpu_trace_times_nested_call_and_child_opcode, |prj, cmd| {
    prj.add_test("NestedOpcodeCpuTrace.t.sol", NESTED_OPCODE_FIXTURE);

    cmd.forge_fuse().args(["test", "--cpu-trace", "--opcodes", "CALL,SSTORE", "-vvvvv"]);
    assert_cpu_trace(&mut cmd).stdout_eq(str![[r#"
...
Ran 1 test for test/NestedOpcodeCpuTrace.t.sol:NestedOpcodeCpuTest
[PASS] testNestedOpcodeCpu() ([GAS])
Traces:
  [153397 gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] NestedOpcodeCpuTest::testNestedOpcodeCpu()
    ├─ [77223 gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] → new NestedOpcodeCpuTarget@0x5615dEB798BB3E4dFa0139dFa1b3D433Cc23b72f
    │   └─ ← [Return] 110 bytes of code
    ├─ [1056835668 gas | [CPU]] CALL
    ├─ [43290 gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] NestedOpcodeCpuTarget::write()
    │   ├─ [22100 gas | [CPU]] SSTORE 0x0: 0x0 → 0x2a
    │   └─ ← [Stop]
    └─ ← [Stop]

Suite result: ok. 1 passed; 0 failed; 0 skipped; [ELAPSED]

Ran 1 test suite [ELAPSED]: 1 tests passed, 0 failed, 0 skipped (1 total tests)

"#]]);
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

    cmd.forge_fuse().args(["test", "--fork-url", &handle.http_endpoint(), "--cpu-trace", "-vvvv"]);
    assert_cpu_trace(&mut cmd).stdout_eq(str![[r#"
...
Ran 1 test for test/ForkCpuTrace.t.sol:ForkCpuTraceTest
[PASS] testForkCpuTrace() ([GAS])
Traces:
  [124424 gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] ForkCpuTraceTest::testForkCpuTrace()
    ├─ [91291 gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] → new ForkCpuTraceTarget@0x5615dEB798BB3E4dFa0139dFa1b3D433Cc23b72f
    │   └─ ← [Return] 175 bytes of code
    ├─ [310 gas | [CPU] | [SELF_CPU] | [CPU_PERCENT]] ForkCpuTraceTarget::ping() [staticcall]
    │   └─ ← [Return] 42
    └─ ← [Stop]

Suite result: ok. 1 passed; 0 failed; 0 skipped; [ELAPSED]

Ran 1 test suite [ELAPSED]: 1 tests passed, 0 failed, 0 skipped (1 total tests)

"#]]);
});
