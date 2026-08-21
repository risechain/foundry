// SPDX-License-Identifier: MIT OR Apache-2.0
pragma solidity ^0.8.18;

import "utils/Test.sol";

contract CpuSnapshotsTest is Test {
    function testCpuSnapshotDefaultGroup() public {
        vm.startSnapshotCpu("default");
        consumeCpu();
        assertGt(vm.stopSnapshotCpu(), 0);
    }

    function testCpuSnapshotNamedOverloads() public {
        vm.startSnapshotCpu("group", "explicit");
        consumeCpu();
        assertGt(vm.stopSnapshotCpu("group", "explicit"), 0);

        vm.startSnapshotCpu("implicit");
        consumeCpu();
        assertGt(vm.stopSnapshotCpu("implicit"), 0);
    }

    function testCpuSnapshotRejectsNestedSnapshots() public {
        vm.startSnapshotCpu("first");
        (bool success,) = address(vm).call(abi.encodeWithSignature("startSnapshotCpu(string)", "second"));
        assertTrue(!success);
        vm.stopSnapshotCpu();
    }

    function testCpuSnapshotRejectsStopWithoutStart() public {
        (bool success,) = address(vm).call(abi.encodeWithSignature("stopSnapshotCpu()"));
        assertTrue(!success);
    }

    function consumeCpu() internal pure returns (uint256 result) {
        for (uint256 i; i < 100; ++i) {
            result += i;
        }
    }
}
