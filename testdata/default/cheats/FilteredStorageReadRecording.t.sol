// SPDX-License-Identifier: Unlicense
pragma solidity ^0.8.18;

import "forge-std/Test.sol";

interface VmFilteredStorageReadRecording {
    error FilteredStorageReadRecordingAlreadyStarted();
    error FilteredStorageReadRecordingNotStarted();
    error FilteredStorageReadAccountFilterEmpty();
    error FilteredStorageReadAccountFilterTooLarge(uint256 count, uint256 max);

    struct FilteredStorageRead {
        address account;
        bytes32 slot;
    }

    function startFilteredStorageReadRecording(bytes32[32] calldata accounts, uint256 count) external;

    function stopAndReturnFilteredStorageReadRecording() external returns (FilteredStorageRead[] memory reads);
}

contract FilteredStorageReadTarget {
    function loadSlot(uint256 slot) external view returns (uint256 value) {
        assembly {
            value := sload(slot)
        }
    }

    function loadSequence(FilteredStorageReadTarget selected, FilteredStorageReadTarget unselected, uint256 unknownSlot)
        external
        view
        returns (uint256 value)
    {
        assembly {
            value := sload(0)
        }
        value ^= selected.loadSlot(1);
        value ^= unselected.loadSlot(2);
        assembly {
            value := xor(value, sload(unknownSlot))
            value := xor(value, sload(0))
        }
    }
}

contract FilteredStorageReadProxy {
    function delegateLoad(address implementation, uint256 slot) external returns (uint256 value) {
        (bool success, bytes memory output) =
            implementation.delegatecall(abi.encodeCall(FilteredStorageReadTarget.loadSlot, slot));
        require(success);
        value = abi.decode(output, (uint256));
    }
}

contract FilteredStorageReadRecordingTest is Test {
    VmFilteredStorageReadRecording private constant filteredVm =
        VmFilteredStorageReadRecording(address(uint160(uint256(keccak256("hevm cheat code")))));

    function testRecordsSelectedReadsInGlobalOrderWithDuplicatesAndUnknownSlots() public {
        FilteredStorageReadTarget root = new FilteredStorageReadTarget();
        FilteredStorageReadTarget nested = new FilteredStorageReadTarget();
        FilteredStorageReadTarget unselected = new FilteredStorageReadTarget();
        bytes32[32] memory accounts;
        accounts[0] = _accountWord(address(root));
        accounts[1] = _accountWord(address(nested));
        uint256 unknownSlot = uint256(keccak256("unknown selected slot"));

        filteredVm.startFilteredStorageReadRecording(accounts, 2);
        root.loadSequence(nested, unselected, unknownSlot);
        VmFilteredStorageReadRecording.FilteredStorageRead[] memory reads =
            filteredVm.stopAndReturnFilteredStorageReadRecording();

        assertEq(reads.length, 4);
        _assertRead(reads[0], address(root), bytes32(uint256(0)));
        _assertRead(reads[1], address(nested), bytes32(uint256(1)));
        _assertRead(reads[2], address(root), bytes32(unknownSlot));
        _assertRead(reads[3], address(root), bytes32(uint256(0)));
    }

    function testDelegatecallUsesProxyStorageContext() public {
        FilteredStorageReadTarget implementation = new FilteredStorageReadTarget();
        FilteredStorageReadProxy proxy = new FilteredStorageReadProxy();
        bytes32[32] memory accounts;
        accounts[0] = _accountWord(address(proxy));
        accounts[1] = _accountWord(address(implementation));

        filteredVm.startFilteredStorageReadRecording(accounts, 2);
        proxy.delegateLoad(address(implementation), 777);
        VmFilteredStorageReadRecording.FilteredStorageRead[] memory reads =
            filteredVm.stopAndReturnFilteredStorageReadRecording();

        assertEq(reads.length, 1);
        _assertRead(reads[0], address(proxy), bytes32(uint256(777)));
    }

    /// forge-config: default.allow_internal_expect_revert = true
    function testLifecycleAndBoundsUseTypedErrorsWithoutPartialStarts() public {
        vm.expectRevert(VmFilteredStorageReadRecording.FilteredStorageReadRecordingNotStarted.selector);
        filteredVm.stopAndReturnFilteredStorageReadRecording();

        bytes32[32] memory accounts;
        vm.expectRevert(VmFilteredStorageReadRecording.FilteredStorageReadAccountFilterEmpty.selector);
        filteredVm.startFilteredStorageReadRecording(accounts, 0);

        accounts[0] = _accountWord(address(1));
        vm.expectRevert(
            abi.encodeWithSelector(
                VmFilteredStorageReadRecording.FilteredStorageReadAccountFilterTooLarge.selector, 33, 32
            )
        );
        filteredVm.startFilteredStorageReadRecording(accounts, 33);

        filteredVm.startFilteredStorageReadRecording(accounts, 1);
        vm.expectRevert(VmFilteredStorageReadRecording.FilteredStorageReadRecordingAlreadyStarted.selector);
        filteredVm.startFilteredStorageReadRecording(accounts, 1);
        assertEq(filteredVm.stopAndReturnFilteredStorageReadRecording().length, 0);
    }

    function testFilteredRecordingCoexistsWithDebugTrace() public {
        FilteredStorageReadTarget target = new FilteredStorageReadTarget();
        bytes32[32] memory accounts;
        accounts[0] = _accountWord(address(target));

        vm.startDebugTraceRecording();
        filteredVm.startFilteredStorageReadRecording(accounts, 1);
        target.loadSlot(5);
        VmFilteredStorageReadRecording.FilteredStorageRead[] memory reads =
            filteredVm.stopAndReturnFilteredStorageReadRecording();
        Vm.DebugStep[] memory steps = vm.stopAndReturnDebugTraceRecording();

        assertEq(reads.length, 1);
        _assertRead(reads[0], address(target), bytes32(uint256(5)));
        bool foundLoad;
        for (uint256 i = 0; i < steps.length; ++i) {
            if (
                steps[i].opcode == 0x54 && steps[i].contractAddr == address(target) && steps[i].stack.length > 0
                    && steps[i].stack[0] == 5
            ) {
                foundLoad = true;
                break;
            }
        }
        assertTrue(foundLoad);
    }

    function _assertRead(VmFilteredStorageReadRecording.FilteredStorageRead memory read, address account, bytes32 slot)
        private
        pure
    {
        assertEq(read.account, account);
        assertEq(read.slot, slot);
    }

    function _accountWord(address account) private pure returns (bytes32) {
        return bytes32(uint256(uint160(account)));
    }
}
