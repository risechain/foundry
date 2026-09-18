// SPDX-License-Identifier: MIT OR Apache-2.0
pragma solidity >=0.8.29 <0.9.0;

import {Test} from "forge-std/Test.sol";

contract RiskFormulaRuntimeBlob {
    constructor() {
        bytes memory runtime =
            hex"__RUNTIME_HEX__";
        assembly ("memory-safe") {
            return(add(runtime, 0x20), mload(runtime))
        }
    }
}

contract JournalProbeTest is Test {
    address private constant RISK_FORMULA = address(0xf100);
    bytes32 private constant BLOB_CODE_HASH = __BLOB_CODE_HASH__;
    bytes32 private constant LOADER_SCHEMA_HASH = __LOADER_SCHEMA_HASH__;
    bytes32 private constant OK_HEADER = 0x0000000000000000000000000000000100010000000100000000000152534631;
    bytes32 private constant DESCRIPTOR_SLOT =
        0x311eaf2cf29c4b2f8b8ce017889b075a91bdb7242e61b82230138ee4fa0a3700;
    bytes32 private constant TRANSIENT_KEY = keccak256("risex.journal-probe.transient");

    event ChildMutation(uint32 epoch);
    event ParentObservation(uint32 epoch);

    address private s_blob;
    uint256 private s_parentDescriptor;

    function setUp() public {
        s_blob = address(new RiskFormulaRuntimeBlob());
        assertEq(s_blob.codehash, BLOB_CODE_HASH, "fixture runtime hash");
    }

    function testParentJournalAndNestedRevertIsolation() public {
        s_parentDescriptor = _installDescriptor(1);
        bytes memory parentRequest = _request(7, 1, 7);

        bytes memory first = _callFormula(parentRequest);
        _assertOkResponse(first, 7);

        (bool childSuccess, bytes memory childResult) = address(this).call(abi.encodeCall(this.mutateCallAndRevert, ()));
        assertFalse(childSuccess, "child must revert");
        assertEq(childResult.length, 0, "child revert data");
        assertEq(_load(DESCRIPTOR_SLOT), bytes32(s_parentDescriptor), "child storage leaked");
        assertEq(_transientLoad(TRANSIENT_KEY), bytes32(0), "child transient storage leaked");

        bytes32 childProviderSlot = _portfolioCrossSlot(8, 1);
        (uint256 childOnlyReadCost, bytes32 childBitmap) = _sloadCost(childProviderSlot);
        assertEq(childBitmap, bytes32(0), "unexpected child bitmap");
        assertEq(childOnlyReadCost, 2_110, "reverted nested provider read stayed warm");

        bytes memory second = _callFormula(parentRequest);
        _assertOkResponse(second, 7);

        bytes32 parentProviderSlot = _portfolioCrossSlot(7, 1);
        (uint256 parentReadCost, bytes32 parentBitmap) = _sloadCost(parentProviderSlot);
        assertEq(parentBitmap, bytes32(0), "unexpected parent bitmap");
        assertEq(parentReadCost, 110, "parent provider read was not warm");
        emit ParentObservation(1);
    }

    function mutateCallAndRevert() external {
        require(msg.sender == address(this), "self only");
        uint256 childDescriptor = _packedDescriptor(s_blob, 2);
        bytes32 descriptorSlot = DESCRIPTOR_SLOT;
        bytes32 transientKey = TRANSIENT_KEY;
        assembly {
            sstore(descriptorSlot, childDescriptor)
            tstore(transientKey, 0xc0de)
        }
        emit ChildMutation(2);

        bytes memory response = _callFormula(_request(8, 1, 9));
        _assertOkResponse(response, 9);
        assembly ("memory-safe") {
            revert(0, 0)
        }
    }

    function _installDescriptor(uint32 epoch) private returns (uint256 packed) {
        packed = _packedDescriptor(s_blob, epoch);
        bytes32 descriptorSlot = DESCRIPTOR_SLOT;
        assembly {
            sstore(descriptorSlot, packed)
        }
    }

    function _packedDescriptor(address blob, uint32 epoch) private pure returns (uint256) {
        return uint256(uint160(blob)) | (uint256(epoch) << 160);
    }

    function _request(uint32 userId, uint16 targetMarketId, uint256 baseBalance)
        private
        pure
        returns (bytes memory request)
    {
        uint256 metadata = (uint256(targetMarketId) << 152) | (uint256(userId) << 120) | (uint256(1) << 104)
            | (uint256(1) << 72) | (uint256(1) << 32) | (uint256(3) << 8) | 1;
        request = abi.encodePacked(
            bytes32(metadata), LOADER_SCHEMA_HASH, bytes32(baseBalance), bytes32(0), bytes32(uint256(1))
        );
        assert(request.length == 160);
    }

    function _callFormula(bytes memory request) private view returns (bytes memory response) {
        (bool success, bytes memory result) = RISK_FORMULA.staticcall(request);
        assertTrue(success, "F100 call failed");
        assertEq(result.length, 160, "F100 response length");
        return result;
    }

    function _assertOkResponse(bytes memory response, uint256 expectedCrossBalance) private pure {
        uint256 metadata;
        uint256 crossBalance;
        uint256 totalInitialMargin;
        uint256 targetBuySize;
        uint256 targetSellSize;
        assembly ("memory-safe") {
            metadata := mload(add(response, 0x20))
            crossBalance := mload(add(response, 0x40))
            totalInitialMargin := mload(add(response, 0x60))
            targetBuySize := mload(add(response, 0x80))
            targetSellSize := mload(add(response, 0xa0))
        }
        assert(bytes32(metadata) == OK_HEADER);
        assert(crossBalance == expectedCrossBalance);
        assert(totalInitialMargin == 0);
        assert(targetBuySize == 0);
        assert(targetSellSize == 0);
    }

    function _portfolioCrossSlot(uint32 userId, uint16 marketId) private pure returns (bytes32) {
        bytes32 root = 0x65d6085942bc2703c9817a6c1a5115836175a71beefe9943884b3c39ebb09800;
        bytes32 portfolio = keccak256(abi.encode(uint256(userId), uint256(root)));
        return keccak256(abi.encode(uint256(marketId >> 8), uint256(portfolio)));
    }

    function _load(bytes32 slot) private view returns (bytes32 value) {
        assembly {
            value := sload(slot)
        }
    }

    function _transientLoad(bytes32 key) private view returns (bytes32 value) {
        assembly {
            value := tload(key)
        }
    }

    function _sloadCost(bytes32 slot) private view returns (uint256 cost, bytes32 value) {
        assembly {
            let beforeGas := gas()
            value := sload(slot)
            cost := sub(beforeGas, gas())
        }
    }
}
