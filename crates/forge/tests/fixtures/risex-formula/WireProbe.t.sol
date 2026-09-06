// SPDX-License-Identifier: MIT OR Apache-2.0
pragma solidity >=0.8.29 <0.9.0;

contract WireProbeTest {
    address private constant RISK_FORMULA = address(0xf100);
    bytes32 private constant UNSUPPORTED_SCHEMA_HEADER =
        0x0000000000000000000000000000000000000000000000000000040152534631;
    bytes32 private constant UNSUPPORTED_ABI_HEADER =
        0x0000000000000000000000000000000000000000000000000000020152534631;

    function testExactWireAndMalformedRejection() external view {
        bytes memory request =
            hex"00000000000000000000005566112233440001000000010000000001000003010123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefffffffffffffffffffffffffffffffffffffffffffffffeeddccbbaa99887767112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        require(request.length == 160, "request length");

        (bool success, bytes memory response) = RISK_FORMULA.staticcall(request);
        require(success, "canonical call");
        _assertStatusOnly(response, UNSUPPORTED_SCHEMA_HEADER);

        bytes memory malformed = new bytes(159);
        for (uint256 i; i < malformed.length; ++i) {
            malformed[i] = request[i];
        }
        (success, response) = RISK_FORMULA.staticcall(malformed);
        require(success, "malformed call");
        _assertStatusOnly(response, UNSUPPORTED_ABI_HEADER);
    }

    function _assertStatusOnly(bytes memory response, bytes32 expectedHeader) private pure {
        require(response.length == 160, "response length");
        bytes32 header;
        assembly ("memory-safe") {
            header := mload(add(response, 0x20))
        }
        require(header == expectedHeader, "response header");
        for (uint256 i = 32; i < response.length; ++i) {
            require(response[i] == 0, "nonzero response tail");
        }
    }
}

contract ProviderOffProbeTest {
    function testProviderOffReturnsEmpty() external view {
        bytes memory request =
            hex"00000000000000000000005566112233440001000000010000000001000003010123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefffffffffffffffffffffffffffffffffffffffffffffffeeddccbbaa99887767112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        require(request.length == 160, "request length");
        (bool success, bytes memory response) = address(0xf100).staticcall(request);
        require(success, "provider-off call");
        require(response.length == 0, "provider-off response");
    }
}
