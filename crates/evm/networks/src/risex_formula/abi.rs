//! Canonical fixed-width RISEx risk-formula wire codec.

use alloy_primitives::{B256, U256};

const REQUEST_LENGTH: usize = 160;
const RESPONSE_LENGTH: usize = 160;
const WORD_LENGTH: usize = 32;
pub(super) const SUPPORTED_LOADER_VERSION: u32 = 1;
pub(super) const SUPPORTED_OPERATION_SET_VERSION: u16 = 1;
pub(super) const SUPPORTED_OUTPUT_SCHEMA_ID: u16 = 1;

/// A decoded canonical risk-formula request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Request {
    pub expected_loader_version: u32,
    pub expected_operation_set_version: u16,
    pub user_id: u32,
    pub target_market_id: u16,
    pub expected_loader_schema_hash: B256,
    /// Raw two's-complement `int256`; the arithmetic layer interprets its sign.
    pub base_balance: U256,
    pub source_policy: U256,
    pub target_mark_price: U256,
}

impl Request {
    /// Decodes a V1 request, rejecting every non-canonical packed field.
    pub fn decode(input: &[u8]) -> Result<Self, Status> {
        if input.len() != REQUEST_LENGTH {
            return Err(Status::UnsupportedAbi);
        }

        let metadata = &input[..WORD_LENGTH];
        if metadata[..11].iter().any(|byte| *byte != 0)
            || u16::from_be_bytes([metadata[28], metadata[29]]) != 0
            || metadata[23..27].iter().any(|byte| *byte != 0)
            || metadata[31] != 1
            || metadata[27] != 1
            || metadata[30] != 3
        {
            return Err(Status::UnsupportedAbi);
        }

        Ok(Self {
            expected_loader_version: u32::from_be_bytes([
                metadata[19],
                metadata[20],
                metadata[21],
                metadata[22],
            ]),
            expected_operation_set_version: u16::from_be_bytes([metadata[17], metadata[18]]),
            user_id: u32::from_be_bytes([metadata[13], metadata[14], metadata[15], metadata[16]]),
            target_market_id: u16::from_be_bytes([metadata[11], metadata[12]]),
            expected_loader_schema_hash: B256::from_slice(&input[32..64]),
            base_balance: U256::from_be_slice(&input[64..96]),
            source_policy: U256::from_be_slice(&input[96..128]),
            target_mark_price: U256::from_be_slice(&input[128..160]),
        })
    }
}

/// Result status encoded in a risk-formula response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    Unavailable = 1,
    UnsupportedAbi = 2,
    UnsupportedLoader = 3,
    UnsupportedLoaderSchema = 4,
    UnsupportedOperationSet = 5,
    FormulaInactive = 6,
    BlobCodeHashMismatch = 7,
    FormulaInvalid = 8,
    SchemaMismatch = 9,
    BoundExceeded = 10,
    ArithmeticError = 11,
    StateLoadError = 12,
}

impl Status {
    const fn to_wire(self) -> u8 {
        self as u8
    }
}

/// Canonical fixed-width risk-formula response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Response {
    pub status: Status,
    /// Raw two's-complement `int256`; the arithmetic layer interprets its sign.
    pub cross_balance: U256,
    pub total_cross_initial_margin: U256,
    pub effective_target_buy_size: U256,
    pub effective_target_sell_size: U256,
}

impl Response {
    /// Encodes the canonical five-word V1 response.
    pub fn encode(&self) -> [u8; RESPONSE_LENGTH] {
        let mut output = [0; RESPONSE_LENGTH];
        let metadata = &mut output[..WORD_LENGTH];
        metadata[28..32].copy_from_slice(&0x5253_4631_u32.to_be_bytes());
        metadata[27] = 1;
        metadata[26] = self.status.to_wire();
        if self.status != Status::Ok {
            return output;
        }
        metadata[18..22].copy_from_slice(&SUPPORTED_LOADER_VERSION.to_be_bytes());
        metadata[16..18].copy_from_slice(&SUPPORTED_OPERATION_SET_VERSION.to_be_bytes());
        metadata[14..16].copy_from_slice(&SUPPORTED_OUTPUT_SCHEMA_ID.to_be_bytes());

        copy_word(&mut output, 1, self.cross_balance);
        copy_word(&mut output, 2, self.total_cross_initial_margin);
        copy_word(&mut output, 3, self.effective_target_buy_size);
        copy_word(&mut output, 4, self.effective_target_sell_size);
        output
    }

    /// Produces a zeroed canonical response for validation failures before any provider is
    /// selected.
    pub const fn with_status(status: Status) -> Self {
        Self {
            status,
            cross_balance: U256::ZERO,
            total_cross_initial_margin: U256::ZERO,
            effective_target_buy_size: U256::ZERO,
            effective_target_sell_size: U256::ZERO,
        }
    }
}

fn copy_word(output: &mut [u8; RESPONSE_LENGTH], index: usize, value: U256) {
    let start = index * WORD_LENGTH;
    output[start..start + WORD_LENGTH].copy_from_slice(&value.to_be_bytes::<WORD_LENGTH>());
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, U256};

    use super::{Request, Response, Status};

    // Synthetic fixture identities are golden-vector-only and activation-ineligible.
    const SHADOW_SNAPSHOT: &str = "0000000000000000000000e5f6a1b2c3d400010000000100000000010000030189abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567fffffffffffffffffffffffffffffffffefdfcfbfaf9f8f7f6f5f4f3f2f1f0f033445566778899aabbccddeeff00112233445566778899aabbccddeeff001122cdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab";
    const OK_RESPONSE: &str = "0000000000000000000000000000000100010000000100000000000152534631ffffffffffffffffffffffffffffffffffffffffffffffeeddccbbaa9988776700000000000000000000000000000000000000000000002132435465768798a90000000000000000000000000000000000000000000000718293a4b5c6d7e8f900000000000000000000000000000000000000000000008192a3b4c5d6e7f809";

    fn bytes(hex: &str) -> Vec<u8> {
        alloy_primitives::hex::decode(hex).unwrap()
    }

    #[test]
    fn request_decodes_literal_shadow_snapshot_vector() {
        let request = Request::decode(&bytes(SHADOW_SNAPSHOT)).unwrap();

        assert_eq!(request.expected_loader_version, 1);
        assert_eq!(request.expected_operation_set_version, 1);
        assert_eq!(request.user_id, 0xa1b2_c3d4);
        assert_eq!(request.target_market_id, 0xe5f6);
        assert_eq!(
            request.expected_loader_schema_hash,
            B256::from_slice(&bytes(
                "89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567"
            )),
        );
        assert_eq!(
            request.base_balance,
            word("fffffffffffffffffffffffffffffffffefdfcfbfaf9f8f7f6f5f4f3f2f1f0f0"),
        );
        assert_eq!(
            request.source_policy,
            word("33445566778899aabbccddeeff00112233445566778899aabbccddeeff001122"),
        );
        assert_eq!(
            request.target_mark_price,
            word("cdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab"),
        );
    }

    #[test]
    fn request_rejects_noncanonical_wire() {
        let valid = bytes(SHADOW_SNAPSHOT);
        let too_long = [valid.as_slice(), &[0]].concat();

        assert_eq!(Request::decode(&valid[..159]), Err(Status::UnsupportedAbi));
        assert_eq!(Request::decode(&too_long), Err(Status::UnsupportedAbi));
        assert_eq!(Request::decode(&replace_byte(&valid, 0, 1)), Err(Status::UnsupportedAbi));
        assert_eq!(Request::decode(&replace_byte(&valid, 29, 1)), Err(Status::UnsupportedAbi));
        assert_eq!(Request::decode(&replace_byte(&valid, 30, 4)), Err(Status::UnsupportedAbi));
        assert_eq!(Request::decode(&replace_byte(&valid, 27, 0)), Err(Status::UnsupportedAbi));
        assert_eq!(Request::decode(&replace_byte(&valid, 31, 2)), Err(Status::UnsupportedAbi));
    }

    #[test]
    fn response_encodes_literal_ok_vector_with_raw_signed_words() {
        assert_eq!(literal_ok_response().encode().as_slice(), bytes(OK_RESPONSE));
    }

    #[test]
    fn response_status_words_match_literal_fixture_vectors() {
        let cases = [
            Status::Unavailable,
            Status::UnsupportedAbi,
            Status::UnsupportedLoader,
            Status::UnsupportedLoaderSchema,
            Status::UnsupportedOperationSet,
            Status::FormulaInactive,
            Status::BlobCodeHashMismatch,
            Status::FormulaInvalid,
            Status::SchemaMismatch,
            Status::BoundExceeded,
            Status::ArithmeticError,
            Status::StateLoadError,
        ];

        for status in cases {
            let mut response = literal_ok_response();
            response.status = status;
            assert_eq!(response.encode(), Response::with_status(status).encode());
        }
    }

    fn literal_ok_response() -> Response {
        Response {
            status: Status::Ok,
            cross_balance: word("ffffffffffffffffffffffffffffffffffffffffffffffeeddccbbaa99887767"),
            total_cross_initial_margin: U256::from(0x0021_3243_5465_7687_98a9_u128),
            effective_target_buy_size: U256::from(0x0071_8293_a4b5_c6d7_e8f9_u128),
            effective_target_sell_size: U256::from(0x0081_92a3_b4c5_d6e7_f809_u128),
        }
    }

    fn replace_byte(wire: &[u8], index: usize, value: u8) -> Vec<u8> {
        let mut malformed = wire.to_vec();
        malformed[index] = value;
        malformed
    }

    fn word(hex: &str) -> U256 {
        let bytes: [u8; 32] = bytes(hex).try_into().unwrap();
        U256::from_be_bytes::<32>(bytes)
    }
}
