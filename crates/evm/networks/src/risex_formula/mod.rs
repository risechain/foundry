//! RISEx risk-formula precompile support.

mod abi;
mod config;
#[cfg(feature = "risex-risk-precompile")]
mod formula;
#[cfg(feature = "risex-risk-precompile")]
pub(crate) mod loader;
mod metrics;
mod precompile;
#[cfg(feature = "risex-risk-precompile")]
#[allow(dead_code)]
pub(crate) mod storage;

pub use abi::{Request, Response, Status};
#[cfg(test)]
pub(crate) use config::reset_provider_mode_for_test;
pub use config::{ProviderMode, ProviderModeConflict, provider_mode, set_provider_mode};
pub use metrics::{
    InvocationMetadata, begin_invocation, clear_metrics, drain_metrics, peak_rss_bytes,
    serialize_jsonl, set_metrics_enabled, submit_invocation,
};
pub use precompile::{RISEX_RISK_FORMULA_ADDRESS, risk_formula_precompile};

#[cfg(test)]
mod tests {
    #[cfg(feature = "risex-risk-precompile")]
    use super::{
        ProviderMode, RISEX_RISK_FORMULA_ADDRESS, config::reset_provider_mode_for_test,
        set_provider_mode,
    };
    use crate::NetworkConfigs;
    use alloy_evm::precompiles::PrecompilesMap;
    #[cfg(feature = "risex-risk-precompile")]
    use alloy_evm::{Evm, EvmEnv, eth::EthEvmBuilder, precompiles::Precompile};
    use alloy_primitives::address;
    #[cfg(feature = "risex-risk-precompile")]
    use alloy_primitives::{Address, Bytes, U256, keccak256};
    use revm::precompile::Precompiles;
    #[cfg(feature = "risex-risk-precompile")]
    use revm::{
        bytecode::Bytecode, context::TxEnv, database::InMemoryDB, primitives::TxKind,
        state::AccountInfo,
    };

    #[test]
    fn risex_formula_address_is_outside_osaka_static_precompiles() {
        let address = address!("000000000000000000000000000000000000f100");

        assert!(Precompiles::osaka().get(&address).is_none());
    }

    #[test]
    #[cfg(not(feature = "risex-risk-precompile"))]
    fn risex_formula_address_is_absent_without_feature() {
        let address = address!("000000000000000000000000000000000000f100");
        let mut precompiles = PrecompilesMap::from_static(Precompiles::osaka());

        NetworkConfigs::default().inject_precompiles(&mut precompiles);

        assert!(precompiles.get(&address).is_none());
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn risex_formula_address_registration_follows_provider_mode() {
        let _reset = reset_provider_mode_for_test();
        let mut precompiles = PrecompilesMap::from_static(Precompiles::osaka());

        NetworkConfigs::default().inject_precompiles(&mut precompiles);

        assert!(precompiles.get(&RISEX_RISK_FORMULA_ADDRESS).is_none());
        assert!(
            !NetworkConfigs::default()
                .precompiles_label(None, None)
                .contains_key(&RISEX_RISK_FORMULA_ADDRESS)
        );

        set_provider_mode(ProviderMode::Specialized).unwrap();
        let mut precompiles = PrecompilesMap::from_static(Precompiles::osaka());

        assert!(precompiles.get(&RISEX_RISK_FORMULA_ADDRESS).is_none());
        NetworkConfigs::default().inject_precompiles(&mut precompiles);
        assert!(precompiles.get(&RISEX_RISK_FORMULA_ADDRESS).is_some());
        assert!(!precompiles.get(&RISEX_RISK_FORMULA_ADDRESS).unwrap().supports_caching());
        assert_eq!(
            NetworkConfigs::default()
                .precompiles_label(None, None)
                .get(&RISEX_RISK_FORMULA_ADDRESS),
            Some(&"RISExRiskFormula".to_string())
        );
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn risex_formula_address_accepts_static_execution_contexts() {
        assert!(execute_risex_formula_probe(guarded_call(0xfa, RISEX_RISK_FORMULA_ADDRESS), None,));

        let relay = address!("000000000000000000000000000000000000c002");
        assert!(execute_risex_formula_probe(
            guarded_call(0xfa, relay),
            Some(guarded_call(0xf1, RISEX_RISK_FORMULA_ADDRESS)),
        ));
    }

    #[test]
    #[cfg(feature = "risex-risk-precompile")]
    fn risex_formula_address_rejects_non_static_or_indirect_execution() {
        assert!(
            !execute_risex_formula_probe(guarded_call(0xf1, RISEX_RISK_FORMULA_ADDRESS), None,)
        );
        assert!(
            !execute_risex_formula_probe(guarded_call(0xf4, RISEX_RISK_FORMULA_ADDRESS), None,)
        );
        assert!(
            !execute_risex_formula_probe(guarded_call(0xf2, RISEX_RISK_FORMULA_ADDRESS), None,)
        );
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn execute_risex_formula_probe(caller_code: Vec<u8>, relay_code: Option<Vec<u8>>) -> bool {
        let _reset = reset_provider_mode_for_test();
        set_provider_mode(ProviderMode::Specialized).unwrap();
        let caller = address!("000000000000000000000000000000000000c001");
        let relay = address!("000000000000000000000000000000000000c002");
        let tx_caller = address!("000000000000000000000000000000000000c003");
        let mut db = InMemoryDB::default();
        insert_contract(&mut db, caller, caller_code);
        if let Some(relay_code) = relay_code {
            insert_contract(&mut db, relay, relay_code);
        }
        db.insert_account_info(
            tx_caller,
            AccountInfo { balance: U256::from(1_000_000_000_u64), ..Default::default() },
        );

        let mut precompiles = PrecompilesMap::from_static(Precompiles::osaka());
        NetworkConfigs::default().inject_precompiles(&mut precompiles);
        let mut evm = EthEvmBuilder::new(db, EvmEnv::default()).precompiles(precompiles).build();

        evm.transact(
            TxEnv::builder()
                .caller(tx_caller)
                .kind(TxKind::Call(caller))
                .gas_limit(1_000_000)
                .build()
                .unwrap(),
        )
        .unwrap()
        .result
        .is_success()
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn insert_contract(db: &mut InMemoryDB, address: Address, code: Vec<u8>) {
        db.insert_account_info(
            address,
            AccountInfo {
                code_hash: keccak256(&code),
                code: Some(Bytecode::new_raw(Bytes::from(code))),
                ..Default::default()
            },
        );
    }

    #[cfg(feature = "risex-risk-precompile")]
    fn guarded_call(opcode: u8, target: Address) -> Vec<u8> {
        let mut code = vec![0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0];
        if matches!(opcode, 0xf1 | 0xf2) {
            code.extend([0x60, 0]);
        }
        code.push(0x73);
        code.extend_from_slice(target.as_slice());
        code.extend([0x61, 0xff, 0xff, opcode, 0x15, 0x60]);
        let jump_destination = code.len();
        code.extend([0, 0x57, 0x00, 0x5b, 0x60, 0, 0x60, 0, 0xfd]);
        code[jump_destination] = (jump_destination + 3) as u8;
        code
    }
}
