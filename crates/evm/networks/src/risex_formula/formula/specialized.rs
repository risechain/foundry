use alloy_primitives::{I256, U256, U512};

use crate::risex_formula::loader::{MarginMode, MarketRow};

const WAD: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
const I256_MIN_MAGNITUDE: U256 = U256::from_limbs([0, 0, 0, 1_u64 << 63]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArithmeticError {
    DivisionByZero,
    U256Overflow,
    U256ProductOverflow,
    I128Overflow,
    I256Overflow,
    I256ProductOverflow,
    AbsMinOverflow,
    I256ConversionOverflow,
    U112Overflow,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RowOutputs {
    pub(crate) balance_contribution: I256,
    pub(crate) initial_margin: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AggregateOutputs {
    pub(crate) cross_balance: I256,
    pub(crate) total_initial_margin: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReductionAttempt {
    pub(crate) result: Result<(), ArithmeticError>,
    pub(crate) operations: u8,
}

pub(crate) struct SpecializedEvaluator;

impl SpecializedEvaluator {
    pub(crate) fn evaluate(row: &MarketRow) -> Result<RowOutputs, ArithmeticError> {
        let mut cross_balance_delta = I256::ZERO;
        let mut initial_margin = U256::ZERO;
        let mut isolated_usage = U256::ZERO;

        match row.margin_mode {
            MarginMode::Cross => {
                let position_size = widen_i128(row.effective_position_size);
                let position_quote = widen_i128(row.effective_position_quote);
                let position_abs = abs_i256_checked(position_size)?;
                let is_negative = position_size.is_negative();
                let unsettled_usdc = signed_mul_div_unsigned_checked_toward_zero(
                    position_size,
                    row.mark_price,
                    WAD,
                )?
                .checked_add(position_quote)
                .ok_or(ArithmeticError::I256Overflow)?;
                let funding_payment = if position_size.is_zero() {
                    I256::ZERO
                } else {
                    let funding_index_delta = row
                        .accumulated_funding_payment
                        .checked_sub(row.effective_last_funding_payment)
                        .ok_or(ArithmeticError::I128Overflow)?;
                    signed_mul_div_checked_toward_zero(
                        widen_i128(funding_index_delta),
                        position_size,
                        WAD,
                    )?
                };
                let buy_skew = if is_negative {
                    distance(position_abs, row.effective_buy_order_size)
                } else {
                    position_abs
                        .checked_add(row.effective_buy_order_size)
                        .ok_or(ArithmeticError::U256Overflow)?
                };
                let sell_skew = if is_negative {
                    position_abs
                        .checked_add(row.effective_sell_order_size)
                        .ok_or(ArithmeticError::U256Overflow)?
                } else {
                    distance(position_abs, row.effective_sell_order_size)
                };
                cross_balance_delta = unsettled_usdc
                    .checked_sub(funding_payment)
                    .ok_or(ArithmeticError::I256Overflow)?;
                initial_margin = full_mul_div_up(
                    buy_skew.max(sell_skew),
                    row.mark_price,
                    row.effective_leverage_wad,
                )?;
            }
            MarginMode::Isolated => {
                if row.effective_isolated_balance >= (1_u128 << 112) {
                    return Err(ArithmeticError::U112Overflow);
                }
                isolated_usage = U256::from(row.effective_isolated_balance)
                    .checked_add(mul_div_checked_down(
                        row.effective_order_notional,
                        WAD,
                        row.effective_leverage_wad,
                    )?)
                    .ok_or(ArithmeticError::U256Overflow)?;
            }
        }

        let balance_contribution = row
            .projected_settlement_pnl
            .checked_add(cross_balance_delta)
            .ok_or(ArithmeticError::I256Overflow)?
            .checked_sub(u256_to_i256_checked(isolated_usage)?)
            .ok_or(ArithmeticError::I256Overflow)?;
        Ok(RowOutputs { balance_contribution, initial_margin })
    }
}

impl AggregateOutputs {
    pub(crate) const fn new(base_balance: I256) -> Self {
        Self { cross_balance: base_balance, total_initial_margin: U256::ZERO }
    }

    pub(crate) const fn reduce_observed(&mut self, row: RowOutputs) -> ReductionAttempt {
        let Some(cross_balance) = self.cross_balance.checked_add(row.balance_contribution) else {
            return ReductionAttempt { result: Err(ArithmeticError::I256Overflow), operations: 1 };
        };
        self.cross_balance = cross_balance;
        let Some(total_initial_margin) = self.total_initial_margin.checked_add(row.initial_margin)
        else {
            return ReductionAttempt { result: Err(ArithmeticError::U256Overflow), operations: 2 };
        };
        self.total_initial_margin = total_initial_margin;
        ReductionAttempt { result: Ok(()), operations: 2 }
    }
}

fn widen_i128(value: i128) -> I256 {
    let raw = U256::from(value as u128);
    I256::from_raw(if value.is_negative() { raw | (U256::MAX << 128) } else { raw })
}

fn distance(a: U256, b: U256) -> U256 {
    if a >= b { a - b } else { b - a }
}

fn abs_i256_checked(value: I256) -> Result<U256, ArithmeticError> {
    if value == I256::MIN {
        return Err(ArithmeticError::AbsMinOverflow);
    }
    Ok(value.unsigned_abs())
}

fn u256_to_i256_checked(value: U256) -> Result<I256, ArithmeticError> {
    if value > I256::MAX.into_raw() {
        return Err(ArithmeticError::I256ConversionOverflow);
    }
    Ok(I256::from_raw(value))
}

fn mul_div_checked_down(a: U256, b: U256, denominator: U256) -> Result<U256, ArithmeticError> {
    if denominator.is_zero() {
        return Err(ArithmeticError::DivisionByZero);
    }
    a.checked_mul(b)
        .ok_or(ArithmeticError::U256ProductOverflow)
        .map(|product| product / denominator)
}

fn full_mul_div_up(a: U256, b: U256, denominator: U256) -> Result<U256, ArithmeticError> {
    if denominator.is_zero() {
        return Err(ArithmeticError::DivisionByZero);
    }
    let product = U512::from(a) * U512::from(b);
    let denominator = U512::from(denominator);
    let quotient = product.div_ceil(denominator);
    if quotient > U512::from(U256::MAX) {
        return Err(ArithmeticError::U256Overflow);
    }
    let limbs = quotient.as_limbs();
    Ok(U256::from_limbs([limbs[0], limbs[1], limbs[2], limbs[3]]))
}

fn signed_mul_div_unsigned_checked_toward_zero(
    a: I256,
    b: U256,
    denominator: U256,
) -> Result<I256, ArithmeticError> {
    signed_magnitudes_mul_div(a.is_negative(), a.unsigned_abs(), b, denominator)
}

fn signed_mul_div_checked_toward_zero(
    a: I256,
    b: I256,
    denominator: U256,
) -> Result<I256, ArithmeticError> {
    signed_magnitudes_mul_div(
        a.is_negative() != b.is_negative(),
        a.unsigned_abs(),
        b.unsigned_abs(),
        denominator,
    )
}

fn signed_magnitudes_mul_div(
    negative: bool,
    magnitude_a: U256,
    magnitude_b: U256,
    denominator: U256,
) -> Result<I256, ArithmeticError> {
    if denominator.is_zero() {
        return Err(ArithmeticError::DivisionByZero);
    }
    let limit = if negative { I256_MIN_MAGNITUDE } else { I256::MAX.into_raw() };
    if !magnitude_a.is_zero() && magnitude_b > limit / magnitude_a {
        return Err(ArithmeticError::I256ProductOverflow);
    }
    let quotient = magnitude_a * magnitude_b / denominator;
    if !negative || quotient.is_zero() {
        return Ok(I256::from_raw(quotient));
    }
    Ok(I256::from_raw((!quotient) + U256::ONE))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use alloy_primitives::{I256, U256};
    use serde::Deserialize;

    use super::{
        AggregateOutputs, ArithmeticError, SpecializedEvaluator, abs_i256_checked, full_mul_div_up,
        u256_to_i256_checked,
    };
    use crate::risex_formula::loader::{MarginMode, MarketRow};

    const CORPUS: &str = include_str!("../testdata/portfolio_order_risk_v1.json");

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct VectorCorpus {
        row_vectors: Vec<RowVector>,
        arithmetic_error_vectors: Vec<ArithmeticVector>,
        aggregate_vectors: Vec<AggregateVector>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct RowVector {
        name: String,
        expected_status: String,
        row: RowFixture,
        expected: ExpectedOutputs,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct AggregateVector {
        name: String,
        expected_status: String,
        reduction_order: Vec<u16>,
        row_names: Vec<String>,
        expected: ExpectedOutputs,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct ArithmeticVector {
        name: String,
        expected_status: String,
        expected_error: Option<String>,
        kind: String,
        row: Option<RowFixture>,
        rows: Option<Vec<RowFixture>>,
        reduction_order: Option<Vec<u16>>,
        underlying_error: Option<String>,
        inputs: Option<Vec<String>>,
        opcode: Option<String>,
        #[serde(rename = "type")]
        value_type: Option<String>,
        expected: Option<String>,
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RowFixture {
        market_id: String,
        margin_mode: String,
        effective_position_size: String,
        effective_position_quote: String,
        effective_last_funding_payment: String,
        effective_leverage_wad: String,
        effective_isolated_balance: String,
        projected_settlement_pnl: String,
        effective_buy_order_size: String,
        effective_sell_order_size: String,
        effective_order_notional: String,
        mark_price: String,
        accumulated_funding_payment: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ExpectedOutputs {
        cross_balance_delta: String,
        total_initial_margin: String,
    }

    impl RowFixture {
        fn decode(&self) -> MarketRow {
            MarketRow {
                market_id: self.market_id.parse().unwrap(),
                margin_mode: match self.margin_mode.as_str() {
                    "0" => MarginMode::Cross,
                    "1" => MarginMode::Isolated,
                    mode => panic!("unknown generated margin mode {mode}"),
                },
                effective_position_size: self.effective_position_size.parse().unwrap(),
                effective_position_quote: self.effective_position_quote.parse().unwrap(),
                effective_last_funding_payment: self
                    .effective_last_funding_payment
                    .parse()
                    .unwrap(),
                effective_leverage_wad: parse_u256(&self.effective_leverage_wad),
                effective_isolated_balance: self.effective_isolated_balance.parse().unwrap(),
                projected_settlement_pnl: parse_i256(&self.projected_settlement_pnl),
                effective_buy_order_size: parse_u256(&self.effective_buy_order_size),
                effective_sell_order_size: parse_u256(&self.effective_sell_order_size),
                effective_order_notional: parse_u256(&self.effective_order_notional),
                mark_price: parse_u256(&self.mark_price),
                accumulated_funding_payment: self.accumulated_funding_payment.parse().unwrap(),
            }
        }
    }

    #[test]
    fn generated_row_vectors_match_exact_outputs() {
        let corpus = corpus();
        assert_eq!(corpus.row_vectors.len(), 3);

        for vector in &corpus.row_vectors {
            assert_eq!(vector.expected_status, "OK", "{}", vector.name);
            let output = SpecializedEvaluator::evaluate(&vector.row.decode()).unwrap();
            assert_eq!(
                output.balance_contribution,
                parse_i256(&vector.expected.cross_balance_delta),
                "{} balance",
                vector.name,
            );
            assert_eq!(
                output.initial_margin,
                parse_u256(&vector.expected.total_initial_margin),
                "{} initial margin",
                vector.name,
            );
        }
    }

    #[test]
    fn generated_row_and_operation_errors_match_exact_failure_class() {
        let corpus = corpus();
        assert_eq!(corpus.arithmetic_error_vectors.len(), 10);

        for vector in &corpus.arithmetic_error_vectors {
            match vector.kind.as_str() {
                "row" => {
                    let result =
                        SpecializedEvaluator::evaluate(&vector.row.as_ref().unwrap().decode());
                    if vector.expected_status == "OK" {
                        assert!(result.is_ok(), "{}: {result:?}", vector.name);
                        assert_eq!(
                            vector.expected_error.as_deref(),
                            Some("INACTIVE_GUARD_NO_ERROR")
                        );
                    } else {
                        assert_eq!(vector.expected_status, "ARITHMETIC_ERROR");
                        assert_eq!(result, Err(error(&vector.expected_error)));
                    }
                    assert!(vector.rows.is_none());
                    assert!(vector.reduction_order.is_none());
                    assert!(vector.underlying_error.is_none());
                    assert!(vector.inputs.is_none());
                    assert!(vector.opcode.is_none());
                    assert!(vector.value_type.is_none());
                    assert!(vector.expected.is_none());
                }
                "operation" => {
                    assert_eq!(vector.value_type.as_deref(), Some("i256"));
                    let input = vector.inputs.as_ref().unwrap();
                    assert_eq!(input.len(), 1);
                    let result = match vector.opcode.as_deref().unwrap() {
                        "ABS_CHECKED" => {
                            abs_i256_checked(parse_i256(&input[0])).map(I256::from_raw)
                        }
                        "U256_TO_I256_CHECKED" => u256_to_i256_checked(parse_u256(&input[0])),
                        opcode => panic!("unknown generated opcode {opcode}"),
                    };
                    if vector.expected_status == "OK" {
                        assert_eq!(result.unwrap(), parse_i256(vector.expected.as_ref().unwrap()));
                        assert!(vector.expected_error.is_none());
                    } else {
                        assert_eq!(vector.expected_status, "ARITHMETIC_ERROR");
                        assert_eq!(result, Err(error(&vector.expected_error)));
                        assert!(vector.expected.is_none());
                    }
                    assert!(vector.row.is_none());
                    assert!(vector.rows.is_none());
                    assert!(vector.reduction_order.is_none());
                    assert!(vector.underlying_error.is_none());
                }
                "aggregate" => {}
                kind => panic!("unknown generated vector kind {kind}"),
            }
        }
    }

    #[test]
    fn generated_aggregate_vectors_reduce_sequentially_in_declared_order() {
        let corpus = corpus();
        let rows: HashMap<_, _> = corpus.row_vectors.iter().map(|row| (&*row.name, row)).collect();
        assert_eq!(corpus.aggregate_vectors.len(), 1);

        for vector in &corpus.aggregate_vectors {
            assert_eq!(vector.expected_status, "OK", "{}", vector.name);
            assert_eq!(vector.row_names.len(), vector.reduction_order.len());
            let mut aggregate = AggregateOutputs::new(I256::ZERO);
            for (row_name, expected_market_id) in
                vector.row_names.iter().zip(&vector.reduction_order)
            {
                let row = rows[row_name.as_str()].row.decode();
                assert_eq!(row.market_id, *expected_market_id);
                aggregate
                    .reduce_observed(SpecializedEvaluator::evaluate(&row).unwrap())
                    .result
                    .unwrap();
            }
            assert_eq!(aggregate.cross_balance, parse_i256(&vector.expected.cross_balance_delta));
            assert_eq!(
                aggregate.total_initial_margin,
                parse_u256(&vector.expected.total_initial_margin),
            );
        }
    }

    #[test]
    fn generated_ordered_overflow_reports_the_first_reducer_failure() {
        let vector = corpus()
            .arithmetic_error_vectors
            .into_iter()
            .find(|vector| vector.kind == "aggregate")
            .unwrap();
        assert_eq!(vector.name, "ordered_sum_overflow");
        assert_eq!(vector.expected_status, "ARITHMETIC_ERROR");
        assert_eq!(vector.expected_error.as_deref(), Some("ORDERED_SUM_OVERFLOW"));
        assert_eq!(vector.underlying_error.as_deref(), Some("I256_OVERFLOW"));
        assert!(vector.row.is_none());
        assert!(vector.inputs.is_none());
        assert!(vector.opcode.is_none());
        assert!(vector.value_type.is_none());
        assert!(vector.expected.is_none());
        let rows = vector.rows.unwrap();
        let reduction_order = vector.reduction_order.unwrap();
        assert_eq!(rows.len(), reduction_order.len());

        let mut aggregate = AggregateOutputs::new(I256::ZERO);
        for (index, (row, expected_market_id)) in rows.iter().zip(reduction_order).enumerate() {
            let row = row.decode();
            assert_eq!(row.market_id, expected_market_id);
            let result =
                aggregate.reduce_observed(SpecializedEvaluator::evaluate(&row).unwrap()).result;
            if index == 0 {
                assert_eq!(result, Ok(()));
            } else {
                assert_eq!(result, Err(ArithmeticError::I256Overflow));
            }
        }
    }

    #[test]
    fn full_width_mul_div_and_rounding_match_solidity_boundaries() {
        let large = U256::ONE << 200;
        assert_eq!(full_mul_div_up(large, U256::ONE << 100, U256::ONE << 100), Ok(large));
        assert_eq!(full_mul_div_up(U256::from(5), U256::from(3), U256::from(2)), Ok(U256::from(8)));
        assert_eq!(
            full_mul_div_up(
                U256::MAX - U256::ONE,
                U256::MAX - U256::ONE,
                U256::MAX - U256::from(2)
            ),
            Err(ArithmeticError::U256Overflow),
        );
    }

    #[test]
    fn denominator_zero_precedes_product_overflow_and_signed_division_truncates_toward_zero() {
        assert_eq!(
            super::signed_mul_div_unsigned_checked_toward_zero(I256::MAX, U256::MAX, U256::ZERO,),
            Err(ArithmeticError::DivisionByZero),
        );
        assert_eq!(
            super::signed_mul_div_unsigned_checked_toward_zero(
                I256::try_from(-5_i128).unwrap(),
                U256::from(3),
                U256::from(2),
            ),
            Ok(I256::try_from(-7_i128).unwrap()),
        );
        assert_eq!(
            super::signed_mul_div_unsigned_checked_toward_zero(I256::MIN, U256::ONE, U256::ONE),
            Ok(I256::MIN),
        );
        assert_eq!(
            super::signed_mul_div_unsigned_checked_toward_zero(I256::MIN, U256::from(2), U256::ONE,),
            Err(ArithmeticError::I256ProductOverflow),
        );
        assert_eq!(
            super::signed_mul_div_unsigned_checked_toward_zero(I256::MAX, U256::ONE, U256::ONE,),
            Ok(I256::MAX),
        );
        assert_eq!(
            super::signed_mul_div_checked_toward_zero(I256::MIN, I256::MINUS_ONE, U256::MAX,),
            Err(ArithmeticError::I256ProductOverflow),
        );
    }

    #[test]
    fn zero_position_skips_narrow_funding_subtraction_and_checked_mul_div_rounds_down() {
        let row = MarketRow {
            market_id: 1,
            margin_mode: MarginMode::Cross,
            effective_position_size: 0,
            effective_position_quote: 0,
            effective_last_funding_payment: i128::MIN,
            effective_leverage_wad: U256::ONE,
            effective_isolated_balance: 0,
            projected_settlement_pnl: I256::ZERO,
            effective_buy_order_size: U256::ZERO,
            effective_sell_order_size: U256::ZERO,
            effective_order_notional: U256::ZERO,
            mark_price: U256::ZERO,
            accumulated_funding_payment: i128::MAX,
        };

        assert_eq!(SpecializedEvaluator::evaluate(&row), Ok(super::RowOutputs::default()));
        assert_eq!(
            super::mul_div_checked_down(U256::from(5), U256::from(3), U256::from(2)),
            Ok(U256::from(7)),
        );
    }

    #[test]
    fn base_balance_and_each_reducer_fail_in_declared_sequence() {
        let mut aggregate = AggregateOutputs::new(I256::MAX);
        let result = aggregate
            .reduce_observed(super::RowOutputs {
                balance_contribution: I256::ONE,
                initial_margin: U256::MAX,
            })
            .result;

        assert_eq!(result, Err(ArithmeticError::I256Overflow));
        assert_eq!(aggregate.cross_balance, I256::MAX);
        assert_eq!(aggregate.total_initial_margin, U256::ZERO);
    }

    #[test]
    fn reducer_preserves_prior_fields_when_a_later_sum_overflows() {
        let mut aggregate = AggregateOutputs::new(I256::ZERO);
        let result = aggregate
            .reduce_observed(super::RowOutputs {
                balance_contribution: I256::ONE,
                initial_margin: U256::MAX,
            })
            .result;
        assert_eq!(result, Ok(()));
        let result = aggregate
            .reduce_observed(super::RowOutputs {
                balance_contribution: I256::ONE,
                initial_margin: U256::ONE,
            })
            .result;
        assert_eq!(result, Err(ArithmeticError::U256Overflow));
        assert_eq!(aggregate.cross_balance, I256::try_from(2_i128).unwrap());
        assert_eq!(aggregate.total_initial_margin, U256::MAX);
    }

    fn corpus() -> VectorCorpus {
        let value: serde_json::Value = serde_json::from_str(CORPUS).unwrap();
        serde_json::from_value(serde_json::json!({
            "rowVectors": value["rowVectors"],
            "arithmeticErrorVectors": value["arithmeticErrorVectors"],
            "aggregateVectors": value["aggregateVectors"],
        }))
        .unwrap()
    }

    fn parse_u256(value: &str) -> U256 {
        U256::from_str(value).unwrap()
    }

    fn parse_i256(value: &str) -> I256 {
        I256::from_str(value).unwrap()
    }

    fn error(value: &Option<String>) -> ArithmeticError {
        match value.as_deref().unwrap() {
            "DIVISION_BY_ZERO" => ArithmeticError::DivisionByZero,
            "U256_OVERFLOW" => ArithmeticError::U256Overflow,
            "U256_PRODUCT_OVERFLOW" => ArithmeticError::U256ProductOverflow,
            "I128_OVERFLOW" => ArithmeticError::I128Overflow,
            "I256_OVERFLOW" => ArithmeticError::I256Overflow,
            "I256_PRODUCT_OVERFLOW" => ArithmeticError::I256ProductOverflow,
            "ABS_MIN_OVERFLOW" => ArithmeticError::AbsMinOverflow,
            "I256_CONVERSION_OVERFLOW" => ArithmeticError::I256ConversionOverflow,
            "U112_OVERFLOW" => ArithmeticError::U112Overflow,
            error => panic!("unknown generated arithmetic error {error}"),
        }
    }
}
