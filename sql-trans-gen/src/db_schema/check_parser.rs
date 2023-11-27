use std::ops::{Range, RangeInclusive};
use rusqlite::types::Type;
use sqlparser::ast::{BinaryOperator, Expr};
use sqlparser::ast::Value;

pub enum CheckRangeExpr {
    Val(Value),
    Ident(String),
    Comparison {
        left: Box<CheckRangeExpr>,
        right: Box<CheckRangeExpr>,
        op: BinaryOperator,
    }
}

// BinaryOp {
//  left: BinaryOp {
//      left: Value(Number("0", false)),
//      op: Lt,
//      right: Identifier(Ident { value: "grade", quote_style: None })
//  },
//  op: Lt,
//  right: Value(Number("10", false))
// }
fn visit_expr(expr: Expr) -> Option<CheckRangeExpr> {
    match expr {
        Expr::Identifier(ident) => {
            Some(CheckRangeExpr::Ident(ident.value))
        }
        // TODO Make this one work
        Expr::Between { .. } => None,
        Expr::BinaryOp { left, right, op } => {
            let left_visited = visit_expr(*left);
            let right_visited = visit_expr(*right);
            left_visited.zip(right_visited)
                .map(|(left, right)| CheckRangeExpr::Comparison {
                    left: Box::new(left),
                    right: Box::new(right),
                    op
                })
        }
        Expr::Nested(nested_expr) => {
            visit_expr(*nested_expr)
        }
        Expr::Value(val) => {
            Some(CheckRangeExpr::Val(val))
        }
        _ => None,
    }
}

enum IntermediateLiteral {
    Int(i64),
    BoundInt,
    Float(f64),
    BoundFloat,
}

impl IntermediateLiteral {
    pub fn make_self_bound(&self) -> IntermediateLiteral {
        match self {
            IntermediateLiteral::Int(_) |
            IntermediateLiteral::BoundInt => Self::BoundInt,
            IntermediateLiteral::Float(_) |
            IntermediateLiteral::BoundFloat => Self::BoundFloat,
        }
    }
}

impl From<i64> for IntermediateLiteral {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for IntermediateLiteral {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

pub enum NumericalRange {
    IntRange(Range<i64>),
    FloatRange(Range<f64>),
    IntRangeInclusive(RangeInclusive<i64>),
    FloatRangeInclusive(RangeInclusive<f64>),
}

impl NumericalRange {
    fn create_from_bounds(lower: IntermediateLiteral, upper: IntermediateLiteral, lower_inclusive: bool, upper_inclusive: bool) -> Self {
        let inclusive_range = match lower {
            IntermediateLiteral::Int(lower_int) => {
                match upper {
                    IntermediateLiteral::Int(upper_int) => Self::IntRangeInclusive(lower_int..=upper_int),
                    IntermediateLiteral::BoundInt => Self::IntRangeInclusive(lower_int..=i64::MAX),
                    _ => panic!("lower is int, but upper is not")
                }
            }
            IntermediateLiteral::Float(lower_float) => {
                match upper {
                    IntermediateLiteral::Float(upper_float) => Self::FloatRangeInclusive(lower_float..=upper_float),
                    IntermediateLiteral::BoundFloat => Self::FloatRangeInclusive(lower_float..=f64::MAX),
                    _ => panic!("lower is float, but upper is not")
                }
            }
            IntermediateLiteral::BoundInt => {
                match upper {
                    IntermediateLiteral::Int(upper_int) => Self::IntRangeInclusive(i64::MIN..=upper_int),
                    IntermediateLiteral::BoundInt => Self::IntRangeInclusive(i64::MIN..=i64::MAX),
                    _ => panic!("lower is int, but upper is not")
                }
            }
            IntermediateLiteral::BoundFloat => {
                match upper {
                    IntermediateLiteral::Float(upper_float) => Self::FloatRangeInclusive(f64::MIN..=upper_float),
                    IntermediateLiteral::BoundFloat => Self::FloatRangeInclusive(f64::MIN..=f64::MAX),
                    _ => panic!("lower is float, but upper is not")
                }
            }
        };

        let fixed_lower = if lower_inclusive {
            inclusive_range
        } else {
            match inclusive_range {
                NumericalRange::IntRangeInclusive(range) => NumericalRange::IntRangeInclusive((range.start() + 1)..=*range.end()),
                NumericalRange::FloatRangeInclusive(range) => NumericalRange::FloatRangeInclusive((range.start() + 1f64)..=*range.end()),
                _ => panic!("Not inclusive range!")
            }
        };

        let upper_fixed = if upper_inclusive {
            fixed_lower
        } else {
            match fixed_lower {
                NumericalRange::IntRange(range) => NumericalRange::IntRange(range.start..range.end),
                NumericalRange::FloatRange(range) => NumericalRange::FloatRange(range.start..range.end),
                _ => panic!("Not inclusive range!")
            }
        };

        upper_fixed
    }

    pub fn from_i64(value: i64, is_lower: bool) -> Self {
        if is_lower {
            NumericalRange::IntRange(value..i64::MAX)
        } else {
            NumericalRange::IntRange(i64::MIN..value)
        }
    }

    pub fn from_f64(value: f64, is_lower: bool) -> Self {
        if is_lower {
            NumericalRange::FloatRange(value..f64::MAX)
        } else {
            NumericalRange::FloatRange(f64::MIN..value)
        }
    }
}

enum IntermediateNumericalRange {
    Literal(IntermediateLiteral),
    Range(NumericalRange),
}

fn make_range(lower: Option<IntermediateNumericalRange>, upper: Option<IntermediateNumericalRange>, lower_inclusive: bool, upper_inclusive: bool) -> Option<IntermediateNumericalRange> {
    let lower_bound = lower.map(|lower_range| {
        match lower_range {
            IntermediateNumericalRange::Range(range) => {
                match range {
                    NumericalRange::IntRange(int_range) => IntermediateLiteral::from(int_range.start),
                    NumericalRange::FloatRange(float_range) => IntermediateLiteral::Float(float_range.start),
                    NumericalRange::IntRangeInclusive(int_range_inc) => IntermediateLiteral::Int(*int_range_inc.start()),
                    NumericalRange::FloatRangeInclusive(float_range_inc) => IntermediateLiteral::Float(*float_range_inc.start()),
                }
            }
            IntermediateNumericalRange::Literal(lit) => lit
        }
    });

    let upper_bound = upper.map(|upper_range| {
        match upper_range {
            IntermediateNumericalRange::Range(range) => {
                match range {
                    NumericalRange::IntRange(int_range) => IntermediateLiteral::from(int_range.end),
                    NumericalRange::FloatRange(float_range) => IntermediateLiteral::Float(float_range.end),
                    NumericalRange::IntRangeInclusive(int_range_inc) => IntermediateLiteral::Int(*int_range_inc.end()),
                    NumericalRange::FloatRangeInclusive(float_range_inc) => IntermediateLiteral::Float(*float_range_inc.end()),
                }
            }
            IntermediateNumericalRange::Literal(lit) => lit
        }
    });

    let bounds = if lower_bound.is_some() && upper_bound.is_some() {
        Some((lower_bound.unwrap(), upper_bound.unwrap()))
    } else if lower_bound.is_some() {
        let lower = lower_bound.unwrap();
        let upper = lower.make_self_bound();
        Some((lower, upper))
    } else if upper_bound.is_some() {
        let upper = upper_bound.unwrap();
        let lower = upper.make_self_bound();
        Some((lower, upper))
    } else {
        None
    };

    if bounds.is_some() {
        let (lower, upper) = bounds.unwrap();
        Some(IntermediateNumericalRange::Range(NumericalRange::create_from_bounds(lower, upper, lower_inclusive, upper_inclusive)))
    } else {
        None
    }
}

fn transform_check_range_expr(check_range_expr: CheckRangeExpr, column_type: &Type) -> Option<IntermediateNumericalRange> {
    match check_range_expr {
        CheckRangeExpr::Val(value) => {
            let formatted_value = format!("{}", value);

            match column_type {
                Type::Integer => formatted_value.parse::<i64>().ok()
                    .map(|ival| IntermediateNumericalRange::Literal(ival.into())),
                Type::Real => formatted_value.parse::<f64>().ok()
                    .map(|float_val| IntermediateNumericalRange::Literal(float_val.into())),
                _ => None
            }
        }
        CheckRangeExpr::Ident(_) => None,
        CheckRangeExpr::Comparison { left, right, op } => {
            let left_v = transform_check_range_expr(*left, column_type);
            let right_v = transform_check_range_expr(*right, column_type);
            match op {
                BinaryOperator::Gt => {
                    make_range(right_v, left_v, true, false)
                }
                BinaryOperator::Lt => {
                    make_range(left_v, right_v, false, true)
                }
                BinaryOperator::GtEq => {
                    make_range(right_v, left_v, true, true)
                }
                BinaryOperator::LtEq => {
                    make_range(left_v, right_v, true, true)
                }
                _ => None,
            }
        }
    }
}

pub fn extract_range_from_check_expr(check_expr: Expr, column_type: &Type) -> Option<NumericalRange> {
    let check_range_expr = visit_expr(check_expr);
    if check_range_expr.is_none() {
        return None;
    }

    let check_range_expr = check_range_expr.unwrap();
    let intermediate_range = transform_check_range_expr(check_range_expr, column_type);
    if intermediate_range.is_none() {
        return None;
    }

    let intermediate_range = intermediate_range.unwrap();
    match intermediate_range {
        IntermediateNumericalRange::Range(range) => Some(range),
        _ => panic!("Ended on non-range value")
    }
}
