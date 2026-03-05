use loom::ast::{Expression, Literal, PipeFlow, Source, Span};
use loom::formatter::Formatter;
use loom::parser::parse;
use loom::runtime::Runtime;
use loom::runtime::env::Value;
use proptest::prelude::*;

fn eval_binary(left: f64, op: &str, right: f64) -> Result<Value, String> {
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(left))),
            op.to_string(),
            Box::new(Expression::Literal(Literal::Number(right))),
        )),
        operations: vec![],
        on_fail: None,
    };

    let mut runtime = Runtime::new();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async { runtime.execute_flow(&flow).await.map_err(|e| e.to_string()) })
}

proptest! {
    #[test]
    fn parser_accepts_or_rejects_arbitrary_programs_without_panicking(input in "[\\PC\\n\\t]{0,400}") {
        let _ = parse(&input);
    }

    #[test]
    fn parser_round_trips_import_and_pipe_programs(path in "[a-zA-Z0-9._/]{1,24}", alias in "[a-z]{1,8}") {
        let source = format!("@import \"{}\" as {}\n\"input.txt\" >> \"out.txt\"", path, alias);
        let parsed = parse(&source).expect("generated program should parse");
        let formatted = Formatter::format(&parsed);
        let reparsed = parse(&formatted).expect("formatted program should parse");
        prop_assert_eq!(parsed.statements.len(), reparsed.statements.len());
    }

    #[test]
    fn evaluator_addition_is_commutative(a in -1_000_000f64..1_000_000f64, b in -1_000_000f64..1_000_000f64) {
        let left = eval_binary(a, "+", b).expect("a + b should evaluate");
        let right = eval_binary(b, "+", a).expect("b + a should evaluate");
        prop_assert!(matches!(left, Value::Number(_)), "left result must be numeric");
        prop_assert!(matches!(right, Value::Number(_)), "right result must be numeric");
        let Value::Number(left_n) = left else { unreachable!() };
        let Value::Number(right_n) = right else { unreachable!() };
        prop_assert!((left_n - right_n).abs() < f64::EPSILON);
    }

    #[test]
    fn evaluator_multiplication_is_commutative(a in -10_000f64..10_000f64, b in -10_000f64..10_000f64) {
        let left = eval_binary(a, "*", b).expect("a * b should evaluate");
        let right = eval_binary(b, "*", a).expect("b * a should evaluate");
        prop_assert!(matches!(left, Value::Number(_)), "left result must be numeric");
        prop_assert!(matches!(right, Value::Number(_)), "right result must be numeric");
        let Value::Number(left_n) = left else { unreachable!() };
        let Value::Number(right_n) = right else { unreachable!() };
        prop_assert!((left_n - right_n).abs() < 1e-9);
    }

    #[test]
    fn evaluator_division_by_zero_always_fails(a in -1_000f64..1_000f64) {
        let result = eval_binary(a, "/", 0.0);
        prop_assert!(result.is_err());
    }
}
