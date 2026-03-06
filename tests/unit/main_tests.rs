#[cfg(test)]
mod tests {
    use super::{Mode, parse_cli_args_with_require_default};
    use loom::runtime::security::TrustMode;

    #[test]
    fn strict_mode_is_default() {
        let args = vec!["loom".to_string(), "script.loom".to_string()];
        let parsed = parse_cli_args_with_require_default(&args, true).expect("args should parse");
        assert_eq!(
            parsed,
            Mode::Run {
                file_path: "script.loom".to_string(),
                strict: true,
                policy_path: None,
                trust_mode_override: None,
                require_policy: true,
            }
        );
    }

    #[test]
    fn strict_mode_can_be_disabled_explicitly() {
        let args = vec![
            "loom".to_string(),
            "--no-strict".to_string(),
            "script.loom".to_string(),
        ];
        let parsed = parse_cli_args_with_require_default(&args, true).expect("args should parse");
        assert_eq!(
            parsed,
            Mode::Run {
                file_path: "script.loom".to_string(),
                strict: false,
                policy_path: None,
                trust_mode_override: None,
                require_policy: true,
            }
        );
    }

    #[test]
    fn policy_and_trust_mode_flags_parse() {
        let args = vec![
            "loom".to_string(),
            "--policy".to_string(),
            ".loomrc.json".to_string(),
            "--trust-mode".to_string(),
            "restricted".to_string(),
            "--require-policy".to_string(),
            "script.loom".to_string(),
        ];
        let parsed = parse_cli_args_with_require_default(&args, false).expect("args should parse");
        assert_eq!(
            parsed,
            Mode::Run {
                file_path: "script.loom".to_string(),
                strict: true,
                policy_path: Some(".loomrc.json".to_string()),
                trust_mode_override: Some(TrustMode::Restricted),
                require_policy: true,
            }
        );
    }
}
