#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn std_out_is_suggested_for_std_import_prefix() {
        let labels = collect_std_import_candidates(Path::new("/"), "std.o");
        assert!(labels.iter().any(|label| label == "std.out"));
    }
}
