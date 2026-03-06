#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsp::completion::extract_string_literal_prefix;

    #[test]
    fn utf16_conversion_handles_multibyte_and_surrogate_pairs() {
        let line = "aé🙂z";
        assert_eq!(utf16_col_to_byte_idx(line, 0), 0);
        assert_eq!(utf16_col_to_byte_idx(line, 1), "a".len());
        assert_eq!(utf16_col_to_byte_idx(line, 2), "aé".len());
        assert_eq!(utf16_col_to_byte_idx(line, 3), "aé".len());
        assert_eq!(utf16_col_to_byte_idx(line, 4), "aé🙂".len());
        assert_eq!(utf16_col_to_byte_idx(line, 5), line.len());
    }

    #[test]
    fn get_word_at_position_works_with_unicode_line() {
        let text = "alpha🙂beta";
        let got = get_word_at_position(text, 0, 7);
        assert_eq!(got, "beta");
    }

    #[test]
    fn extract_string_prefix_uses_utf16_columns() {
        let text = "\"é🙂ab\"";
        let out = extract_string_literal_prefix(text, 0, 6).expect("inside string");
        assert_eq!(out.0, "é🙂ab");
        assert_eq!(out.1, 1);
    }
}
