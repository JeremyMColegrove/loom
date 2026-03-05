pub(crate) fn utf16_col_to_byte_idx(line: &str, character: u32) -> usize {
    let mut utf16_col = 0u32;
    for (byte_idx, ch) in line.char_indices() {
        if utf16_col >= character {
            return byte_idx;
        }
        let next = utf16_col + ch.len_utf16() as u32;
        if next > character {
            return byte_idx;
        }
        utf16_col = next;
    }
    line.len()
}

pub(crate) fn utf16_col_to_char_idx(line: &str, character: u32) -> usize {
    let mut utf16_col = 0u32;
    let mut char_idx = 0usize;
    for ch in line.chars() {
        if utf16_col >= character {
            return char_idx;
        }
        let next = utf16_col + ch.len_utf16() as u32;
        if next > character {
            return char_idx;
        }
        utf16_col = next;
        char_idx += 1;
    }
    char_idx
}

pub(crate) fn byte_idx_to_utf16_col(line: &str, byte_idx: usize) -> u32 {
    let clamped = byte_idx.min(line.len());
    line[..clamped].chars().map(|c| c.len_utf16() as u32).sum()
}

pub(crate) fn get_word_at_position(text: &str, line: u32, character: u32) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let line_idx = line as usize;
    if line_idx >= lines.len() {
        return String::new();
    }
    let line_text = lines[line_idx];
    let col = utf16_col_to_char_idx(line_text, character);

    let chars: Vec<char> = line_text.chars().collect();
    let mut start = col;
    while start > 0 {
        let c = chars[start - 1];
        if c.is_alphanumeric() || c == '_' || c == '@' || c == '.' {
            start -= 1;
        } else {
            break;
        }
    }
    let mut end = col;
    while end < chars.len() {
        let c = chars[end];
        if c.is_alphanumeric() || c == '_' || c == '.' {
            end += 1;
        } else {
            break;
        }
    }
    chars[start..end].iter().collect()
}

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
