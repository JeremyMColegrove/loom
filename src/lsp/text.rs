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


include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/unit/lsp_text_tests.rs"));
