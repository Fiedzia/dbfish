use unicode_segmentation::UnicodeSegmentation;

pub mod fileorstdout;

pub fn truncate_text(text: &str, max_length: u64) -> String {
    UnicodeSegmentation::graphemes(text, true)
        .take(max_length as usize)
        .collect::<Vec<&str>>()
        .join("")
}

pub fn truncate_text_with_note(text: String, truncate: Option<u64>) -> String {
    match truncate {
        None => text,
        Some(max_length) => {
            let truncated_text = truncate_text(&text, max_length);
            if truncated_text.len() < text.len() {
                format!(
                    "{} ...({} bytes trimmed)",
                    truncated_text,
                    text.len() - truncated_text.len()
                )
            } else {
                text
            }
        }
    }
}

pub fn report_query_error(query: &str, error: &str) {
    eprintln!(
        "The following query have failed:\n\n{}\n\nwith error:\n\n{}",
        query, error
    )
}

///use std::ascii::escape_default to create printable string from binary data
///it keeps printable asciii characters and escapes non-printable ones
pub fn escape_binary_data(value: &[u8]) -> String {
    let mut result: String = String::with_capacity(value.len());
    for v in value {
        for ch in std::ascii::escape_default(*v) {
            result.push(ch as char);
        }
    }
    result
}

#[cfg(test)]
mod tests {

    use super::escape_binary_data;

    #[test]
    fn test_escape_binary_data() {
        assert_eq!(
            escape_binary_data(&vec!['a' as u8, 0x0, 'b' as u8, 0x9]),
            "a\\x00b\\t"
        );
    }
}
