use unicode_segmentation::UnicodeSegmentation;

pub mod fileorstdout;


pub fn truncate_text(text: &str, max_length: u64) -> String {
    UnicodeSegmentation::graphemes(text, true).take(max_length as usize).collect::<Vec<&str>>().join("")
}

pub fn truncate_text_with_note(text: String, truncate: Option<u64>) -> String {
    match truncate {
        None => text,
        Some(max_length) => {
            let truncated_text = truncate_text(&text, max_length);
            if truncated_text.len() < text.len() {
                format!("{} ...({} bytes trimmed)", truncated_text, text.len() -  truncated_text.len())
            } else {
                text
            }
        }

    }
}


pub fn report_query_error(query: &str, error: &str) {
    eprintln!("The following query have failed:\n\n{}\n\nwith error:\n\n{}", query, error)
}
