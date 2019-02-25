use std;
use std::path::Path;
use std::io::Write;

use atty;
use termcolor;
use termcolor::WriteColor;
use unicode_segmentation::UnicodeSegmentation;


use crate::commands::TextVerticalDestinationOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataDestination};
use crate::utils::fileorstdout::FileOrStdout;

pub struct TextVerticalDestination {
    filename: String,
    truncate: Option<u64>,
    column_names: Vec<String>,
    use_color: bool,
    writer: FileOrStdout,
}

impl TextVerticalDestination {

    pub fn init(options: &TextVerticalDestinationOptions) -> TextVerticalDestination {
        
        let use_color =  options.filename == "-" && atty::is(atty::Stream::Stdout);
        TextVerticalDestination {
            filename: options.filename.clone(),
            truncate: options.truncate,
            column_names: vec![],
            use_color,
            writer: match options.filename.as_ref() {
                "-" =>  FileOrStdout::ColorStdout(termcolor::StandardStream::stdout(termcolor::ColorChoice::Auto)),
                _ => FileOrStdout::File(std::fs::File::create(options.filename.clone()).unwrap())
            }
        }
    }
}

impl DataDestination for TextVerticalDestination {
    
    fn prepare(&mut self, source: &DataSource) {
        self.column_names = source
            .get_column_info()
            .iter()
            .map(|col| { col.name.clone() })
            .collect();
    }
    fn add_rows(&mut self, rows: &[Row]) {

        for row in rows {
            self.writer.write(&"------\n".to_string().into_bytes());
            for (idx, col) in row.iter().enumerate() {
                let content = match col {
                    Value::U64(value) => value.to_string(),
                    Value::I64(value) => value.to_string(),
                    Value::U32(value) => value.to_string(),
                    Value::I32(value) => value.to_string(),
                    Value::U16(value) => value.to_string(),
                    Value::I16(value) => value.to_string(),
                    Value::U8(value) => value.to_string(),
                    Value::I8(value) => value.to_string(),
                    Value::F64(value) => value.to_string(),
                    Value::F32(value) => value.to_string(),
                    Value::String(value) => value.to_string(),
                    Value::Bool(value) => value.to_string(),
                    //Value::Bytes(value) => value.to_string(),
                    Value::None => "".to_string(),
                    Value::Timestamp(value) => value.to_string(),
                    Value::Date(date) => format!("{}", date.format("%Y-%m-%d")),
                    Value::Time(time) => format!("{}", time.format("%H:%M:%S")),
                    Value::DateTime(datetime) => format!("{}", datetime.format("%Y-%m-%d %H:%M:%S")),
                   
                    _ => panic!(format!("text-vertical: unsupported type: {:?}", col))
                };
                let content_bytes_length = content.len();
                let (truncated, new_content) = match self.truncate {
                    None => (false, content),
                    Some(max_length) => (content.len() > max_length as usize , if content.len() > max_length as usize {
                        UnicodeSegmentation::graphemes(content.as_str(), true).take(max_length as usize).collect::<Vec<&str>>().join("")
                    } else {
                        content
                    })
                };

                match (self.use_color, truncated) {
                    (true, true) =>  {
                        if let FileOrStdout::ColorStdout(ref mut s) = self.writer {
                            s.set_color(termcolor::ColorSpec::new().set_bold(true)).unwrap();
                            write!(s, "{}", self.column_names[idx]).unwrap();
                            s.set_color(&termcolor::ColorSpec::new()).unwrap();
                            writeln!(s, ": {} ...(bytes trimmed: {})", new_content, content_bytes_length - new_content.len()).unwrap();
                        }
                    },
                    (true, false) => {
                         if let FileOrStdout::ColorStdout(ref mut s) = self.writer {
                            s.set_color(termcolor::ColorSpec::new().set_bold(true)).unwrap();
                            write!(s, "{}", self.column_names[idx]).unwrap();
                            s.set_color(&termcolor::ColorSpec::new()).unwrap();
                            writeln!(s, ": {}", new_content).unwrap();
                        }
                   
                    
                    }, 
                    (false, true) => {
                        self.writer.write(
                            &format!(
                                "{}: {}...({} bytes trimmed)\n",
                                self.column_names[idx],
                                new_content,
                                content_bytes_length - new_content.len()
                            ).into_bytes()
                        ).unwrap();
                    },
                    (false, false) => {
                        self.writer.write(
                            &format!(
                                "{}: {}\n",
                                self.column_names[idx],
                                new_content
                            ).into_bytes()
                        ).unwrap();
                    }
                }
            }
        }
    }

    fn close(&mut self) { self.writer.flush(); }

}

