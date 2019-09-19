use std;
use std::io::Write;

use atty;
use termcolor;
use termcolor::WriteColor;


use crate::commands::{ApplicationArguments, export::TextVerticalDestinationOptions, UseColor};
use crate::definitions::{Value, Row, DataSourceBatchIterator, DataDestination};
use crate::utils::fileorstdout::FileOrStdout;
use crate::utils::{escape_binary_data, truncate_text_with_note};

pub struct TextVerticalDestination {
    truncate: Option<u64>,
    column_names: Vec<String>,
    use_color: bool,
    writer: FileOrStdout,
    sort_columns: bool,
}

impl TextVerticalDestination {

    pub fn init(args: &ApplicationArguments, options: &TextVerticalDestinationOptions) -> TextVerticalDestination {
        let use_color = match args.color {
            UseColor::Yes => true,
            UseColor::No => false,
            UseColor::Auto => options.filename == "-" && atty::is(atty::Stream::Stdout),
        };
        let writer = match options.filename.as_str() {
            "-" => FileOrStdout::ColorStdout(termcolor::StandardStream::stdout(if use_color { termcolor::ColorChoice::Always} else { termcolor::ColorChoice::Never })),
            _ => FileOrStdout::File(std::fs::File::create(options.filename.to_string()).unwrap())
        };
      
        TextVerticalDestination {
            truncate: options.truncate,
            sort_columns: options.sort_columns,
            column_names: vec![],
            use_color,
            writer,
        }
    }
}

impl DataDestination for TextVerticalDestination {
    
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        self.column_names = result_iterator
            .get_column_info()
            .iter()
            .map(|col| { col.name.clone() })
            .collect();
    }
    fn add_rows(&mut self, rows: &[Row]) {

        for row in rows {
            //<column index, value>
            let mut row_data: Vec<(usize, String)> = Vec::with_capacity(self.column_names.len());
            self.writer.write_all(&"──────────\n".to_string().into_bytes()).unwrap();
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
                    Value::String(value) => truncate_text_with_note(value.to_string(), self.truncate),
                    Value::Bool(value) => value.to_string(),
                    Value::Bytes(value) => escape_binary_data(&value),
                    Value::None => "".to_string(),
                    Value::Timestamp(value) => value.to_string(),
                    Value::Date(date) => format!("{}", date.format("%Y-%m-%d")),
                    Value::Time(time) => format!("{}", time.format("%H:%M:%S")),
                    Value::DateTime(datetime) => format!("{}", datetime.format("%Y-%m-%d %H:%M:%S")),
                   
                    _ => panic!(format!("text-vertical: unsupported type: {:?}", col))
                };
                row_data.push((idx, content));
            }
            if self.sort_columns {
                row_data.sort_by(|a, b| {self.column_names[a.0].cmp(&self.column_names[b.0])});
            }
            for (idx, content) in row_data {

                if self.use_color {
                    if let FileOrStdout::ColorStdout(ref mut s) = self.writer {
                        s.set_color(termcolor::ColorSpec::new().set_bold(true)).unwrap();
                        write!(s, "{}", self.column_names[idx]).unwrap();
                        s.set_color(&termcolor::ColorSpec::new()).unwrap();
                        writeln!(s, ": {}", content).unwrap();
                    }
                } else {
                    self.writer.write_all(
                        &format!(
                            "{}: {}\n",
                            self.column_names[idx],
                            content
                        ).into_bytes()
                    ).unwrap();
                }
            }
        }
    }

    fn close(&mut self) { self.writer.flush().unwrap(); }
}
