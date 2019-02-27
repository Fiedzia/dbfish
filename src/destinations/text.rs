use std;
use std::io::Write;

use atty;
use prettytable::{self, Table, Cell};
use termcolor;


use crate::commands::TextDestinationOptions;
use crate::definitions::{Value, Row, DataSource, DataDestination};
use crate::utils::fileorstdout::FileOrStdout;
use crate::utils::truncate_text_with_note;

pub struct TextDestination {
    filename: String,
    truncate: Option<u64>,
    column_names: Vec<String>,
    use_color: bool,
    writer: FileOrStdout,
    table: Table,
}

impl TextDestination {

    pub fn init(options: &TextDestinationOptions) -> TextDestination {
        
        let use_color =  options.filename == "-" && atty::is(atty::Stream::Stdout);
        let mut table = Table::new();
        table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);

        TextDestination {
            filename: options.filename.clone(),
            truncate: options.truncate,
            column_names: vec![],
            use_color,
            writer: match options.filename.as_ref() {
                "-" =>  FileOrStdout::ColorStdout(termcolor::StandardStream::stdout(termcolor::ColorChoice::Auto)),
                _ => FileOrStdout::File(std::fs::File::create(options.filename.clone()).unwrap())
            },
            table,

        }
    }
}

impl DataDestination for TextDestination {
    
    fn prepare(&mut self, source: &DataSource) {
        self.column_names = source
            .get_column_info()
            .iter()
            .map(|col| { col.name.clone() })
            .collect();
        self.table.add_row(
            prettytable::Row::new(
                self.column_names
                    .iter()
                    .map(|name| { Cell::new(name) })
                    .collect()
            )
        );
    }
    fn add_rows(&mut self, rows: &[Row]) {

        for row in rows {
            //<column index, value, original length, truncated>
            let mut row_data: Vec<String> = Vec::with_capacity(self.column_names.len());
            for col in row.iter() {
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
                    //Value::Bytes(value) => value.to_string(),
                    Value::None => "".to_string(),
                    Value::Timestamp(value) => value.to_string(),
                    Value::Date(date) => format!("{}", date.format("%Y-%m-%d")),
                    Value::Time(time) => format!("{}", time.format("%H:%M:%S")),
                    Value::DateTime(datetime) => format!("{}", datetime.format("%Y-%m-%d %H:%M:%S")),
                   
                    _ => panic!(format!("text: unsupported type: {:?}", col))
                };
                row_data.push(content);
            }

            self.table.add_row(
                prettytable::Row::new(
                    row_data
                        .iter()
                        .map(|content| {
                            Cell::new(&content)
                        })
                        .collect()
                )
            );
        }
    }

    fn close(&mut self) { self.table.print(&mut self.writer); self.writer.flush(); }

}

