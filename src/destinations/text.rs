use std;
use std::io::Write;

use atty;
use prettytable::{self, Table, Cell};
use termcolor;


use crate::commands::{ApplicationArguments, export::TextDestinationOptions, UseColor};
use crate::definitions::{Value, Row, DataSourceBatchIterator, DataDestination};
use crate::utils::fileorstdout::FileOrStdout;
use crate::utils::{escape_binary_data, truncate_text_with_note};

pub struct TextDestination {
    truncate: Option<u64>,
    column_names: Vec<String>,  
    writer: FileOrStdout,
    table: Table,
    use_color: bool,
}

impl TextDestination {

    pub fn init(args: &ApplicationArguments, options: &TextDestinationOptions) -> TextDestination {
        let use_color = match args.color {
            UseColor::Yes => true,
            UseColor::No => false,
            UseColor::Auto => options.filename == "-" && atty::is(atty::Stream::Stdout),
        };
       
        let mut table = Table::new();
        table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);

        TextDestination {
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
    
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        self.column_names = result_iterator
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
                    Value::Bytes(value) => escape_binary_data(&value),
                    Value::None => "".to_string(),
                    Value::Timestamp(value) => value.to_string(),
                    Value::Date(date) => format!("{}", date.format("%Y-%m-%d")),
                    Value::Time(time) => format!("{}", time.format("%H:%M:%S")),
                    Value::DateTime(datetime) => format!("{}", datetime.format("%Y-%m-%d %H:%M:%S")),
                   
                    _ => panic!("text: unsupported type: {:?}", col)
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

    fn close(&mut self) {
        self.table.print(&mut self.writer).unwrap();
        self.writer.flush().unwrap();
    }


}

