use std::io::Write;
use json;

use atty;
use json_color;

use crate::commands::{ApplicationArguments, JSONDestinationOptions, UseColor};
use crate::definitions::{Value, Row, DataSource, DataDestination};
use crate::utils::fileorstdout::FileOrStdout;
use crate::utils::truncate_text_with_note;

pub struct JSONDestination {
    writer: FileOrStdout,
    truncate: Option<u64>,
    indent: u16,
    compact: bool,
    first_row: bool,
    column_names: Vec<String>,
    json_colorizer: json_color::Colorizer,
    use_color: bool,
}

impl JSONDestination
{
    pub fn init(args: &ApplicationArguments, json_options: &JSONDestinationOptions) -> JSONDestination {
        let use_color = match args.color {
            UseColor::Yes => true,
            UseColor::No => false,
            UseColor::Auto => json_options.filename == "-" && atty::is(atty::Stream::Stdout),
        };
        let writer = match json_options.filename.as_str() {
            "-" => FileOrStdout::ColorStdout(termcolor::StandardStream::stdout(if use_color { termcolor::ColorChoice::Always} else { termcolor::ColorChoice::Never })),
            _ => FileOrStdout::File(std::fs::File::create(json_options.filename.to_string()).unwrap())
        };
        JSONDestination {
            use_color,
            column_names: vec![],
            compact: json_options.compact,
            first_row: true,
            indent: json_options.indent,
            truncate: json_options.truncate,
            writer,
            json_colorizer: json_color::Colorizer::arbitrary()
        }
    }

    pub fn row_to_json_value(&self, row: &Row) -> json::JsonValue {
        let mut json_row = json::object::Object::new();
            row.iter().enumerate().for_each(|(idx, v)| {
                let value = match v {
                    Value::U64(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::I64(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::U32(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::I32(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::U16(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::I16(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::U8(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::I8(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::F64(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::F32(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::String(value) => json::JsonValue::String(truncate_text_with_note(value.to_string(), self.truncate)),
                    Value::Bool(value) => json::JsonValue::Boolean(*value),
                    //Value::Bytes(value) => value.to_string(),
                    Value::None => json::JsonValue::Null,
                    Value::Timestamp(value) => json::JsonValue::Number(json::number::Number::from(*value)),
                    Value::Date(date) => json::JsonValue::String(format!("{}", date.format("%Y-%m-%d"))),
                    Value::Time(time) => json::JsonValue::String(format!("{}", time.format("%H:%M:%S"))),
                    Value::DateTime(datetime) => json::JsonValue::String(format!("{}", datetime.format("%Y-%m-%d %H:%M:%S"))),
                    _ => panic!(format!("json: unsupported type: {:?}", v))
                };
                json_row.insert(&self.column_names[idx], value);
            });
        json::JsonValue::Object(json_row)
    }
}

impl DataDestination for JSONDestination
{
    fn prepare(&mut self, source: &DataSource) {
        self.column_names = source
            .get_column_info()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        self.writer.write_all(if self.compact { b"[" } else { b"[\n" }).unwrap();

    }
    fn add_rows(&mut self, rows: &[Row]) {
        for row in rows {
            let json_row = self.row_to_json_value(row);
            if !self.first_row {
                self.writer.write_all(if self.compact { b"," } else { b",\n" }).unwrap();
            };
            let json_string = if self.compact {
                json::stringify(json_row)
            } else {
                json::stringify_pretty(json_row, self.indent)
            };

            self.writer.write_all(
                if self.use_color && !self.compact {
                    self.json_colorizer.colorize_json_str(&json_string).unwrap_or(json_string)
                } else {
                    json_string
                }.as_bytes()).unwrap();
            self.first_row = false;
        }
    }

    fn close(&mut self) {
        self.writer.write_all(if self.compact { b"]" } else { b"\n]" }).unwrap();
        self.writer.flush().unwrap();
    }
}
