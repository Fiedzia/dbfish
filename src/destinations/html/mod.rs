use std;
use std::io::Write;

use askama_escape::{self, escape};

use crate::commands::export::HTMLDestinationOptions;
use crate::definitions::{Value, Row, DataSource, DataSourceBatchIterator, DataDestination};
use crate::utils::fileorstdout::FileOrStdout;
use crate::utils::truncate_text_with_note;

pub struct HTMLDestination {
    truncate: Option<u64>,
    column_names: Vec<String>,
    writer: FileOrStdout,
    title: String,
}


impl HTMLDestination {

    pub fn init(options: &HTMLDestinationOptions) -> HTMLDestination {
        
        HTMLDestination {
            truncate: options.truncate,
            column_names: vec![],
            writer: match options.filename.as_ref() {
                "-" =>  FileOrStdout::ColorStdout(termcolor::StandardStream::stdout(termcolor::ColorChoice::Auto)),
                _ => FileOrStdout::File(std::fs::File::create(options.filename.clone()).unwrap())
            },
            title: options.title.clone().unwrap_or_else(|| "".to_string()),
        }
    }
}

impl DataDestination for HTMLDestination {
    
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &DataSourceBatchIterator) {
        self.writer
            .write_all(format!(include_str!("html_prefix.html"), title=escape(&self.title, askama_escape::Html)).as_bytes())
            .unwrap();

        self.column_names = result_iterator
            .get_column_info()
            .iter()
            .map(|col| { col.name.clone() })
            .collect();
        self.writer.write_all(b"<thead><tr>\n").unwrap();
        for name in self.column_names.iter() {
            self.writer
                .write_all(
                    ("    <th>".to_string() + escape(&name, askama_escape::Html).to_string().as_ref() + "</th>\n")
                    .as_bytes())
                .unwrap();
        };
        self.writer.write_all(b"</tr></thead><tbody>\n").unwrap();
    }
    fn add_rows(&mut self, rows: &[Row]) {

        for row in rows {
            //<column index, value, original length, truncated>
            let mut row_data: Vec<String> = Vec::with_capacity(self.column_names.len());
            for col in row.iter() {
                let content = escape(& match col {
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
                }, askama_escape::Html).to_string();
                row_data.push(content);
            }
            let row_str = "<tr>\n".to_string() + row_data.iter().map(|v| "    <td>".to_string() + v + "</td>\n").collect::<Vec<String>>().join("").as_ref() + "</tr>\n";
            self.writer.write_all(row_str.as_bytes()).unwrap();
        }
    }

    fn close(&mut self) {
        self.writer.write_all(format!(include_str!("html_suffix.html")).as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

}

