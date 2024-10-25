use spsheet::ods;
use spsheet::xlsx;
use spsheet::{Book, Sheet, Cell, style::Style};
use std::path::Path;

use crate::commands::export::SpreadSheetDestinationOptions;
use crate::definitions::{Value, Row, DataSourceBatchIterator, DataDestination};
use crate::utils::{escape_binary_data, truncate_text_with_note};


pub enum SpreadSheetFormat {
    ODS,
    XLSX
}

pub struct SpreadSheetDestination {
    filename: String,
    sheet: Sheet,
    sheet_row_count: usize,
    format: SpreadSheetFormat,
    truncate: Option<u64>,
}


pub fn value_to_cell(value: &Value, truncate: Option<u64>) -> Cell {
    match value {
        Value::U64(value) => Cell::str(value.to_string()),
        Value::I64(value) => Cell::str(value.to_string()),
        Value::U32(value) => Cell::str(value.to_string()),
        Value::I32(value) => Cell::str(value.to_string()),
        Value::U16(value) => Cell::str(value.to_string()),
        Value::I16(value) => Cell::str(value.to_string()),
        Value::U8(value) => Cell::str(value.to_string()),
        Value::I8(value) => Cell::str(value.to_string()),
        Value::F64(value) => Cell::float(*value),
        Value::F32(value) => Cell::float(f64::from(*value)),
        Value::String(value) => Cell::str(truncate_text_with_note(value.to_string(), truncate)),
        Value::Bool(value) => Cell::str(value.to_string()),
        Value::Bytes(value) => Cell::str(escape_binary_data(&value)),
        Value::None => Cell::str("".to_string()),
        Value::Timestamp(value) => Cell::str(value.to_string()),
        Value::Date(date) => Cell::date_with_style(format!("{}", date.format("%Y-%m-%d")), Style::new("YYYY/MM/DD")),
        Value::Time(time) => Cell::str(format!("{}", time.format("%H:%M:%S"))),
        Value::DateTime(datetime) => Cell::date_with_style(format!("{}", datetime.format("%Y-%m-%dT%H:%M:%S")), Style::new("YYYY/MM/DD\\ HH:MM:SS")),
        _ => panic!("spsheet: unsupported type: {:?}", value),
    }
}

impl SpreadSheetDestination 
{
    pub fn init(spreadsheet_options: &SpreadSheetDestinationOptions, format: SpreadSheetFormat) -> SpreadSheetDestination {
        SpreadSheetDestination {
            filename: spreadsheet_options.filename.clone(),
            sheet: Sheet::new("sheet 1"),
            sheet_row_count: 0,
            format,
            truncate: spreadsheet_options.truncate,
        }
    }
}

impl DataDestination for SpreadSheetDestination
{
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        for (idx, column) in result_iterator.get_column_info().iter().enumerate() {
            self.sheet.add_cell(Cell::str(column.name.clone()), 0, idx);
        }
        self.sheet_row_count += 1;
    }

    fn add_rows(&mut self, rows: &[Row]) {
        for row in rows {
            for (idx, val) in row.iter().enumerate() {
                self.sheet.add_cell(value_to_cell(val, self.truncate), self.sheet_row_count, idx);
            }
            self.sheet_row_count += 1;
        }
    }

    fn close(&mut self) {
        let mut book = Book::new();
        book.add_sheet(self.sheet.clone());
        match self.format {
            SpreadSheetFormat::ODS => ods::write(&book, Path::new(&self.filename)).unwrap(),
            SpreadSheetFormat::XLSX => xlsx::write(&book, Path::new(&self.filename)).unwrap(),
        };
    }
}
