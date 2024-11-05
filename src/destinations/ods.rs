use icu_locid::locale;
use spreadsheet_ods;

use crate::commands::export::SpreadSheetDestinationOptions;
use crate::definitions::{DataDestination, DataSourceBatchIterator, Row, Value};
use crate::utils::{escape_binary_data, truncate_text_with_note};

pub struct SpreadSheetODSDestination {
    filename: String,
    workbook: spreadsheet_ods::WorkBook,
    sheet_row_count: usize,
    truncate: Option<u64>,
}

#[allow(deprecated)] //FIXME: spreadsheet_ods uses NaiveDateTime, hopefully it will be updated
                     //someday
pub fn value_to_ods_value(value: &Value, truncate: Option<u64>) -> spreadsheet_ods::Value {
    match value {
        Value::U64(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::I64(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::U32(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::I32(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::U16(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::I16(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::U8(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::I8(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::F64(value) => spreadsheet_ods::Value::Number(*value),
        Value::F32(value) => spreadsheet_ods::Value::Number(*value as f64),
        Value::String(value) => {
            spreadsheet_ods::Value::Text(truncate_text_with_note(value.to_string(), truncate))
        }
        Value::Bool(value) => spreadsheet_ods::Value::Boolean(*value),
        Value::Bytes(value) => spreadsheet_ods::Value::Text(escape_binary_data(value)),
        Value::None => spreadsheet_ods::Value::Text("".to_string()),
        Value::Timestamp(value) => spreadsheet_ods::Value::DateTime(
            chrono::NaiveDateTime::from_timestamp(*value as i64, 0),
        ),
        Value::Date(value) => spreadsheet_ods::Value::DateTime(chrono::NaiveDateTime::new(
            *value,
            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )),
        Value::Time(value) => spreadsheet_ods::Value::Text(format!("{:?}", value)),
        Value::DateTime(value) => spreadsheet_ods::Value::DateTime(*value),
        _ => panic!("spsheet: unsupported type: {:?}", value),
    }
}

impl SpreadSheetODSDestination {
    pub fn init(spreadsheet_options: &SpreadSheetDestinationOptions) -> SpreadSheetODSDestination {
        let mut workbook = spreadsheet_ods::WorkBook::new(locale!("en_US")); //todo!("Support any locales using system settings");
        let sheet = spreadsheet_ods::Sheet::new("Sheet 1");
        workbook.push_sheet(sheet);

        SpreadSheetODSDestination {
            filename: spreadsheet_options.filename.clone(),
            workbook: spreadsheet_ods::WorkBook::new(locale!("en_US")),
            sheet_row_count: 0,
            truncate: spreadsheet_options.truncate,
        }
    }
}

impl DataDestination for SpreadSheetODSDestination {
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        let sheet = self.workbook.sheet_mut(0);
        for (idx, column) in result_iterator.get_column_info().iter().enumerate() {
            //self.sheet.add_cell(Cell::str(column.name.clone()), 0, idx);
            sheet.set_value(0, idx as u32, column.name.clone());
        }
        self.sheet_row_count += 1;
    }

    fn add_rows(&mut self, rows: &[Row]) {
        let sheet = self.workbook.sheet_mut(0);
        for row in rows {
            for (idx, val) in row.iter().enumerate() {
                sheet.set_value(
                    self.sheet_row_count as u32,
                    idx as u32,
                    value_to_ods_value(val, self.truncate),
                );
            }
            self.sheet_row_count += 1;
        }
    }

    fn close(&mut self) {
        spreadsheet_ods::write_ods(&mut self.workbook, self.filename.clone())
            .unwrap_or_else(|_| panic!("write_ods: {}", self.filename))
    }
}
