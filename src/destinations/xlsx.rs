use rust_xlsxwriter;
use std::path::Path;

use crate::commands::export::SpreadSheetDestinationOptions;
use crate::definitions::{Value, Row, DataSourceBatchIterator, DataDestination, ColumnType};
use crate::utils::{escape_binary_data, truncate_text_with_note};


pub struct SpreadSheetXLSXDestination {
    filename: String,
    workbook: rust_xlsxwriter::Workbook,
    sheet_row_count: usize,
    truncate: Option<u64>,
}

/*
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
}*/

impl SpreadSheetXLSXDestination 
{
    pub fn init(spreadsheet_options: &SpreadSheetDestinationOptions) -> SpreadSheetXLSXDestination {
        SpreadSheetXLSXDestination {
            filename: spreadsheet_options.filename.clone(),
            workbook: rust_xlsxwriter::Workbook::new(),
            sheet_row_count: 0,
            truncate: spreadsheet_options.truncate,
        }
    }
}

impl DataDestination for SpreadSheetXLSXDestination
{
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        let mut worksheet = self.workbook.add_worksheet();
        let datetime_format = rust_xlsxwriter::Format::new().set_num_format("yyyy-mm-ddThh:mm:ss");
        let date_format = rust_xlsxwriter::Format::new().set_num_format("yyyy-mm-dd");
        let time_format = rust_xlsxwriter::Format::new().set_num_format("hh:mm:ss");


        for (idx, column) in result_iterator.get_column_info().iter().enumerate() {
            worksheet.write(0, idx as u16, column.name.clone());
            match column.data_type {
                ColumnType::Date =>  { worksheet.set_column_format(idx as u16, &date_format).unwrap();},
                ColumnType::DateTime => { worksheet.set_column_format(idx as u16, &datetime_format).unwrap();},
                ColumnType::Time => { worksheet.set_column_format(idx as u16, &time_format).unwrap();},
                ColumnType::Timestamp => {worksheet.set_column_format(idx as u16, &datetime_format).unwrap();},
                _ => {}
            };
        }
        self.sheet_row_count += 1;
    }

    fn add_rows(&mut self, rows: &[Row]) {
        let mut worksheet = self.workbook.worksheet_from_index(0).unwrap();
        for row in rows {
            for (idx, val) in row.iter().enumerate() {
                match val {
                    Value::U64(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value as f64),
                    Value::I64(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value as f64),
                    Value::U32(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::I32(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::U16(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::I16(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::U8(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::I8(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::F64(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::F32(value) => worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value),
                    Value::String(value) => worksheet.write_string(self.sheet_row_count as u32, idx as u16, truncate_text_with_note(value.to_string(), self.truncate)),
                    Value::Bool(value) => worksheet.write_boolean(self.sheet_row_count as u32, idx as u16, *value),
                    Value::Bytes(value) => worksheet.write_string(self.sheet_row_count as u32, idx as u16, truncate_text_with_note(escape_binary_data(&value), self.truncate)),
                    Value::None => worksheet.write_string(self.sheet_row_count as u32, idx as u16, ""),
                    Value::Timestamp(value) => worksheet.write_datetime(self.sheet_row_count as u32, idx as u16, chrono::NaiveDateTime::from_timestamp(*value as i64, 0)),
                    Value::Date(date) => worksheet.write_datetime(self.sheet_row_count as u32, idx as u16, date),
                    Value::Time(time) => worksheet.write_datetime(self.sheet_row_count as u32, idx as u16, time),
                    Value::DateTime(datetime) => worksheet.write_datetime(self.sheet_row_count as u32, idx as u16, datetime),
                    _ => panic!("spsheet: unsupported type: {:?}", val),

                };


            }
            self.sheet_row_count += 1;
        }
    }

    fn close(&mut self) {
        self.workbook.save(self.filename.clone()).unwrap()
    }
}
