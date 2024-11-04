use rust_xlsxwriter;

use crate::commands::export::SpreadSheetDestinationOptions;
use crate::definitions::{ColumnType, DataDestination, DataSourceBatchIterator, Row, Value};
use crate::utils::{escape_binary_data, truncate_text_with_note};

pub struct SpreadSheetXLSXDestination {
    filename: String,
    workbook: rust_xlsxwriter::Workbook,
    sheet_row_count: usize,
    truncate: Option<u64>,
}

impl SpreadSheetXLSXDestination {
    pub fn init(spreadsheet_options: &SpreadSheetDestinationOptions) -> SpreadSheetXLSXDestination {
        SpreadSheetXLSXDestination {
            filename: spreadsheet_options.filename.clone(),
            workbook: rust_xlsxwriter::Workbook::new(),
            sheet_row_count: 0,
            truncate: spreadsheet_options.truncate,
        }
    }
}

impl DataDestination for SpreadSheetXLSXDestination {
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        let worksheet = self.workbook.add_worksheet();
        let datetime_format = rust_xlsxwriter::Format::new().set_num_format("yyyy-mm-ddThh:mm:ss");
        let date_format = rust_xlsxwriter::Format::new().set_num_format("yyyy-mm-dd");
        let time_format = rust_xlsxwriter::Format::new().set_num_format("hh:mm:ss");

        for (idx, column) in result_iterator.get_column_info().iter().enumerate() {
            worksheet.write(0, idx as u16, column.name.clone()).unwrap();
            match column.data_type {
                ColumnType::Date => {
                    worksheet
                        .set_column_format(idx as u16, &date_format)
                        .unwrap();
                }
                ColumnType::DateTime => {
                    worksheet
                        .set_column_format(idx as u16, &datetime_format)
                        .unwrap();
                }
                ColumnType::Time => {
                    worksheet
                        .set_column_format(idx as u16, &time_format)
                        .unwrap();
                }
                ColumnType::Timestamp => {
                    worksheet
                        .set_column_format(idx as u16, &datetime_format)
                        .unwrap();
                }
                _ => {}
            };
        }
        self.sheet_row_count += 1;
    }

    fn add_rows(&mut self, rows: &[Row]) {
        let worksheet = self.workbook.worksheet_from_index(0).unwrap();
        for row in rows {
            for (idx, val) in row.iter().enumerate() {
                match val {
                    Value::U64(value) => worksheet.write_number(
                        self.sheet_row_count as u32,
                        idx as u16,
                        *value as f64,
                    ),
                    Value::I64(value) => worksheet.write_number(
                        self.sheet_row_count as u32,
                        idx as u16,
                        *value as f64,
                    ),
                    Value::U32(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::I32(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::U16(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::I16(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::U8(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::I8(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::F64(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::F32(value) => {
                        worksheet.write_number(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::String(value) => worksheet.write_string(
                        self.sheet_row_count as u32,
                        idx as u16,
                        truncate_text_with_note(value.to_string(), self.truncate),
                    ),
                    Value::Bool(value) => {
                        worksheet.write_boolean(self.sheet_row_count as u32, idx as u16, *value)
                    }
                    Value::Bytes(value) => worksheet.write_string(
                        self.sheet_row_count as u32,
                        idx as u16,
                        truncate_text_with_note(escape_binary_data(&value), self.truncate),
                    ),
                    Value::None => {
                        worksheet.write_string(self.sheet_row_count as u32, idx as u16, "")
                    }
                    Value::Timestamp(value) => worksheet.write_datetime(
                        self.sheet_row_count as u32,
                        idx as u16,
                        rust_xlsxwriter::ExcelDateTime::from_timestamp(*value as i64).unwrap(),
                    ),
                    Value::Date(date) => {
                        worksheet.write_datetime(self.sheet_row_count as u32, idx as u16, date)
                    }
                    Value::Time(time) => {
                        worksheet.write_datetime(self.sheet_row_count as u32, idx as u16, time)
                    }
                    Value::DateTime(datetime) => {
                        worksheet.write_datetime(self.sheet_row_count as u32, idx as u16, datetime)
                    }
                    _ => panic!("spsheet: unsupported type: {:?}", val),
                }
                .unwrap();
            }
            self.sheet_row_count += 1;
        }
    }

    fn close(&mut self) {
        self.workbook.save(self.filename.clone()).unwrap()
    }
}
