use std::fs::File;

use csv;

use crate::commands::CSVDestinationOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataDestination};

pub struct CSVDestination {
    csv_writer: csv::Writer<File>,
}

impl CSVDestination 
{
    pub fn init(csv_options: &CSVDestinationOptions) -> CSVDestination {
        let csv_writer = csv::Writer::from_path(&csv_options.filename).unwrap();
        CSVDestination { csv_writer }
    }

    pub fn row_to_csv_row(row: &Row) -> Vec<String> {
        row.iter().map(|v| {
            match v {
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
                _ => panic!(format!("csv: unsupported type: {:?}", v))
            }
        }).collect()
    }
}

impl DataDestination for CSVDestination
{
    fn prepare(&mut self, source: &DataSource) {
        let headers: Vec<String> = source
            .get_column_info()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        self.csv_writer.write_record(headers);

    }
    fn add_rows(&mut self, rows: &[Row]) {
        for row in rows {
            self.csv_writer.write_record(CSVDestination::row_to_csv_row(&row));
        }
    }

    fn close(&mut self) {
        self.csv_writer.flush();
    }
}
