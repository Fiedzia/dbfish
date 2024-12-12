use std::path::Path;

use duckdb;

use crate::commands::export::DuckDBDestinationOptions;
use crate::definitions::{ColumnType, DataDestination, DataSourceBatchIterator, Row, Value};
use crate::utils::truncate_text_with_note;

pub struct DuckDBDestination {
    connection: duckdb::Connection,
    table: String,
    column_names: Vec<String>,
    truncate: Option<u64>,
}

impl DuckDBDestination {
    pub fn init(duckdb_options: &DuckDBDestinationOptions) -> DuckDBDestination {
        let path = Path::new(&duckdb_options.filename);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
        DuckDBDestination {
            connection: duckdb::Connection::open(&duckdb_options.filename).unwrap(),
            table: duckdb_options.table.clone(),
            column_names: vec![],
            truncate: duckdb_options.truncate,
        }
    }
}

impl DataDestination for DuckDBDestination {
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        let columns = result_iterator
            .get_column_info()
            .iter()
            .map(|col| {
                format!(
                    "{} {}",
                    col.name,
                    match col.data_type {
                        ColumnType::U64
                        | ColumnType::I64
                        | ColumnType::U32
                        | ColumnType::I32
                        | ColumnType::U16
                        | ColumnType::I16
                        | ColumnType::U8
                        | ColumnType::I8 => "integer".to_string(),
                        ColumnType::String => "text".to_string(),
                        ColumnType::Bytes => "blob".to_string(),
                        ColumnType::F64 | ColumnType::F32 => "float".to_string(),
                        ColumnType::Bool => "bool".to_string(),
                        ColumnType::DateTime => "datetime".to_string(),
                        ColumnType::Date => "date".to_string(),
                        ColumnType::Time => "time".to_string(),
                        ColumnType::Decimal => "numeric".to_string(),
                        _ => panic!("duckdb: unsupported column type: {:?}", col.data_type),
                    }
                )
            })
            .collect::<Vec<String>>()
            .join(", ");
        self.column_names = result_iterator
            .get_column_info()
            .iter()
            .map(|col| col.name.clone())
            .collect();

        let create_table_query = format!("create table {} ({})", self.table, columns);
        self.connection.execute(&create_table_query, []).unwrap();
    }

    fn add_rows(&mut self, rows: &[Row]) {
        let values_part = self
            .column_names
            .iter()
            .map(|_| "?".to_string())
            .collect::<Vec<String>>()
            .join(", ");
        let mut sql = format!(
            "insert into {} ({}) values ({})",
            self.table,
            self.column_names.join(", "),
            values_part
        );
        for _v in 1..rows.len() {
            sql.push_str(&format!(",({})", values_part));
        }
        let mut statement = self.connection.prepare(&sql).unwrap();
        //let mut cursor = statement.iter();
        let mut data: Vec<duckdb::types::Value> = Vec::with_capacity(self.column_names.len());
        for row in rows {
            for col in row.iter() {
                match col {
                    /*Value::U64(value) => data.push(sqlite::Value::Integer(*value as i64)),
                    Value::I64(value) => data.push(sqlite::Value::Integer(*value)),
                    Value::U32(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::I32(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::U16(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::I16(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::U8(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::I8(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::Bool(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::String(value) => data.push(sqlite::Value::String(
                        truncate_text_with_note(value.to_string(), self.truncate),
                    )),
                    Value::F64(value) => data.push(sqlite::Value::Float(*value)),
                    Value::F32(value) => data.push(sqlite::Value::Float(f64::from(*value))),
                    Value::Bytes(value) => data.push(sqlite::Value::Binary(value.clone())),*/
                    _ => panic!("duckdb: unsupported type: {:?}", col),
                }
            }
        }
        let data_: Vec<&dyn duckdb::types::ToSql> = data.iter().map(|v| v as &dyn duckdb::types::ToSql).collect();
        statement.execute(&data_[..]).unwrap();
    }

    fn close(&mut self) {}
}
