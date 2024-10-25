use std::path::Path;

use sqlite;

use crate::commands::export::SqliteDestinationOptions;
use crate::definitions::{ColumnType, Value, Row, DataDestination, DataSourceBatchIterator};
use crate::utils::truncate_text_with_note;


pub struct SqliteDestination {
    connection: sqlite::Connection,
    table: String,
    column_names: Vec<String>,
    truncate: Option<u64>,
}

impl SqliteDestination {

    pub fn init(sqlite_options: &SqliteDestinationOptions) -> SqliteDestination {
        let path = Path::new(&sqlite_options.filename);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
        SqliteDestination {
            connection: sqlite::Connection::open(&sqlite_options.filename).unwrap(),
            table: sqlite_options.table.clone(),
            column_names: vec![],
            truncate: sqlite_options.truncate,
        }
    }
}

impl DataDestination for SqliteDestination
{
    
    fn prepare(&mut self) {}

    fn prepare_for_results(&mut self, result_iterator: &dyn DataSourceBatchIterator) {
        let columns = result_iterator
            .get_column_info()
            .iter()
            .map(|col| { format!("{} {}", col.name, match col.data_type {
                ColumnType::U64 | ColumnType::I64
                | ColumnType::U32 | ColumnType::I32
                | ColumnType::U16 | ColumnType::I16
                | ColumnType::U8 | ColumnType::I8 => "integer".to_string(),
                ColumnType::String => "text".to_string(),
                ColumnType::Bytes => "blob".to_string(),
                ColumnType::F64 | ColumnType::F32 => "float".to_string(),
                ColumnType::Bool => "bool".to_string(),
                ColumnType::DateTime => "datetime".to_string(),
                ColumnType::Date => "date".to_string(),
                ColumnType::Time => "time".to_string(),
                ColumnType::Decimal => "numeric".to_string(),
                _ => panic!("sqlite: unsupported column type: {:?}", col.data_type)
            })})
            .collect::<Vec<String>>()
            .join(", ");
        self.column_names = result_iterator
            .get_column_info()
            .iter()
            .map(|col| { col.name.clone() })
            .collect();

        let create_table_query =format!("create table {} ({})", self.table, columns);
        self.connection.execute(create_table_query).unwrap();
    }

    fn add_rows(&mut self, rows: &[Row]) {
        let values_part = self.column_names.iter().map(|_| {"?".to_string()}).collect::<Vec<String>>().join(", ");
        let mut sql = format!(
            "insert into {} ({}) values ({})",
            self.table,
            self.column_names.join(", "),
            values_part
        );
        for _v in 1..rows.len() {
            sql.push_str(&format!(",({})", values_part));
        }
        let statement = self.connection.prepare(sql).unwrap();
        let mut cursor = statement.cursor();
        let mut data: Vec<sqlite::Value> = Vec::with_capacity(self.column_names.len());
        for row in rows {
            for col in row.iter() {
                match col {
                    Value::U64(value) => data.push(sqlite::Value::Integer(*value as i64)),
                    Value::I64(value) => data.push(sqlite::Value::Integer(*value)),
                    Value::U32(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::I32(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::U16(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::I16(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::U8(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::I8(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::Bool(value) => data.push(sqlite::Value::Integer(i64::from(*value))),
                    Value::String(value) => data.push(sqlite::Value::String(truncate_text_with_note(value.to_string(), self.truncate))),
                    Value::F64(value) => data.push(sqlite::Value::Float(*value)),
                    Value::F32(value) => data.push(sqlite::Value::Float(f64::from(*value))),
                    Value::Bytes(value) => data.push(sqlite::Value::Binary(value.clone())),
                    _ => panic!("sqlite: unsupported type: {:?}", col)
                }
            }
        }
        cursor.bind(&data).unwrap();
        cursor.next().unwrap();
       
    }

    fn close(&mut self) { }

}

