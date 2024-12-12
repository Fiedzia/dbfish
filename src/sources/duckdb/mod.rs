use std::fs::File;
use std::io::Read;

use duckdb;
use arrow::datatypes::DataType;

use crate::commands::{common::DuckDBConfigOptions, export::DuckDBSourceOptions};
use crate::definitions::{
    ColumnInfo, ColumnType, DataSource, DataSourceBatchIterator, DataSourceConnection, Row, Value,
};
use crate::utils::report_query_error;

pub trait GetDuckDBConnectionParams {
    fn get_filename(&self) -> &Option<String>;
    fn get_init(&self) -> &Vec<String>;
}

impl GetDuckDBConnectionParams for DuckDBSourceOptions {
    fn get_filename(&self) -> &Option<String> {
        &self.filename
    }
    fn get_init(&self) -> &Vec<String> {
        &self.init
    }
}

impl GetDuckDBConnectionParams for DuckDBConfigOptions {
    fn get_filename(&self) -> &Option<String> {
        &self.filename
    }
    fn get_init(&self) -> &Vec<String> {
        &self.init
    }
}

pub fn establish_duckdb_connection(options: &dyn GetDuckDBConnectionParams) -> duckdb::Connection {
    match options.get_filename() {
        Some(fname) => duckdb::Connection::open(fname).unwrap(),
        None => duckdb::Connection::open_in_memory().unwrap(),
    }
}

pub struct DuckDBSource {
    options: DuckDBSourceOptions,
}

pub struct DuckDBSourceConnection<'source> {
    connection: duckdb::Connection,
    source: &'source DuckDBSource,
}

pub struct DuckDBSourceBatchIterator<'conn> {
    batch_size: u64,
    _connection: &'conn duckdb::Connection,
    count: Option<u64>,
    done: bool, //sqlite iterator resets once done for some reason
    row_iterator: Option<Box<dyn FnMut() -> u32>>,
    statement: duckdb::Statement<'conn>,
}

impl DuckDBSource {
    pub fn init(duckdb_options: &DuckDBSourceOptions) -> DuckDBSource {
        DuckDBSource {
            options: duckdb_options.to_owned(),
        }
    }
}

impl<'source, 'conn> DataSource<'source, 'conn, DuckDBSourceConnection<'source>> for DuckDBSource
where
    'source: 'conn,
{
    fn connect(&'source self) -> DuckDBSourceConnection {
        let connection = establish_duckdb_connection(&self.options);
        if !self.options.init.is_empty() {
            for sql in self.options.init.iter() {
                match connection.execute(sql, []) {
                    Ok(_) => {}
                    Err(e) => {
                        report_query_error(sql, &format!("{:?}", e));
                        std::process::exit(1);
                    }
                }
            }
        }

        DuckDBSourceConnection {
            connection,
            source: self,
        }
    }

    fn get_type_name(&self) -> String {
        "duckdb".to_string()
    }
    fn get_name(&self) -> String {
        "duckdb".to_string()
    }
}

impl<'source: 'conn, 'conn> DataSourceConnection<'conn> for DuckDBSourceConnection<'source> {
    fn batch_iterator(
        &'conn mut self,
        batch_size: u64,
    ) -> Box<(dyn DataSourceBatchIterator<'conn> + 'conn)> {
        let query = match &self.source.options.query {
            Some(q) => q.to_owned(),
            None => match &self.source.options.query_file {
                Some(path_buf) => {
                    let mut sql = String::new();
                    File::open(path_buf)
                        .unwrap()
                        .read_to_string(&mut sql)
                        .unwrap();
                    sql
                }
                None => panic!("You need to pass either q or query-file option"),
            },
        };

        let mut db_iterator = Box::new(DuckDBSourceBatchIterator {
            batch_size,
            _connection: &self.connection,
            count: None,
            done: false,
            statement: match self.connection.prepare(&query) {
                Ok(v) => v,
                Err(e) => {
                    report_query_error(&query, &format!("{:?}", e));
                    std::process::exit(1);
                }
            },
            row_iterator: None
        });
        db_iterator.row_iterator = Some(Box::new(|| { let mut result = db_iterator.statement.query([]);  1}));
        db_iterator
    }
}

impl<'conn> DataSourceBatchIterator<'conn> for DuckDBSourceBatchIterator<'conn> {
    fn get_column_info(&self) -> Vec<ColumnInfo> {
        let columns: Vec<ColumnInfo> = (0..self.statement.column_count())
            .map(|idx| ColumnInfo {
                name: self.statement.column_name(idx).unwrap_or(&"".to_string()).to_string(),
                data_type: match self.statement.column_type(idx) {
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => ColumnType::Bytes,
                    DataType::Float16 | DataType::Float32 => ColumnType::F32,
                    DataType::Float64 => ColumnType::F64,
                    DataType::Boolean => ColumnType::Bool,
                    DataType::Int8 => ColumnType::I8,
                    DataType::UInt8 => ColumnType::U8,
                    DataType::Int16 => ColumnType::I16,
                    DataType::UInt16 => ColumnType::U16,
                    DataType::Int32 => ColumnType::I32,
                    DataType::UInt32 => ColumnType::U32,
                    DataType::Int64 => ColumnType::U64,
                    DataType::UInt64 => ColumnType::I64,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ColumnType::String,
                    type_ => panic!("duckdb: unsupported type: {:?}", type_)

/*

Null,
    Timestamp(TimeUnit, Option<Arc<str>>),
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Duration(TimeUnit),
    Interval(IntervalUnit),
    List(FieldRef),
    ListView(FieldRef),
    FixedSizeList(FieldRef, i32),
    LargeList(FieldRef),
    LargeListView(FieldRef),
    Struct(Fields),
    Union(UnionFields, UnionMode),
    Dictionary(Box<DataType>, Box<DataType>),
    Decimal128(u8, i8),
    Decimal256(u8, i8),
    Map(FieldRef, bool),
    RunEndEncoded(FieldRef, FieldRef),

*/

                    /*duckdb::types::Binary => ColumnType::Bytes,
                    sqlite::Type::Float => ColumnType::F64,
                    sqlite::Type::Integer => ColumnType::I64,
                    sqlite::Type::String => ColumnType::String,
                    sqlite::Type::Null => ColumnType::Bytes,*/
                },
            })
            .collect();
        println!("{:?}", columns);
        columns
    }

    fn get_count(&self) -> Option<u64> {
        self.count
    }

    fn next(&mut self) -> Option<Vec<Row>> {
        if self.done {
            return None;
        };
        let mut rows = vec![];
        /*loop {
            if rows.len() == self.batch_size as usize {
                break;
            }
            match self.statement.next().unwrap() {
                sqlite::State::Done => {
                    self.done = true;
                    break;
                }
                sqlite::State::Row => {
                    let row = (0..self.statement.column_count())
                        .map(|idx| {
                            let value: sqlite::Value = self.statement.read(idx).unwrap();
                            match value {
                                sqlite::Value::String(s) => Value::String(s),
                                sqlite::Value::Binary(b) => match String::from_utf8(b.clone()) {
                                    Ok(s) => Value::String(s),
                                    Err(_) => Value::Bytes(b),
                                },
                                sqlite::Value::Float(f) => Value::F64(f),
                                sqlite::Value::Integer(i) => Value::I64(i),
                                sqlite::Value::Null => Value::None,
                            }
                        })
                        .collect();
                    rows.push(row);
                }
            }
        }*/
        if !rows.is_empty() {
            Some(rows)
        } else {
            None
        }
    }
}
