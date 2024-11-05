use std::fs::File;
use std::io::Read;

use sqlite;

use crate::commands::{common::SqliteConfigOptions, export::SqliteSourceOptions};
use crate::definitions::{
    ColumnInfo, ColumnType, DataSource, DataSourceBatchIterator, DataSourceConnection, Row, Value,
};
use crate::utils::report_query_error;

pub trait GetSqliteConnectionParams {
    fn get_filename(&self) -> &Option<String>;
    fn get_init(&self) -> &Vec<String>;
}

impl GetSqliteConnectionParams for SqliteSourceOptions {
    fn get_filename(&self) -> &Option<String> {
        &self.filename
    }
    fn get_init(&self) -> &Vec<String> {
        &self.init
    }
}

impl GetSqliteConnectionParams for SqliteConfigOptions {
    fn get_filename(&self) -> &Option<String> {
        &self.filename
    }
    fn get_init(&self) -> &Vec<String> {
        &self.init
    }
}

pub fn establish_sqlite_connection(options: &dyn GetSqliteConnectionParams) -> sqlite::Connection {
    sqlite::Connection::open(
        options
            .get_filename()
            .to_owned()
            .unwrap_or_else(|| ":memory:".to_string()),
    )
    .unwrap()
}

pub struct SqliteSource {
    options: SqliteSourceOptions,
}

pub struct SqliteSourceConnection<'source> {
    connection: sqlite::Connection,
    source: &'source SqliteSource,
}

pub struct SqliteSourceBatchIterator<'conn> {
    batch_size: u64,
    _connection: &'conn sqlite::Connection,
    count: Option<u64>,
    done: bool, //sqlite iterator resets once done for some reason
    statement: sqlite::Statement<'conn>,
}

impl SqliteSource {
    pub fn init(sqlite_options: &SqliteSourceOptions) -> SqliteSource {
        SqliteSource {
            options: sqlite_options.to_owned(),
        }
    }
}

impl<'source, 'conn> DataSource<'source, 'conn, SqliteSourceConnection<'source>> for SqliteSource
where
    'source: 'conn,
{
    fn connect(&'source self) -> SqliteSourceConnection {
        let connection = establish_sqlite_connection(&self.options);
        if !self.options.init.is_empty() {
            for sql in self.options.init.iter() {
                match connection.execute(sql) {
                    Ok(_) => {}
                    Err(e) => {
                        report_query_error(sql, &format!("{:?}", e));
                        std::process::exit(1);
                    }
                }
            }
        }

        SqliteSourceConnection {
            connection,
            source: self,
        }
    }

    fn get_type_name(&self) -> String {
        "sqlite".to_string()
    }
    fn get_name(&self) -> String {
        "sqlite".to_string()
    }
}

impl<'source: 'conn, 'conn> DataSourceConnection<'conn> for SqliteSourceConnection<'source> {
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

        Box::new(SqliteSourceBatchIterator {
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
        })
    }
}

impl<'conn> DataSourceBatchIterator<'conn> for SqliteSourceBatchIterator<'conn> {
    fn get_column_info(&self) -> Vec<ColumnInfo> {
        let columns: Vec<ColumnInfo> = (0..self.statement.column_count())
            .map(|idx| ColumnInfo {
                name: self.statement.column_name(idx).unwrap_or("").to_string(),
                data_type: match self.statement.column_type(idx).unwrap() {
                    sqlite::Type::Binary => ColumnType::Bytes,
                    sqlite::Type::Float => ColumnType::F64,
                    sqlite::Type::Integer => ColumnType::I64,
                    sqlite::Type::String => ColumnType::String,
                    sqlite::Type::Null => ColumnType::Bytes,
                },
            })
            .collect();
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
        loop {
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
        }
        if !rows.is_empty() {
            Some(rows)
        } else {
            None
        }
    }
}
