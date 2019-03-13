use sqlite;

use crate::commands::{common::SqliteConfigOptions, export::SqliteSourceOptions};
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataSourceConnection, DataSourceBatchIterator};

pub trait GetSqliteConnectionParams {
    fn get_filename(&self) -> &Option<String>;
    fn get_init(&self) -> &Vec<String>;
}

impl GetSqliteConnectionParams for SqliteSourceOptions {
    fn get_filename(&self) -> &Option<String> { &self.filename }
    fn get_init(&self) -> &Vec<String> { &self.init }
}

impl GetSqliteConnectionParams for SqliteConfigOptions {
    fn get_filename(&self) -> &Option<String> { &self.filename }
    fn get_init(&self) -> &Vec<String> { &self.init }
}

pub fn establish_sqlite_connection(options: &GetSqliteConnectionParams) -> sqlite::Connection{
    sqlite::Connection::open(
        options
            .get_filename()
            .to_owned()
            .unwrap_or_else(||":memory:".to_string())
    ).unwrap()
}

pub struct SqliteSource {
    options: SqliteSourceOptions,
}

pub struct SqliteSourceConnection<'c> {
    connection: sqlite::Connection,
    source: &'c SqliteSource,
}

pub struct SqliteSourceBatchIterator<'c, 'i>
where 'c: 'i
{
    batch_size: u64,
    connection: &'i sqlite::Connection,
    count: Option<u64>,
    statement: sqlite::Statement<'i>,
    source_connection: &'i SqliteSourceConnection<'c>
}

impl SqliteSource {
    pub fn init(sqlite_options: &SqliteSourceOptions) -> SqliteSource {
        SqliteSource { options: sqlite_options.to_owned() }
    }
}


impl <'c, 'i> DataSource<'c, 'i, SqliteSourceConnection<'c>, SqliteSourceBatchIterator<'c, 'i>> for SqliteSource
where 'c: 'i,
{
    fn connect(&'c self) -> SqliteSourceConnection
    {
        
        let connection =  establish_sqlite_connection(&self.options);
        if !self.options.init.is_empty() {
            for sql in self.options.init.iter() {
                connection.execute(sql).unwrap();
            }
        }

        SqliteSourceConnection {
            connection,
            source: &self,
        }
    }

    fn get_type_name(&self) -> String {"sqlite".to_string()}
    fn get_name(&self) -> String { "sqlite".to_string() }


}

impl <'c, 'i>DataSourceConnection<'i, SqliteSourceBatchIterator<'c, 'i>> for SqliteSourceConnection<'c>
{
    fn batch_iterator(&'i self, batch_size: u64) -> SqliteSourceBatchIterator<'c, 'i>
    {
        SqliteSourceBatchIterator {
            batch_size,
            connection: & self.connection,
            count: None,
            statement: self.connection.prepare(&self.source.options.query).unwrap(),
            source_connection: &self,
        }
    }
}


impl <'c, 'i>DataSourceBatchIterator for SqliteSourceBatchIterator<'c, 'i>
{
    fn get_column_info(&self) -> Vec<ColumnInfo> {
        let columns:Vec<ColumnInfo> = (0..self.statement.count()).map(|idx| {
            ColumnInfo {
                name: self.statement.name(idx).to_owned(),
                data_type: match self.statement.kind(idx) {
                    sqlite::Type::Binary => ColumnType::Bytes,
                    sqlite::Type::Float => ColumnType::F64,
                    sqlite::Type::Integer => ColumnType::I64,
                    sqlite::Type::String => ColumnType::Bytes,
                    sqlite::Type::Null   => ColumnType::None,
                },
            }
        }).collect();
        columns
    }

    fn get_count(&self) -> Option<u64> {
        self.count
    }
 
    fn next(&mut self) -> Option<Vec<Row>>
    {
        let mut rows = vec![];
        loop {
            if rows.len() == self.batch_size as usize {
                break;
            }
            match self.statement.next().unwrap() {
                sqlite::State::Done => break,
                sqlite::State::Row => {

                    let row =(0..self.statement.count()).map(|idx| {
                        let value: sqlite::Value = self.statement.read(idx).unwrap();
                        match value {
                            sqlite::Value::String(s) => Value::String(s),
                            sqlite::Value::Binary(b) => Value::Bytes(b),
                            sqlite::Value::Float(f) => Value::F64(f),
                            sqlite::Value::Integer(i) => Value::I64(i),
                            sqlite::Value::Null => Value::None,
                        }
                    }).collect();
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
