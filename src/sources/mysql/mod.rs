use std::fs::File;
use std::io::Read;
use std::time::Duration;

use chrono;
use mysql;
use mysql::consts::ColumnFlags as MyColumnFlags;
use mysql::consts::ColumnType as MyColumnType;
use mysql::prelude::Queryable;

use crate::commands::common::MysqlConfigOptions;
use crate::commands::export::MysqlSourceOptions;
use crate::definitions::{
    ColumnInfo, ColumnType, DataSource, DataSourceBatchIterator, DataSourceConnection, Row, Value,
};
use crate::utils::report_query_error;

pub trait GetMysqlConnectionParams {
    fn get_hostname(&self) -> &Option<String>;
    fn get_username(&self) -> &Option<String>;
    fn get_password(&self) -> &Option<String>;
    fn get_port(&self) -> &Option<u16>;
    fn get_socket(&self) -> &Option<String>;
    fn get_database(&self) -> &Option<String>;
    fn get_init(&self) -> &Vec<String>;
    fn get_timeout(&self) -> &Option<u64>;
}

impl GetMysqlConnectionParams for MysqlSourceOptions {
    fn get_hostname(&self) -> &Option<String> {
        &self.host
    }
    fn get_username(&self) -> &Option<String> {
        &self.user
    }
    fn get_password(&self) -> &Option<String> {
        &self.password
    }
    fn get_port(&self) -> &Option<u16> {
        &self.port
    }
    fn get_socket(&self) -> &Option<String> {
        &self.socket
    }
    fn get_database(&self) -> &Option<String> {
        &self.database
    }
    fn get_init(&self) -> &Vec<String> {
        &self.init
    }
    fn get_timeout(&self) -> &Option<u64> {
        &self.timeout
    }
}

impl GetMysqlConnectionParams for MysqlConfigOptions {
    fn get_hostname(&self) -> &Option<String> {
        &self.host
    }
    fn get_username(&self) -> &Option<String> {
        &self.user
    }
    fn get_password(&self) -> &Option<String> {
        &self.password
    }
    fn get_port(&self) -> &Option<u16> {
        &self.port
    }
    fn get_socket(&self) -> &Option<String> {
        &self.socket
    }
    fn get_database(&self) -> &Option<String> {
        &self.database
    }
    fn get_init(&self) -> &Vec<String> {
        &self.init
    }
    fn get_timeout(&self) -> &Option<u64> {
        &self.timeout
    }
}

pub fn establish_mysql_connection(
    mysql_options: &dyn GetMysqlConnectionParams,
) -> mysql::PooledConn {
    let mut option_builder = mysql::OptsBuilder::new();
    option_builder = option_builder
        .db_name(mysql_options.get_database().to_owned())
        .user(mysql_options.get_username().to_owned())
        .pass(mysql_options.get_password().to_owned());

    if let Some(timeout) = mysql_options.get_timeout() {
        option_builder = option_builder
            .read_timeout(Some(Duration::from_secs(*timeout)))
            .write_timeout(Some(Duration::from_secs(*timeout)))
            .tcp_connect_timeout(Some(Duration::from_secs(*timeout)));
    };

    option_builder = if let Some(ref socket) = mysql_options.get_socket() {
        option_builder.socket(Some(socket.to_owned()))
    } else {
        option_builder
            .ip_or_hostname(
                mysql_options
                    .get_hostname()
                    .to_owned()
                    .or_else(|| Some("localhost".to_string())),
            )
            .tcp_port(mysql_options.get_port().to_owned().unwrap_or(3306))
    };

    if !mysql_options.get_init().is_empty() {
        option_builder = option_builder.init(mysql_options.get_init().to_owned());
    };

    mysql::Pool::new(option_builder)
        .unwrap()
        .get_conn()
        .unwrap()
}

pub struct MysqlSource {
    options: MysqlSourceOptions,
}

impl MysqlSource {
    pub fn init(mysql_options: &MysqlSourceOptions) -> MysqlSource {
        MysqlSource {
            options: mysql_options.to_owned(),
        }
    }
}

pub struct MysqlSourceConnection<'source> {
    connection: mysql::PooledConn,
    source: &'source MysqlSource,
}

pub struct MysqlSourceBatchIterator<'conn, T>
where
    T: mysql::prelude::Protocol,
{
    batch_size: u64,
    //connection: &'conn mysql::PooledConn,
    count: Option<u64>,
    results: mysql::QueryResult<'conn, 'conn, 'conn, T>,
}

impl<'conn, T> MysqlSourceBatchIterator<'conn, T>
where
    T: mysql::prelude::Protocol,
{
    pub fn mysql_to_row(column_info: &[ColumnInfo], mysql_row: mysql::Row) -> Row {
        let mut result = Row::with_capacity(mysql_row.len());
        for (idx, value) in mysql_row.unwrap().iter().enumerate() {
            match &value {
                mysql::Value::NULL => result.push(Value::None),
                mysql::Value::Int(v) => result.push(Value::I64(*v)),
                mysql::Value::UInt(v) => result.push(Value::U64(*v)),
                mysql::Value::Float(v) => result.push(Value::F64(*v as f64)),
                mysql::Value::Double(v) => result.push(Value::F64(*v)),
                mysql::Value::Bytes(v) => match std::str::from_utf8(v) {
                    Ok(s) => result.push(Value::String(s.to_string())),
                    Err(e) => panic!(
                        "mysq: invalid utf8 in '{:?}' for row: {:?} ({})",
                        v, value, e
                    ),
                },
                mysql::Value::Date(year, month, day, hour, minute, second, _microsecond) => {
                    match column_info[idx].data_type {
                        ColumnType::Date => result.push(Value::Date(
                            chrono::NaiveDate::from_ymd_opt(
                                i32::from(*year),
                                u32::from(*month),
                                u32::from(*day),
                            )
                            .unwrap(),
                        )),
                        ColumnType::DateTime => result.push(Value::DateTime(
                            chrono::NaiveDate::from_ymd_opt(
                                i32::from(*year),
                                u32::from(*month),
                                u32::from(*day),
                            )
                            .unwrap()
                            .and_hms_opt(u32::from(*hour), u32::from(*minute), u32::from(*second))
                            .unwrap(),
                        )),
                        ColumnType::Time => result.push(Value::Time(
                            chrono::NaiveTime::from_hms_opt(
                                u32::from(*hour),
                                u32::from(*minute),
                                u32::from(*second),
                            )
                            .unwrap(),
                        )),
                        ColumnType::Timestamp => result.push(Value::DateTime(
                            chrono::NaiveDate::from_ymd_opt(
                                i32::from(*year),
                                u32::from(*month),
                                u32::from(*day),
                            )
                            .unwrap()
                            .and_hms_opt(u32::from(*hour), u32::from(*minute), u32::from(*second))
                            .unwrap(),
                        )),
                        _ => panic!(
                            "mysql: unsupported conversion: {:?} => {:?}",
                            value, column_info[idx]
                        ),
                    }
                }
                //TODO: what to do with negative?
                mysql::Value::Time(_negative, _day, hour, minute, second, _microsecond) => {
                    match column_info[idx].data_type {
                        ColumnType::Time => result.push(Value::Time(
                            chrono::NaiveTime::from_hms_opt(
                                u32::from(*hour),
                                u32::from(*minute),
                                u32::from(*second),
                            )
                            .unwrap(),
                        )),
                        _ => panic!(
                            "mysql: unsupported conversion: {:?} => {:?}",
                            value, column_info[idx]
                        ),
                    }
                }
            }
        }
        result
    }
}

impl<'source: 'conn, 'conn> DataSource<'source, 'conn, MysqlSourceConnection<'source>>
    for MysqlSource
//impl <'source, 'conn> DataSource<'source, 'conn, MysqlSourceConnection<'source>, MysqlSourceBatchIterator<'source, 'conn>> for MysqlSource
{
    fn connect(&'source self) -> MysqlSourceConnection {
        let connection = establish_mysql_connection(&self.options);

        MysqlSourceConnection {
            connection,
            source: self,
        }
    }

    fn get_type_name(&self) -> String {
        "mysql".to_string()
    }
    fn get_name(&self) -> String {
        "mysql".to_string()
    }
}

impl<'source: 'conn, 'conn> DataSourceConnection<'conn> for MysqlSourceConnection<'source> {
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

        let count: Option<u64> = if self.source.options.count {
            let count_query = format!("select count(*) from ({}) q", query);
            let count_value: u64 = self
                .connection
                .exec_first::<mysql::Row, _, _>(count_query.as_str(), ())
                .unwrap()
                .unwrap()
                .get(0)
                .unwrap();
            Some(count_value)
        } else {
            None
        };
        let mysql_result = match self.connection.exec_iter(query.clone(), ()) {
            Ok(v) => v,
            Err(e) => {
                report_query_error(&query, &format!("{:?}", e));
                std::process::exit(1);
            }
        };

        Box::new(MysqlSourceBatchIterator {
            batch_size,
            //connection: &self.connection,
            count,
            results: mysql_result,
        })
    }
}

impl<'conn, T> DataSourceBatchIterator<'conn> for MysqlSourceBatchIterator<'conn, T>
where
    T: mysql::prelude::Protocol,
{
    fn get_column_info(&self) -> Vec<ColumnInfo> {
        let mut result = vec![];
        for column in self.results.columns().as_ref() {
            let column_type = column.column_type();
            let flags = column.flags();
            result.push(ColumnInfo {
                name: column.name_str().into_owned(),
                data_type: match column_type {
                    MyColumnType::MYSQL_TYPE_DECIMAL => ColumnType::Decimal,
                    MyColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnType::Decimal,
                    MyColumnType::MYSQL_TYPE_TINY => {
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {
                            ColumnType::U8
                        } else {
                            ColumnType::I8
                        }
                    }
                    MyColumnType::MYSQL_TYPE_SHORT => {
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {
                            ColumnType::U16
                        } else {
                            ColumnType::I16
                        }
                    }
                    MyColumnType::MYSQL_TYPE_LONG => {
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {
                            ColumnType::U32
                        } else {
                            ColumnType::I32
                        }
                    }
                    MyColumnType::MYSQL_TYPE_LONGLONG => {
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {
                            ColumnType::U64
                        } else {
                            ColumnType::I64
                        }
                    }
                    MyColumnType::MYSQL_TYPE_INT24 => {
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {
                            ColumnType::U32
                        } else {
                            ColumnType::I32
                        }
                    }
                    MyColumnType::MYSQL_TYPE_VARCHAR
                    | MyColumnType::MYSQL_TYPE_VAR_STRING
                    | MyColumnType::MYSQL_TYPE_STRING => ColumnType::String,
                    MyColumnType::MYSQL_TYPE_FLOAT => ColumnType::F32,
                    MyColumnType::MYSQL_TYPE_DOUBLE => ColumnType::F64,
                    MyColumnType::MYSQL_TYPE_JSON => ColumnType::JSON,
                    MyColumnType::MYSQL_TYPE_TINY_BLOB
                    | MyColumnType::MYSQL_TYPE_MEDIUM_BLOB
                    | MyColumnType::MYSQL_TYPE_LONG_BLOB
                    | MyColumnType::MYSQL_TYPE_BLOB => ColumnType::Bytes,

                    MyColumnType::MYSQL_TYPE_TIMESTAMP => ColumnType::Timestamp,
                    MyColumnType::MYSQL_TYPE_DATE => ColumnType::Date,
                    MyColumnType::MYSQL_TYPE_TIME => ColumnType::Time,
                    MyColumnType::MYSQL_TYPE_TIME2 => ColumnType::Time,
                    MyColumnType::MYSQL_TYPE_DATETIME => ColumnType::DateTime,
                    MyColumnType::MYSQL_TYPE_DATETIME2 => ColumnType::DateTime,
                    MyColumnType::MYSQL_TYPE_YEAR => ColumnType::I64,
                    MyColumnType::MYSQL_TYPE_NEWDATE => ColumnType::Date,
                    MyColumnType::MYSQL_TYPE_TIMESTAMP2 => ColumnType::Timestamp,

                    /*
                    MyColumnType::MYSQL_TYPE_NULL,
                    MyColumnType::MYSQL_TYPE_BIT,
                    MyColumnType::MYSQL_TYPE_ENUM,
                    MyColumnType::MYSQL_TYPE_SET,
                    MyColumnType::MYSQL_TYPE_GEOMETR
                    */
                    _ => panic!("mysql: unsupported column type: {:?}", column_type),
                },
            });
        }
        result
    }

    fn get_count(&self) -> Option<u64> {
        self.count
    }

    fn next(&mut self) -> Option<Vec<Row>> {
        let ci = self.get_column_info();
        let results: Vec<Row> = self
            .results
            .by_ref()
            .take(self.batch_size as usize)
            .map(|v| MysqlSourceBatchIterator::<T>::mysql_to_row(&ci, v.unwrap()))
            .collect();
        match results.len() {
            0 => None,
            _ => Some(results),
        }
    }
}
