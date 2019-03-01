use chrono;
use mysql;
use mysql::consts::ColumnType as MyColumnType;
use mysql::consts::ColumnFlags as MyColumnFlags;

use crate::commands::MysqlSourceOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource};

pub fn get_mysql_url(mysql_options: &MysqlSourceOptions) -> String {
    format!(
        "mysql://{user}:{password}@{hostname}:{port}/{database}",
        user=mysql_options.user,
        hostname=mysql_options.host,
        password=mysql_options.password.clone().unwrap_or("".to_string()),
        port=mysql_options.port,
        database=mysql_options.database.clone().unwrap_or("".to_string()),
    )
}


pub fn establish_connection(mysql_options: &MysqlSourceOptions) -> mysql::Pool {

    let database_url = get_mysql_url(&mysql_options);
    let pool = mysql::Pool::new(database_url).unwrap();

    if let Some(ref init) = mysql_options.init {
        pool.prep_exec(init, ()).unwrap();
    }
    pool
}


pub struct MysqlSource<'a> {
    results: mysql::QueryResult<'a>,
    count: Option<u64>,
}

impl <'a>MysqlSource<'a> {
    pub fn init(mysql_options: &MysqlSourceOptions) -> MysqlSource {

        let pool = establish_connection(&mysql_options);
        let count: Option<u64> = match mysql_options.count {
            true => {
                let count_query = format!("select count(*) from ({}) q", mysql_options.query);
                let count_value = pool.first_exec(count_query, ()).unwrap().unwrap().get(0).unwrap();
                Some(count_value)
            },
            false => None,
        };
        let mysql_result = pool.prep_exec(mysql_options.query.clone(), ()).unwrap();

        MysqlSource {
            count,
            results: mysql_result,
        }
    }

    pub fn mysql_to_row(column_info: &[ColumnInfo], mysql_row: mysql::Row) -> Row {
        let mut result = Row::with_capacity(mysql_row.len());
        for (idx, value) in mysql_row.unwrap().iter().enumerate() {
            match &value {
                mysql::Value::NULL => result.push(Value::None),
                mysql::Value::Int(v) => result.push(Value::I64(*v)),
                mysql::Value::UInt(v) => result.push(Value::U64(*v)),
                mysql::Value::Float(v) => result.push(Value::F64(*v)),
                mysql::Value::Bytes(v) => match std::str::from_utf8(&v) {
                    Ok(s) => result.push(Value::String(s.to_string())),
                    Err(e) => panic!(format!("mysq: invalid utf8 in '{:?}' for row: {:?} ({})", v, value, e))
                },
                mysql::Value::Date(year, month, day, hour, minute, second, _microsecond) => {
                    match column_info[idx].data_type {
                        ColumnType::Date => result.push(
                            Value::Date(chrono::NaiveDate::from_ymd(*year as i32, *month as u32, *day as u32))
                        ),
                        ColumnType::DateTime => result.push(
                            Value::DateTime(chrono::NaiveDate::from_ymd(*year as i32, *month as u32, *day as u32).and_hms( *hour as u32, *minute as u32, *second as u32))
                        ),
                        ColumnType::Time => result.push(
                            Value::Time(chrono::NaiveTime::from_hms(*hour as u32, *minute as u32, *second as u32))
                        ),
                        ColumnType::Timestamp => result.push(
                            Value::DateTime(chrono::NaiveDate::from_ymd(*year as i32, *month as u32, *day as u32).and_hms(*hour as u32, *minute as u32, *second as u32))
                        ),
                        _ => panic!("mysql: unsupported conversion: {:?} => {:?}", value, column_info[idx])
                    }
                },
                //TODO: what to do with negative?
                mysql::Value::Time(_negative, _day, hour, minute, second, _microsecond) => {
                    match column_info[idx].data_type {
                        ColumnType::Time => result.push(
                            Value::Time(chrono::NaiveTime::from_hms(*hour as u32, *minute as u32, *second as u32))
                        ),
                        _ => panic!("mysql: unsupported conversion: {:?} => {:?}", value, column_info[idx])
                    }
                },
            }
        }
        result
    }
}

impl <'a>DataSource for MysqlSource<'a> {

    fn get_name(&self) -> String { "mysql".to_string() }

    fn get_column_info(&self) -> Vec<ColumnInfo> {
        let mut result = vec![];
        for column in  self.results.columns_ref() {
            let column_type = column.column_type();
            let flags = column.flags();
            result.push(ColumnInfo {
                name: column.name_str().into_owned(),
                data_type:  match column_type {
                    MyColumnType::MYSQL_TYPE_DECIMAL => ColumnType::Decimal,
                    MyColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnType::Decimal,
                    MyColumnType::MYSQL_TYPE_TINY => 
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {ColumnType::U8} else {ColumnType::I8},
                    MyColumnType::MYSQL_TYPE_SHORT =>
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {ColumnType::U16} else {ColumnType::I16},
                    MyColumnType::MYSQL_TYPE_LONG =>
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {ColumnType::U32} else {ColumnType::I32},
                    MyColumnType::MYSQL_TYPE_LONGLONG =>
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {ColumnType::U64} else {ColumnType::I64},
                    MyColumnType::MYSQL_TYPE_INT24 =>
                        if flags.contains(MyColumnFlags::UNSIGNED_FLAG) {ColumnType::U32} else {ColumnType::I32},
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
                    _ => panic!(format!("mysql: unsupported column type: {:?}", column_type))
                },
            });
        }
        result
    }

    fn get_count(&self) -> Option<u64> { self.count }

    fn get_rows(&mut self, count: u32) -> Option<Vec<Row>> {
        let ci = self.get_column_info();
        let results: Vec<Row> =  self.results
            .by_ref()
            .take(count as usize)
            .map(|v|{ MysqlSource::mysql_to_row(&ci, v.unwrap())})
            .collect();
        match results.len() {
            0 => None,
            _ => Some(results)
        }
    }
}


