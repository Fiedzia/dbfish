use mysql;
use mysql::consts::ColumnType as MyColumnType;
use mysql::consts::ColumnFlags as MyColumnFlags;

use crate::commands::MysqlSourceOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataDestination};

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
    pool: mysql::Pool,
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
            pool,
            results: mysql_result,
        }
    }

    pub fn mysql_to_row(mysql_row: mysql::Row) -> Row {
        let mut result = Row::with_capacity(mysql_row.len());
        for value in mysql_row.unwrap() {
            match &value {
                mysql::Value::NULL => result.push(Value::None),
                mysql::Value::Int(v) => result.push(Value::I64(*v)),
                mysql::Value::UInt(v) => result.push(Value::U64(*v)),
                mysql::Value::Float(v) => result.push(Value::F64(*v)),
                mysql::Value::Bytes(v) => match std::str::from_utf8(&v) {
                    Ok(s) => result.push(Value::String(s.to_string())),
                    Err(e) => panic!(format!("mysq: invalid utf8 in '{:?}' for row: {:?}", v, value))
                },
                /*
                Date(u16, u8, u8, u8, u8, u8, u32)
                year, month, day, hour, minutes, seconds, micro seconds

                Time(bool, u32, u8, u8, u8, u32)
                is negative, days, hours, minutes, seconds, micro seconds
                */

                _ => panic!(format!("unsupported mysql data type: {:?}", value))
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
                    /*MyColumnType::MYSQL_TYPE_TIMESTAMP => ,
                    MyColumnType::MYSQL_TYPE_DATE,
                    MyColumnType::MYSQL_TYPE_TIME,
                    MyColumnType::MYSQL_TYPE_DATETIME,
                    MyColumnType::MYSQL_TYPE_YEAR,*/




                    /*
                    MyColumnType::MYSQL_TYPE_NULL,
                    MyColumnType::MYSQL_TYPE_NEWDATE,
                    MyColumnType::MYSQL_TYPE_BIT,
                    MyColumnType::MYSQL_TYPE_TIMESTAMP2,
                    MyColumnType::MYSQL_TYPE_DATETIME2,
                    MyColumnType::MYSQL_TYPE_TIME2,
                    MyColumnType::MYSQL_TYPE_NEWDECIMAL,
                    MyColumnType::MYSQL_TYPE_ENUM,
                    MyColumnType::MYSQL_TYPE_SET,
                    MyColumnType::MYSQL_TYPE_GEOMETRY,*/
                    _ => panic!(format!("mysql: unsupported column type: {:?}", column_type))
                },
            });
        }
        result
    }

    fn get_count(&self) -> Option<u64> { self.count }

    fn get_rows(&mut self, count: u32) -> Option<Vec<Row>> {
        let mut results: Vec<Row> =  self.results
            .by_ref()
            .take(count as usize)
            .map(|v|{ MysqlSource::mysql_to_row(v.unwrap())})
            .collect();
        match results.len() {
            0 => None,
            _ => Some(results)
        }
    }
}


