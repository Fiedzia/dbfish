use chrono;
use postgres::{self, Connection, rows::Rows, TlsMode};


use crate::commands::PostgresSourceOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataDestination};

pub fn get_postgres_url(postgres_options: &PostgresSourceOptions) -> String {
    format!(
        "postgres://{user}:{password}@{hostname}:{port}/{database}",
        user=postgres_options.user,
        hostname=postgres_options.host,
        password=postgres_options.password.clone().unwrap_or("".to_string()),
        port=postgres_options.port,
        database=postgres_options.database.clone().unwrap_or("".to_string()),
    )
}


pub fn establish_connection(postgres_options: &PostgresSourceOptions) -> Connection {

    let database_url = get_postgres_url(&postgres_options);
    let conn = Connection::connect(database_url, TlsMode::None).unwrap();

    if let Some(ref init) = postgres_options.init {
        conn.execute(init, &[]).unwrap();
    }
    conn
}


pub struct PostgresSource {
    connection: Connection,
    results:  Rows,
    count: Option<u64>,
}

impl PostgresSource {
    pub fn init(postgres_options: &PostgresSourceOptions) -> PostgresSource {

        let conn = establish_connection(&postgres_options);
        let count: Option<u64> = match postgres_options.count {
            true => {
                let count_query = format!("select count(*) from ({}) q", postgres_options.query);
                let count_value:i64 = conn.query(count_query.as_str(), &[]).unwrap().get(0).get(0);
                Some(count_value as u64)
            },
            false => None,
        };
        let postgres_result = conn.query(postgres_options.query.as_str(), &[]).unwrap();

        PostgresSource {
            count,
            connection: conn,
            results: postgres_result,
        }
    }

    pub fn postgres_to_row(column_info: &[ColumnInfo], postgres_row: postgres::rows::Row) -> Row {
        let mut result = Row::with_capacity(postgres_row.len());
        /*for (idx, value) in mysql_row.unwrap().iter().enumerate() {
            match &value {
                mysql::Value::NULL => result.push(Value::None),
                mysql::Value::Int(v) => result.push(Value::I64(*v)),
                mysql::Value::UInt(v) => result.push(Value::U64(*v)),
                mysql::Value::Float(v) => result.push(Value::F64(*v)),
                mysql::Value::Bytes(v) => match std::str::from_utf8(&v) {
                    Ok(s) => result.push(Value::String(s.to_string())),
                    Err(e) => panic!(format!("mysq: invalid utf8 in '{:?}' for row: {:?}", v, value))
                },
                mysql::Value::Date(year, month, day, hour, minute, second, microsecond) => {
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
                mysql::Value::Time(negative, day, hour, minute, second, microsecond) => {
                    match column_info[idx].data_type {
                        ColumnType::Time => result.push(
                            Value::Time(chrono::NaiveTime::from_hms(*hour as u32, *minute as u32, *second as u32))
                        ),
                        _ => panic!("mysql: unsupported conversion: {:?} => {:?}", value, column_info[idx])
                    }
                },
            }
        }*/
        result
    }
}

impl DataSource for PostgresSource {

    fn get_name(&self) -> String { "postgresql".to_string() }

    fn get_column_info(&self) -> Vec<ColumnInfo> {
        let mut result = vec![];
        /*for column in self.results.columns(){
            result.push(ColumnInfo {
                name: column.name,to_string(),
                data_type: {
                    let type_ = column.type();
                }
            }
        }*/
        result
    }

    fn get_count(&self) -> Option<u64> { self.count }

    fn get_rows(&mut self, count: u32) -> Option<Vec<Row>> {
        let ci = self.get_column_info();
        let mut results: Vec<Row> =  self.results
            .iter()
            .by_ref()
            .take(count as usize)
            .map(|v|{ PostgresSource::postgres_to_row(&ci, v)})
            .collect();
        match results.len() {
            0 => None,
            _ => Some(results)
        }
    }
}


