use std::fs::File;
use std::io::Read;

use postgres::{self, NoTls, Client, types::Kind};
use postgres::fallible_iterator::FallibleIterator;
use urlencoding;

use crate::commands::common::PostgresConfigOptions;
use crate::commands::export::PostgresSourceOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataSourceConnection, DataSourceBatchIterator};
use crate::utils::report_query_error;


pub trait GetPostgresConnectionParams {
    fn get_hostname(&self) -> &Option<String>;
    fn get_username(&self) -> &Option<String>;
    fn get_password(&self) -> &Option<String>;
    fn get_port(&self) -> &Option<u16>;
    fn get_database(&self) -> &Option<String>;
    fn get_init(&self) -> &Vec<String>;
    fn get_timeout(&self) -> &Option<u64>;
}

impl GetPostgresConnectionParams for PostgresSourceOptions {
    fn get_hostname(&self) -> &Option<String> { &self.host }
    fn get_username(&self) -> &Option<String> { &self.user }
    fn get_password(&self) -> &Option<String> { &self.password }
    fn get_port(&self) -> &Option<u16> { &self.port }
    fn get_database(&self) -> &Option<String> { &self.database }
    fn get_init(&self) -> &Vec<String> { &self.init }
    fn get_timeout(&self) -> &Option<u64> { &self.timeout }
}

impl GetPostgresConnectionParams for PostgresConfigOptions {
    fn get_hostname(&self) -> &Option<String> { &self.host }
    fn get_username(&self) -> &Option<String> { &self.user }
    fn get_password(&self) -> &Option<String> { &self.password }
    fn get_port(&self) -> &Option<u16> { &self.port }
    fn get_database(&self) -> &Option<String> { &self.database }
    fn get_init(&self) -> &Vec<String> { &self.init }
    fn get_timeout(&self) -> &Option<u64> { &self.timeout }
}


pub fn get_postgres_url(postgres_options: &dyn GetPostgresConnectionParams) -> String {
    format!(
        "postgres://{user}{password}{hostname}{port}{database}",
        user=match &postgres_options.get_username() { None => "", Some(v) => v},
        hostname=match &postgres_options.get_hostname() {
            None => "".to_string(),
            Some(v) => format!("@{}", urlencoding::encode(v))
        },
        password=match &postgres_options.get_password() {
            None => "".to_string(),
            Some(p) => format!(":{}", urlencoding::encode(p))
        },
        port=match &postgres_options.get_port() {
            None => "".to_string(),
            Some(p) => format!(":{}", p)
        },
        database=match &postgres_options.get_database() {
            None => "".to_string(),
            Some(d) => format!("/{}", urlencoding::encode(d))
        },
    )
}


pub fn establish_postgres_connection(postgres_options: &dyn GetPostgresConnectionParams) -> Client {

    let database_url = get_postgres_url(postgres_options);
    let mut conn = Client::connect(&database_url, NoTls).unwrap();

    if !postgres_options.get_init().is_empty() {
        for sql in postgres_options.get_init().iter() {
            conn.execute(sql, &[]).unwrap();
        }
    }
    conn
}



pub struct PostgresSource {
    options: PostgresSourceOptions,
}

pub struct PostgresSourceConnection<'source> {
    connection: Client,
    source: &'source  PostgresSource,
}

pub struct PostgresSourceBatchIterator<'conn>
{
    batch_size: u64,
    result_iterator: postgres::RowIter<'conn>,
    first_row: Option<postgres::row::Row>,
}

impl PostgresSource {
    pub fn init(postgres_options: &PostgresSourceOptions) -> PostgresSource {
        PostgresSource { options: postgres_options.to_owned() }
    }
}

impl <'source: 'conn, 'conn>PostgresSourceConnection<'source> {

    pub fn _batch_iterator(source: &'source PostgresSource, connection: &'source mut Client, batch_size: u64) -> Box<(dyn DataSourceBatchIterator<'conn> + 'conn)> {


        let query = match &source.options.query {
            Some(q) => q.to_owned(),
            None => match &source.options.query_file {
                Some(path_buf) => {
                    let mut sql = String::new();
                    File::open(path_buf).unwrap().read_to_string(&mut sql).unwrap();
                    sql
                },
                None => panic!("You need to pass either q or query-file option"),
            }
        };


        let mut batch_iterator = connection.query_raw::<str,Vec<String>  ,_ >(&query, vec![])
            .unwrap();
        let first_row = batch_iterator
            .by_ref()
            .peekable()
            .peek()
            .unwrap()
            .map(|r|r.clone());
        /*let batch_iterator = 
                    row_iterator = match conn.query_raw::<str,Vec<String>  ,_ >(&query, vec![]) {
                        Ok(r) => Some(r.peekable()),
                        Err(e) => {
                            report_query_error(&query, &format!("{:?}", e));
                            std::process::exit(1);
                        }
                    };
                };
            });*/

        Box::new(PostgresSourceBatchIterator {
            batch_size,
            first_row,
            result_iterator: batch_iterator,
        })

    }

}

impl <'source: 'conn, 'conn> DataSource<'source, 'conn, PostgresSourceConnection<'source>> for PostgresSource
{
    fn connect(&'source self) -> PostgresSourceConnection
    {
        
        let mut connection =  establish_postgres_connection(&self.options);
        if !self.options.init.is_empty() {
            for sql in self.options.init.iter() {
                match connection.execute(sql, &[]) {
                    Ok(_) => {},
                    Err(e) => {
                        report_query_error(&sql, &format!("{:?}", e));
                        std::process::exit(1);
                    }
                }
            }
        }

        PostgresSourceConnection {
            connection,
            source: &self,
        }
    }

    fn get_type_name(&self) -> String {"postgres".to_string()}
    fn get_name(&self) -> String { "postgres".to_string() }


}

impl <'source: 'conn, 'conn>DataSourceConnection<'conn> for PostgresSourceConnection<'source>
{
    fn batch_iterator(&'conn mut self, batch_size: u64) -> Box<(dyn DataSourceBatchIterator<'conn> + 'conn)>
    {
        PostgresSourceConnection::_batch_iterator(self.source, &mut self.connection, batch_size)
    }
}

pub fn postgres_to_row(column_info: &[(String,  postgres::types::Type)], postgres_row: &postgres::row::Row) -> Row {
    let mut result = Row::with_capacity(postgres_row.len());
    for (idx, (_name, type_)) in column_info.iter().enumerate() {
        match (type_.kind(), type_.name()) {
            (Kind::Simple, "int4") => result.push(Value::I32( postgres_row.get(idx) )),
            (Kind::Simple, "int8") => result.push(Value::I64( postgres_row.get(idx) )),
            (Kind::Simple, "float4") => result.push(Value::F32( postgres_row.get(idx) )),
            (Kind::Simple, "float8") => result.push(Value::F64( postgres_row.get(idx) )),
            (Kind::Simple, "text") => result.push(Value::String( postgres_row.get(idx) )),
            _ => panic!("postgres: unsupported type: {:?}", type_ )
        }
    }

    result
}


impl <'conn>DataSourceBatchIterator<'conn> for PostgresSourceBatchIterator<'conn>

{
    fn get_column_info(&self) -> Vec<ColumnInfo> {
       let mut result = vec![];
       match &self.first_row {
           Some(row) => {

                for column in row.columns().iter() {
                    //println!("name={}; type_name={}; kind={:?};", column.name(), column.type_().name(), column.type_().kind() );
                    match (column.type_().kind(), column.type_().name()) {
                        (Kind::Simple, "int4") => result.push(ColumnInfo{name: column.name().to_string(), data_type: ColumnType::I32}),
                        (Kind::Simple, "int8") => result.push(ColumnInfo{name: column.name().to_string(), data_type: ColumnType::I64}),
                        (Kind::Simple, "float4") => result.push(ColumnInfo{name: column.name().to_string(), data_type: ColumnType::F32}),
                        (Kind::Simple, "float8") => result.push(ColumnInfo{name: column.name().to_string(), data_type: ColumnType::F64}),
                        (Kind::Simple, "text") => result.push(ColumnInfo{name: column.name().to_string(), data_type: ColumnType::String}),
                        _ => panic!("postgres: unsupported type: {:?}", column.type_() )
                    };
                }
            },
            _ => {}
        }
        result
    }

    fn get_count(&self) -> Option<u64> {
        self.result_iterator.rows_affected()
    }
 
    fn next(&mut self) -> Option<Vec<Row>>
    {
        let rows :Vec<Row> = self
            .result_iterator
            .by_ref()
            .take(self.batch_size as usize)
            .map(|postgres_row| {
                let mut result = Row::with_capacity(postgres_row.len());
                for (idx, column) in postgres_row.columns().iter().enumerate() {
                    match (column.type_().kind(), column.type_().name()) {
                        (Kind::Simple, "int4") => result.push(Value::I32( postgres_row.get(idx) )),
                        (Kind::Simple, "int8") => result.push(Value::I64( postgres_row.get(idx) )),
                        (Kind::Simple, "float4") => result.push(Value::F32( postgres_row.get(idx) )),
                        (Kind::Simple, "float8") => result.push(Value::F64( postgres_row.get(idx) )),
                        (Kind::Simple, "text") => result.push(Value::String( postgres_row.get(idx) )),
                        _ => panic!("postgres: unsupported type: {:?}", column.type_() )
                    }
                }
                Ok(result)
            }).collect()
            .unwrap();

        if !rows.is_empty() {
            Some(rows)
        } else {
            None
        }
    }
}
