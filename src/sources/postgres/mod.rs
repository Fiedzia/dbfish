use postgres::{self, Connection, TlsMode, types::Kind};

use crate::commands::common::PostgresConfigOptions;
use crate::commands::export::PostgresSourceOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataSourceConnection, DataSourceBatchIterator};


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


pub fn get_postgres_url(postgres_options: &GetPostgresConnectionParams) -> String {
    //TODO: encode parameter values for url
    format!(
        "postgres://{user}{password}@{hostname}{port}{database}",
        user=match &postgres_options.get_username() { None => "", Some(v) => v},
        hostname=match &postgres_options.get_hostname() { None => "", Some(v) => v},
        password=match &postgres_options.get_password() {None => "".to_string(), Some(p) => format!(":{}", p)},
        port=match &postgres_options.get_port() { None => "".to_string(), Some(p) => format!(":{}", p)},
        database=match &postgres_options.get_database() { None => "".to_string(), Some(d) => format!("/{}", d)},
    )
}


pub fn establish_postgres_connection(postgres_options: &GetPostgresConnectionParams) -> Connection {

    let database_url = get_postgres_url(postgres_options);
    let conn = Connection::connect(database_url, TlsMode::None).unwrap();

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

pub struct PostgresSourceConnection<'c> {
    connection: Connection,
    results: postgres::rows::Rows,
    source: &'c  PostgresSource,
}

pub struct PostgresSourceBatchIterator<'c, 'i>
where 'c: 'i
{
    batch_size: u64,
    connection: &'i Connection,
    result_iterator: postgres::rows::Iter<'i>,
    source_connection: &'i PostgresSourceConnection<'c>
}

impl PostgresSource {
    pub fn init(postgres_options: &PostgresSourceOptions) -> PostgresSource {
        PostgresSource { options: postgres_options.to_owned() }
    }
}


impl <'c, 'i> DataSource<'c, 'i, PostgresSourceConnection<'c>, PostgresSourceBatchIterator<'c, 'i>> for PostgresSource
where 'c: 'i,
{
    fn connect(&'c self) -> PostgresSourceConnection
    {
        
        let connection =  establish_postgres_connection(&self.options);
        if !self.options.init.is_empty() {
            for sql in self.options.init.iter() {
                connection.execute(sql, &[]).unwrap();
            }
        }

        let results = connection.query(&self.options.query, &[]).unwrap();
        PostgresSourceConnection {
            connection,
            source: &self,
            results,
        }
    }

    fn get_type_name(&self) -> String {"postgres".to_string()}
    fn get_name(&self) -> String { "postgres".to_string() }


}

impl <'c, 'i>DataSourceConnection<'i, PostgresSourceBatchIterator<'c, 'i>> for PostgresSourceConnection<'c>
{
    fn batch_iterator(&'i self, batch_size: u64) -> PostgresSourceBatchIterator<'c, 'i>
    {
        PostgresSourceBatchIterator {
            batch_size,
            connection: & self.connection,
            result_iterator: self.results.iter(),
            source_connection: &self,
        }
    }
}

pub fn postgres_to_row(column_info: &[(String,  postgres::types::Type)], postgres_row: &postgres::rows::Row) -> Row {
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


impl <'c, 'i>DataSourceBatchIterator for PostgresSourceBatchIterator<'c, 'i>
{
    fn get_column_info(&self) -> Vec<ColumnInfo> {
       let mut result = vec![];
        for column in self.source_connection.results.columns().iter() {
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
        result
    }

    fn get_count(&self) -> Option<u64> {
        Some(self.source_connection.results.len() as u64)
    }
 
    fn next(&mut self) -> Option<Vec<Row>>
    {
        let rows :Vec<Row> = self.result_iterator
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
                result
            }).collect();

        if !rows.is_empty() {
            Some(rows)
        } else {
            None
        }
    }
}
