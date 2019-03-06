//TODO: basic data types
//TODO: error handling
//TODO: use generators when available in stable Rust. Or figure out how to iterator into closure.

use std::sync::mpsc::sync_channel;
use std::thread;

//use chrono;
use fallible_iterator::FallibleIterator;
use postgres::{self, Connection, TlsMode, types::Kind};


use crate::commands::PostgresSourceOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource};

pub fn get_postgres_url(postgres_options: &PostgresSourceOptions) -> String {
    //TODO: encode parameter values for url
    format!(
        "postgres://{user}{password}@{hostname}{port}/{database}",
        user=match &postgres_options.user { None => "", Some(v) => v},
        hostname=match &postgres_options.host { None => "", Some(v) => v},
        password=match &postgres_options.password {None => "".to_string(), Some(p) => format!(":{}", p)},
        port=match &postgres_options.port { None => "".to_string(), Some(p) => format!(":{}", p)},
        database=match &postgres_options.database { None => "".to_string(), Some(d) => format!("/{}", d)},
    )
}


pub fn establish_connection(postgres_options: &PostgresSourceOptions) -> Connection {

    let database_url = get_postgres_url(&postgres_options);
    let conn = Connection::connect(database_url, TlsMode::None).unwrap();

    if postgres_options.init.len() > 0 {
        for sql in postgres_options.init.iter() {
            conn.execute(sql, &[]).unwrap();
        }
    }
    conn
}

enum Message {
    Columns(Vec<(String, postgres::types::Type)>),
    Row(Option<Row>)
}

pub struct PostgresSource {
    columns: Vec<(String, postgres::types::Type)>, //(name, type)
    count: Option<u64>,
    receiving_channel:  std::sync::mpsc::Receiver<Message>,
    no_more_results: bool,
}

impl PostgresSource {
    pub fn init(postgres_options: &PostgresSourceOptions) -> PostgresSource {

        let (sender, receiver) = sync_channel(100);

        let conn = establish_connection(&postgres_options);
        let count: Option<u64> = match postgres_options.count {
            true => {
                let count_query = format!("select count(*) from ({}) q", postgres_options.query);
                let count_value:i64 = conn.query(count_query.as_str(), &[]).unwrap().get(0).get(0);
                Some(count_value as u64)
            },
            false => None,
        };


        let postgres_options_copy = postgres_options.to_owned();
        thread::spawn(move|| {

            let conn = establish_connection(&postgres_options_copy);
            let stmt = conn.prepare(&postgres_options_copy.query).unwrap();
            let trans = conn.transaction().unwrap();
            let mut rows = stmt.lazy_query(&trans, &[], 100).unwrap();
            let columns: Vec<(String,  postgres::types::Type)> = rows
                .columns()
                .iter()
                .map(|c| {(c.name().to_string(), c.type_().clone())})
                .collect();
            sender.send(Message::Columns(columns.clone())).unwrap();
            while let Some(row) = rows.next().unwrap() {
                sender.send(Message::Row(Some(PostgresSource::postgres_to_row(&columns, &row)))).unwrap();
            };
            sender.send(Message::Row(None))
        });

        let columns = match receiver.recv().unwrap() {
            Message::Columns(columns) => columns,
            Message::Row(_) => panic!("postgres: missing column info")
        };

        PostgresSource {
            count,
            columns,
            receiving_channel: receiver,
            no_more_results: false,
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
}

impl DataSource for PostgresSource {

    fn get_name(&self) -> String { "postgresql".to_string() }

    fn get_column_info(&self) -> Vec<ColumnInfo> {
        let mut result = vec![];
        for (name, type_) in self.columns.iter() {
            println!("name={}; type_name={}; kind={:?};", name, type_.name(), type_.kind() );
            match (type_.kind(), type_.name()) {
                (Kind::Simple, "int4") => result.push(ColumnInfo{name: name.to_string(), data_type: ColumnType::I32}),
                (Kind::Simple, "int8") => result.push(ColumnInfo{name: name.to_string(), data_type: ColumnType::I64}),
                (Kind::Simple, "float4") => result.push(ColumnInfo{name: name.to_string(), data_type: ColumnType::F32}),
                (Kind::Simple, "float8") => result.push(ColumnInfo{name: name.to_string(), data_type: ColumnType::F64}),
                (Kind::Simple, "text") => result.push(ColumnInfo{name: name.to_string(), data_type: ColumnType::String}),
                _ => panic!("postgres: unsupported type: {:?}", type_ )
            };
        }
        result
    }

    fn get_count(&self) -> Option<u64> { self.count }

    fn get_rows(&mut self, count: u32) -> Option<Vec<Row>> {
        if self.no_more_results {
            return None;
        };

        let mut results: Vec<Row> = vec![];
        let mut done:bool = results.len() >= count as usize; //in case of count being 0
        while !done {
            match self.receiving_channel.recv().unwrap() {
                Message::Row(potential_row) => match potential_row {
                    Some(row) => { results.push(row); },
                    None => {
                        self.no_more_results = true;
                        done = true;
                    }
                },
                _ => panic!("postgres: expected row message, got something else", ),
            };
            if results.len() == count as usize {
                done = true;
            }
        }

        Some(results)
    }
}


