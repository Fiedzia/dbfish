use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;



use chrono;
use sqlite;

use crate::commands::SqliteSourceOptions;
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource};

enum Message {
    Columns(Vec<ColumnInfo>),
    Row(Option<Row>)
}

pub struct SqliteSource {
    columns: Vec<ColumnInfo>,
    count: Option<u64>,
    receiving_channel:  std::sync::mpsc::Receiver<Message>,
    no_more_results: bool,
}

impl SqliteSource {
    pub fn init(sqlite_options: &SqliteSourceOptions) -> SqliteSource {

        let (sender, receiver) = sync_channel(100);
        let connection = sqlite::Connection::open(
            &sqlite_options.filename.clone().unwrap_or(":memory:".to_string())
        ).unwrap();
        if sqlite_options.init.len() > 0 {
            for sql in sqlite_options.init.iter() {
                connection.execute(sql).unwrap();
            }
        }

        let mut count: Option<u64> = None;
        if sqlite_options.count {
            let count_query = format!("select count(*) from ({})", sqlite_options.query);
            connection.iterate(count_query, |row| { count = Some( row[0].1.unwrap().parse::<u64>().unwrap() ); false } ).unwrap();
        };

        let sqlite_options_copy = sqlite_options.to_owned();
        thread::spawn(move|| {
            let connection = sqlite::Connection::open(&sqlite_options_copy.filename.unwrap_or(":memory:".to_string())).unwrap();
            if sqlite_options_copy.init.len() > 0 {
                for sql in sqlite_options_copy.init.iter() {
                    connection.execute(sql).unwrap();
                }
            }

            let mut statement = connection.prepare(&sqlite_options_copy.query).unwrap();
            let columns:Vec<ColumnInfo> = (0..statement.count()).map(|idx| {
                ColumnInfo {
                    name: statement.name(idx).to_owned(),
                    data_type: match statement.kind(idx) {
                        sqlite::Type::Binary => ColumnType::Bytes,
                        sqlite::Type::Float => ColumnType::F64,
                        sqlite::Type::Integer => ColumnType::I64,
                        sqlite::Type::String => ColumnType::Bytes,
                        sqlite::Type::Null   => ColumnType::None,
                    },
                }
            }).collect();

            sender.send(Message::Columns(columns.clone())).unwrap();
            while let sqlite::State::Row = statement.next().unwrap() {
                sender.send(Message::Row(Some((0..statement.count()).map(|idx| {
                    let value: sqlite::Value = statement.read(idx).unwrap();
                    match value {
                        sqlite::Value::String(s) => Value::String(s),
                        sqlite::Value::Binary(b) => Value::Bytes(b),
                        sqlite::Value::Float(f) => Value::F64(f),
                        sqlite::Value::Integer(i) => Value::I64(i),
                        sqlite::Value::Null => Value::None,
                    }
                }).collect()  )));
            }

            sender.send(Message::Row(None));
        });

        let columns = match receiver.recv().unwrap() {
            Message::Columns(columns) => columns,
            Message::Row(_) => panic!("sqlite: missing column info")
        };

        SqliteSource {
            columns,
            count,
            receiving_channel: receiver,
            no_more_results: false,
        }
    }
 }

impl DataSource for SqliteSource {

    fn get_name(&self) -> String { "sqlite".to_string() }

    fn get_column_info(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
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
                _ => panic!("sqlite: expected row message, got something else", ),
            };
            if results.len() == count as usize {
                done = true;
            }
        }

        Some(results)

    }
}


