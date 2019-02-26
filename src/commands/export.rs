use std;
use std::fs::File;

use chrono::{DateTime, Utc};
use humantime;
use indicatif::ProgressBar;


use crate::commands::{ApplicationArguments, ExportCommand, SourceCommand, MysqlSourceOptions, DestinationCommand, SqliteDestinationOptions, CSVDestinationOptions, TextDestinationOptions, TextVerticalDestinationOptions};
use crate::definitions::{ColumnType, Value, Row, ColumnInfo, DataSource, DataDestination};
use crate::sources::mysql::MysqlSource;
use crate::destinations::csv::CSVDestination;
use crate::destinations::sqlite::SqliteDestination;
use crate::destinations::text::TextDestination;
use crate::destinations::text_vertical::TextVerticalDestination;


pub fn export (args: &ApplicationArguments, export_command: &ExportCommand) {

    let time_start: DateTime<Utc> = Utc::now();
    let (mut source, mut destination) = match export_command.source {
        SourceCommand::Mysql(ref mysql_options) => {
            let mut source = MysqlSource::init(&mysql_options);
            let mut destination: Box<dyn DataDestination> = match &mysql_options.destination {
                DestinationCommand::Sqlite(sqlite_options) => Box::new(SqliteDestination::init(&sqlite_options)),
                DestinationCommand::CSV(csv_options) => Box::new(CSVDestination::init(&csv_options)),
                DestinationCommand::Text(text_options) => Box::new(TextDestination::init(&text_options)),
                DestinationCommand::TextVertical(text_vertical_options) => Box::new(TextVerticalDestination::init(&text_vertical_options)),
            };
            (source, destination)
        }
    };
    destination.prepare(&source);
    let mut done:bool = false;
    let mut processed = 0;
    let mut progress_bar = match args.verbose {
        true => {
            let pb = ProgressBar::new(
                match source.get_count() {
                    Some(c) => c,
                    None => 0
                }
            );
            pb.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template("Processed {pos:>7}/{len:7} rows in {elapsed_precise}")
            );
            Some(pb)
        },
        false => None
    };

    while !done {
        let rows_option = source.get_rows(export_command.batch_size);
        match rows_option {
            Some(rows) => {
                destination.add_rows(&rows);
                processed += rows.len();
                if let Some(ref pb) = progress_bar {
                    pb.inc(rows.len() as u64);
                }
            },
            None => { done = true; break; }
        }
    };
    destination.close();
    let duration = Utc::now().signed_duration_since(time_start).to_std().unwrap();
    if let Some(ref pb) = progress_bar {
        pb.tick();
        pb.finish();
    };
    if args.verbose {
        println!("Done. Exported {} rows in {}", processed, humantime::format_duration(duration).to_string());
    }
}