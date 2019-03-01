use chrono::{DateTime, Utc};
use humantime;
use indicatif::ProgressBar;


use crate::commands::{ApplicationArguments, ExportCommand, SourceCommand,  DestinationCommand};
use crate::definitions::{DataSource, DataDestination};
#[cfg(feature = "use_mysql")]
use crate::sources::mysql::MysqlSource;
#[cfg(feature = "use_spsheet")]
use crate::destinations::ods_xlsx::{SpreadsheetDestination, SpreadsheetFormat};
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::PostgresSource;
use crate::destinations::csv::CSVDestination;
use crate::destinations::html::HTMLDestination;
#[cfg(feature = "use_sqlite")]
use crate::destinations::sqlite::SqliteDestination;
use crate::destinations::text::TextDestination;
use crate::destinations::text_vertical::TextVerticalDestination;


pub fn export (args: &ApplicationArguments, export_command: &ExportCommand) {

    let time_start: DateTime<Utc> = Utc::now();
    let (mut source, mut destination) = match export_command.source {
        #[cfg(feature = "use_mysql")]
        SourceCommand::Mysql(ref mysql_options) => {
            let source: Box<dyn DataSource>  = Box::new(MysqlSource::init(&mysql_options));
            let destination: Box<dyn DataDestination> = match &mysql_options.destination {
                DestinationCommand::CSV(csv_options) => Box::new(CSVDestination::init(&csv_options)),
                DestinationCommand::HTML(html_options) => Box::new(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Box::new(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::XLSX)),
                DestinationCommand::Text(text_options) => Box::new(TextDestination::init(&text_options)),
                DestinationCommand::TextVertical(text_vertical_options) => Box::new(TextVerticalDestination::init(&text_vertical_options)),
            };
            (source, destination)
        },
        #[cfg(feature = "use_postgres")]
        SourceCommand::Postgres(ref postgres_options) => {
            let source: Box<dyn DataSource> = Box::new(PostgresSource::init(&postgres_options));
            let destination: Box<dyn DataDestination> = match &postgres_options.destination {
                DestinationCommand::CSV(csv_options) => Box::new(CSVDestination::init(&csv_options)),
                DestinationCommand::HTML(html_options) => Box::new(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Box::new(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::XLSX)),
                DestinationCommand::Text(text_options) => Box::new(TextDestination::init(&text_options)),
                DestinationCommand::TextVertical(text_vertical_options) => Box::new(TextVerticalDestination::init(&text_vertical_options)),
            };
            (source, destination)
        },

    };
    destination.prepare(&*source);
    let mut processed = 0;
    let progress_bar = match args.verbose {
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

    loop {
        let rows_option = source.get_rows(export_command.batch_size);
        match rows_option {
            Some(rows) => {
                destination.add_rows(&rows);
                processed += rows.len();
                if let Some(ref pb) = progress_bar {
                    pb.inc(rows.len() as u64);
                }
            },
            None => { break; }
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
