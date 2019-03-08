use chrono::{DateTime, Utc};
use humantime;
use indicatif::ProgressBar;


use crate::commands::{ApplicationArguments, ExportCommand, SourceCommand,  SourceCommandWrapper, DestinationCommand};
use crate::definitions::{DataSource, DataDestination, DataSourceConnection, DataSourceBatchIterator};
use crate::destinations::Destination;

use crate::sources::Source;

#[cfg(feature = "use_mysql")]
use crate::sources::mysql::MysqlSource;
#[cfg(feature = "use_spsheet")]
use crate::destinations::ods_xlsx::{SpreadsheetDestination, SpreadsheetFormat};
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::PostgresSource;
#[cfg(feature = "use_csv")]
use crate::destinations::csv::CSVDestination;
#[cfg(feature = "use_html")]
use crate::destinations::html::HTMLDestination;
#[cfg(feature = "use_json")]
use crate::destinations::json::JSONDestination;
#[cfg(feature = "use_sqlite")]
use crate::destinations::sqlite::SqliteDestination;
#[cfg(feature = "use_sqlite")]
use crate::sources::sqlite::SqliteSource;
#[cfg(feature = "use_text")]
use crate::destinations::text::TextDestination;
#[cfg(feature = "use_text")]
use crate::destinations::text_vertical::TextVerticalDestination;


pub fn export (args: &ApplicationArguments, export_command: &ExportCommand) {

    let time_start: DateTime<Utc> = Utc::now();
    let (mut source, mut destination) = match export_command.source {
        #[cfg(feature = "use_mysql")]
        SourceCommandWrapper(SourceCommand::Mysql(ref mysql_options)) => {
            let source: Source  = Source::Mysql(MysqlSource::init(&mysql_options));
            let destination: Box<dyn DataDestination> = match &mysql_options.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => Box::new(CSVDestination::init(&csv_options)),
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => Box::new(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => Box::new(JSONDestination::init(&args, &json_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Box::new(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::XLSX)),
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => Box::new(TextDestination::init(&text_options)),
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => Box::new(TextVerticalDestination::init(&text_vertical_options)),
            };
            (source, destination)
        },

        #[cfg(feature = "use_postgres")]
        SourceCommandWrapper(SourceCommand::Postgres(ref postgres_options)) => {
            let source: Source  = Source::Postgres(PostgresSource::init(&postgres_options));
            let destination: Box<dyn DataDestination> = match &postgres_options.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => Box::new(CSVDestination::init(&csv_options)),
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => Box::new(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => Box::new(JSONDestination::init(&args, &json_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Box::new(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::XLSX)),
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => Box::new(TextDestination::init(&text_options)),
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => Box::new(TextVerticalDestination::init(&text_vertical_options)),
            };
            (source, destination)
        },
        #[cfg(feature = "use_sqlite")]
        SourceCommandWrapper(SourceCommand::Sqlite(ref sqlite_options)) => {
            let source: Source = Source::Sqlite(SqliteSource::init(&sqlite_options));
            let destination: Destination = match &sqlite_options.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => Box::new(CSVDestination::init(&csv_options)),
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => Box::new(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => Box::new(JSONDestination::init(&args, &json_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Destination::Sqlite(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Box::new(SpreadsheetDestination::init(&spreadsheet_options, SpreadsheetFormat::XLSX)),
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => Box::new(TextDestination::init(&text_options)),
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => Box::new(TextVerticalDestination::init(&text_vertical_options)),
            };
            (source, destination)
        },
    };
    //destination.prepare(&*source);
    let mut source_connection = source.connect();
    let mut it = source_connection.batch_iterator(export_command.batch_size);
    let mut processed = 0;
    let progress_bar = match args.verbose {
        true => {
            let pb = ProgressBar::new(
                match it.get_count() {
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
        let rows_option = it.next();
        match rows_option {
            Some(rows) => {
                //destination.add_rows(&rows);
                processed += rows.len();
                if let Some(ref pb) = progress_bar {
                    pb.inc(rows.len() as u64);
                }
            },
            None => { break; }
        }
    };
    //destination.close();
    let duration = Utc::now().signed_duration_since(time_start).to_std().unwrap();
    if let Some(ref pb) = progress_bar {
        pb.tick();
        pb.finish();
    };
    if args.verbose {
        println!("Done. Exported {} rows in {}", processed, humantime::format_duration(duration).to_string());
    }
}
