use std::path::PathBuf;

use chrono::{DateTime, Utc};
use clap::Parser;
use humantime;
use indicatif::ProgressBar;

use crate::commands::data_source::DataSourceCommand;
use crate::commands::ApplicationArguments;
use crate::definitions::{DataDestination, DataSource, DataSourceConnection};
use crate::destinations::Destination;
use crate::sources::Source;

#[cfg(feature = "use_csv")]
use crate::destinations::csv::CSVDestination;
use crate::destinations::debug::DebugDestination;
#[cfg(feature = "use_html")]
use crate::destinations::html::HTMLDestination;
#[cfg(feature = "use_json")]
use crate::destinations::json::JSONDestination;
#[cfg(feature = "use_ods")]
use crate::destinations::ods::SpreadSheetODSDestination;
#[cfg(feature = "use_text")]
use crate::destinations::text::TextDestination;
#[cfg(feature = "use_text")]
use crate::destinations::text_vertical::TextVerticalDestination;
#[cfg(feature = "use_xlsx")]
use crate::destinations::xlsx::SpreadSheetXLSXDestination;
#[cfg(feature = "use_mysql")]
use crate::{commands::common::MysqlConfigOptions, sources::mysql::MysqlSource};
#[cfg(feature = "use_postgres")]
use crate::{commands::common::PostgresConfigOptions, sources::postgres::PostgresSource};
#[cfg(feature = "use_sqlite")]
use crate::{
    commands::common::SqliteConfigOptions, destinations::sqlite::SqliteDestination,
    sources::sqlite::SqliteSource,
};

pub fn export(
    args: &ApplicationArguments,
    src: &DataSourceCommand,
    export_command: &ExportCommand,
) {
    let time_start: DateTime<Utc> = Utc::now();
    let (source, mut destination) = match src {
        #[cfg(feature = "use_mysql")]
        DataSourceCommand::Mysql(ref mysql_options) => {
            let source: Source = Source::Mysql(MysqlSource::init(&mysql_options));
            let destination: Destination = match &export_command.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => {
                    Destination::CSV(CSVDestination::init(&csv_options))
                }
                DestinationCommand::Debug(debug_options) => {
                    Destination::Debug(DebugDestination::init(&args, &debug_options))
                }
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => {
                    Destination::HTML(HTMLDestination::init(&html_options))
                }
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => {
                    Destination::JSON(JSONDestination::init(&args, &json_options))
                }
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => {
                    Destination::Sqlite(SqliteDestination::init(&sqlite_options))
                }
                #[cfg(feature = "use_ods")]
                DestinationCommand::ODS(options) => {
                    Destination::SpreadSheetODS(SpreadSheetODSDestination::init(options))
                }
                #[cfg(feature = "use_xlsx")]
                DestinationCommand::XLSX(options) => {
                    Destination::SpreadSheetXLSX(SpreadSheetXLSXDestination::init(options))
                }
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => {
                    Destination::Text(TextDestination::init(&args, &text_options))
                }
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => {
                    Destination::TextVertical(TextVerticalDestination::init(
                        &args,
                        &text_vertical_options,
                    ))
                }
            };
            (source, destination)
        }

        #[cfg(feature = "use_postgres")]
        DataSourceCommand::Postgres(ref postgres_options) => {
            let source: Source = Source::Postgres(PostgresSource::init(&postgres_options));
            let destination: Destination = match &export_command.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => {
                    Destination::CSV(CSVDestination::init(&csv_options))
                }
                DestinationCommand::Debug(debug_options) => {
                    Destination::Debug(DebugDestination::init(&args, &debug_options))
                }
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => {
                    Destination::HTML(HTMLDestination::init(&html_options))
                }
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => {
                    Destination::JSON(JSONDestination::init(&args, &json_options))
                }
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => {
                    Destination::Sqlite(SqliteDestination::init(&sqlite_options))
                }
                #[cfg(feature = "use_ods")]
                DestinationCommand::ODS(options) => {
                    Destination::SpreadSheetODS(SpreadSheetODSDestination::init(options))
                }
                #[cfg(feature = "use_xlsx")]
                DestinationCommand::XLSX(options) => {
                    Destination::SpreadSheetXLSX(SpreadSheetXLSXDestination::init(options))
                }
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => {
                    Destination::Text(TextDestination::init(&args, &text_options))
                }
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => {
                    Destination::TextVertical(TextVerticalDestination::init(
                        &args,
                        &text_vertical_options,
                    ))
                }
            };
            (source, destination)
        }
        #[cfg(feature = "use_sqlite")]
        DataSourceCommand::Sqlite(ref sqlite_options) => {
            let source: Source = Source::Sqlite(SqliteSource::init(&sqlite_options));
            let destination: Destination = match &export_command.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => {
                    Destination::CSV(CSVDestination::init(&csv_options))
                }
                DestinationCommand::Debug(debug_options) => {
                    Destination::Debug(DebugDestination::init(&args, &debug_options))
                }
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => {
                    Destination::HTML(HTMLDestination::init(&html_options))
                }
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => {
                    Destination::JSON(JSONDestination::init(&args, &json_options))
                }
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => {
                    Destination::Sqlite(SqliteDestination::init(&sqlite_options))
                }
                #[cfg(feature = "use_ods")]
                DestinationCommand::ODS(options) => {
                    Destination::SpreadSheetODS(SpreadSheetODSDestination::init(options))
                }
                #[cfg(feature = "use_xlsx")]
                DestinationCommand::XLSX(options) => {
                    Destination::SpreadSheetXLSX(SpreadSheetXLSXDestination::init(options))
                }
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => {
                    Destination::Text(TextDestination::init(&args, &text_options))
                }
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => {
                    Destination::TextVertical(TextVerticalDestination::init(
                        &args,
                        &text_vertical_options,
                    ))
                }
            };
            (source, destination)
        }
    };
    destination.prepare();
    let mut source_connection = source.connect();
    let mut it = source_connection.batch_iterator(export_command.batch_size);
    destination.prepare_for_results(&*it);
    let mut processed = 0;
    let progress_bar = if args.verbose {
        let pb = ProgressBar::new(match it.get_count() {
            Some(c) => c,
            None => 0,
        });
        pb.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("Processed {pos:>7}/{len:7} rows in {elapsed_precise}")
                .unwrap(),
        );
        Some(pb)
    } else {
        None
    };

    loop {
        let rows_option = it.next();
        match rows_option {
            Some(rows) => {
                destination.add_rows(&rows);
                processed += rows.len();
                if let Some(ref pb) = progress_bar {
                    pb.inc(rows.len() as u64);
                }
            }
            None => {
                break;
            }
        }
    }
    destination.close();
    let duration = Utc::now()
        .signed_duration_since(time_start)
        .to_std()
        .unwrap();
    if let Some(ref pb) = progress_bar {
        pb.tick();
        pb.finish();
    };
    if args.verbose {
        println!(
            "Done. Exported {} rows in {}",
            processed,
            humantime::format_duration(duration).to_string()
        );
    }
}

#[derive(Debug, Parser)]
pub struct ExportCommand {
    #[arg(
        short = 'b',
        long = "batch-size",
        help = "batch size",
        default_value = "500"
    )]
    batch_size: u64,
    #[command(subcommand)]
    pub destination: DestinationCommand,
}

#[derive(Clone, Debug, Parser)]
pub enum SourceCommand {
    #[cfg(feature = "use_mysql")]
    #[command(name = "mysql", about = "mysql")]
    Mysql(MysqlSourceOptions),
    #[cfg(feature = "use_postgres")]
    #[command(name = "postgres", about = "postgres")]
    Postgres(PostgresSourceOptions),
    #[cfg(feature = "use_sqlite")]
    #[command(name = "sqlite", about = "sqlite")]
    Sqlite(SqliteSourceOptions),
}

#[derive(Clone, Debug)]
pub struct SourceCommandWrapper(pub SourceCommand);

#[derive(Clone, Debug, Parser)]
pub enum DestinationCommand {
    #[cfg(feature = "use_csv")]
    #[command(name = "csv", about = "CSV")]
    CSV(CSVDestinationOptions),
    #[cfg(feature = "use_ods")]
    #[command(name = "ods", about = "ODS spreadsheet")]
    ODS(SpreadSheetDestinationOptions),
    #[cfg(feature = "use_xlsx")]
    #[command(name = "xlsx", about = "XLSX spreadsheet")]
    XLSX(SpreadSheetDestinationOptions),
    #[cfg(feature = "use_sqlite")]
    #[command(name = "sqlite", about = "Sqlite file")]
    Sqlite(SqliteDestinationOptions),
    #[cfg(feature = "use_text")]
    #[command(name = "text", about = "Text")]
    Text(TextDestinationOptions),
    #[cfg(feature = "use_text")]
    #[command(name = "text-vertical", about = "Text (columns displayed vertically)")]
    TextVertical(TextVerticalDestinationOptions),
    #[cfg(feature = "use_html")]
    #[command(name = "html", about = "HTML")]
    HTML(HTMLDestinationOptions),
    #[cfg(feature = "use_json")]
    #[command(name = "json", about = "JSON")]
    JSON(JSONDestinationOptions),
    #[command(name = "debug", about = "Debug output")]
    Debug(DebugDestinationOptions),
}

#[cfg(feature = "use_sqlite")]
#[derive(Clone, Debug, Parser)]
pub struct SqliteDestinationOptions {
    #[arg(help = "sqlite filename")]
    pub filename: String,
    #[arg(help = "sqlite table name", default_value = "data")]
    pub table: String,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
}

#[cfg(feature = "use_csv")]
#[derive(Clone, Debug, Parser)]
pub struct CSVDestinationOptions {
    #[arg(help = "csv filename. Use '-' for stdout")]
    pub filename: String,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
    #[arg(long = "no-headers", help = "skip header")]
    pub no_headers: bool,
}

#[derive(Clone, Debug, Parser)]
pub struct DebugDestinationOptions {
    #[arg(help = "output filename")]
    pub filename: String,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
}

#[cfg(any(feature = "use_ods", feature = "use_xlsx"))]
#[derive(Clone, Debug, Parser)]
pub struct SpreadSheetDestinationOptions {
    #[arg(help = "spreadsheet filename")]
    pub filename: String,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
}

#[cfg(feature = "use_text")]
#[derive(Clone, Debug, Parser)]
pub struct TextDestinationOptions {
    #[arg(help = "text filename")]
    pub filename: String,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
}

#[cfg(feature = "use_text")]
#[derive(Clone, Debug, Parser)]
pub struct TextVerticalDestinationOptions {
    #[arg(help = "filename")]
    pub filename: String,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
    #[arg(short = 's', long = "sort-columns", help = "sort columns by name")]
    pub sort_columns: bool,
}

#[cfg(feature = "use_html")]
#[derive(Clone, Debug, Parser)]
pub struct HTMLDestinationOptions {
    #[arg(help = "html filename")]
    pub filename: String,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
    #[arg(long = "title", help = "html page title")]
    pub title: Option<String>,
}

#[cfg(feature = "use_json")]
#[derive(Clone, Debug, Parser)]
pub struct JSONDestinationOptions {
    #[arg(help = "json filename")]
    pub filename: String,
    #[arg(short = 'c', long = "compact", help = "Do not indent json content")]
    pub compact: bool,
    #[arg(
        short = 't',
        long = "truncate",
        help = "truncate data to given amount of graphemes"
    )]
    pub truncate: Option<u64>,
    #[arg(
        short = 'i',
        long = "indent",
        help = "amount of spaces for indentation",
        default_value = "4"
    )]
    pub indent: u16,
}

#[cfg(feature = "use_mysql")]
#[derive(Clone, Debug, Parser)]
pub struct MysqlSourceOptions {
    #[arg(long = "host", help = "hostname")]
    pub host: Option<String>,
    #[arg(short = 'u', long = "user", help = "username")]
    pub user: Option<String>,
    #[arg(short = 'p', long = "password", help = "password")]
    pub password: Option<String>,
    #[arg(short = 'P', long = "port", help = "port")]
    pub port: Option<u16>,
    #[arg(short = 'S', long = "socket", help = "socket")]
    pub socket: Option<String>,
    #[arg(short = 'D', long = "database", help = "database name")]
    pub database: Option<String>,
    #[arg(short = 'i', long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[arg(short = 'q', long = "query", help = "sql query")]
    pub query: Option<String>,
    #[arg(short = 'f', long = "query-file", help = "read sql query from file")]
    pub query_file: Option<PathBuf>,
    #[arg(
        short = 'c',
        long = "count",
        help = "run another query to get row count first"
    )]
    pub count: bool,
    #[arg(
        short = 't',
        long = "timeout",
        help = "connect/read/write timeout in seconds"
    )]
    pub timeout: Option<u64>,
}

#[cfg(feature = "use_mysql")]
impl MysqlSourceOptions {
    //fill any values that are set in config options and not overriden
    pub fn update_from_config_options(&mut self, config_options: &MysqlConfigOptions) {
        if self.host.is_none() && config_options.host.is_some() {
            self.host = config_options.host.clone();
        }
        if self.port.is_none() && config_options.port.is_some() {
            self.port = config_options.port;
        }
        if self.user.is_none() && config_options.user.is_some() {
            self.user = config_options.user.clone();
        }
        if self.password.is_none() && config_options.password.is_some() {
            self.password = config_options.password.clone();
        }
        if self.socket.is_none() && config_options.socket.is_some() {
            self.socket = config_options.socket.clone();
        }
        if self.database.is_none() && config_options.database.is_some() {
            self.database = config_options.database.clone();
        }
        if self.init.is_empty() && !config_options.init.is_empty() {
            self.init.extend(config_options.init.iter().cloned());
        }
        if self.timeout.is_none() && config_options.timeout.is_some() {
            self.timeout = config_options.timeout;
        }
    }
}

#[cfg(feature = "use_postgres")]
#[derive(Clone, Debug, Parser)]
pub struct PostgresSourceOptions {
    #[arg(long = "host", help = "hostname")]
    pub host: Option<String>,
    #[arg(short = 'u', long = "user", help = "username")]
    pub user: Option<String>,
    #[arg(short = 'p', long = "password", help = "password")]
    pub password: Option<String>,
    #[arg(short = 'P', long = "port", help = "port")]
    pub port: Option<u16>,
    #[arg(short = 'D', long = "database", help = "database name")]
    pub database: Option<String>,
    #[arg(short = 'i', long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[arg(long = "timeout", help = "connect timeout in seconds")]
    pub timeout: Option<u64>,
    #[arg(short = 'q', long = "query", help = "sql query")]
    pub query: Option<String>,
    #[arg(short = 'f', long = "query-file", help = "read sql query from file")]
    pub query_file: Option<PathBuf>,
    #[arg(
        short = 'c',
        long = "count",
        help = "run another query to get row count first"
    )]
    pub count: bool,
}

#[cfg(feature = "use_postgres")]
impl PostgresSourceOptions {
    //fill any values that are set in config options and not overriden
    pub fn update_from_config_options(&mut self, config_options: &PostgresConfigOptions) {
        if self.host.is_none() && config_options.host.is_some() {
            self.host = config_options.host.clone();
        }
        if self.port.is_none() && config_options.port.is_some() {
            self.port = config_options.port;
        }
        if self.user.is_none() && config_options.user.is_some() {
            self.user = config_options.user.clone();
        }
        if self.password.is_none() && config_options.password.is_some() {
            self.password = config_options.password.clone();
        }
        if self.database.is_none() && config_options.database.is_some() {
            self.database = config_options.database.clone();
        }
        if self.init.is_empty() && !config_options.init.is_empty() {
            self.init.extend(config_options.init.iter().cloned());
        }
        if self.timeout.is_none() && config_options.timeout.is_some() {
            self.timeout = config_options.timeout;
        }
    }
}

#[cfg(feature = "use_sqlite")]
#[derive(Clone, Debug, Parser)]
pub struct SqliteSourceOptions {
    #[arg(help = "sqlite filename")]
    pub filename: Option<String>,
    #[arg(short = 'i', long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[arg(short = 'q', long = "query", help = "sql query")]
    pub query: Option<String>,
    #[arg(short = 'f', long = "query-file", help = "read sql query from file")]
    pub query_file: Option<PathBuf>,
    #[arg(
        short = 'c',
        long = "count",
        help = "run another query to get row count first"
    )]
    pub count: bool,
}

#[cfg(feature = "use_sqlite")]
impl SqliteSourceOptions {
    //fill any values that are set in config options and not overriden
    pub fn update_from_config_options(&mut self, config_options: &SqliteConfigOptions) {
        if self.filename.is_none() && config_options.filename.is_some() {
            self.filename = config_options.filename.clone();
        }
        if self.init.is_empty() && !config_options.init.is_empty() {
            self.init.extend(config_options.init.iter().cloned());
        }
    }
}
