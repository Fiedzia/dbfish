use std::path::PathBuf;

use chrono::{DateTime, Utc};
use humantime;
use indicatif::ProgressBar;

use crate::commands::ApplicationArguments;
use crate::commands::common::SourceConfigCommand;
use crate::config;
use crate::definitions::{DataSource, DataDestination, DataSourceConnection, DataSourceBatchIterator};
use crate::destinations::Destination;

use crate::sources::Source;

#[cfg(feature = "use_mysql")]
use crate::{commands::common::MysqlConfigOptions, sources::mysql::MysqlSource};
#[cfg(feature = "use_spsheet")]
use crate::destinations::ods_xlsx::{SpreadSheetDestination, SpreadSheetFormat};
#[cfg(feature = "use_postgres")]
use crate::{commands::common::PostgresConfigOptions, sources::postgres::PostgresSource};
#[cfg(feature = "use_csv")]
use crate::destinations::csv::CSVDestination;
#[cfg(feature = "use_html")]
use crate::destinations::html::HTMLDestination;
#[cfg(feature = "use_json")]
use crate::destinations::json::JSONDestination;
#[cfg(feature = "use_sqlite")]
use crate::{commands::common::SqliteConfigOptions, destinations::sqlite::SqliteDestination, sources::sqlite::SqliteSource};
#[cfg(feature = "use_text")]
use crate::destinations::text::TextDestination;
#[cfg(feature = "use_text")]
use crate::destinations::text_vertical::TextVerticalDestination;


pub fn export (args: &ApplicationArguments, export_command: &ExportCommand) {

    let time_start: DateTime<Utc> = Utc::now();
    let (source, mut destination) = match export_command.source {
        #[cfg(feature = "use_mysql")]
        SourceCommandWrapper(SourceCommand::Mysql(ref mysql_options)) => {
            let source: Source  = Source::Mysql(MysqlSource::init(&mysql_options));
            let destination: Destination = match &mysql_options.destination {

                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => Destination::CSV(CSVDestination::init(&csv_options)),
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => Destination::HTML(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => Destination::JSON(JSONDestination::init(&args, &json_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Destination::Sqlite(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Destination::SpreadSheet(SpreadSheetDestination::init(&spreadsheet_options, SpreadSheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Destination::SpreadSheet(SpreadSheetDestination::init(&spreadsheet_options, SpreadSheetFormat::XLSX)),
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => Destination::Text(TextDestination::init(&args, &text_options)),
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => Destination::TextVertical(TextVerticalDestination::init(&args, &text_vertical_options)),

            };
            (source, destination)
        },

        #[cfg(feature = "use_postgres")]
        SourceCommandWrapper(SourceCommand::Postgres(ref postgres_options)) => {
            let source: Source  = Source::Postgres(PostgresSource::init(&postgres_options));
            let destination: Destination = match &postgres_options.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => Destination::CSV(CSVDestination::init(&csv_options)),
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => Destination::HTML(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => Destination::JSON(JSONDestination::init(&args, &json_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Destination::Sqlite(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Destination::SpreadSheet(SpreadSheetDestination::init(&spreadsheet_options, SpreadSheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Destination::SpreadSheet(SpreadSheetDestination::init(&spreadsheet_options, SpreadSheetFormat::XLSX)),
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => Destination::Text(TextDestination::init(&args, &text_options)),
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => Destination::TextVertical(TextVerticalDestination::init(&args, &text_vertical_options)),
            };
            (source, destination)
        },
        #[cfg(feature = "use_sqlite")]
        SourceCommandWrapper(SourceCommand::Sqlite(ref sqlite_options)) => {
            let source: Source = Source::Sqlite(SqliteSource::init(&sqlite_options));
            let destination: Destination = match &sqlite_options.destination {
                #[cfg(feature = "use_csv")]
                DestinationCommand::CSV(csv_options) => Destination::CSV(CSVDestination::init(&csv_options)),
                #[cfg(feature = "use_html")]
                DestinationCommand::HTML(html_options) => Destination::HTML(HTMLDestination::init(&html_options)),
                #[cfg(feature = "use_json")]
                DestinationCommand::JSON(json_options) => Destination::JSON(JSONDestination::init(&args, &json_options)),
                #[cfg(feature = "use_sqlite")]
                DestinationCommand::Sqlite(sqlite_options) => Destination::Sqlite(SqliteDestination::init(&sqlite_options)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::ODS(spreadsheet_options) => Destination::SpreadSheet(SpreadSheetDestination::init(&spreadsheet_options, SpreadSheetFormat::ODS)),
                #[cfg(feature = "use_spsheet")]
                DestinationCommand::XLSX(spreadsheet_options) => Destination::SpreadSheet(SpreadSheetDestination::init(&spreadsheet_options, SpreadSheetFormat::XLSX)),
                #[cfg(feature = "use_text")]
                DestinationCommand::Text(text_options) => Destination::Text(TextDestination::init(&args, &text_options)),
                #[cfg(feature = "use_text")]
                DestinationCommand::TextVertical(text_vertical_options) => Destination::TextVertical(TextVerticalDestination::init(&args, &text_vertical_options)),
            };
            (source, destination)
        },
    };
    destination.prepare();
    let source_connection = source.connect();
    let mut it = source_connection.batch_iterator(export_command.batch_size);
    destination.prepare_for_results(&it);
    let mut processed = 0;
    let progress_bar = if args.verbose {
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



#[derive(StructOpt)]
pub struct ExportCommand {
    #[structopt(short = "b", long = "batch-size", help = "batch size", default_value="500")]
    batch_size: u64,
    #[structopt(subcommand)]
    pub source: SourceCommandWrapper,
}

#[derive(Clone, Debug, StructOpt)]
pub enum SourceCommand {
    #[cfg(feature = "use_mysql")]
    #[structopt(name = "mysql", about="mysql")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Mysql(MysqlSourceOptions),
    #[cfg(feature = "use_postgres")]
    #[structopt(name = "postgres", about="postgres")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Postgres(PostgresSourceOptions),
    #[cfg(feature = "use_sqlite")]
    #[structopt(name = "sqlite", about="sqlite")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Sqlite(SqliteSourceOptions),
}

pub struct SourceCommandWrapper (pub SourceCommand);

impl SourceCommandWrapper {

    pub fn augment_clap<'a, 'b>(
            app: ::structopt::clap::App<'a, 'b>,
        ) -> ::structopt::clap::App<'a, 'b> {
        let mut app = SourceCommand::augment_clap(app);
        let sources = config::get_sources_list();

        for (source_name, source_config_command) in sources {

            match source_config_command.get_type_name().as_str() {

                #[cfg(feature = "use_mysql")]
                "mysql" => {
                    let subcmd = MysqlSourceOptions::augment_clap(
                        structopt::clap::SubCommand::with_name(&source_name)
                            .setting(structopt::clap::AppSettings::ColoredHelp)
                    );
                    app = app.subcommand(subcmd);
                },
                #[cfg(feature = "use_postgres")]
                "postgres" => {
                    let subcmd = PostgresSourceOptions::augment_clap(
                        structopt::clap::SubCommand::with_name(&source_name)
                            .setting(structopt::clap::AppSettings::ColoredHelp)
                    );
                    app = app.subcommand(subcmd);
                },
                #[cfg(feature = "use_sqlite")]
                "sqlite" => {
                    let subcmd = SqliteSourceOptions::augment_clap(
                        structopt::clap::SubCommand::with_name(&source_name)
                            .setting(structopt::clap::AppSettings::ColoredHelp)
                    );
                    app = app.subcommand(subcmd);
                },

                unknown => { eprintln!("unknown database type: {} for source: {}", unknown, source_config_command.get_type_name());}
            }
        }
        app
    }

    pub fn from_subcommand<'a, 'b> (
        sub: (&'b str, Option<&'b ::structopt::clap::ArgMatches<'a>>),
    ) -> Option<Self> {

        let result = SourceCommand::from_subcommand(sub);
        //no default sources were matching subcommand, it might be user defined source
        if result.is_none() {

            if let (source_name, Some(matches)) = sub {
                match config::USER_DEFINED_SOURCES.get(source_name) {
                    None => None,
                    Some(source) => match source {
                        #[cfg(feature = "use_mysql")]
                        SourceConfigCommand::Mysql(mysql_config_options) => {

                            let mut mysql_options = <MysqlSourceOptions as ::structopt::StructOpt>
                                ::from_clap(matches);
                            mysql_options.update_from_config_options(mysql_config_options);

                            Some(
                                SourceCommandWrapper(
                                    SourceCommand::Mysql(mysql_options)
                                )
                            )
                        },
                        #[cfg(feature = "use_postgres")]
                        SourceConfigCommand::Postgres(postgres_config_options) => {

                            let mut postgres_options = <PostgresSourceOptions as ::structopt::StructOpt>
                                ::from_clap(matches);
                            postgres_options.update_from_config_options(postgres_config_options);

                            Some(
                                SourceCommandWrapper(
                                    SourceCommand::Postgres(postgres_options)
                                )
                            )
                        },
                        #[cfg(feature = "use_sqlite")]
                        SourceConfigCommand::Sqlite(sqlite_config_options) => {

                            let mut sqlite_options = <SqliteSourceOptions as ::structopt::StructOpt>
                                ::from_clap(matches);
                            sqlite_options.update_from_config_options(sqlite_config_options);

                            Some(
                                SourceCommandWrapper(
                                    SourceCommand::Sqlite(sqlite_options)
                                )
                            )
                        },
                    }
                }
            } else {
                None
            }
        } else {
            result.map(SourceCommandWrapper)
        }
    }

}

#[derive(Clone, Debug, StructOpt)]
pub enum DestinationCommand {
    #[cfg(feature = "use_csv")]
    #[structopt(name = "csv", about="CSV")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    CSV(CSVDestinationOptions),
    #[cfg(feature = "use_spsheet")]
    #[structopt(name = "ods", about="ODS spreadsheet")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    ODS(SpreadSheetDestinationOptions),
    #[cfg(feature = "use_spsheet")]
    #[structopt(name = "xlsx", about="XLSX spreadsheet")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    XLSX(SpreadSheetDestinationOptions),
    #[cfg(feature = "use_sqlite")]
    #[structopt(name = "sqlite", about="Sqlite file")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Sqlite(SqliteDestinationOptions),
    #[cfg(feature = "use_text")]
    #[structopt(name = "text", about="Text")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Text(TextDestinationOptions),
    #[cfg(feature = "use_text")]
    #[structopt(name = "text-vertical", about="Text (columns displayed vertically)")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    TextVertical(TextVerticalDestinationOptions),
    #[cfg(feature = "use_html")]
    #[structopt(name = "html", about="HTML")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    HTML(HTMLDestinationOptions),
    #[cfg(feature = "use_json")]
    #[structopt(name = "json", about="JSON")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    JSON(JSONDestinationOptions),
}


#[cfg(feature = "use_sqlite")]
#[derive(Clone, Debug, StructOpt)]
pub struct SqliteDestinationOptions {
    #[structopt(help = "sqlite filename")]
    pub filename: String,
    #[structopt(help = "sqlite table name", default_value="data")]
    pub table: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[cfg(feature = "use_csv")]
#[derive(Clone, Debug, StructOpt)]
pub struct CSVDestinationOptions {
    #[structopt(help = "csv filename. Use '-' for stdout")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[cfg(feature = "use_spsheet")]
#[derive(Clone, Debug, StructOpt)]
pub struct SpreadSheetDestinationOptions {
    #[structopt(help = "spreadsheet filename")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[cfg(feature = "use_text")]
#[derive(Clone, Debug, StructOpt)]
pub struct TextDestinationOptions {
    #[structopt(help = "text filename")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[cfg(feature = "use_text")]
#[derive(Clone, Debug, StructOpt)]
pub struct TextVerticalDestinationOptions {
    #[structopt(help = "filename")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
    #[structopt(short = "s", long = "sort-columns", help = "sort columns by name")]
    pub sort_columns: bool,
}

#[cfg(feature = "use_html")]
#[derive(Clone, Debug, StructOpt)]
pub struct HTMLDestinationOptions {
    #[structopt(help = "html filename")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
    #[structopt(long = "title", help = "html page title")]
    pub title: Option<String>,
}


#[cfg(feature = "use_json")]
#[derive(Clone, Debug, StructOpt)]
pub struct JSONDestinationOptions {
    #[structopt(help = "json filename")]
    pub filename: String,
    #[structopt(short = "c", long = "compact", help = "Do not indent json content")]
    pub compact: bool,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
    #[structopt(short = "i", long = "indent", help = "amount of spaces for indentation", default_value="4")]
    pub indent: u16,
}


#[cfg(feature = "use_mysql")]
#[derive(Clone, Debug, StructOpt)]
pub struct MysqlSourceOptions {
    #[structopt(short = "h", long = "host", help = "hostname")]
    pub host: Option<String>,
    #[structopt(short = "u", long = "user", help = "username")]
    pub user: Option<String>,
    #[structopt(short = "p", long = "password", help = "password")]
    pub password: Option<String>,
    #[structopt(short = "P", long = "port", help = "port")]
    pub port: Option<u16>,
    #[structopt(short = "S", long = "socket", help = "socket")]
    pub socket: Option<String>,
    #[structopt(short = "D", long = "database", help = "database name")]
    pub database: Option<String>,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[structopt(short = "q", long = "query", help = "sql query", required_unless = "query_file")]
    pub query: Option<String>,
    #[structopt(short = "f", long = "query-file", parse(from_os_str), help = "read sql query from file")]
    pub query_file: Option<PathBuf>,
    #[structopt(short = "c", long = "count", help = "run another query to get row count first")]
    pub count: bool,
    #[structopt(long = "timeout", help = "connect/read/write timeout in seconds")]
    pub timeout: Option<u64>,
    #[structopt(subcommand)]
    pub destination: DestinationCommand
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
#[derive(Clone, Debug, StructOpt)]
pub struct PostgresSourceOptions {
    #[structopt(short = "h", long = "host", help = "hostname")]
    pub host: Option<String>,
    #[structopt(short = "u", long = "user", help = "username")]
    pub user: Option<String>,
    #[structopt(short = "p", long = "password", help = "password")]
    pub password: Option<String>,
    #[structopt(short = "P", long = "port", help = "port")]
    pub port: Option<u16>,
    #[structopt(short = "D", long = "database", help = "database name")]
    pub database: Option<String>,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[structopt(long = "timeout", help = "connect timeout in seconds")]
    pub timeout: Option<u64>,
    #[structopt(short = "q", long = "query", help = "sql query", required_unless="query_file")]
    pub query: Option<String>,
    #[structopt(short = "f", long = "query-file", parse(from_os_str), help = "read sql query from file")]
    pub query_file: Option<PathBuf>,
    #[structopt(short = "c", long = "count", help = "run another query to get row count first")]
    pub count: bool,
    #[structopt(subcommand)]
    pub destination: DestinationCommand
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
#[derive(Clone, Debug, StructOpt)]
pub struct SqliteSourceOptions {
    #[structopt(help = "sqlite filename")]
    pub filename: Option<String>,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[structopt(short = "q", long = "query", help = "sql query", required_unless = "query_file")]
    pub query: Option<String>,
    #[structopt(short = "f", long = "query-file", parse(from_os_str), help = "read sql query from file")]
    pub query_file: Option<PathBuf>,
    #[structopt(short = "c", long = "count", help = "run another query to get row count first")]
    pub count: bool,
    #[structopt(subcommand)]
    pub destination: DestinationCommand
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
