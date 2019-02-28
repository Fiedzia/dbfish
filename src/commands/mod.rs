pub mod export;


#[derive(StructOpt)]
#[structopt(name = "export", about="Export data from database to sqlite/csv/text/html/json file.", after_help="Choose a command to run or to print help for, ie. synonyms --help")]
#[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
pub struct ApplicationArguments {
    #[structopt(short = "v", long = "verbose", help = "Be verbose")]
    pub verbose: bool,
    #[structopt(subcommand)]
    pub command: Command,
}



#[derive(StructOpt)]
pub enum Command {
    #[structopt(name = "export", about="export data")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Export(ExportCommand)
}

#[derive(StructOpt)]
pub struct ExportCommand {
    #[structopt(short = "b", long = "batch-size", help = "batch size", default_value="500")]
    batch_size: u32,
    #[structopt(subcommand)]
    pub source: SourceCommand,
}


#[derive(StructOpt)]
pub enum SourceCommand {
    #[cfg(feature = "mysql")]
    #[structopt(name = "mysql", about="mysql")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Mysql(MysqlSourceOptions),
    #[cfg(feature = "postgres")]
    #[structopt(name = "postgres", about="postgres")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Postgres(PostgresSourceOptions),
}


#[derive(Clone, StructOpt)]
pub enum DestinationCommand {
    #[structopt(name = "csv", about="CSV")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    CSV(CSVDestinationOptions),
    #[cfg(feature = "spsheet")]
    #[structopt(name = "ods", about="ODS spreadsheet")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    ODS(SpreadsheetDestinationOptions),
    #[cfg(feature = "spsheet")]
    #[structopt(name = "ods", about="ODS spreadsheet")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    XLSX(SpreadsheetDestinationOptions),
    #[cfg(feature = "sqlite")]
    #[structopt(name = "sqlite", about="Sqlite file")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Sqlite(SqliteDestinationOptions),
    #[structopt(name = "text", about="Text")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Text(TextDestinationOptions),
    #[structopt(name = "text-vertical", about="Text (columns displayed vertically)")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    TextVertical(TextVerticalDestinationOptions),
    /*#[structopt(name = "json", about="JSON")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    JSON(JSONDestinationOptions),
    #[structopt(name = "html", about="HTML")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    HTML(HTMLDestinationOptions),
    */
}


#[cfg(feature = "sqlite")]
#[derive(Clone, StructOpt)]
pub struct SqliteDestinationOptions {
    #[structopt(help = "sqlite filename")]
    pub filename: String,
    #[structopt(help = "sqlite table name", default_value="data")]
    pub table: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[derive(Clone, StructOpt)]
pub struct CSVDestinationOptions {
    #[structopt(help = "csv filename. Use '-' for stdout")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[cfg(feature = "spsheet")]
#[derive(Clone, StructOpt)]
pub struct SpreadsheetDestinationOptions {
    #[structopt(help = "spreadsheet filename")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[derive(Clone, StructOpt)]
pub struct TextDestinationOptions {
    #[structopt(help = "text filename")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
}

#[derive(Clone, StructOpt)]
pub struct TextVerticalDestinationOptions {
    #[structopt(help = "filename")]
    pub filename: String,
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
    #[structopt(short = "s", long = "sort-columns", help = "sort columns by name")]
    pub sort_columns: bool,
}

#[derive(Clone, StructOpt)]
pub struct HTMLDestinationOptions {
    #[structopt(help = "html filename")]
    pub filename: String,
    #[structopt(help = "html page title")]
    pub title: String,
}

#[derive(Clone, StructOpt)]
pub struct JSONDestinationOptions {
    #[structopt(help = "json filename")]
    pub filename: String,
    #[structopt(short = "c", long = "compact", help = "Do not indent json content")]
    pub compact: bool,
}

#[cfg(feature = "mysql")]
#[derive(Clone, StructOpt)]
pub struct MysqlSourceOptions {
    #[structopt(short = "h", long = "host", help = "hostname", default_value = "localhost")]
    pub host: String,
    #[structopt(short = "u", long = "user", help = "username")]
    pub user: String,
    #[structopt(short = "p", long = "password", help = "password")]
    pub password: Option<String>,
    #[structopt(short = "P", long = "port", help = "port", default_value = "3306")]
    pub port: u16,
    #[structopt(short = "D", long = "database", help = "database name")]
    pub database: Option<String>,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Option<String>,
    #[structopt(short = "q", long = "query", help = "sql query")]
    pub query: String,
    #[structopt(short = "c", long = "count", help = "run another query to get row count first")]
    pub count: bool,
    #[structopt(subcommand)]
    pub destination: DestinationCommand
}


#[cfg(feature = "postgres")]
#[derive(Clone, StructOpt)]
pub struct PostgresSourceOptions {
    #[structopt(short = "h", long = "host", help = "hostname", default_value = "localhost")]
    pub host: String,
    #[structopt(short = "u", long = "user", help = "username")]
    pub user: String,
    #[structopt(short = "p", long = "password", help = "password")]
    pub password: Option<String>,
    #[structopt(short = "P", long = "port", help = "port", default_value = "5432")]
    pub port: u16,
    #[structopt(short = "D", long = "database", help = "database name")]
    pub database: Option<String>,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Option<String>,
    #[structopt(short = "q", long = "query", help = "sql query")]
    pub query: String,
    #[structopt(short = "c", long = "count", help = "run another query to get row count first")]
    pub count: bool,
    #[structopt(subcommand)]
    pub destination: DestinationCommand
}
