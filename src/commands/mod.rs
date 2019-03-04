use serde_derive::Serialize;
use toml;

pub mod export;
pub mod sources;

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
    Export(ExportCommand),
    #[structopt(name = "sources", about="manage data sources")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Sources(SourcesCommand),
   
}

#[derive(StructOpt)]
pub struct ExportCommand {
    #[structopt(short = "b", long = "batch-size", help = "batch size", default_value="500")]
    batch_size: u32,
    #[structopt(subcommand)]
    pub source: SourceCommand,
}


#[derive(Clone, StructOpt)]
pub enum SourceCommand {
    #[cfg(feature = "use_mysql")]
    #[structopt(name = "mysql", about="mysql")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Mysql(MysqlSourceOptions),
    #[cfg(feature = "use_postgres")]
    #[structopt(name = "postgres", about="postgres")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Postgres(PostgresSourceOptions),
}


#[derive(Clone, StructOpt)]
pub struct SourcesCommand {
    #[structopt(subcommand)]
    pub command: SourcesSubCommand,
}

#[derive(Clone, StructOpt)]
pub enum SourceConfigCommand {
    #[cfg(feature = "use_mysql")]
    #[structopt(name = "mysql", about="mysql")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Mysql(MysqlConfigOptions),
    #[cfg(feature = "use_postgres")]
    #[structopt(name = "postgres", about="postgres")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Postgres(PostgresConfigOptions),
}

impl SourceConfigCommand {

    pub fn get_name(&self) -> String {
        match self {
            #[cfg(feature = "use_mysql")]
            SourceConfigCommand::Mysql(_) => "mysql".to_string(),
            #[cfg(feature = "use_postgres")]
            SourceConfigCommand::Postgres(_) => "postgres".to_string(),
        }
    }

    pub fn to_toml(&self) -> toml::Value {

        match self {
            #[cfg(feature = "use_mysql")]
            SourceConfigCommand::Mysql(options) => toml::to_string(options).unwrap().parse::<toml::Value>().unwrap(),
            #[cfg(feature = "use_postgres")]
            SourceConfigCommand::Postgres(options) => toml::to_string(options).unwrap().parse::<toml::Value>().unwrap(),
        }
    }

}


#[derive(Clone, StructOpt)]
pub enum SourcesSubCommand {
    #[structopt(name = "add", about="add source")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Add(SourcesAddOptions),
    #[structopt(name = "delete", about="delete source")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Delete(SourcesDeleteOptions),
    #[structopt(name = "edit", about="edit source definition")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Edit(SourcesEditOptions),
    #[structopt(name = "list", about="list sources")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    List(SourcesListOptions),
}

#[derive(Clone, StructOpt)]
pub struct SourcesAddOptions {
    #[structopt(help = "source name")]
    pub name: String,
    #[structopt(subcommand)]
    pub source: SourceConfigCommand,
}

#[derive(Clone, StructOpt)]
pub struct SourcesDeleteOptions {
    #[structopt(help = "source name")]
    pub name: String,
}

#[derive(Clone, StructOpt)]
pub struct SourcesEditOptions {
    #[structopt(help = "source name")]
    pub name: String,
}

#[derive(Clone, StructOpt)]
pub struct SourcesListOptions {
}


#[derive(Clone, StructOpt)]
pub enum DestinationCommand {
    #[structopt(name = "csv", about="CSV")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    CSV(CSVDestinationOptions),
    #[cfg(feature = "use_spsheet")]
    #[structopt(name = "ods", about="ODS spreadsheet")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    ODS(SpreadsheetDestinationOptions),
    #[cfg(feature = "use_spsheet")]
    #[structopt(name = "xlsx", about="XLSX spreadsheet")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    XLSX(SpreadsheetDestinationOptions),
    #[cfg(feature = "use_sqlite")]
    #[structopt(name = "sqlite", about="Sqlite file")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Sqlite(SqliteDestinationOptions),
    #[structopt(name = "text", about="Text")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Text(TextDestinationOptions),
    #[structopt(name = "text-vertical", about="Text (columns displayed vertically)")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    TextVertical(TextVerticalDestinationOptions),
    #[structopt(name = "html", about="HTML")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    HTML(HTMLDestinationOptions),
    #[cfg(feature = "use_json")]
    #[structopt(name = "json", about="JSON")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    JSON(JSONDestinationOptions),
}


#[cfg(feature = "use_sqlite")]
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

#[cfg(feature = "use_spsheet")]
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
    #[structopt(short = "t", long = "truncate", help = "truncate data to given amount of graphemes")]
    pub truncate: Option<u64>,
    #[structopt(long = "title", help = "html page title")]
    pub title: Option<String>,
}


#[cfg(feature = "use_json")]
#[derive(Clone, StructOpt)]
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
#[derive(Clone, Serialize, StructOpt)]
pub struct MysqlConfigOptions {
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
    #[structopt(long = "timeout", help = "connect/read/write timout in seconds")]
    pub timeout: Option<u64>,
}

#[cfg(feature = "use_mysql")]
#[derive(Clone, StructOpt)]
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
    #[structopt(short = "q", long = "query", help = "sql query")]
    pub query: String,
    #[structopt(short = "c", long = "count", help = "run another query to get row count first")]
    pub count: bool,
    #[structopt(long = "timeout", help = "connect/read/write timout in seconds")]
    pub timeout: Option<u64>,
    #[structopt(subcommand)]
    pub destination: DestinationCommand
}

#[cfg(feature = "use_postgres")]
#[derive(Clone, Serialize, StructOpt)]
pub struct PostgresConfigOptions {
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
}

#[cfg(feature = "use_postgres")]
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

#[cfg(feature = "use_sqlite")]
#[derive(Clone, Serialize, StructOpt)]
pub struct SqliteConfigOptions {
    #[structopt(help = "sqlite filename")]
    pub filename: String,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
}

#[cfg(feature = "use_sqlite")]
#[derive(Clone, StructOpt)]
pub struct SqliteSourceOptions {
    #[structopt(help = "sqlite filename")]
    pub filename: String,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[structopt(short = "q", long = "query", help = "sql query")]
    pub query: String,
    #[structopt(short = "c", long = "count", help = "run another query to get row count first")]
    pub count: bool,
    #[structopt(subcommand)]
    pub destination: DestinationCommand
}
