use serde_derive::{Deserialize, Serialize};
use structopt;
use structopt::clap::{arg_enum, _clap_count_exprs};
use toml;

use crate::config;

pub mod export;
pub mod sources;


arg_enum! {
    #[derive(Debug, PartialEq)]
    pub enum UseColor {
        Yes,
        No,
        Auto
    }
}


#[derive(StructOpt)]
#[structopt(name = "export", about="Export data from database to sqlite/csv/text/html/json file.", after_help="Choose a command to run or to print help for, ie. synonyms --help")]
#[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
pub struct ApplicationArguments {
    #[structopt(short = "v", long = "verbose", help = "Be verbose")]
    pub verbose: bool,
    #[structopt(short = "c", long = "color", help = "use color", default_value="auto", raw(possible_values = "&UseColor::variants()", case_insensitive = "true"))]
    pub color: UseColor,
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
        if let None = result {

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

                       _ => None,
                    }
                }
            } else {
                None
            }
        } else {
            result.map(|v| SourceCommandWrapper(v))
        }
    }

}

#[derive(Clone, StructOpt)]
pub struct SourcesCommand {
    #[structopt(subcommand)]
    pub command: SourcesSubCommand,
}

#[derive(Clone, Debug, StructOpt)]
pub enum SourceConfigCommand {
    #[cfg(feature = "use_mysql")]
    #[structopt(name = "mysql", about="mysql")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Mysql(MysqlConfigOptions),
    #[cfg(feature = "use_postgres")]
    #[structopt(name = "postgres", about="postgres")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Postgres(PostgresConfigOptions),
    #[cfg(feature = "use_sqlite")]
    #[structopt(name = "sqlite", about="sqlite")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Sqlite(SqliteConfigOptions),
}

impl SourceConfigCommand {

    pub fn get_type_name(&self) -> String {
        match self {
            #[cfg(feature = "use_mysql")]
            SourceConfigCommand::Mysql(_) => "mysql".to_string(),
            #[cfg(feature = "use_postgres")]
            SourceConfigCommand::Postgres(_) => "postgres".to_string(),
            #[cfg(feature = "use_sqlite")]
            SourceConfigCommand::Sqlite(_) => "sqlite".to_string(),
        }
    }

    pub fn to_toml(&self) -> toml::Value {

        match self {
            #[cfg(feature = "use_mysql")]
            SourceConfigCommand::Mysql(options) =>
                toml::to_string(options).unwrap().parse::<toml::Value>().unwrap(),
            #[cfg(feature = "use_postgres")]
            SourceConfigCommand::Postgres(options)
                => toml::to_string(options).unwrap().parse::<toml::Value>().unwrap(),
            #[cfg(feature = "use_sqlite")]
            SourceConfigCommand::Sqlite(options)
                => toml::to_string(options).unwrap().parse::<toml::Value>().unwrap(),
        }
    }

    pub fn from_toml(toml_value: &toml::Value) -> Self {
        let toml_table = toml_value.as_table().unwrap();
        let data_type = toml_table.get("type").unwrap().as_str().unwrap();
        match data_type {
            #[cfg(feature = "use_mysql")]
            "mysql" => SourceConfigCommand::Mysql(
                toml::from_str(
                    toml::to_string(
                        toml_table
                            .get("mysql")
                            .unwrap()
                        )
                    .unwrap()
                    .as_str())
                .unwrap()
            ),
            #[cfg(feature = "use_postgres")]
            "postgres" => SourceConfigCommand::Postgres(
                toml::from_str(
                    toml::to_string(
                        toml_table
                            .get("postgres")
                            .unwrap()
                        )
                    .unwrap()
                    .as_str())
                .unwrap()
            ),
            #[cfg(feature = "use_sqlite")]
            "sqlite" => SourceConfigCommand::Sqlite(
                toml::from_str(
                    toml::to_string(
                        toml_table
                            .get("sqlite")
                            .unwrap()
                        )
                    .unwrap()
                    .as_str())
                .unwrap()
            ),
            _ => panic!("source from toml: unknown source type: {}", data_type),
        }
    }
}


#[derive(Clone, Debug, StructOpt)]
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

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesAddOptions {
    #[structopt(help = "source name")]
    pub name: String,
    #[structopt(subcommand)]
    pub source: SourceConfigCommand,
}

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesDeleteOptions {
    #[structopt(help = "source name")]
    pub name: String,
}

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesEditOptions {
    #[structopt(help = "source name")]
    pub name: String,
}

#[derive(Clone, Debug, StructOpt)]
pub struct SourcesListOptions {
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
#[derive(Clone, Debug, Deserialize, Serialize, StructOpt)]
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
    #[structopt(long = "timeout", help = "connect/read/write timeout in seconds")]
    pub timeout: Option<u64>,
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
    #[structopt(short = "q", long = "query", help = "sql query")]
    pub query: String,
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
            self.port = config_options.port.clone();
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
        if self.init.len() == 0 && config_options.init.len() > 0 {
            self.init.extend(config_options.init.iter().cloned());
        }
        if self.timeout.is_none() && config_options.timeout.is_some() {
            self.timeout = config_options.timeout.clone();
        }
    }
}

#[cfg(feature = "use_postgres")]
#[derive(Clone, Debug, Deserialize, Serialize, StructOpt)]
pub struct PostgresConfigOptions {
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
    #[structopt(short = "q", long = "query", help = "sql query")]
    pub query: String,
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
            self.port = config_options.port.clone();
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
        if self.init.len() == 0 && config_options.init.len() > 0 {
            self.init.extend(config_options.init.iter().cloned());
        }
        if self.timeout.is_none() && config_options.timeout.is_some() {
            self.timeout = config_options.timeout.clone();
        }
    }
}

#[cfg(feature = "use_sqlite")]
#[derive(Clone, Debug, Deserialize, Serialize, StructOpt)]
pub struct SqliteConfigOptions {
    #[structopt(help = "sqlite filename")]
    pub filename: Option<String>,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
}

#[cfg(feature = "use_sqlite")]
#[derive(Clone, Debug, StructOpt)]
pub struct SqliteSourceOptions {
    #[structopt(help = "sqlite filename")]
    pub filename: Option<String>,
    #[structopt(short = "i", long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
    #[structopt(short = "q", long = "query", help = "sql query")]
    pub query: String,
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
        if self.init.len() == 0 && config_options.init.len() > 0 {
            self.init.extend(config_options.init.iter().cloned());
        }
    }
}
