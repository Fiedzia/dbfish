use structopt;
use structopt::StructOpt;
use structopt::clap::arg_enum;

use crate::config;

#[cfg(feature = "use_mysql")]
use crate::{commands::common::MysqlConfigOptions, sources::mysql::MysqlSource};
#[cfg(feature = "use_spsheet")]
use crate::destinations::ods_xlsx::{SpreadSheetDestination, SpreadSheetFormat};
#[cfg(feature = "use_postgres")]
use crate::{commands::common::PostgresConfigOptions, sources::postgres::PostgresSource};
#[cfg(feature = "use_csv")]
use crate::destinations::csv::CSVDestination;
use crate::destinations::debug::DebugDestination;
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



pub mod common;
pub mod export;
pub mod schema;
pub mod shell;
pub mod sources;


arg_enum! {
    #[derive(Debug, PartialEq)]
    pub enum UseColor {
        Yes,
        No,
        Auto
    }
}

pub fn ah() -> String {
    "abc".to_string()
}


#[derive(StructOpt)]
#[structopt(name = "export", about="Export data from database to sqlite/csv/text/html/json file.", after_help="Choose a command to run or to print help for, ie. :sources --help", rename_all = "verbatim")]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
pub struct ApplicationArguments {
    #[structopt(short = "v", long = "verbose", help = "Be verbose")]
    pub verbose: bool,
    #[structopt(short = "c", long = "color", help = "use color", default_value="auto", possible_values = &UseColor::variants(), case_insensitive = true)]
    pub color: UseColor,
    #[structopt(subcommand)]
    pub command: CommandWrapper,
}


#[derive(StructOpt)]
pub enum Command {
    /*#[structopt(name = ":export", about="export data", rename_all = "verbatim")]
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Export(export::ExportCommand),
    #[structopt(name = ":shell", about="jump to shell", rename_all = "verbatim")]
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Shell(shell::ShellCommand),
    #[structopt(name = ":schema", about="show source schema", rename_all = "verbatim")]
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Schema(schema::SchemaCommand),*/
    #[structopt(name = ":sources", about="manage data sources", rename_all = "verbatim")]
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Sources(sources::SourcesCommand),
}


pub struct CommandWrapper (pub Command);

impl CommandWrapper {

    pub fn augment_clap<'a, 'b>(
            app: ::structopt::clap::App<'a, 'b>,
        ) -> ::structopt::clap::App<'a, 'b> {
        
            let mut app = Command::augment_clap(app);

            let sources = config::get_sources_list();

            for (source_name, source_config_command) in sources {

                match source_config_command.get_type_name().as_str() {

                    #[cfg(feature = "use_mysql")]
                    "mysql" => {
                        let subcmd = export::MysqlSourceOptions::augment_clap(
                            structopt::clap::SubCommand::with_name(&source_name)
                                .setting(structopt::clap::AppSettings::ColoredHelp)
                                //.setting(structopt::clap::AppSettings::Hidden)
                        );
                        app = app.subcommand(subcmd);
                    },
                    #[cfg(feature = "use_postgres")]
                    "postgres" => {
                        let subcmd = export::PostgresSourceOptions::augment_clap(
                            structopt::clap::SubCommand::with_name(&source_name)
                                .setting(structopt::clap::AppSettings::ColoredHelp)
                                //.setting(structopt::clap::AppSettings::Hidden)
                        );
                        app = app.subcommand(subcmd);
                    },
                    #[cfg(feature = "use_sqlite")]
                    "sqlite" => {
                        let subcmd = export::SqliteSourceOptions::augment_clap(
                            structopt::clap::SubCommand::with_name(&source_name)
                                .setting(structopt::clap::AppSettings::ColoredHelp)
                                //.setting(structopt::clap::AppSettings::Hidden)
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

        let result = Command::from_subcommand(sub);
        if result.is_none() {
            None
        } else {
            result.map(CommandWrapper)
        }
    }
}
