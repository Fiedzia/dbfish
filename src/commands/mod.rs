use clap::{ArgMatches, Args, Command, FromArgMatches, Parser, Subcommand, ValueEnum};

pub use crate::commands::data_source::DataSourceCommand;
use crate::commands::export::export;
use crate::commands::schema::schema;
use crate::commands::shell::shell;
use crate::config;

pub mod common;
pub mod data_source;
pub mod export;
pub mod schema;
pub mod shell;
pub mod sources;

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
pub enum UseColor {
    Yes,
    No,
    Auto,
}

pub fn ah() -> String {
    "abc".to_string()
}

#[derive(Clone, Copy, Debug, Parser)]
#[command(version, about)]
pub struct ApplicationArguments {
    #[arg(short = 'v', long = "verbose", help = "Be verbose")]
    pub verbose: bool,
    #[arg(
        short = 'c',
        long = "color",
        help = "use color",
        default_value = "auto",
        ignore_case = true
    )]
    pub color: UseColor,
}

#[derive(Debug, Parser)]
pub enum SourceLevelCommand {
    Export(export::ExportCommand),
    Schema(schema::SchemaCommand),
    Shell(shell::ShellCommand),
}

pub fn handle_source_level_command(
    app_args: &ApplicationArguments,
    src: &DataSourceCommand,
    cmd: &SourceLevelCommand,
) {
    match cmd {
        SourceLevelCommand::Export(export_cmd) => {
            export(app_args, src, export_cmd);
        }
        SourceLevelCommand::Schema(schema_cmd) => {
            schema(app_args, src, schema_cmd);
        }
        SourceLevelCommand::Shell(shell_cmd) => shell(app_args, src, shell_cmd),
    }
}

#[derive(Debug, Parser)]
pub enum CommandSource {
    #[cfg(feature = "use_mysql")]
    #[command(name = "mysql", about = "mysql")]
    Mysql(export::MysqlSourceOptions),
    #[cfg(feature = "use_postgres")]
    #[command(name = "postgres", about = "postgres")]
    Postgres(export::PostgresSourceOptions),
    #[cfg(feature = "use_sqlite")]
    #[command(name = "sqlite", about = "sqlite")]
    Sqlite(export::SqliteSourceOptions),


    #[command(name = "sources", about="manage data sources", rename_all = "verbatim")]
    Sources(sources::SourcesCommand),
}

#[derive(Debug)]
pub struct CommandWrapper(pub CommandSource);

impl FromArgMatches for CommandWrapper {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        let result = CommandSource::from_arg_matches(matches).map(Self);
        match result {
            Ok(_) => result,
            Err(ref e) => {
                if e.kind() == clap::error::ErrorKind::InvalidSubcommand {
                    println!("err again {:?}", e);
                };
                result
            }
        }
    }
    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        self.0.update_from_arg_matches(matches)
    }

    // Provided methods
    fn from_arg_matches_mut(matches: &mut ArgMatches) -> Result<Self, clap::Error> {
        let r = CommandSource::from_arg_matches_mut(matches).map(Self);
        println!("here2:: {:#?}", r);
        r
    }
    fn update_from_arg_matches_mut(&mut self, matches: &mut ArgMatches) -> Result<(), clap::Error> {
        self.0.update_from_arg_matches_mut(matches)
    }
}

impl Subcommand for CommandWrapper {
    fn augment_subcommands(cmd: Command) -> Command {
        let mut new_cmd = CommandSource::augment_subcommands(cmd);

        #[cfg(feature = "use_mysql")]
        {
            new_cmd = new_cmd.mut_subcommand("mysql", |subcmd| {
                SourceLevelCommand::augment_subcommands(subcmd)
            });
        }
        #[cfg(feature = "use_postgres")]
        {
            new_cmd = new_cmd.mut_subcommand("postgres", |subcmd| {
                SourceLevelCommand::augment_subcommands(subcmd)
            });
        }
        #[cfg(feature = "use_sqlite")]
        {
            new_cmd = new_cmd.mut_subcommand("sqlite", |subcmd| {
                SourceLevelCommand::augment_subcommands(subcmd)
            });
        }

        let sources = config::get_sources_list();

        for (source_name, source_config_command) in sources {
            match source_config_command.get_type_name().as_str() {
                #[cfg(feature = "use_mysql")]
                "mysql" => {
                    let mut subcmd = export::MysqlSourceOptions::augment_args(
                        clap::Command::new(&source_name).about(source_config_command.description()),
                    );
                    subcmd = SourceLevelCommand::augment_subcommands(subcmd);
                    new_cmd = new_cmd.subcommand(subcmd);
                }
                #[cfg(feature = "use_postgres")]
                "postgres" => {
                    let mut subcmd = export::PostgresSourceOptions::augment_args(
                        clap::Command::new(&source_name).about(source_config_command.description()),
                    );
                    subcmd = SourceLevelCommand::augment_subcommands(subcmd);
                    new_cmd = new_cmd.subcommand(subcmd);
                }
                #[cfg(feature = "use_sqlite")]
                "sqlite" => {
                    let mut subcmd = export::SqliteSourceOptions::augment_args(
                        clap::Command::new(&source_name).about(source_config_command.description()),
                    );
                    subcmd = SourceLevelCommand::augment_subcommands(subcmd);
                    new_cmd = new_cmd.subcommand(subcmd);
                }

                unknown => {
                    eprintln!(
                        "unknown database type: {} for source: {}",
                        unknown,
                        source_config_command.get_type_name()
                    );
                }
            }
        }

        new_cmd
    }
    fn augment_subcommands_for_update(cmd: Command) -> Command {
        cmd
    }
    fn has_subcommand(name: &str) -> bool {
        CommandSource::has_subcommand(name)
    }
}
