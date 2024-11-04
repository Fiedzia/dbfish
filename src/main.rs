#[macro_use]
extern crate clap;

use clap::{CommandFactory, FromArgMatches, Subcommand};

pub mod commands;
pub mod config;
pub mod definitions;
pub mod destinations;
pub mod sources;
pub mod utils;

use commands::{
    handle_source_level_command, ApplicationArguments, CommandSource, CommandWrapper,
    DataSourceCommand, SourceLevelCommand,
};

fn main() {
    let mut app = ApplicationArguments::command_for_update();
    app = CommandWrapper::augment_subcommands(app).subcommand_required(true);

    let args = &app.get_matches();

    let app_args = ApplicationArguments::from_arg_matches(&args).unwrap();
    /*match app_args.color {
        UseColor::No => { app = app.disable_colored_help(true);},
        _ => {}
    }*/
    let subcmd = CommandWrapper::from_arg_matches(&args);
    match &subcmd {
        Ok(cmd) => {
            match cmd {
                CommandWrapper(CommandSource::Sources(sources_cmd)) => {
                    commands::sources::sources(&app_args, &sources_cmd);
                }
                #[cfg(feature = "use_mysql")]
                CommandWrapper(CommandSource::Mysql(mysql_options)) => {
                    let src_subcmd =
                        SourceLevelCommand::from_arg_matches(&args.subcommand().unwrap().1);
                    match src_subcmd {
                        Ok(cmd) => {
                            handle_source_level_command(
                                &app_args,
                                &DataSourceCommand::Mysql(mysql_options.clone()),
                                &cmd,
                            );
                        }
                        Err(_) => {}
                    }
                }
                #[cfg(feature = "use_postgres")]
                CommandWrapper(CommandSource::Postgres(postgres_options)) => {
                    let src_subcmd =
                        SourceLevelCommand::from_arg_matches(&args.subcommand().unwrap().1);
                    match src_subcmd {
                        Ok(cmd) => {
                            handle_source_level_command(
                                &app_args,
                                &DataSourceCommand::Postgres(postgres_options.clone()),
                                &cmd,
                            );
                        }
                        Err(_) => {}
                    }
                }
                #[cfg(feature = "use_sqlite")]
                CommandWrapper(CommandSource::Sqlite(sqlite_options)) => {
                    let src_subcmd =
                        SourceLevelCommand::from_arg_matches(&args.subcommand().unwrap().1);
                    match src_subcmd {
                        Ok(cmd) => {
                            handle_source_level_command(
                                &app_args,
                                &DataSourceCommand::Sqlite(sqlite_options.clone()),
                                &cmd,
                            );
                        }
                        Err(_) => {}
                    }
                }
            };
        }
        Err(e) => {
            panic!("error: {}", e);
        }
    }
}
