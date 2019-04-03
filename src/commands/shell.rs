use std::process::Command;

#[cfg(feature = "use_mysql")]
use mysql;

use crate::commands::{ApplicationArguments};
use crate::commands::common::{SourceConfigCommandWrapper, SourceConfigCommand};

#[cfg(feature = "use_mysql")]
use crate::sources::mysql::{establish_mysql_connection};
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::establish_postgres_connection;
#[cfg(feature = "use_sqlite")]
use crate::sources::sqlite::establish_sqlite_connection;


#[derive(StructOpt)]
pub struct ShellCommand {
    #[structopt(subcommand)]
    pub source: SourceConfigCommandWrapper,
}



pub fn shell (_args: &ApplicationArguments, shell_command: &ShellCommand) {

    match &shell_command.source.0 {
        #[cfg(feature = "use_mysql")]
        SourceConfigCommand::Mysql(mysql_config_options) => {
            let mut cmd = Command::new("mysql");
            if let Some(hostname) =  &mysql_config_options.host {
                cmd.arg("-h").arg(hostname);
            }
            if let Some(username) =  &mysql_config_options.user {
                cmd.arg("-u").arg(username);
            }
            if let Some(port) =  &mysql_config_options.port {
                cmd.arg("-P").arg(port.to_string());
            }

            if let Some(password) = &mysql_config_options.password {
                cmd.arg("-p".to_string() + password);
            }

            if let Some(database) =  &mysql_config_options.database {
                cmd.arg(database);
            }

            cmd
                .status()
                .expect(&format!("failed to execute mysql ({:?})", cmd));

        },
        #[cfg(feature = "use_sqlite")]
        SourceConfigCommand::Sqlite(sqlite_config_options) => {
        },
        #[cfg(feature = "use_postgres")]
        SourceConfigCommand::Postgres(postgres_config_options) => {
        }
    }
}
