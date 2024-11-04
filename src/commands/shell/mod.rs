use std::io::Write;
use std::process::Command as OsCommand;

use clap::{Parser, ValueEnum};

use crate::commands::data_source::DataSourceCommand;
use crate::commands::export;
use crate::commands::ApplicationArguments;
use crate::config;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum KnownShells {
    Default,
    Python,
    Litecli,
    Sqlite,
    Mycli,
    Mysql,
    Pgcli,
    Psql,
}

#[derive(Debug, Parser)]
pub struct ShellCommand {
    #[arg(
        short = 'c',
        long = "client",
        help = "select shell (client)",
        default_value = "default"
    )]
    pub client: KnownShells,
}

pub fn create_python_virtualenv(path: &std::path::PathBuf) {
    if !path.exists() {
        OsCommand::new("python3")
            .arg("-m")
            .arg("virtualenv")
            .arg("-p")
            .arg("python3")
            .arg(path.clone())
            .status()
            .expect("creation of virtualenv failed");
    }
}

#[cfg(feature = "use_mysql")]
pub fn mysql_client(mysql_config_options: &export::MysqlSourceOptions) {
    let mut cmd = OsCommand::new("mysql");
    if let Some(hostname) = &mysql_config_options.host {
        cmd.arg("-h").arg(hostname);
    }
    if let Some(username) = &mysql_config_options.user {
        cmd.arg("-u").arg(username);
    }
    if let Some(port) = &mysql_config_options.port {
        cmd.arg("-P").arg(port.to_string());
    }

    if let Some(password) = &mysql_config_options.password {
        cmd.arg("-p".to_string() + password);
    }

    if let Some(database) = &mysql_config_options.database {
        cmd.arg(database);
    }

    cmd.status()
        .expect(&format!("failed to execute mysql ({:?})", cmd));
}

#[cfg(feature = "use_mysql")]
pub fn mycli_client(mysql_config_options: &export::MysqlSourceOptions) {
    let mut cmd = OsCommand::new("mycli");
    if let Some(hostname) = &mysql_config_options.host {
        cmd.arg("-h").arg(hostname);
    }
    if let Some(username) = &mysql_config_options.user {
        cmd.arg("-u").arg(username);
    }
    if let Some(port) = &mysql_config_options.port {
        cmd.arg("-P").arg(port.to_string());
    }

    if let Some(password) = &mysql_config_options.password {
        cmd.arg("-p".to_string() + password);
    }

    if let Some(database) = &mysql_config_options.database {
        cmd.arg(database);
    }

    cmd.status()
        .expect(&format!("failed to execute mysql ({:?})", cmd));
}

#[cfg(feature = "use_mysql")]
pub fn mysql_python_client(mysql_config_options: &export::MysqlSourceOptions) {
    config::ensure_config_directory_exists();
    let python_venv_dir = config::get_config_directory().join("python_venv");
    if !python_venv_dir.exists() {
        std::fs::create_dir(&python_venv_dir).unwrap();
    }

    let python_mysql_venv = python_venv_dir.join("mysql");
    if !python_mysql_venv.exists() {
        create_python_virtualenv(&python_mysql_venv);
        OsCommand::new(python_mysql_venv.join("bin").join("pip"))
            .arg("install")
            .arg("ipython")
            .arg("pymysql")
            .status()
            .expect("could not install dependencies via pip");
    }
    let python_file = python_mysql_venv.join("run.py");
    if !python_file.exists() {
        let content = include_str!("mysql.py");
        std::fs::File::create(&python_file)
            .unwrap()
            .write_all(content.as_ref())
            .unwrap();
    }

    if let Some(hostname) = &mysql_config_options.host {
        std::env::set_var("MYSQL_HOST", hostname);
    }
    if let Some(username) = &mysql_config_options.user {
        std::env::set_var("MYSQL_USER", username);
    }
    if let Some(port) = &mysql_config_options.port {
        std::env::set_var("MYSQL_PORT", port.to_string());
    }
    if let Some(password) = &mysql_config_options.password {
        std::env::set_var("MYSQL_PASSWORD", password);
    }
    if let Some(database) = &mysql_config_options.database {
        std::env::set_var("MYSQL_DATABASE", database);
    }

    OsCommand::new(python_mysql_venv.join("bin").join("python"))
        .arg(python_file.clone())
        .status()
        .expect(&format!(
            "could not run python script: {}",
            python_file.to_str().unwrap()
        ));
}

#[cfg(feature = "use_sqlite")]
pub fn litecli_client(sqlite_config_options: &export::SqliteSourceOptions) {
    let mut cmd = OsCommand::new("litecli");
    if let Some(filename) = &sqlite_config_options.filename {
        cmd.arg(filename);
    }

    cmd.status()
        .expect(&format!("failed to execute litecli ({:?})", cmd));
}

#[cfg(feature = "use_postgres")]
pub fn pgcli_client(postgres_config_options: &export::PostgresSourceOptions) {
    let mut cmd = OsCommand::new("pgcli");
    if let Some(hostname) = &postgres_config_options.host {
        cmd.arg("-h").arg(hostname);
    }
    if let Some(username) = &postgres_config_options.user {
        cmd.arg("-U").arg(username);
    }
    if let Some(port) = &postgres_config_options.port {
        cmd.arg("-p").arg(port.to_string());
    }
    if let Some(database) = &postgres_config_options.database {
        cmd.arg("-d").arg(database);
    }

    cmd.status()
        .expect(&format!("failed to execute pgcli ({:?})", cmd));
}

#[cfg(feature = "use_postgres")]
pub fn postgres_python_client(postgres_config_options: &export::PostgresSourceOptions) {
    config::ensure_config_directory_exists();
    let python_venv_dir = config::get_config_directory().join("python_venv");
    if !python_venv_dir.exists() {
        std::fs::create_dir(&python_venv_dir).unwrap();
    }

    let python_postgres_venv = python_venv_dir.join("postgres");
    if !python_postgres_venv.exists() {
        create_python_virtualenv(&python_postgres_venv);
        OsCommand::new(python_postgres_venv.join("bin").join("pip"))
            .arg("install")
            .arg("ipython")
            .arg("py-postgresql")
            .status()
            .expect("could not install dependencies via pip");
    }
    let python_file = python_postgres_venv.join("run.py");
    if !python_file.exists() {
        let content = include_str!("postgres.py");
        std::fs::File::create(&python_file)
            .unwrap()
            .write_all(content.as_ref())
            .unwrap();
    }

    if let Some(hostname) = &postgres_config_options.host {
        std::env::set_var("POSTGRES_HOST", hostname);
    }
    if let Some(username) = &postgres_config_options.user {
        std::env::set_var("POSTGRES_USER", username);
    }
    if let Some(port) = &postgres_config_options.port {
        std::env::set_var("POSTGRES_PORT", port.to_string());
    }
    if let Some(password) = &postgres_config_options.password {
        std::env::set_var("POSTGREs_PASSWORD", password);
    }
    if let Some(database) = &postgres_config_options.database {
        std::env::set_var("POSTGRES_DATABASE", database);
    }

    OsCommand::new(python_postgres_venv.join("bin").join("python"))
        .arg(python_file.clone())
        .status()
        .expect(&format!(
            "could not run python script: {}",
            python_file.to_str().unwrap()
        ));
}

#[cfg(feature = "use_postgres")]
pub fn psql_client(postgres_config_options: &export::PostgresSourceOptions) {
    let mut cmd = OsCommand::new("psql");
    if let Some(hostname) = &postgres_config_options.host {
        cmd.arg("-h").arg(hostname);
    }
    if let Some(username) = &postgres_config_options.user {
        cmd.arg("-U").arg(username);
    }
    if let Some(port) = &postgres_config_options.port {
        cmd.arg("-p").arg(port.to_string());
    }
    if let Some(database) = &postgres_config_options.database {
        cmd.arg("-d").arg(database);
    }

    cmd.status()
        .expect(&format!("failed to execute psql ({:?})", cmd));
}

#[cfg(feature = "use_sqlite")]
pub fn sqlite_client(sqlite_config_options: &export::SqliteSourceOptions) {
    let mut cmd = OsCommand::new("sqlite3");
    if let Some(filename) = &sqlite_config_options.filename {
        cmd.arg(filename);
    }

    cmd.status()
        .expect(&format!("failed to execute sqlite3 ({:?})", cmd));
}

#[cfg(feature = "use_sqlite")]
pub fn sqlite_python_client(sqlite_config_options: &export::SqliteSourceOptions) {
    config::ensure_config_directory_exists();
    let python_venv_dir = config::get_config_directory().join("python_venv");
    if !python_venv_dir.exists() {
        std::fs::create_dir(&python_venv_dir).unwrap();
    }

    let python_sqlite_venv = python_venv_dir.join("sqlite");

    if !python_sqlite_venv.exists() {
        create_python_virtualenv(&python_sqlite_venv);
        OsCommand::new(python_sqlite_venv.join("bin").join("pip"))
            .arg("install")
            .arg("ipython")
            .status()
            .expect("could not install dependencies via pip");
    }
    let python_file = python_sqlite_venv.join("run.py");
    if !python_file.exists() {
        let content = include_str!("sqlite.py");
        std::fs::File::create(&python_file)
            .unwrap()
            .write_all(content.as_ref())
            .unwrap();
    }

    if let Some(filename) = &sqlite_config_options.filename {
        std::env::set_var("SQLITE_FILE", filename);
    }

    OsCommand::new(python_sqlite_venv.join("bin").join("python"))
        .arg(python_file.clone())
        .status()
        .expect(&format!(
            "could not run python script: {}",
            python_file.to_str().unwrap()
        ));
}

pub fn shell(_args: &ApplicationArguments, src: &DataSourceCommand, shell_command: &ShellCommand) {
    match &src {
        #[cfg(feature = "use_mysql")]
        DataSourceCommand::Mysql(mysql_config_options) => match shell_command.client {
            KnownShells::Mycli => {
                mycli_client(&mysql_config_options);
            }
            KnownShells::Default | KnownShells::Mysql => {
                mysql_client(&mysql_config_options);
            }
            KnownShells::Python => {
                mysql_python_client(&mysql_config_options);
            }
            _ => {
                eprintln!(
                    "client unknown or unsuitable for given source: {:?}",
                    shell_command.client
                );
                std::process::exit(1);
            }
        },

        #[cfg(feature = "use_sqlite")]
        DataSourceCommand::Sqlite(sqlite_config_options) => match shell_command.client {
            KnownShells::Litecli => litecli_client(&sqlite_config_options),
            KnownShells::Default | KnownShells::Sqlite => sqlite_client(&sqlite_config_options),
            KnownShells::Python => sqlite_python_client(&sqlite_config_options),
            _ => {
                eprintln!(
                    "client unknown or unsuitable for given source: {:?}",
                    shell_command.client
                );
                std::process::exit(1);
            }
        },
        #[cfg(feature = "use_postgres")]
        DataSourceCommand::Postgres(postgres_config_options) => match shell_command.client {
            KnownShells::Pgcli => pgcli_client(&postgres_config_options),
            KnownShells::Default | KnownShells::Psql => psql_client(&postgres_config_options),
            KnownShells::Python => postgres_python_client(&postgres_config_options),
            _ => {
                eprintln!(
                    "client unknown or unsuitable for given source: {:?}",
                    shell_command.client
                );
                std::process::exit(1);
            }
        },
    }
}
