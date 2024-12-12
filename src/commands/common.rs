use clap;
use clap::{ArgMatches, FromArgMatches, Parser};
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct SourceConfigCommandWrapper(pub SourceConfigCommand);

impl FromArgMatches for SourceConfigCommandWrapper {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        SourceConfigCommand::from_arg_matches(matches).map(Self)
    }
    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        self.0.update_from_arg_matches(matches)
    }

    // Provided methods
    fn from_arg_matches_mut(matches: &mut ArgMatches) -> Result<Self, clap::Error> {
        SourceConfigCommand::from_arg_matches_mut(matches).map(Self)
    }
    fn update_from_arg_matches_mut(&mut self, matches: &mut ArgMatches) -> Result<(), clap::Error> {
        self.0.update_from_arg_matches_mut(matches)
    }
}

#[derive(Clone, Debug, Parser)]
pub enum SourceConfigCommand {
    #[cfg(feature = "use_duckdb")]
    #[command(name = "duckdb", about = "duckdb")]
    DuckDB(DuckDBConfigOptions),
    #[cfg(feature = "use_mysql")]
    #[command(name = "mysql", about = "mysql")]
    Mysql(MysqlConfigOptions),
    #[cfg(feature = "use_postgres")]
    #[command(name = "postgres", about = "postgres")]
    Postgres(PostgresConfigOptions),
    #[cfg(feature = "use_sqlite")]
    #[command(name = "sqlite", about = "sqlite")]
    Sqlite(SqliteConfigOptions),
}

impl SourceConfigCommand {
    pub fn get_type_name(&self) -> String {
        match self {
            #[cfg(feature = "use_duckdb")]
            SourceConfigCommand::DuckDB(_) => "duckdb".to_string(),
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
            SourceConfigCommand::Mysql(options) => toml::to_string(options)
                .unwrap()
                .parse::<toml::Value>()
                .unwrap(),
            #[cfg(feature = "use_postgres")]
            SourceConfigCommand::Postgres(options) => toml::to_string(options)
                .unwrap()
                .parse::<toml::Value>()
                .unwrap(),
            #[cfg(feature = "use_sqlite")]
            SourceConfigCommand::Sqlite(options) => toml::to_string(options)
                .unwrap()
                .parse::<toml::Value>()
                .unwrap(),
            #[cfg(feature = "use_duckdb")]
            SourceConfigCommand::DuckDB(options) => toml::to_string(options)
                .unwrap()
                .parse::<toml::Value>()
                .unwrap(),
        }
    }

    pub fn description(&self) -> String {
        match self {
            #[cfg(feature = "use_mysql")]
            SourceConfigCommand::Mysql(options) => match (&options.host, &options.database) {
                (Some(host), Some(db)) => format!("mysql {}/{}", host, db),
                (Some(host), None) => format!("mysql {}", host),
                (None, Some(db)) => format!("mysql /{}", db),
                (None, None) => "mysql".to_string(),
            },
            #[cfg(feature = "use_postgres")]
            SourceConfigCommand::Postgres(options) => match (&options.host, &options.database) {
                (Some(host), Some(db)) => format!("postgres {}/{}", host, db),
                (Some(host), None) => format!("postgres {}", host),
                (None, Some(db)) => format!("mysql /{}", db),
                (None, None) => "mysql".to_string(),
            },

            #[cfg(feature = "use_sqlite")]
            SourceConfigCommand::Sqlite(options) => {
                if let Some(filename) = &options.filename {
                    format!("sqlite: {}", filename)
                } else {
                    "sqlite".to_string()
                }
            },

            #[cfg(feature = "use_duckdb")]
            SourceConfigCommand::DuckDB(options) => {
                if let Some(filename) = &options.filename {
                    format!("duckdb: {}", filename)
                } else {
                    "duckdb".to_string()
                }
            },

        }
    }

    pub fn to_full_toml(&self) -> toml::Value {
        let type_name = self.get_type_name();
        let mut toml_table = toml::value::Table::new();
        toml_table.insert("type".to_string(), toml::Value::String(type_name.clone()));
        toml_table.insert(type_name, self.to_toml());

        toml::Value::Table(toml_table)
    }

    pub fn from_toml(toml_value: &toml::Value) -> Self {
        let toml_table = toml_value.as_table().unwrap();
        let data_type = toml_table.get("type").unwrap().as_str().unwrap();
        match data_type {
            #[cfg(feature = "use_mysql")]
            "mysql" => SourceConfigCommand::Mysql(
                toml::from_str(
                    toml::to_string(toml_table.get("mysql").unwrap())
                        .unwrap()
                        .as_str(),
                )
                .unwrap(),
            ),
            #[cfg(feature = "use_postgres")]
            "postgres" => SourceConfigCommand::Postgres(
                toml::from_str(
                    toml::to_string(toml_table.get("postgres").unwrap())
                        .unwrap()
                        .as_str(),
                )
                .unwrap(),
            ),
            #[cfg(feature = "use_sqlite")]
            "sqlite" => SourceConfigCommand::Sqlite(
                toml::from_str(
                    toml::to_string(toml_table.get("sqlite").unwrap())
                        .unwrap()
                        .as_str(),
                )
                .unwrap(),
            ),
            #[cfg(feature = "use_duckdb")]
            "duckdb" => SourceConfigCommand::DuckDB(
                toml::from_str(
                    toml::to_string(toml_table.get("duckdb").unwrap())
                        .unwrap()
                        .as_str(),
                )
                .unwrap(),
            ),
            _ => panic!("source from toml: unknown source type: {}", data_type),
        }
    }
}

fn empty_vec() -> Vec<String> {
    vec![]
}

#[cfg(feature = "use_mysql")]
#[derive(Clone, Debug, Deserialize, Serialize, Parser)]
pub struct MysqlConfigOptions {
    #[arg(short = 'h', long = "host", help = "hostname")]
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
    #[serde(default = "empty_vec")]
    pub init: Vec<String>,
    #[arg(long = "timeout", help = "connect/read/write timeout in seconds")]
    pub timeout: Option<u64>,
}

#[cfg(feature = "use_mysql")]
impl MysqlConfigOptions {
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
#[derive(Clone, Debug, Deserialize, Serialize, Parser)]
pub struct PostgresConfigOptions {
    #[arg(short = 'h', long = "host", help = "hostname")]
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
}

#[cfg(feature = "use_postgres")]
impl PostgresConfigOptions {
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
#[derive(Clone, Debug, Deserialize, Serialize, Parser)]
pub struct SqliteConfigOptions {
    #[arg(help = "sqlite filename")]
    pub filename: Option<String>,
    #[arg(short = 'i', long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
}

#[cfg(feature = "use_sqlite")]
impl SqliteConfigOptions {
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

#[cfg(feature = "use_duckdb")]
#[derive(Clone, Debug, Deserialize, Serialize, Parser)]
pub struct DuckDBConfigOptions {
    #[arg(help = "duckdb filename")]
    pub filename: Option<String>,
    #[arg(short = 'i', long = "init", help = "initial sql commands")]
    pub init: Vec<String>,
}

#[cfg(feature = "use_duckdb")]
impl DuckDBConfigOptions {
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
