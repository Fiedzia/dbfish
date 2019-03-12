use serde_derive::{Deserialize, Serialize};

use crate::config;

pub struct SourceConfigCommandWrapper (pub SourceConfigCommand);

impl SourceConfigCommandWrapper {

    pub fn augment_clap<'a, 'b>(
            app: ::structopt::clap::App<'a, 'b>,
        ) -> ::structopt::clap::App<'a, 'b> {
        let mut app = SourceConfigCommand::augment_clap(app);
        let sources = config::get_sources_list();

        for (source_name, source_config_command) in sources {

            match source_config_command.get_type_name().as_str() {

                #[cfg(feature = "use_mysql")]
                "mysql" => {
                    let subcmd = MysqlConfigOptions::augment_clap(
                        structopt::clap::SubCommand::with_name(&source_name)
                            .setting(structopt::clap::AppSettings::ColoredHelp)
                    );
                    app = app.subcommand(subcmd);
                },
                #[cfg(feature = "use_postgres")]
                "postgres" => {
                    let subcmd = PostgresConfigOptions::augment_clap(
                        structopt::clap::SubCommand::with_name(&source_name)
                            .setting(structopt::clap::AppSettings::ColoredHelp)
                    );
                    app = app.subcommand(subcmd);
                },
                #[cfg(feature = "use_sqlite")]
                "sqlite" => {
                    let subcmd = SqliteConfigOptions::augment_clap(
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

        let result = SourceConfigCommand::from_subcommand(sub);
        //no default sources were matching subcommand, it might be user defined source
        if let None = result {

            if let (source_name, Some(matches)) = sub {
                match config::USER_DEFINED_SOURCES.get(source_name) {
                    None => None,
                    Some(source) => match source {
                        #[cfg(feature = "use_mysql")]
                        SourceConfigCommand::Mysql(mysql_config_options) => {

                            let mut mysql_options = <MysqlConfigOptions as ::structopt::StructOpt>
                                ::from_clap(matches);
                            mysql_options.update_from_config_options(mysql_config_options);

                            Some(
                                SourceConfigCommandWrapper(
                                    SourceConfigCommand::Mysql(mysql_options)
                                )
                            )
                        },
                        #[cfg(feature = "use_postgres")]
                        SourceConfigCommand::Postgres(postgres_config_options) => {

                            let mut postgres_options = <PostgresConfigOptions as ::structopt::StructOpt>
                                ::from_clap(matches);
                            postgres_options.update_from_config_options(postgres_config_options);

                            Some(
                                SourceConfigCommandWrapper(
                                    SourceConfigCommand::Postgres(postgres_options)
                                )
                            )
                        },
                        #[cfg(feature = "use_sqlite")]
                        SourceConfigCommand::Sqlite(sqlite_config_options) => {

                            let mut sqlite_options = <SqliteConfigOptions as ::structopt::StructOpt>
                                ::from_clap(matches);
                            sqlite_options.update_from_config_options(sqlite_config_options);

                            Some(
                                SourceConfigCommandWrapper(
                                    SourceConfigCommand::Sqlite(sqlite_options)
                                )
                            )
                        },
                    }
                }
            } else {
                None
            }
        } else {
            result.map(|v| SourceConfigCommandWrapper(v))
        }
    }

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
impl MysqlConfigOptions {

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
impl PostgresConfigOptions {
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
impl SqliteConfigOptions {

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
