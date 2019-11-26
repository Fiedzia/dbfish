use structopt;

use crate::commands::export;


#[derive(Debug, StructOpt)]
pub enum DataSourceCommand {
    #[cfg(feature = "use_mysql")]
    #[structopt(name = "mysql", about="mysql")]
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Mysql(export::MysqlSourceOptions),
    #[cfg(feature = "use_postgres")]
    #[structopt(name = "postgres", about="postgres")]
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Postgres(export::PostgresSourceOptions),
    #[cfg(feature = "use_sqlite")]
    #[structopt(name = "sqlite", about="sqlite")]
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Sqlite(export::SqliteSourceOptions),
}

