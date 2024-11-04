use clap::{FromArgMatches, Parser};

use crate::commands::export;


#[derive(Clone, Debug, Parser)]
pub enum DataSourceCommand {
    #[cfg(feature = "use_mysql")]
    #[command(name = "mysql", about="mysql")]
    Mysql(export::MysqlSourceOptions),
    #[cfg(feature = "use_postgres")]
    #[command(name = "postgres", about="postgres")]
    Postgres(export::PostgresSourceOptions),
    #[cfg(feature = "use_sqlite")]
    #[command(name = "sqlite", about="sqlite")]
    Sqlite(export::SqliteSourceOptions),
}

