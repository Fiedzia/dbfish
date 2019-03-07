#[cfg(feature = "use_mysql")] 
pub mod mysql;
#[cfg(feature = "use_postgres")] 
pub mod postgres;
#[cfg(feature = "use_sqlite")] 
pub mod sqlite;

use crate::definitions::DataSource;

pub enum Source {
#[cfg(feature = "use_sqlite")]
    Sqlite(sqlite::SqliteSource)
}

impl DataSource for Source {

    
}
