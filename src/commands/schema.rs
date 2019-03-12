#[cfg(feature = "use_mysql")]
use mysql;

use crate::commands::{ApplicationArguments};
use crate::commands::common::{SourceConfigCommandWrapper, SourceConfigCommand};
use crate::config;
use crate::definitions::{DataSource, DataDestination, DataSourceConnection, DataSourceBatchIterator};
use crate::destinations::Destination;

use crate::sources::Source;

#[cfg(feature = "use_mysql")]
use crate::sources::mysql::{establish_mysql_connection, MysqlSource};
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::PostgresSource;
#[cfg(feature = "use_sqlite")]
use crate::sources::sqlite::{establish_sqlite_connection, SqliteSource};


#[derive(StructOpt)]
pub struct SchemaCommand {
    #[structopt(subcommand)]
    pub source: SourceConfigCommandWrapper,
}



pub fn schema (args: &ApplicationArguments, schema_command: &SchemaCommand) {

    match &schema_command.source.0 {
        #[cfg(feature = "use_mysql")]
        SourceConfigCommand::Mysql(mysql_config_options) => {
            let conn = establish_mysql_connection(mysql_config_options);
            let results = conn.prep_exec("
                select table_schema, table_name
                from information_schema.tables
                order by table_schema, table_name
                ", ()).unwrap();

            for row in results {

                let (a, b):(String, String) = mysql::from_row(row.unwrap());
                println!("{}: {}", a, b);
            }
        },
        #[cfg(feature = "use_sqlite")]
        SourceConfigCommand::Sqlite(sqlite_config_options) => {
            let conn = establish_sqlite_connection(sqlite_config_options);
            conn.iterate("
                SELECT 
                  m.name as table_name, 
                  p.name as name,
                  p.type as type,
                  p.`notnull` as nullability,
                  p.dflt_value as default_value,
                  p.pk as primary_key
                
                FROM 
                  sqlite_master AS m
                JOIN 
                  pragma_table_info(m.name) AS p
                ORDER BY 
                  m.name, 
                  p.cid
                ",
                |pairs| {
                    for &(column, value) in pairs.iter() {
                        println!("{}: {}", column, value.unwrap_or("NULL"))
                    }
                    true
                
                }
                ).unwrap();


            
        },
        #[cfg(feature = "use_postgres")]
        SourceConfigCommand::Postgres(postgres_config_options) => {
        }
    }
}
