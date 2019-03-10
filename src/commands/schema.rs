use crate::commands::{ApplicationArguments};
use crate::commands::common::SourceConfigCommandWrapper;
use crate::config;
use crate::definitions::{DataSource, DataDestination, DataSourceConnection, DataSourceBatchIterator};
use crate::destinations::Destination;

use crate::sources::Source;

#[cfg(feature = "use_mysql")]
use crate::sources::mysql::MysqlSource;
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::PostgresSource;
#[cfg(feature = "use_sqlite")]
use crate::sources::sqlite::SqliteSource;


#[derive(StructOpt)]
pub struct SchemaCommand {
    #[structopt(subcommand)]
    pub source: SourceConfigCommandWrapper,
}



pub fn schema (args: &ApplicationArguments, schema_command: &SchemaCommand) {

    let src = schema_command.source.0.get_type_name();

    /*match config::USER_DEFINED_SOURCES.get(&schema_command.source.0.as_str()) {
        None => {
            eprintln!("Source not found: {}", schema_command.source);
            std::process::exit(1);
        },
        Some(source) => {}, /*match source {
        }*/
    }*/


}
