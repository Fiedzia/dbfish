#[cfg(feature = "use_mysql")] 
pub mod mysql;
#[cfg(feature = "use_postgres")] 
pub mod postgres;
#[cfg(feature = "use_sqlite")] 
pub mod sqlite;


use crate::definitions::{DataSource, DataSourceConnection, DataSourceBatchIterator, ColumnInfo, Row};


pub enum Source {
    #[cfg(feature = "use_sqlite")]
    Sqlite(sqlite::SqliteSource),
    #[cfg(feature = "use_mysql")]
    Mysql(mysql::MysqlSource),
    #[cfg(feature = "use_postgres")]
    Postgres(postgres::PostgresSource),
}


pub enum SourceConnection<'source> {
    #[cfg(feature = "use_sqlite")]
    SqliteConnection(sqlite::SqliteSourceConnection<'source>),
    #[cfg(feature = "use_mysql")]
    MysqlConnection(mysql::MysqlSourceConnection<'source>),
    #[cfg(feature = "use_postgres")]
    PostgresConnection(postgres::PostgresSourceConnection<'source>),
}


/*pub enum SourceBatchIterator<'c, 'i> {
    #[cfg(feature = "use_sqlite")]
    SqliteBatchIterator(sqlite::SqliteSourceBatchIterator<'c, 'i>),
    #[cfg(feature = "use_mysql")]
    MysqlBatchIterator(mysql::MysqlSourceBatchIterator<'c, 'i>),
    #[cfg(feature = "use_postgres")]
    PostgresBatchIterator(Box<dyn DataSourceBatchIterator>),
}*/


impl <'source: 'conn, 'conn>DataSource<'source, 'conn, SourceConnection<'source>> for Source
{
fn connect(&'source self) -> SourceConnection<'source> { //impl DataSourceConnection {
        match self {
            #[cfg(feature = "use_sqlite")]
            Source::Sqlite(sqlite_source) => SourceConnection::SqliteConnection(sqlite_source.connect()), 
            #[cfg(feature = "use_mysql")]
            Source::Mysql(mysql_source) => SourceConnection::MysqlConnection(mysql_source.connect()), 
            #[cfg(feature = "use_postgres")]
            Source::Postgres(postgres_source) => SourceConnection::PostgresConnection(postgres_source.connect()), 
            //Source::Postgres(postgres_source) => postgres_source.connect(),
        }
    }

    fn get_type_name(&self) -> String {
        match self {
            #[cfg(feature = "use_sqlite")]
            Source::Sqlite(sqlite_source) => sqlite_source.get_type_name(), 
            #[cfg(feature = "use_mysql")]
            Source::Mysql(mysql_source) => mysql_source.get_type_name(), 
            #[cfg(feature = "use_postgres")]
            Source::Postgres(postgres_source) => postgres_source.get_type_name(), 
        }
    }

    fn get_name(&self) -> String {
        match self {
            #[cfg(feature = "use_sqlite")]
            Source::Sqlite(sqlite_source) => sqlite_source.get_name(), 
            #[cfg(feature = "use_mysql")]
            Source::Mysql(mysql_source) => mysql_source.get_name(), 
            #[cfg(feature = "use_postgres")]
            Source::Postgres(postgres_source) => postgres_source.get_name(), 
        }
    }
}



impl <'source, 'conn>DataSourceConnection<'conn> for SourceConnection<'source> {
    //fn batch_iterator(mut self, batch_size: u64) -> impl DataSourceBatchIterator <'c, 'i> {
    fn batch_iterator(&'conn mut self, batch_size: u64) -> impl DataSourceBatchIterator<'conn> {
        match self {
            #[cfg(feature = "use_sqlite")]
            SourceConnection::SqliteConnection(sqlite_connection) => SourceBatchIterator::SqliteBatchIterator((*sqlite_connection).batch_iterator(batch_size)), 
            #[cfg(feature = "use_mysql")]
            SourceConnection::MysqlConnection(mysql_connection) => SourceBatchIterator::MysqlBatchIterator(mysql_connection.batch_iterator(batch_size)), 
            #[cfg(feature = "use_postgres")]
            //SourceConnection::PostgresConnection(postgres_connection) => SourceBatchIterator::PostgresBatchIterator(Box::new(postgres_connection.batch_iterator(batch_size))), 
            SourceConnection::PostgresConnection(postgres_connection) => postgres_connection.batch_iterator(batch_size), 
        }
   
    }
}


/*impl <'c, 'i>DataSourceBatchIterator for SourceBatchIterator<'c, 'i> {

    fn get_column_info(&self) -> Vec<ColumnInfo> {
        match self {
            #[cfg(feature = "use_sqlite")]
            SourceBatchIterator::SqliteBatchIterator(sqlite_source) => sqlite_source.get_column_info(), 
            #[cfg(feature = "use_mysql")]
            SourceBatchIterator::MysqlBatchIterator(mysql_source) => mysql_source.get_column_info(), 
            #[cfg(feature = "use_postgres")]
            SourceBatchIterator::PostgresBatchIterator(postgres_source) => postgres_source.get_column_info(), 
        }
    }
    
    fn get_count(&self) -> Option<u64> {
        match self {
            #[cfg(feature = "use_sqlite")]
            SourceBatchIterator::SqliteBatchIterator(sqlite_source) => sqlite_source.get_count(), 
            #[cfg(feature = "use_mysql")]
            SourceBatchIterator::MysqlBatchIterator(mysql_source) => mysql_source.get_count(), 
            #[cfg(feature = "use_postgres")]
            SourceBatchIterator::PostgresBatchIterator(postgres_source) => postgres_source.get_count(), 
        }
   
    }

    fn next(&mut self) -> Option<Vec<Row>> {
        match self {
            #[cfg(feature = "use_sqlite")]
            SourceBatchIterator::SqliteBatchIterator(sqlite_source) => sqlite_source.next(), 
            #[cfg(feature = "use_mysql")]
            SourceBatchIterator::MysqlBatchIterator(mysql_source) => mysql_source.next(), 
            #[cfg(feature = "use_postgres")]
            SourceBatchIterator::PostgresBatchIterator(postgres_source) => postgres_source.next(), 
        }
    }
}*/
