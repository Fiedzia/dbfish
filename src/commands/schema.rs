#[cfg(feature = "use_mysql")]
use mysql;

use crate::commands::{ApplicationArguments};
use crate::commands::common::{SourceConfigCommandWrapper, SourceConfigCommand};
use crate::utils::report_query_error;

#[cfg(feature = "use_mysql")]
use crate::sources::mysql::{establish_mysql_connection};
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::establish_postgres_connection;
#[cfg(feature = "use_sqlite")]
use crate::sources::sqlite::establish_sqlite_connection;


#[derive(StructOpt)]
pub struct SchemaCommand {
    pub query: Option<String>,
    #[structopt(subcommand)]
    pub source: SourceConfigCommandWrapper,
}


#[derive(Clone)]
pub struct DBItem {
    name: String,
    items: Vec<DBItem>
}

impl DBItem {
    pub fn print(&self, indentation_level: usize) {
        println!("{:indent$}{name}", "", indent=indentation_level, name=self.name);
        for item in self.items.iter() {
            item.print(indentation_level+4);
        }

    }

    pub fn matches(&self, query: &str) -> bool {
        return self.name.contains(query)
    }

    pub fn subtree_matching_query(&self, query: &str) -> Option<DBItem> {
       None 
    }

}

#[derive(Clone)]
struct DBItems (Vec<DBItem>);


impl DBItems {
    pub fn print(&self) {
       for dbitem in self.0.iter() {
           dbitem.print(0)
       } 
    }

    pub fn subtree_matching_query(&self, query: &str) -> DBItems {
        let mut dbitems = DBItems(vec![]);
        for dbitem in self.0.iter() {
            match dbitem.subtree_matching_query(query) {
                Some(item) => dbitems.0.push(item),
                None => {}
            }
        };
        dbitems
    }
}

pub fn schema (_args: &ApplicationArguments, schema_command: &SchemaCommand) {

    match &schema_command.source.0 {
        #[cfg(feature = "use_mysql")]
        SourceConfigCommand::Mysql(mysql_config_options) => {
            let conn = establish_mysql_connection(mysql_config_options);
            let mut where_parts = vec![];
            let mut params = vec![];
            if let Some(dbname) = &mysql_config_options.database {
                where_parts.push("t.table_schema=?");
                params.push(dbname);
            }
            let where_clause = match where_parts.is_empty() {
                true => "".to_string(),
                false => format!(" where {}", where_parts.iter().map(|v| format!("({})", v) ).collect::<Vec<String>>().join(" AND "))
            };

            let query = format!("
                select
                    t.table_schema, t.table_name,
                    c.column_name, c.column_type
                from
                    information_schema.tables t
                left join
                    information_schema.columns c
                on
                    t.table_schema=c.table_schema
                    and t.table_name=c.table_name
                {}
                order by t.table_schema, t.table_name, c.column_name
                ", where_clause);

            let result = conn.prep_exec(&query, params);
            let results = match result {
                Ok(v) => v,
                Err(e) => {
                    report_query_error(&query, &format!("{:?}", e));
                    std::process::exit(1);
                }
            };
            let mut dbitems = DBItems(vec![]);
            for row in results {
                let (schema_name, table_name, column_name, column_type):(String, String, String, String) = mysql::from_row(row.unwrap());
                if dbitems.0.is_empty() {
                    dbitems.0.push( DBItem {name: schema_name.clone(), items: vec![]} );
                } else {
                    if dbitems.0.last().unwrap().name != schema_name {
                        dbitems.0.push( DBItem {name: schema_name.clone(), items: vec![]} );
                    }
                };
                dbitems.0.last_mut().unwrap().items.push(DBItem { name: table_name.clone(), items: vec![]} );
            }
            if let Some(q) = &schema_command.query {
                dbitems = dbitems.subtree_matching_query(&q);
            }
            dbitems.print();
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
          let _conn = establish_postgres_connection(postgres_config_options);
        }
    }
}
