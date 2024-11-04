use std::collections::HashMap;

use clap::{Parser};
use id_tree::{InsertBehavior, Node, NodeId, Tree};

#[cfg(feature = "use_mysql")]
use mysql;
use regex::RegexBuilder;

use crate::commands::{ApplicationArguments};
use crate::commands::common::{SourceConfigCommand};
use crate::commands::data_source::DataSourceCommand;
use crate::utils::report_query_error;

#[cfg(feature = "use_mysql")]
use mysql::prelude::Queryable;
#[cfg(feature = "use_mysql")]
use crate::sources::mysql::{establish_mysql_connection};
#[cfg(feature = "use_postgres")]
use crate::sources::postgres::establish_postgres_connection;
#[cfg(feature = "use_sqlite")]
use crate::sources::sqlite::establish_sqlite_connection;


#[derive(Debug, Parser)]
pub struct SchemaCommand {
    #[arg(short = 'r', long = "regex", help = "use regular expression engine")]
    pub regex: bool,
    #[arg(short = 'q', long = "query", help = "show items matching query")]
    pub query: Option<String>,
}


#[derive(Clone, Debug)]
pub struct DBItem {
    name: String,
    description: Option<String>
}

impl DBItem {
    pub fn print(&self, indentation_level: usize) {
        match &self.description {
            None => println!("{:indent$}{name}", "", indent=indentation_level * 4, name=self.name),
            Some(desc) => println!("{:indent$}{name} {desc}", "", indent=indentation_level * 4, name=self.name, desc=desc),
        }
    }

    pub fn matches(&self, query: &str, is_regex: bool) -> bool {
        if is_regex {
            let re = RegexBuilder::new(query).case_insensitive(true).build().unwrap();
            re.is_match(&self.name)
        } else {
            self.name.to_lowercase().contains(query)
        }
    }

}

#[derive(Clone, Debug)]
struct DBItems(Tree<DBItem>);

impl DBItems {

    pub fn new() -> DBItems {
        DBItems(Tree::new())
    }

    pub fn print(&self) {
        match self.0.root_node_id() {
            None => {},
            Some(root_node_id) => {
                for node_id in self.0.traverse_pre_order_ids(&root_node_id).unwrap() {
                    let node = self.0.get(&node_id).unwrap();
                    if node.parent().is_some() {
                        let indentation_level = self.0.ancestors(&node_id).unwrap().count() - 1;
                        node.data().print(indentation_level);
                    }
                }
            }
        }
    }

    pub fn subtree_matching_query(&self, query: &str, is_regex:bool) -> DBItems {
        match self.0.root_node_id() {
            None => DBItems::new(),
            Some(root_node_id) => {
                let mut new_dbitems = DBItems::new();
                let mut node_map = HashMap::new();
                for node_id in self.0.traverse_post_order_ids(&root_node_id).unwrap() {
                    if self.0.get(&node_id).unwrap().data().matches(query, is_regex) {
                        let mut ancestor_ids:Vec<NodeId> = self.0
                            .ancestor_ids(&node_id)
                            .unwrap()
                            .cloned()
                            .collect();
                        ancestor_ids.reverse();
                        for node_id in ancestor_ids {
                            let node = self.0.get(&node_id).unwrap();
                            if node_map.contains_key(&node_id) {
                                continue
                            };
                            let new_node_id = new_dbitems.0.insert(
                                Node::new(node.data().clone()),
                                match node.parent() {
                                    None => InsertBehavior::AsRoot,
                                    Some(parent_id) => InsertBehavior::UnderNode(node_map.get(parent_id).unwrap())
                                }
                            ).unwrap();
                            node_map.insert(node_id, new_node_id);
                        }
                        if !node_map.contains_key(&node_id) {

                            let node = self.0.get(&node_id).unwrap();
                            let new_node_id = new_dbitems.0.insert(
                                Node::new(node.data().clone()),
                                match node.parent() {
                                    None => InsertBehavior::AsRoot,
                                    Some(parent_id) => InsertBehavior::UnderNode(node_map.get(parent_id).unwrap())
                                }
                            ).unwrap();
                            node_map.insert(node_id, new_node_id);
                        }
                    }
                }
                new_dbitems
            }
        }
    }
}

pub fn schema(_args: &ApplicationArguments, src: &DataSourceCommand, schema_command: &SchemaCommand) {

    match &src {
        #[cfg(feature = "use_mysql")]
        DataSourceCommand::Mysql(mysql_config_options) => {
            let mut conn = establish_mysql_connection(mysql_config_options);
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
                    t.table_schema,
                    t.table_name,
                    c.column_name,
                    c.column_type,
                    c.is_nullable
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

            let result = conn.exec(&query, params);
            let results: Vec<mysql::Row> = match result {
                Ok(v) => v,
                Err(e) => {
                    report_query_error(&query, &format!("{:?}", e));
                    std::process::exit(1);
                }
            };
            let mut dbitems = DBItems::new();
            let root_node = dbitems.0.insert(
                Node::new(
                    DBItem{name: "".to_string(), description: None}
                ),
                InsertBehavior::AsRoot
            ).unwrap();
            let mut current_schema = None;
            let mut current_table = None;

            for row in results {
                let (schema_name, table_name, column_name, column_type, is_nullable):(String, String, String, String, String) = mysql::from_row(row);
                let field_description = format!(
                    "({}{})",
                    column_type,
                    match is_nullable.as_ref() {
                        "NO" => " NOT NULL",
                        _ => ""
                    }
                );

                match &current_schema {
                    None => {
                        current_schema = Some(
                            dbitems.0.insert(
                                Node::new(
                                    DBItem{name: schema_name.to_string(), description: None}
                                ),
                                InsertBehavior::UnderNode(&root_node)
                            ).unwrap()
                        );
                    },
                    Some(node_id) => {
                        if schema_name != dbitems.0.get(node_id).unwrap().data().name {
                            current_table = None;
                            current_schema = Some(
                                dbitems.0.insert(
                                    Node::new(
                                        DBItem{name: schema_name.to_string(), description: None}
                                    ),
                                    InsertBehavior::UnderNode(&root_node)
                                ).unwrap()
                            );
                        }
                    }
                }

                match &current_table {
                    None => {
                        current_table = Some(
                            dbitems.0.insert(
                                Node::new(
                                    DBItem{name: table_name.to_string(), description: None}
                                ),
                                InsertBehavior::UnderNode(current_schema.as_ref().unwrap())
                            ).unwrap()
                        );
                    },
                    Some(node_id) => {
                        if table_name != dbitems.0.get(node_id).unwrap().data().name {
                            current_table = Some(
                                dbitems.0.insert(
                                    Node::new(
                                        DBItem{name: table_name.to_string(), description: None}
                                    ),
                                    InsertBehavior::UnderNode(current_schema.as_ref().unwrap())
                                ).unwrap()
                            );
                        }
                    }
                }

                dbitems.0.insert(
                    Node::new(
                        DBItem{name: column_name.to_string(), description: Some(field_description)}
                    ),
                    InsertBehavior::UnderNode(current_table.as_ref().unwrap())
                ).unwrap();

            }
            if let Some(query) = &schema_command.query {
                dbitems = dbitems.subtree_matching_query(&query.to_lowercase(), schema_command.regex);
            }
            dbitems.print();
        },
        #[cfg(feature = "use_sqlite")]
        DataSourceCommand::Sqlite(sqlite_config_options) => {
            let conn = establish_sqlite_connection(sqlite_config_options);
            let mut dbitems = DBItems::new();
            let root_node = dbitems.0.insert(
                Node::new(
                    DBItem{name: "".to_string(), description: None}
                ),
                InsertBehavior::AsRoot
            ).unwrap();
            let mut current_parent = None;
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
                |row| {
                    let table_name = row[0].1.unwrap();
                    let field_name = row[1].1.unwrap();
                    let field_description = format!(
                        "({}{}{})",
                        row[2].1.unwrap(),
                        if row[3].1.unwrap() == "0" {
                            ""
                        } else {
                            " NOT NULL"
                        },
                        if row[5].1.unwrap() == "1" {
                            " PRIMARY KEY"
                        } else {
                            ""
                        }
                        
                    );
                    match &current_parent {
                        None => {
                            current_parent = Some(
                                dbitems.0.insert(
                                    Node::new(
                                        DBItem{name: table_name.to_string(), description: None}
                                    ),
                                    InsertBehavior::UnderNode(&root_node)
                                ).unwrap()
                            );
                        },
                        Some(node_id) => {
                            if table_name != dbitems.0.get(node_id).unwrap().data().name {
                                current_parent = Some(
                                    dbitems.0.insert(
                                        Node::new(
                                            DBItem{name: table_name.to_string(), description: None}
                                        ),
                                        InsertBehavior::UnderNode(&root_node)
                                    ).unwrap()
                                );
                            }
                        }
                    }
                    dbitems.0.insert(
                        Node::new(
                            DBItem{name: field_name.to_string(), description: Some(field_description)}
                        ),
                        InsertBehavior::UnderNode(current_parent.as_ref().unwrap())
                    ).unwrap();
                    true
                }
            ).unwrap();
            if let Some(query) = &schema_command.query {
                dbitems = dbitems.subtree_matching_query(&query.to_lowercase(), schema_command.regex);
            }
            dbitems.print();
        },
        #[cfg(feature = "use_postgres")]
        DataSourceCommand::Postgres(postgres_config_options) => {
          let mut conn = establish_postgres_connection(postgres_config_options);
          let mut where_parts = vec!["t.table_schema='public'"];
          let mut params:Vec<&(dyn postgres::types::ToSql + Sync)> = vec![];
          if let Some(dbname) = &postgres_config_options.database {
              where_parts.push("t.table_catalog=$1");
              //params.push(&dbname.as_str() as &dyn postgres::types::ToSql);
              params.push(dbname as &(dyn postgres::types::ToSql + Sync));
          }
          let where_clause = match where_parts.is_empty() {
              true => "".to_string(),
              false => format!(" where {}", where_parts.iter().map(|v| format!("({})", v) ).collect::<Vec<String>>().join(" AND "))
          };

          let query = format!("
              select
                  t.table_catalog,
                  t.table_name,
                  c.column_name,
                  c.data_type,
                  c.is_nullable
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
          let result = &conn.query(&query, params.as_slice());
          let results = match result {
              Ok(v) => v,
              Err(e) => {
                  report_query_error(&query, &format!("{:?}", e));
                  std::process::exit(1);
              }
          };
          let mut dbitems = DBItems::new();
          let root_node = dbitems.0.insert(
              Node::new(
                  DBItem{name: "".to_string(), description: None}
              ),
              InsertBehavior::AsRoot
          ).unwrap();
          let mut current_schema = None;
          let mut current_table = None;

          for row in results {
              let schema_name:String = row.get(0);
              let table_name:String  = row.get(1);
              let column_name:String = row.get(2);
              let column_type:String = row.get(3);
              let is_nullable:String = row.get(4);
              let field_description = format!(
                  "({}{})",
                  column_type,
                  match is_nullable.as_ref() {
                      "NO" => " NOT NULL",
                      _ => ""
                  }
              );

              match &current_schema {
                  None => {
                      current_schema = Some(
                          dbitems.0.insert(
                              Node::new(
                                  DBItem{name: schema_name.to_string(), description: None}
                              ),
                              InsertBehavior::UnderNode(&root_node)
                          ).unwrap()
                      );
                  },
                  Some(node_id) => {
                      if schema_name != dbitems.0.get(node_id).unwrap().data().name {
                          current_table = None;
                          current_schema = Some(
                              dbitems.0.insert(
                                  Node::new(
                                      DBItem{name: schema_name.to_string(), description: None}
                                  ),
                                  InsertBehavior::UnderNode(&root_node)
                              ).unwrap()
                          );
                      }
                  }
              }

              match &current_table {
                  None => {
                      current_table = Some(
                          dbitems.0.insert(
                              Node::new(
                                  DBItem{name: table_name.to_string(), description: None}
                              ),
                              InsertBehavior::UnderNode(current_schema.as_ref().unwrap())
                          ).unwrap()
                      );
                  },
                  Some(node_id) => {
                      if table_name != dbitems.0.get(node_id).unwrap().data().name {
                          current_table = Some(
                              dbitems.0.insert(
                                  Node::new(
                                      DBItem{name: table_name.to_string(), description: None}
                                  ),
                                  InsertBehavior::UnderNode(current_schema.as_ref().unwrap())
                              ).unwrap()
                          );
                      }
                  }
              }

              dbitems.0.insert(
                  Node::new(
                      DBItem{name: column_name.to_string(), description: Some(field_description)}
                  ),
                  InsertBehavior::UnderNode(current_table.as_ref().unwrap())
              ).unwrap();

          }
          if let Some(query) = &schema_command.query {
              dbitems = dbitems.subtree_matching_query(&query.to_lowercase(), schema_command.regex);
          }
          dbitems.print();



        }
    }
}
