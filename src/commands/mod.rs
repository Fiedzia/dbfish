use serde_derive::{Deserialize, Serialize};
use structopt;
use structopt::clap::{arg_enum, _clap_count_exprs};
use toml;

use crate::config;

pub mod common;
pub mod export;
pub mod schema;
pub mod sources;


arg_enum! {
    #[derive(Debug, PartialEq)]
    pub enum UseColor {
        Yes,
        No,
        Auto
    }
}


#[derive(StructOpt)]
#[structopt(name = "export", about="Export data from database to sqlite/csv/text/html/json file.", after_help="Choose a command to run or to print help for, ie. synonyms --help")]
#[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
pub struct ApplicationArguments {
    #[structopt(short = "v", long = "verbose", help = "Be verbose")]
    pub verbose: bool,
    #[structopt(short = "c", long = "color", help = "use color", default_value="auto", raw(possible_values = "&UseColor::variants()", case_insensitive = "true"))]
    pub color: UseColor,
    #[structopt(subcommand)]
    pub command: Command,
}


#[derive(StructOpt)]
pub enum Command {
    #[structopt(name = "export", about="export data")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Export(export::ExportCommand),
    #[structopt(name = "schema", about="show source schema")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Schema(schema::SchemaCommand),
    #[structopt(name = "sources", about="manage data sources")]
    #[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
    Sources(sources::SourcesCommand),
}
