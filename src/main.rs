#[macro_use]
extern crate structopt;

use structopt::StructOpt;

pub mod commands;
pub mod config;
pub mod definitions;
pub mod destinations;
pub mod sources;
pub mod utils;

use commands::{ApplicationArguments, Command};


fn main() {

    let args = ApplicationArguments::from_args();
    match args.command {
        Command::Export(ref export_cmd) => {
            commands::export::export(&args, &export_cmd);
        },
        Command::Schema(ref schema_cmd) => {
            commands::schema::schema(&args, &schema_cmd);
        },
        Command::Shell(ref shell_cmd) => {
            commands::shell::shell(&args, &shell_cmd);
        },
        Command::Sources(ref sources_cmd) => {
            commands::sources::sources(&args, &sources_cmd);
        },
    }
}
