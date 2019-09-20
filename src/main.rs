#[macro_use]
extern crate structopt;

use structopt::StructOpt;

pub mod commands;
pub mod config;
pub mod definitions;
pub mod destinations;
pub mod sources;
pub mod utils;

use commands::{ApplicationArguments, Command, CommandWrapper};


fn main() {

    let args = ApplicationArguments::from_args();
    match args.command {
        /*CommandWrapper(Command::Export(ref export_cmd)) => {
            commands::export::export(&args, &export_cmd);
        },
        CommandWrapper(Command::Schema(ref schema_cmd)) => {
            commands::schema::schema(&args, &schema_cmd);
        },
        CommandWrapper(Command::Shell(ref shell_cmd)) => {
            commands::shell::shell(&args, &shell_cmd);
        },*/
        CommandWrapper(Command::Sources(ref sources_cmd)) => {
            commands::sources::sources(&args, &sources_cmd);
        },
    }
}
