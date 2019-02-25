#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate structopt;

use structopt::StructOpt;

pub mod commands;
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
        }
    }
}
